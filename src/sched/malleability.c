/*
 * Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *
 * Additional copyrights may follow
 *
 */

// slurm headers

// deep-sea headers
#include "malleability.h"

extern diag_stats_t slurmctld_diag_stats;

pthread_mutex_t  node_time_mutex = PTHREAD_MUTEX_INITIALIZER;
node_time_gap_t *node_time = NULL;
int max_node_time_gaps = 500; // matches backfiller job count for now

static List process_manager_registrations = NULL;
pthread_mutex_t  process_manager_registrations_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct process_manager_registration {
	dynpm_server_to_client_connection_t *connection;
	dynpm_register_process_manager_msg_t *registration_data;
	int current_nodes_offered;
	malleable_operation_target_t *target;
	dynpm_allocation_request_msg_t **allocation_requests;
} process_manager_registration_t;

// TODO resv_ptr is actually not set on regular job life cycles
// instead, _add_reservation is called in backfill.c
// that routine operates on the node_space_map_t instead of reservation
// types; a node gap that does not delay the current top priority
// job can be determined with the node space maps.  The so called
// easy backfill makes a reservation for the current top priory
// job, basically the one at the head of the job queue

static void _free_process_manager_registration(process_manager_registration_t **registration){
	// we do not free the connection, since this is done by the call to libdynpm
	// the connections are managed there, and may be discarded based on the disconnection
	// handler based on libevent

	int application_count;
	if((*registration)->registration_data != NULL){
		application_count = (*registration)->registration_data->application_count;
		// TODO check if it is better to have a return code in the libdynpm free routines
		dynpm_free_register_process_manager_msg(&((*registration)->registration_data));
	}

	// TODO maybe better to have a routine to free targets and keep up with any changes
	if((*registration)->target != NULL){
		bit_free((*registration)->target->node_bitmap);
		free((*registration)->target);
	}

	for(int i = 0; i < application_count; i++){
		if((*registration)->allocation_requests[i] != NULL){
			dynpm_free_allocation_request_msg(&((*registration)->allocation_requests[i]));
		}
	}

	free(*registration);
}

// should be called after acquiring the registrations lock only
static void _free_all_process_manager_registrations(){
	process_manager_registration_t *registration;
	ListIterator registration_iterator
		= list_iterator_create(process_manager_registrations);
	while ((registration = list_next(registration_iterator))) {
		_free_process_manager_registration(&registration);
	}
}

static void _print_bitfield(const char *text, bitstr_t *bitdata){
	int size = bit_size(bitdata);
	int bit;
	char *bit_to_string = malloc(size + 1);
	for( bit = 0; bit < size; bit++){
		bit_to_string[bit] = bit_test(bitdata, bit)?'1':'0';
	}
	bit_to_string[bit] = '\0';
	info("%s: %s", text, bit_to_string);
	free(bit_to_string);
}

static void _print_config_list(){
	config_record_t *config_ptr = NULL;
	ListIterator config_iterator;
	bitstr_t *bitmap;

	config_iterator = list_iterator_create(config_list);
	while ((config_ptr = list_next(config_iterator))) {
		info("nodes: %s", config_ptr->nodes);
		node_name2bitmap(config_ptr->nodes, true, &bitmap);
		_print_bitfield("config", bitmap);
		//info("name from bitmap: %s", bitmap2node_name(bitmap));
		xfree(bitmap);
	}
	list_iterator_destroy(config_iterator);
}

static void _print_reservations(List resv_list){
	ListIterator iter;
	slurmctld_resv_t *resv_ptr = NULL;
	time_t start_relative;
	time_t end_relative;
	time_t now = time(NULL);

	iter = list_iterator_create(resv_list);
	while ((resv_ptr = list_next(iter))) {
		if (resv_ptr->flags & RESERVE_FLAG_TIME_FLOAT) {
			start_relative = resv_ptr->start_time + now;
			if (resv_ptr->duration == INFINITE)
				end_relative = start_relative + YEAR_SECONDS;
			else if (resv_ptr->duration && (resv_ptr->duration != NO_VAL)) {
				end_relative =
					start_relative + resv_ptr->duration * 60;
			} else {
				end_relative = resv_ptr->end_time;
				if (start_relative > end_relative)
					start_relative = end_relative;
			}
		} else {
			// expired ones may show negative time here
			start_relative = resv_ptr->start_time_first;
			end_relative = resv_ptr->end_time;
		}
		info("starting: %f; ending: %f from now", start_relative, end_relative);
	}
	list_iterator_destroy(iter);

	info("done printing reservations");

}

static int _print_step(void *object, void *arg) {
	step_record_t *step_ptr = (step_record_t *)object;
	info("step_id: %d", step_ptr->step_id.step_id);
	info("srun_pid: %d", step_ptr->srun_pid);
	info("srun_host: %s", step_ptr->host);
	info("cpu_count: %d", step_ptr->cpu_count);
	info("command: %s", step_ptr->submit_line);
	info("state: %s", step_ptr->state == JOB_RUNNING ? "running" : "stopped" );
	_print_bitfield("step_node_bitmap", step_ptr->step_node_bitmap);
}

static void _print_node_records(){
	int node_index;
	info("%d nodes are available in the system", node_record_count);
	for(node_index = 0; node_index < node_record_count; node_index++){
		info("%d: c: %d; n: %s; p: %d; s: %0x; r: %d", node_index,
				node_record_table_ptr[node_index]->cpus,
				node_record_table_ptr[node_index]->name,
				node_record_table_ptr[node_index]->part_cnt,
				node_record_table_ptr[node_index]->node_state,
				node_record_table_ptr[node_index]->not_responding
				);
	}
}

static void _print_running_and_pending_jobs(){
	ListIterator job_iterator;
	job_record_t *job_ptr = NULL;
	char begin_buf[32], end_buf[32], *node_list;
	int running_jobs = 0;
	int pending_jobs = 0;

	// TODO need to check select_p_job_expand() inside select
	// this is select_g_job_expect() elsewhere

	// TODO
	// for malleability:
	// produce a variant of _update_job from src/slurmctld/job_mgr.c
	// 1.-
	// where the parameters for the update are determined by this
	// scheduler extension, instead of a user message
	// 2.-
	// it does not kill job steps
	//
	// Study the implementation of _update_job to achieve this

	//info("sched_params: %s", slurm_conf.sched_params);

	debug2("################# job data ############################");
	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		if(IS_JOB_RUNNING(job_ptr)){
			running_jobs++;

			debug2("----------------- job: %2d ----------------------------",
					job_ptr->job_id);
			debug2("cpu_cnt: %d", job_ptr->cpu_cnt);
			debug2("started at: %d seconds", job_ptr->start_time);
			debug2("time limit: %d minutes", job_ptr->time_limit);
			//debug2("partition: %s", job_ptr->partition);
			//debug2("resp_host: %s", job_ptr->resp_host);
			//_print_bitfield("node_bitmap", job_ptr->node_bitmap);
			//list_for_each(job_ptr->step_list, _print_step, NULL);
			debug2("min_nodes: %d", job_ptr->details->min_nodes);
			debug2("------------------------------------------------------");
            if(job_ptr->time_limit < 1){
                error("negative time limit");
                exit(-1);
            }
		} else if(IS_JOB_PENDING(job_ptr)){
			pending_jobs++;

			debug2("***************** job: %2d ***************************", job_ptr->job_id);
			debug2("pending job: %d", job_ptr->job_id);
			//node_list = bitmap2node_name(job_ptr->node_bitmap);
			//debug2("starts: %ss", begin_buf, node_list);
			slurm_make_time_str(&job_ptr->start_time, begin_buf, sizeof(begin_buf));
			debug2("starts: %s", begin_buf);
			slurm_make_time_str(&job_ptr->last_sched_eval, end_buf, sizeof(end_buf));
			debug2("last scheduled: %s", end_buf);
			debug2("time limit: %d minutes", job_ptr->time_limit);
			//debug2("min_cpus: %d", job_ptr->details->min_cpus);
			debug2("min_nodes: %d", job_ptr->details->min_nodes);
			debug2("******************************************************");
		}
	}
	list_iterator_destroy(job_iterator);
	debug("-- total running: %d; total pending: %d; --------------", running_jobs, pending_jobs);
	debug("#######################################################");
}

static void _print_nt(node_time_gap_t *node_time) {
	int nt = 0;
	char begin_buf[32], end_buf[32], *node_list;

	while (1) {
		if(bit_set_count(node_time[nt].free_nodes) > 0){ // don't print capped entry
			slurm_make_time_str(&node_time[nt].start_time,
					begin_buf, sizeof(begin_buf));
			slurm_make_time_str(&node_time[nt].end_time,
					end_buf, sizeof(end_buf));
			node_list = bitmap2node_name(node_time[nt].free_nodes);
			info("Begin: %10s", begin_buf);
			info("End:   %10s", end_buf);
			info("Nodes: %s",   node_list);
			xfree(node_list);
		}

		if ((node_time[nt].is_last)) break;
		else nt++;
	}
}

// returns 1 when found (setting registration) or 0 when not found
// requires that the registration lock be ackquired before
static int _find_process_manager_registration(int job_index,
		process_manager_registration_t **registration){

	int found = 0;
	ListIterator registration_iterator
		= list_iterator_create(process_manager_registrations);

	while (((*registration) = list_next(registration_iterator))) {
		if(job_index == (*registration)->registration_data->job_index){
			found = 1;
			break;
		}
	}

	list_iterator_destroy(registration_iterator);
	return found;
}

static void _init_node_time_gap(node_time_gap_t **node_time,
		time_t now, time_t window_end, bitstr_t *free_nodes){
	if(*node_time != NULL) xfree(*node_time);
	*node_time = xcalloc((max_node_time_gaps + 1),
			sizeof(node_time_gap_t));

	// time dimension from now to maximum malleable window
	(*node_time)[0].start_time = now;
	(*node_time)[0].end_time = window_end;
	// node dimension are current free nodes
	// totals are more important here, but later specific bits will be necessary
	(*node_time)[0].free_nodes = bit_copy(free_nodes);
	(*node_time)[0].is_last = 1;
}

/*
 * Scan running jobs to update the malleable job list and
 * free nodes bitfield.
 */
static void _scan_running_malleable_jobs(List *malleable_jobs, bitstr_t **free_nodes){
	job_record_t *job_ptr = NULL;
	step_record_t *step_ptr = NULL;

	xassert(job_list);

	// TODO it is possible to optimize scanning registrations, since we scan again in the manager
	// algorithm; however, since malleable jobs are only a subset of running jobs, this optimization
	// may not bring much benefit. In the future, if we decide to perform modifications to the
	// reseravations of queued jobs, this will need to be optimized, since the number of queued jobs
	// can be much larger than the number of running jobs.

	ListIterator job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		xassert(job_ptr);
		if(IS_JOB_RUNNING(job_ptr)){
			// mark nodes of running jobs as not available
			bit_and_not(*free_nodes, job_ptr->node_bitmap);
			if(job_ptr->step_list == NULL){
				info("job step list not ready for job %d", job_ptr->job_id);
				continue;
			} else if(!list_is_empty(job_ptr->step_list)){
				process_manager_registration_t *registration = NULL;
				int found = _find_process_manager_registration( job_ptr->job_id, &registration);
				if(found){
					info("job %d registered; to be considered for malleability", job_ptr->job_id);
					list_append(*malleable_jobs, job_ptr);
				} else {
					info("job %d not registered; NOT considered for malleability", job_ptr->job_id);
				}
			}
		}
	}
	list_iterator_destroy(job_iterator);
}

static void _scan_free_nodes(bitstr_t **free_nodes){
	job_record_t *job_ptr = NULL;
	step_record_t *step_ptr = NULL;

	xassert(job_list);

	ListIterator job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		xassert(job_ptr);
		if(IS_JOB_RUNNING(job_ptr)){
			// mark nodes of running jobs as not available
			bit_and_not(*free_nodes, job_ptr->node_bitmap);
		}
        // TODO throw the dice here, to determine if malleable
        // can safely reuse some parts of the job_record_t to
        // mark the malleability feature
	}
	list_iterator_destroy(job_iterator);
}

static void _build_node_time_gap(node_time_gap_t **node_time, time_t window_end, bitstr_t *free_nodes){
	ListIterator job_iterator;
	job_record_t *job_ptr = NULL;
	int nodes_available_now_count = bit_set_count(free_nodes);
	int nt = 0;
	/*
	 * TODO
	 * Need to account for all the job's requirements.
	 * The initial implementation will only look at cores and assume 1 partition
	 *
	 * from job_ptr->job_details (in slurmctld.h):
	 * min_cpus, max_cpus, exc_node_bipmap, exc_nodes, min_nodes, max_nodes,
	 * ntasks_per_node, num_tasks, req_node_bitmap
	 *
	 * These are set based on the batch file, and we cannot assume that all
	 * will be specified by the user.  We need to account for defaults and
	 * check if user provided for each one.
	 *
	 * */
	// FIXME don't assume homogeneous; this line below assumes so
	int cores_per_node = node_record_table_ptr[0]->cpus;

	// TODO need to decide if we need to build a job queue, or
	// if traversing the job list is sufficient.  The job
	// queue needs to be sorted to get the order based
	// on priorities (an expensive operation).
	// FIXME may need the queue perform priority based
	// node time gap computations; not clear yet
	//
	// build job queue:
	// - don't reset start times (false)
	// - not clear if we need to define backfill (true)
	//job_queue_rec_t *job_queue_rec = NULL;
	//List job_queue = build_job_queue(false, true);
	//job_queue_rec = list_pop(job_queue);
	//job_record_t *job_ptr     = job_queue_rec->job_ptr;
	//part_record_t *part_ptr   = job_queue_rec->part_ptr;
	//uint32_t bf_job_priority  = job_queue_rec->priority;

	// the job_list can be replaced by job_queue here directly
	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		if (IS_JOB_PENDING(job_ptr)) {
			if(nt >= max_node_time_gaps) break;

			bitstr_t *tmp_nodes_available = bit_copy(free_nodes);
			int required_nodes = job_ptr->details->min_cpus/cores_per_node;
			if(required_nodes == 0) required_nodes = 1;
			//info("job %d requires %d nodes", job_ptr->job_id, required_nodes);

			int nodes_to_be_removed = required_nodes - bit_clear_count(free_nodes);
			//info("nodes_to_be_removed: %d", nodes_to_be_removed);

			// In these cases, we cap the node-time gap and wait to see how the schedule develops
			if(nodes_to_be_removed < 0){
				//info("node gap smaller than tail reservation: capping the node-time gap");
				(*node_time)[nt].free_nodes = bit_copy(free_nodes);
				(*node_time)[nt].is_last = 0;
				(*node_time)[nt].end_time = job_ptr->start_time;

				bit_clear_all(free_nodes);
				(*node_time)[nt+1].free_nodes = bit_copy(free_nodes);
				(*node_time)[nt+1].is_last = 1;
				(*node_time)[nt+1].start_time = job_ptr->start_time;
				(*node_time)[nt+1].end_time = window_end;
				break;
			}
			// TODO instead of capping node-time gap, we can track arbitrary gaps by
			// handling increased and decreased gaps.  Whether this is a good idea or not
			// will depend on further scheduler design based on heuristics evaluated
			// by our simulations

			(*node_time)[nt].end_time = job_ptr->start_time;
			(*node_time)[nt].is_last = 0;

			(*node_time)[nt+1].start_time = job_ptr->start_time;
			(*node_time)[nt+1].end_time = window_end;
			(*node_time)[nt+1].is_last = 1;

			int i;
			nodes_available_now_count -= nodes_to_be_removed;
			for(i = 0; i < node_record_count && nodes_to_be_removed > 0; i++){
				if(bit_test(free_nodes, i)){
					bit_clear(free_nodes, i);
					nodes_to_be_removed--;
				}
			}
			(*node_time)[nt+1].free_nodes = bit_copy(free_nodes);

			nt++;

			//info("%d nodes remaining", nodes_available_now_count);
			if(nodes_available_now_count <= 0){
				//info("no more node-time gaps are possible");
				break;
			}

		}
	}
	list_iterator_destroy(job_iterator);
}

static void _print_registration_data(
		dynpm_register_process_manager_msg_t *registration_data){
	info("pid: %d; hostname: %s; job_id: %d; apps: %d",
			registration_data->pid, registration_data->hostname,
			registration_data->job_index, registration_data->application_count);
	for(int app; app < registration_data->application_count; app++){
		info("app: %d; mall. mode: %d; nodes: %d, %s",
				registration_data->applications[app]->application_index,
				registration_data->applications[app]->malleability_mode,
				registration_data->applications[app]->node_count,
				registration_data->applications[app]->nodes);
	}
}

static void _reset_node_offers(){
	if(list_count(process_manager_registrations) > 0){
		info("resetting node offers in registrations");

		process_manager_registration_t *registration = NULL;
		ListIterator registration_iterator
			= list_iterator_create(process_manager_registrations);
		while ((registration = list_next(registration_iterator))) {
			if(registration->target == NULL){
				//info("there is no malleability operation target for job %d",
				//		registration->registration_data->job_index);
				//info("resetting current_nodes_offered (was %d) for job %d",
				//		registration->current_nodes_offered,
				//		registration->registration_data->job_index);
				registration->current_nodes_offered = 0;
			//} else {
				//info("there is a malleability operation target for job %d",
				//		registration->registration_data->job_index);
				//info("%d nodes offered (will not be reset)",
				//		registration->current_nodes_offered);
			}
		}

	} else {
		info("no registrations available; nothing to reset");
	}
}

char *_strdup_convert(char *node_list){

	char *node_string = malloc(sizeof(char)*1000);
	node_string[0] = '\0';
	char *generic_node_list = malloc(sizeof(char)*3000);
	generic_node_list[0] = '\0';
	//node_name2bitmap(job_desc->req_nodes, false, &new_req_bitmap);

	if(node_list != NULL){
		char *name = NULL;
		//hostlist_t *hl = hostlist_create(node_list);
		hostlist_t *hl = hostlist_create(node_list);
		int i = 0;
		while ((name = hostlist_shift(hl))) {
			//info("%s ", name);
			sprintf(node_string, "%s,", name);
			strcat(generic_node_list, node_string);
			free(name);
		}
		hostlist_destroy(hl);
		generic_node_list[strlen(generic_node_list)-1] = '\0';
		info("generic_node_list = %s", generic_node_list);
		return generic_node_list;
	} else {
		error("failed to generate the node_list from the nt bitmap");
		return NULL;
	}
}

static void _send_resource_offer_from_nt(
		job_record_t *job_ptr,
		process_manager_registration_t *registration,
		int app, int nt, time_t now){
	int node_count = bit_set_count(node_time[nt].free_nodes);

	dynpm_header_t *header = malloc(sizeof(dynpm_header_t));
	header->source_type =  (dynpm_source_type)SCHEDULER;
	header->message_type = (dynpm_message_type)OFFER_RESOURCES;

	dynpm_resource_offer_msg_t *offer = malloc(sizeof(dynpm_resource_offer_msg_t));
	offer->node_count = node_count;
	offer->application_index = app;
	offer->partition_index = 0; // TODO specific node to partition map
	offer->creation_time = now;
	offer->end_time = node_time[nt].end_time;

	if(dynpm_send_message_to_client_connection(&header, (void**)&offer, registration->connection)){
		error("error sending offer to job %d step %d host %s", job_ptr->job_id, app,
				registration->connection->host);
	} else {
		info("sent offer to job %d step %d host %s", job_ptr->job_id, app, registration->connection->host);
		// once the offer is sent, update the registration with the
		// maximum offered; multiple offers are possible depending on
		// the node gaps where the job fits
		if(node_count > registration->current_nodes_offered){
			registration->current_nodes_offered = node_count;
		}
	}
}

static void _match_active_malleability(
		job_record_t *job_ptr,
		process_manager_registration_t *registration,
		int app, int nt){

	if(registration->target == NULL &&
			registration->allocation_requests[app] != NULL){
		info("no target is set and a request is available");
		if(bit_set_count(node_time[nt].free_nodes)
				>= registration->allocation_requests[app]->node_count){
			info("we have sufficient nodes (%d available, %d requested)",
					bit_set_count(node_time[nt].free_nodes),
					registration->allocation_requests[app]->node_count);

			bitstr_t *current_nodes = bit_copy(job_ptr->node_bitmap);
			info("current nodes: %s", bitmap2node_name(current_nodes));

			bitstr_t *delta_nodes = bit_copy(node_time[nt].free_nodes);
			int set_bits = 0;
			for(int i = 0; i < bit_size(delta_nodes); i++){
				// TODO update the node time free_nodes too
				if(bit_test(delta_nodes, i) &&
						(set_bits < registration->allocation_requests[app]->node_count) ){
					set_bits++;
				} else {
					bit_clear(delta_nodes, i);
				}
			}

			info("delta nodes:   %s", bitmap2node_name(delta_nodes));

			bitstr_t *updated_nodes = bit_copy(job_ptr->node_bitmap);
			bit_or(updated_nodes, delta_nodes);
			info("updated nodes: %s", bitmap2node_name(updated_nodes));

			dynpm_header_t *header = malloc(sizeof(dynpm_header_t));
			header->source_type =  (dynpm_source_type)SCHEDULER;
			header->message_type = (dynpm_message_type)REALLOCATION_START;

			dynpm_reallocation_start_msg_t *reallocation_start =
				malloc(sizeof(dynpm_reallocation_start_msg_t));
			reallocation_start->application_index = app;
			// TODO update for the multiple partition case
			reallocation_start->partition_index = 0;
			// TODO update this for the other operation types (e.g. shrink)
			reallocation_start->type = (dynpm_allocation_operation_types)EXPAND;
			reallocation_start->current_node_count = bit_set_count(current_nodes);
			reallocation_start->delta_node_count = bit_set_count(delta_nodes);
			reallocation_start->updated_node_count = bit_set_count(updated_nodes);

			char *current_node_list = bitmap2node_name(current_nodes);
			char *delta_node_list = bitmap2node_name(delta_nodes);
			char *updated_node_list = bitmap2node_name(updated_nodes);

			if(registration->registration_data->applications[app]->malleability_mode
					& DYNPM_SLURMFORMAT_FLAG ){
				info("slurm format set; using regex host lists");
				reallocation_start->current_nodes = strdup(current_node_list);
				reallocation_start->delta_nodes = strdup(delta_node_list);
				reallocation_start->updated_nodes = strdup(updated_node_list);
			} else {
				info("slurm format NOT set; using generic host lists");
				reallocation_start->current_nodes = _strdup_convert(current_node_list);
				reallocation_start->delta_nodes = _strdup_convert(delta_node_list);
				reallocation_start->updated_nodes = _strdup_convert(updated_node_list);
			}

			if(dynpm_send_message_to_client_connection(&header,
						(void**)&reallocation_start, registration->connection)){
				error("error sending reallocation start to job %d host %s",
						registration->registration_data->job_index, registration->registration_data->hostname);
			} else {
				info("sent reallocation start to job %d host %s",
						registration->registration_data->job_index, registration->registration_data->hostname);
				if(registration->target == NULL){
					registration->target = malloc(sizeof(malleable_operation_target_t));
					registration->target->started = 0;
					registration->target->completed = 0;
					// TODO see the todo below
					registration->target->node_bitmap = updated_nodes;
				} else {
					error("registration->target == NULL after reallocation start message");
				}

				if(registration->allocation_requests[app] != NULL){
					dynpm_free_allocation_request_msg(&((registration->allocation_requests[app])));
					registration->allocation_requests[app] = NULL;
					info("removed request for app %d (fulfilled)", app);
				} else {
					error("null allocation_request after fulfilled");
				}
			}

			slurm_bit_free(&current_nodes);
			slurm_bit_free(&delta_nodes);
			// TODO decide if it is better to duplicate the bitstr_t pointer in the registration and free here
			//slurm_bit_free(&updated_nodes);
			xfree(current_node_list);
			xfree(delta_node_list);
			xfree(updated_node_list);
		}
	} else {
		info("cannot perform a reallocation start");

		if(registration->target == NULL){
			info("no target set");
		} else {
			info("target is set");
		}

		if(registration->allocation_requests[app] == NULL){
			info("no request for app %d", app);
		} else {
			info("there is a request for app %d", app);
		}
	}
}

static void _manage_malleable_jobs(node_time_gap_t *node_time, List malleable_jobs, time_t now){
	ListIterator job_iterator;
	job_record_t *job_ptr = NULL;
	int nt = 0, no = 0, last_offer = 0;
	char begin_buf[32], end_buf[32], *node_list;

	// we need to reset the nodes offered on each registration, since
	// new jobs may have be started and the reservations may also be
	// updated (via fifo or backfilling passes), thus modifying
	// node-time gaps
	_reset_node_offers();

	info("================================================================");
	while (!last_offer) {

		if(bit_set_count(node_time[nt].free_nodes) > 0){ // don't print capped entry
			slurm_make_time_str(&node_time[nt].start_time, begin_buf, sizeof(begin_buf));
			slurm_make_time_str(&node_time[nt].end_time, end_buf, sizeof(end_buf));
			node_list = bitmap2node_name(node_time[nt].free_nodes);
			info("matching jobs to node-time gap %d", nt);
			_print_nt(&(node_time[nt]));
			info("================================================================");
			xfree(node_list);
		}

		job_iterator = list_iterator_create(malleable_jobs);
		while ((job_ptr = list_next(job_iterator)) && !last_offer) {
			xassert(IS_JOB_RUNNING(job_ptr)); // must be running

			// TODO jobs created interactively usually do not have an end-time,
			// and therefore will never fit; change this?
			// TODO we need to consider reservations that belong to the same
			// user as the job; this needs to be in the node-time gap pass,
			// and not here. Alternatively, and probably better, we can
			// handle requests that can be fulfilled by a same-user reservation
			// on the request handler instead, since it introduces no contention.
			// The acquisition of the controller locks is required.
			if(job_ptr->end_time <= node_time[nt].end_time &&
					bit_set_count(node_time[nt].free_nodes) > 0){ // not capped
				info("job %d fits; remaining: %d", job_ptr->job_id, job_ptr->end_time - now);

				// TODO need to add a configurable cut-off here: we don't want to
				// trigger a malleability operation on jobs that are about to finish
				// perhaps this should be user-defined, with a system default
				if(job_ptr->end_time == node_time[nt].end_time){
					// the free nodes align perfectly with this job's end time
					// this means that this job is blocking this particular set
					// or a subset of these nodes for the next job reservation
					// this can be used in the heuristic to enhance filling
					info("job %d is time-aligned at node-time gap %d",
							job_ptr->job_id, nt);
				}

				info("managing malleable job %d", job_ptr->job_id);

				if(list_count(process_manager_registrations) > 0){

					process_manager_registration_t *registration = NULL;
					int found = _find_process_manager_registration( job_ptr->job_id, &registration);

					if(found){
						info("found existing registration for job %d", registration->registration_data->job_index);
						_print_registration_data(registration->registration_data);
					} else {
						info("no registration for job %d; skipping...", job_ptr->job_id);
						continue;
					}

					for(int app = 0; app < registration->registration_data->application_count; app++){

						if( registration->registration_data->applications[app]->malleability_mode & DYNPM_ACTIVE_FLAG &&
							registration->registration_data->applications[app]->malleability_mode & DYNPM_PASSIVE_FLAG ){
							info("application %d of job %d has both active and passive malleability enabled",
									app, registration->registration_data->job_index);
						} else if( !(registration->registration_data->applications[app]->malleability_mode & DYNPM_ACTIVE_FLAG) &&
							       !(registration->registration_data->applications[app]->malleability_mode & DYNPM_PASSIVE_FLAG) ){
							info("application %d of job %d has malleability disabled",
									app, registration->registration_data->job_index);
						}

						if(registration->registration_data->applications[app]->malleability_mode & DYNPM_ACTIVE_FLAG ){
							info("application %d of job %d has active malleability enabled",
									app, registration->registration_data->job_index);
							_match_active_malleability(job_ptr, registration, app, nt);
						}

						if(registration->registration_data->applications[app]->malleability_mode & DYNPM_PASSIVE_FLAG ){
							info("application %d of job %d has passive malleability enabled",
									app, registration->registration_data->job_index);
							// TODO add passive malleability matching algorithm here once monitor data is captured
							// previously we just passed nodes arbitrarily without any monitor data
							// perhaps we can add a setting to enable blind (no monitor data) matching
						}

						if(registration->registration_data->applications[app]->malleability_mode & DYNPM_OFFERLISTENER_FLAG ){
							info("application %d of job %d is listening to resource offers",
									app, registration->registration_data->job_index);
							_send_resource_offer_from_nt(job_ptr, registration, app, nt, now);
						}
					}
				} else {
					info("no registrations available; skipping...");
				}


				info("evaluated job %d;", job_ptr->job_id);
			} else {
				info("did not evaluate job %d (no fit)", job_ptr->job_id);
			}
		}
		list_iterator_destroy(job_iterator);

		if ((node_time[nt].is_last)) break;
		else nt++;
	}
	info("================================================================");

}

extern void stop_backfill_agent(void);

void _initialize_nodes(time_t now){
	slurmctld_lock_t all_locks = {
		READ_LOCK, WRITE_LOCK, WRITE_LOCK, READ_LOCK, READ_LOCK };

	// unfortunately, grabbing locks forever is not sufficient to
	// prevent the other scheduler threads from intefeering in the
	// metadata updates (without events) for the simulator.
	// need to add a one liner to the sched/backfill agent:
	//
	// extern void *backfill_agent(void *args)
	// {
	//+       return;
	//
	//this basically prevents the agent thread from doing anything,
	//and instead exit.
	//
	//A binary copy of the patched sched plugin can be kept around
	//for simulator runs.
	//TODO add a sched/dummy plugin and place it in the slurm's
	//plugin directory, then change the simulator init to load that
	//instead.
	lock_slurmctld(all_locks);

	// normally, the node data is initiated with the nodes disabled
	// each node is enabled once its slurmd registers with the slurmctld
	// in the simulator case, we will enable them here
	int node_index;
	info("%d nodes are available in the system; enabling them artificially",
			node_record_count);
	debug("total node records available: %d; table size: %d",
			node_record_count, node_record_table_size);

	// TODO look at src/slurmctld/node_mgr.c
	//extern int validate_node_specs(slurm_msg_t *slurm_msg, bool *newly_up)
	// need to enable the nodes just like when it is enabled via:
	//static void _slurm_rpc_node_registration(slurm_msg_t *msg)
	//from: src/slurmctld/proc_req.c
	node_record_t *node_ptr;
	for(node_index = 0; node_index < node_record_count; node_index++){
		node_ptr = node_record_table_ptr[node_index];
		debug("%d: c: %d; n: %s; p: %d; r: %d; s: %0x -> %s", node_index,
				node_ptr->cpus,
				node_ptr->name,
				node_ptr->part_cnt,
				node_ptr->not_responding,
				node_ptr->node_state,
				node_state_string(node_ptr->node_state)
			 );
		bit_set(avail_node_bitmap, node_ptr->index);
		bit_set(up_node_bitmap, node_ptr->index);

		config_record_t *config_ptr = node_ptr->config_ptr;
		node_ptr->node_state = NODE_STATE_IDLE |
			(node_ptr->node_state & NODE_STATE_FLAGS);
		// TODO double check the effect of this setting in the FIFO
		// and backfill passes; there may be some crosscheck on
		// timeouts to keep track of the node, and we disable the
		// heartbeat logic
		node_ptr->last_busy = now;
		// TODO do we really need to interact with the select
		// plugin?
		// for now it seems safer, since there are complex interactions
		// with it and the different scheduler passes.  Keeping it
		// consistent allows for a closer to a direct reuse of the
		// algorithms (although changes were inevitable)
		// TODO this changed in upstream; need to replace whith appropriate call
		//select_g_update_node_config(node_ptr->index);
	}
	info("nodes available: %d", bit_set_count(avail_node_bitmap));
	//info("nodes booting: %d", bit_set_count(booting_node_bitmap));


}

static int string_ends_with(const char *str, const char *suffix) {
	if (!str) return 0;
	if (!suffix) return 0;

	size_t lenstr = strlen(str);
	size_t lensuffix = strlen(suffix);

	if (lensuffix >  lenstr) return 0;

	return strncmp(str + lenstr - lensuffix, suffix, lensuffix) == 0;
}

static int _retrieve_simulation_parameters(FILE *swf_file, int *max_runtime,
		int *max_nodes, int *max_jobs, int *max_records, int *mall_prob,
		int *mall_scal, char **first_line){
	int line_count = 0;
	size_t line_length;
	ssize_t read;
	char *line = NULL;
	char *tokenize_line;
	char *token;
	char delimiter[] = ";: \t\r\n\v\f";

	while ((read = getline(&line, &line_length, swf_file)) != -1) {
		if(line[0] != ';') break;
		//info("%d,%d: %s", line_count, read, line);

		if(read > 1){ // minimum expected size of a configuration line: ;\n = 2
			tokenize_line = malloc(sizeof(char)*read);
			strcpy(tokenize_line, line);

			// currently we only read MaxRuntime and MaxNodes from SWF header
			// MaxRuntime: maximum simulation time in seconds
			// MaxNodes: total number of nodes available in the machine configuration
			token = strtok(tokenize_line, delimiter);
			while (token) {
				//info("token: %s", token);
				if(!strcmp(token, "MaxRuntime")){
					token = strtok(NULL, delimiter);
					if(token){
						info("MaxRuntime %s", token);
						*max_runtime = atoi(token);
					} else {
						error("MaxRuntime token found, but no value was provided");
						exit(-1);
					}
				}

				if(!strcmp(token, "MaxNodes")){
					token = strtok(NULL, delimiter);
					if(token){
						info("MaxNodes: %s", token);
						*max_nodes = atoi(token);
					} else {
						error("MaxNodes token found, but no value was provided");
						exit(-1);
					}
				}

				if(!strcmp(token, "MaxJobs")){
					token = strtok(NULL, delimiter);
					if(token){
						info("MaxJobs %s", token);
						*max_jobs = atoi(token);
					} else {
						error("MaxJobs token found, but no value was provided");
						exit(-1);
					}
				}

				if(!strcmp(token, "MaxRecords")){
					token = strtok(NULL, delimiter);
					if(token){
						info("MaxRecords %s", token);
						*max_records = atoi(token);
					} else {
						error("MaxRecords token found, but no value was provided");
						exit(-1);
					}
				}

				if(!strcmp(token, "MallProb")){
					token = strtok(NULL, delimiter);
					if(token){
						info("MallProb %s", token);
						*mall_prob = atoi(token);
					} else {
						error("MallProb token found, but no value was provided");
						exit(-1);
					}
				}

				if(!strcmp(token, "MallScal")){
					token = strtok(NULL, delimiter);
					if(token){
						info("MallScal %s", token);
						*mall_scal = atoi(token);
					} else {
						error("MallScal token found, but no value was provided");
						exit(-1);
					}
				}

				token = strtok(NULL, delimiter);
			}
			free(tokenize_line);
		} else {
			error("SWF line too short");
			exit(-1);
		}

		line_count++;
	}
	info("%d configuration lines parsed", line_count);
	*first_line = line;
	return 0;
}

static void _add_registration(
		dynpm_server_to_client_connection_t *connection,
		dynpm_register_process_manager_msg_t *registration_data
		){

	process_manager_registration_t *registration =
		malloc(sizeof(process_manager_registration_t));
	registration->connection = connection;
	registration->registration_data = registration_data;
	registration->current_nodes_offered = 0;
	registration->target = NULL;
	registration->allocation_requests =
		malloc(registration_data->application_count * sizeof(dynpm_allocation_request_msg_t*));
	for(int i = 0; i < registration_data->application_count; i++){
		registration->allocation_requests[i] = NULL;
	}

	list_append(process_manager_registrations, registration);

	if(dynpm_send_header_to_client_connection(
				(dynpm_source_type)SCHEDULER,
				(dynpm_message_type)REGISTER_PROCESS_MANAGER_ACK,
				connection)){
		error("failed to send REGISTER_PROCESS_MANAGER_ACK from SCHEDULER");
	}
}

static void _remove_registration(){
}

// TODO the registration should only be allowed to be updated if there is no
// malleability target; in other words, only if there is no outstanding malleability
// operation by any of the applications managed by the registered process manager
// or launcher; we need to add the checks here and perhaps add a return code
static void _update_registration(dynpm_register_process_manager_msg_t *registration_data,
		process_manager_registration_t **registration){
	//info("current:");
	//_print_registration_data((*registration)->registration_data);

	for(int i = 0; i < (*registration)->registration_data->application_count; i++){
		if((*registration)->allocation_requests[i] != NULL){
			free((*registration)->allocation_requests[i]);
		}
	}
	free((*registration)->allocation_requests);

	dynpm_free_register_process_manager_msg( &((*registration)->registration_data));
	(*registration)->registration_data = registration_data;

	(*registration)->allocation_requests =
		malloc(registration_data->application_count * sizeof(dynpm_allocation_request_msg_t*));
	for(int i = 0; i < registration_data->application_count; i++){
		(*registration)->allocation_requests[i] = NULL;
	}

	//info("updated:");
	//_print_registration_data((*registration)->registration_data);
}

static void _register_process_manager(
		dynpm_server_to_client_connection_t *connection,
		const void *read_buffer){

	pthread_mutex_lock(&process_manager_registrations_mutex);

	dynpm_register_process_manager_msg_t *registration_data;
	dynpm_deserialize_register_process_manager_msg(&registration_data, &read_buffer);

	_print_registration_data(registration_data);

	if(list_count(process_manager_registrations) > 0){
		process_manager_registration_t *registration = NULL;
		int found = _find_process_manager_registration(
				registration_data->job_index, &registration);
		if(found){
			info("------------------------------------------");
			info("found existing registration for job %d; updating it...",
					registration->registration_data->job_index);
			_update_registration(registration_data, &registration);
			info("------------------------------------------");
		} else {
			info("registration not found for job %d; adding it...",
					registration_data->job_index);
			_add_registration(connection, registration_data);
		}
	} else {
		info("no registrations; adding it");
		_add_registration(connection, registration_data);
	}

	pthread_mutex_unlock(&process_manager_registrations_mutex);
}

static void _update_allocation_request(
		dynpm_server_to_client_connection_t *connection,
		const void *read_buffer){
	pthread_mutex_lock(&process_manager_registrations_mutex);

	dynpm_allocation_request_msg_t *request;
	dynpm_deserialize_allocation_request_msg(&request, &read_buffer);

	info("app: %d; partition: %d; type: %d; nodes: %d",
			request->application_index, request->partition_index,
			request->type, request->node_count);

	if(list_count(process_manager_registrations) > 0){
		process_manager_registration_t *registration = NULL;
		ListIterator registration_iterator
			= list_iterator_create(process_manager_registrations);
		int found = 0;
		while ((registration = list_next(registration_iterator))) {
			if(registration->connection == connection){
				info("------------------------------------------");
				info("found existing registration for connection %x; adding or updating requests...",
						connection);
				if(request->application_index < registration->registration_data->application_count){
					if(registration->allocation_requests[request->application_index]){
						info("freeing pre-existing request");
						dynpm_free_allocation_request_msg(
								&(registration->allocation_requests[request->application_index]));
					}
					registration->allocation_requests[request->application_index] = request;
					info("updated request for job %d, application %d: nodes = %d",
							registration->registration_data->job_index, request->application_index, request->node_count);
				} else {
					error("application index exceeds previously registered application count");
				}
				info("------------------------------------------");
				found = 1;
				break;
			}
		}
		list_iterator_destroy(registration_iterator);

		if(!found){
			info("registration not found for connection %x; cannot update requests...",
					connection);
		}
	} else {
		info("no registrations; cannot update requests");
	}

	pthread_mutex_unlock(&process_manager_registrations_mutex);
}

static void *_server_handler(dynpm_server_to_client_connection_t *connection, const void *read_buffer){
	info("=======================================================================");

	dynpm_header_t *header;
	dynpm_deserialize_header(&header, &read_buffer);

	if(header->message_type == (dynpm_message_type)REGISTER_PROCESS_MANAGER){
		info("received REGISTER_PROCESS_MANAGER");
		if(header->source_type != (dynpm_message_type)LAUNCHER
				&& header->source_type != (dynpm_message_type)PROCESS_MANAGER){
			error("do not support registrations from other than LAUNCHER or PROCESS_MANAGER");
			return NULL;
		} else {
			if(header->source_type == (dynpm_message_type)LAUNCHER){
				info("LAUNCHER registering a PROCESS_MANAGER");
			} else {
				info("PROCESS_MANAGER registering itself");
			}
		}
		_register_process_manager(connection, read_buffer);
	} else if(header->message_type == (dynpm_message_type)DEREGISTER_PROCESS_MANAGER){
		info("received DEREGISTER_PROCESS_MANAGER");
	} else if(header->message_type == (dynpm_message_type)ALLOCATION_REQUEST){
		info("received ALLOCATION_REQUEST");
		_update_allocation_request(connection, read_buffer);
	} else if(header->message_type == (dynpm_message_type)REALLOCATION_START_ACK){
		info("received REALLOCATION_START_ACK");
		info("from host: %s; pid: %d", connection->host, connection->pid);
		// TODO confirm the target here, when the app acks
		// TODO need to have a configurable timeout to prevent the nodes being held
		// while the PM and/or application are too slow to react and make use of them
	} else {
		error("message not supported in _server_handler\n%s", (char*)read_buffer);
		error("header->source_type=%d", header->source_type);
		error("header->message_type=%d", header->message_type);
	}

	dynpm_free_header(&header);

	info("=======================================================================");

	return NULL;
}

void *malleable_scheduler_thread(){
	part_record_t *part_ptr = NULL;
	time_t now, window_end; // time_t values are in seconds
	int malleable_window = 60*60*24; // 1 day
	bitstr_t *free_nodes;

	/* Locks: Read config, read job, read node, read partition */
	slurmctld_lock_t job_read_lock = {
		READ_LOCK, READ_LOCK, READ_LOCK, READ_LOCK, READ_LOCK };
	/* Locks: Read config, write job, write node, read partition */
	slurmctld_lock_t job_write_lock = {
		READ_LOCK, WRITE_LOCK, WRITE_LOCK, READ_LOCK, READ_LOCK };

	dynpm_process_conf_entry_t *config_entry;

	if(dynpm_conf_get_process_config(&config_entry, "slurmctld")){
		error("could not find %s configuration", "slurmctld");
		return NULL;
	}

	if(dynpm_server_init(config_entry->port,
				config_entry->threads,
				_server_handler,
				"/var/log/ctld.out", "/var/log/ctld.err")){
		error("error setting up listener threads");
		return NULL;
	} else {
		debug("listener threads created in %s", "slurmctld");
	}

	process_manager_registrations = list_create(NULL);

	// TODO add algorithm summary here
	while(1) {
		lock_slurmctld(job_write_lock);
		pthread_mutex_lock(&process_manager_registrations_mutex);

		List malleable_jobs = list_create(NULL);
		free_nodes = bit_copy(avail_node_bitmap);

		_scan_running_malleable_jobs(&malleable_jobs, &free_nodes);

		if(bit_set_count(free_nodes) > 0 // at least one free node
				&& list_count(malleable_jobs) > 0 // at least one malleable job
		  ){
			info("************************************************************");
			info("beginning malleable scheduler pass");
			info("free nodes: %d; malleable jobs: %d",
					bit_set_count(free_nodes), list_count(malleable_jobs));
			_print_bitfield("free nodes: ", free_nodes);

			// TODO check if we need to adjust to time resolution (see backfill.c)
			now = time(NULL);
			window_end = now + malleable_window;

			pthread_mutex_lock(&node_time_mutex);

			_init_node_time_gap(&node_time, now, window_end, free_nodes);
			_build_node_time_gap(&node_time, window_end, free_nodes);

			info("entering _manage_malleable_jobs");
			_manage_malleable_jobs(node_time, malleable_jobs, now);

			pthread_mutex_unlock(&node_time_mutex);
			info("ending malleable scheduler pass");
			info("************************************************************");
		} else if(list_count(malleable_jobs) == 0
				&& list_count(process_manager_registrations) > 0){
			info("************************************************************");
			info("no malleable jobs running; clearing %d stale registrations...",
					list_count(process_manager_registrations));
			// first clear registrations and make a new empty list
			_free_all_process_manager_registrations();
			list_destroy(process_manager_registrations);
			process_manager_registrations = list_create(NULL);

			// then clear dynpm connections
			dynpm_lock_client_connections();
			dynpm_print_client_connections();
			if(list_count(malleable_jobs) == 0) dynpm_clear_client_connections();
			dynpm_unlock_client_connections();
			info("************************************************************");
		}

		list_destroy(malleable_jobs);
		bit_free(free_nodes);

		unlock_slurmctld(job_write_lock);
		pthread_mutex_unlock(&process_manager_registrations_mutex);

		// TODO confirm we don't need a thread yield strategy under high loads
		// if this value is lower than the backfiller period, then malleable applications
		// get resources first, if it's higher, small applications are backfilled first

		// TODO make this configurable, or reuse a bf_* setting
		sleep(10);
	}

	list_destroy(process_manager_registrations);

}

// TODO update this interface as necessary
static int _malleable_update_job(job_record_t *job_ptr,
		malleable_operation_target_t *target);

// TODO minimize copies; currently impossible to remove all without direct
// changes to upstream. These are all copies from:
// src/slurmctld/job_mgr.c
static uint32_t _max_switch_wait(uint32_t input_wait);
static void _send_job_kill(job_record_t *job_ptr);
static void _merge_job_licenses(job_record_t *shrink_job_ptr, job_record_t *expand_job_ptr);
static void _realloc_nodes(job_record_t *job_ptr, bitstr_t *orig_node_bitmap);

/*
 * malleable_update_job: this is a carefully modified version of update_job.
 * It combines the original update_job (visible for us to call), with the
 * internal (static) _update_job.  Unfortunately we cannot avoid this copy,
 * since we require changes to the internal call.
 *
 * We attepmt to modify it as little as possible, in order to ease merging
 * upstream changes, while also supporting DEEP-SEA malleability scheduling
 * goals.  Unfortunately, this cannot be avoided unless we submit a patch
 * to upstream, and even then, this code is so long and complex that it may
 * require a careful rework and modularization of the code to make it more
 * manageable.
 *
 * TODO make sure to enumerate the changes here and cross-reference with the
 * original _update_job from src/slurmctld/job_mgr.c
 *
 *
 * original description from src/slurmctld/job_mgr.c
 *
 * update_job - update a job's parameters per the supplied specifications
 * IN msg - RPC to update job, including change specification
 * IN uid - uid of user issuing RPC
 * IN send_msg - whether to send msg back or not
 * RET returns an error code from slurm_errno.h
 * global: job_list - global list of job entries
 *	last_job_update - time of last job table update
 */
// original interface:
//int malleable_update_job(slurm_msg_t *msg, uid_t uid, bool send_msg) {
// TODO we will just merge the calls into 1; the regular update_record does
// not much to justify it remains separate
//
// the bitfield needs to be updated for the job_ptr before calling?
static int _malleable_update_job(
		job_record_t *job_ptr,
		malleable_operation_target_t *target
		) {}

static uint32_t _max_switch_wait(uint32_t input_wait)
{
	static time_t sched_update = 0;
	static uint32_t max_wait = 300;	/* default max_switch_wait, seconds */
	int i;

	if (sched_update != slurm_conf.last_update) {
		char *tmp_ptr;
		sched_update = slurm_conf.last_update;
		if ((tmp_ptr = xstrcasestr(slurm_conf.sched_params,
		                           "max_switch_wait="))) {
			/*                  0123456789012345 */
			i = atoi(tmp_ptr + 16);
			if (i < 0) {
				error("ignoring SchedulerParameters: "
				      "max_switch_wait of %d", i);
			} else {
				max_wait = i;
			}
		}
	}

	if (max_wait > input_wait)
		return input_wait;
	return max_wait;
}

static void _send_job_kill(job_record_t *job_ptr)
{
	agent_arg_t *agent_args = NULL;
#ifdef HAVE_FRONT_END
	front_end_record_t *front_end_ptr;
#else
	int i;
	node_record_t *node_ptr;
#endif
	kill_job_msg_t *kill_job;

	agent_args = xmalloc(sizeof(agent_arg_t));
	agent_args->msg_type = REQUEST_TERMINATE_JOB;
	agent_args->retry = 0;	/* re_kill_job() resends as needed */
	agent_args->hostlist = hostlist_create(NULL);

	last_node_update    = time(NULL);

#ifdef HAVE_FRONT_END
	if (job_ptr->batch_host &&
	    (front_end_ptr = job_ptr->front_end_ptr)) {
		agent_args->protocol_version = front_end_ptr->protocol_version;
		hostlist_push_host(agent_args->hostlist, job_ptr->batch_host);
		agent_args->node_count++;
	}
#else
	if (!job_ptr->node_bitmap_cg)
		build_cg_bitmap(job_ptr);
	agent_args->protocol_version = SLURM_PROTOCOL_VERSION;
	for (i = 0; (node_ptr = next_node(&i)); i++) {
		if (!bit_test(job_ptr->node_bitmap_cg, node_ptr->index))
			continue;
		if (agent_args->protocol_version > node_ptr->protocol_version)
			agent_args->protocol_version =
				node_ptr->protocol_version;
		hostlist_push_host(agent_args->hostlist, node_ptr->name);
		agent_args->node_count++;
	}
#endif
	if (agent_args->node_count == 0) {
		if (job_ptr->details->expanding_jobid == 0) {
			error("%s: %pJ allocated no nodes to be killed on",
			      __func__, job_ptr);
		}
		hostlist_destroy(agent_args->hostlist);
		xfree(agent_args);
		return;
	}

	kill_job = create_kill_job_msg(job_ptr, agent_args->protocol_version);
	kill_job->nodes = xstrdup(job_ptr->nodes);

	agent_args->msg_args = kill_job;
	set_agent_arg_r_uid(agent_args, SLURM_AUTH_UID_ANY);
	agent_queue_request(agent_args);
	return;
}

static void _merge_job_licenses(job_record_t *shrink_job_ptr,
				job_record_t *expand_job_ptr)
{
	xassert(shrink_job_ptr);
	xassert(expand_job_ptr);

	/* FIXME: do we really need to update accounting here?  It
	 * might already happen */

	if (!shrink_job_ptr->licenses)		/* No licenses to add */
		return;

	if (!expand_job_ptr->licenses) {	/* Just transfer licenses */
		expand_job_ptr->licenses = shrink_job_ptr->licenses;
		shrink_job_ptr->licenses = NULL;
		FREE_NULL_LIST(expand_job_ptr->license_list);
		expand_job_ptr->license_list = shrink_job_ptr->license_list;
		shrink_job_ptr->license_list = NULL;
		return;
	}

	/* Merge the license information into expanding job */
	xstrcat(expand_job_ptr->licenses, ",");
	xstrcat(expand_job_ptr->licenses, shrink_job_ptr->licenses);
	xfree(shrink_job_ptr->licenses);
	FREE_NULL_LIST(expand_job_ptr->license_list);
	FREE_NULL_LIST(shrink_job_ptr->license_list);
	license_job_merge(expand_job_ptr);
	return;
}

/* Allocate nodes to new job. Old job info will be cleared at epilog complete */
static void _realloc_nodes(job_record_t *job_ptr, bitstr_t *orig_node_bitmap)
{
	int i, i_first, i_last;
	node_record_t *node_ptr;

	xassert(job_ptr);
	xassert(orig_node_bitmap);
	if (!job_ptr->job_resrcs || !job_ptr->job_resrcs->node_bitmap)
		return;
	i_first = bit_ffs(job_ptr->job_resrcs->node_bitmap);
	if (i_first >= 0)
		i_last = bit_fls(job_ptr->job_resrcs->node_bitmap);
	else
		i_last = -1;
	for (i = i_first; i <= i_last; i++) {
		if (!bit_test(job_ptr->job_resrcs->node_bitmap, i) ||
		    bit_test(orig_node_bitmap, i))
			continue;
		node_ptr = node_record_table_ptr[i];
		make_node_alloc(node_ptr, job_ptr);
	}
}

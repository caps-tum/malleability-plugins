/*
 * Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *
 * Additional copyrights may follow
 *
 */

// slurm headers

// deep-sea headers
#include "malleability.h"

pthread_mutex_t simulator_mutex = PTHREAD_MUTEX_INITIALIZER;
// plugin data
pthread_mutex_t  node_time_mutex = PTHREAD_MUTEX_INITIALIZER;
node_time_gap_t *node_time = NULL;
int max_node_time_gaps = 500; // matches backfiller job count for now
// from server.c in libdynpm

extern diag_stats_t slurmctld_diag_stats;

// TODO resv_ptr is actually not set on regular job life cycles
// instead, _add_reservation is called in backfill.c
// that routine operates on the node_space_map_t instead of reservation
// types; a node gap that does not delay the current top priority
// job can be determined with the node space maps.  The so called
// easy backfill makes a reservation for the current top priory
// job, basically the one at the head of the job queue

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

	info("==================================================================");
	while (1) {
		if(bit_set_count(node_time[nt].free_nodes) > 0){ // don't print capped entry
			slurm_make_time_str(&node_time[nt].start_time,
					begin_buf, sizeof(begin_buf));
			slurm_make_time_str(&node_time[nt].end_time,
					end_buf, sizeof(end_buf));
			node_list = bitmap2node_name(node_time[nt].free_nodes);
			info("Begin:%10s End:%10s Nodes:%s",
					begin_buf, end_buf, node_list);
			xfree(node_list);
		}

		if ((node_time[nt].is_last)) break;
		else nt++;
	}
	info("==================================================================");
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
static void _scan_running_jobs(List *malleable_jobs, bitstr_t **free_nodes){
	job_record_t *job_ptr = NULL;
	step_record_t *step_ptr = NULL;

	xassert(job_list);

	ListIterator job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		xassert(job_ptr);
		if(IS_JOB_RUNNING(job_ptr)){
			// mark nodes of running jobs as not available
			bit_and_not(*free_nodes, job_ptr->node_bitmap);
			if(job_ptr->step_list == NULL){
				debug("job step list not ready for job %d", job_ptr->job_id);
				continue;
			}
			if(!list_is_empty(job_ptr->step_list)){
				ListIterator step_iterator = list_iterator_create(job_ptr->step_list);
				int malleable_step_count = 0;
				while ((step_ptr = list_next(step_iterator))) {
					if(step_ptr->submit_line == NULL){
						debug("submit line not available"); fflush(0);
						continue;
					} else if(strncmp(step_ptr->submit_line,
								"drun ", strlen("drun ")) == 0){
						list_append(*malleable_jobs, job_ptr);
						break;
					}
				}
				list_iterator_destroy(step_iterator);
			}
		}
	}
	list_iterator_destroy(job_iterator);
}

static void _simulate_scan_running_jobs(List *malleable_jobs, bitstr_t **free_nodes, time_t now, int mall_prob){
	job_record_t *job_ptr = NULL;
    static int malleable_job_count = 0;

	ListIterator job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		if(IS_JOB_RUNNING(job_ptr)){
			// mark nodes of running jobs as not available
            bit_and_not(*free_nodes, job_ptr->node_bitmap);
            if(job_ptr->comment == NULL){
                // parameter: malleable job probability
                if((rand() % 100) < mall_prob){
                    verbose("marking job %d as malleable; comment: %d; resize_time: %d; total: %d; data: %d",
                            job_ptr->job_id, job_ptr->comment, job_ptr->resize_time,
                            malleable_job_count, sizeof(char*));
                    job_ptr->comment = (char*)2; // malleable
                    // TODO in future simulators, set the requested time limit
                    // separately from the actual end time (see SWF comments)
                    job_ptr->end_time = (time_t)(now + job_ptr->time_limit*60);
                    malleable_job_count++;
                } else {
                    job_ptr->comment = (char*)1; // rigid
                }
            }

            if(job_ptr->comment == (char*)2){
                verbose("malleable job %d is ready for reallocation", job_ptr->job_id);
                verbose("started: %d; will complete: %d", job_ptr->start_time, job_ptr->end_time);
                list_append(*malleable_jobs, job_ptr);
            }

            if(job_ptr->comment == (char*)3){
                verbose("malleable job %d is ongoing a remapping", job_ptr->job_id);
            }

            if(job_ptr->comment == (char*)4){ // other status?
                verbose("malleable job %d is on its warmup period", job_ptr->job_id);
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

/*
 * This operation is meant to be used by the periodic scheduler in "passive" malleable
 * scenarios.  The node-time gaps can be generated while handling requests in the
 * "active" malleable scenarios, as documented in D5.2
 */
static void _generate_offers(node_time_gap_t *node_time, List malleable_jobs, time_t now){
	ListIterator job_iterator;
	job_record_t *job_ptr = NULL;
	step_record_t *step_ptr = NULL;
	int nt = 0, no = 0, last_offer = 0;
	char begin_buf[32], end_buf[32], *node_list;

	info("==================================================================");
	while (!last_offer) {
		job_iterator = list_iterator_create(malleable_jobs);
		while ((job_ptr = list_next(job_iterator)) && !last_offer) {
			xassert(IS_JOB_RUNNING(job_ptr)); // must be running

            // TODO jobs created interactively usually do not have an end-time,
            // and therefore will never fit; change this?
			if(job_ptr->end_time <= node_time[nt].end_time &&
					bit_set_count(node_time[nt].free_nodes) > 0){ // not capped
				info("job %d fits; remaining: %d", job_ptr->job_id,
						job_ptr->end_time - now);

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

				if(job_ptr->step_list == NULL ||
                        list_is_empty(job_ptr->step_list)){
					info("job %d has no steps; skipping...", job_ptr->job_id);
					continue;
				}

                info("job %d has %d steps", job_ptr->job_id, list_count(job_ptr->step_list));
				ListIterator step_iterator = list_iterator_create(job_ptr->step_list);
				int malleable_step_count = 0;
				while ((step_ptr = list_next(step_iterator))) {
					if(step_ptr->submit_line == NULL){
						debug("submit line not available");
						continue;
					} else if(strncmp(step_ptr->submit_line,
								"drun ", strlen("drun ")) == 0){
						malleable_step_count++;
						info("sending offer to job %d step %d",
								job_ptr->job_id, step_ptr->step_id.step_id);

						dynpm_lock_client_connections();
						dynpm_server_to_client_connection_t *connection = NULL;
						int found = dynpm_find_client_connection_in_list(
								step_ptr->host,
								step_ptr->srun_pid,
								&connection);

						if(found && connection){
							// TODO match registrations here
							// TODO send offers here

                            // need regular malloc calls here!
							//dynpm_header_t *header = malloc(sizeof(dynpm_header_t));
							//header->source_type =  (dynpm_source_type)SCHEDULER;
							//header->message_type = (dynpm_message_type)OFFER_RESOURCES;

                            //// need regular malloc calls here!
							//dynpm_resource_offer_msg_t *offer = malloc(sizeof(dynpm_resource_offer_msg_t));
							//offer->node_count = bit_set_count(node_time[nt].free_nodes);
							//// TODO partition and application index should come from the connection data
							//offer->application_index = 0;
							//offer->partition_index = 0;
							//offer->creation_time = now;
							//offer->end_time = node_time[nt].end_time;

							//if(dynpm_send_message_to_client_connection(header, (void*)offer, connection)){
							//	error("error sending offer to job %d step %d host %s",
							//		job_ptr->job_id, step_ptr->step_id.step_id, step_ptr->host);
							//} else {
							//	info("sent offer to job %d step %d host %s",
							//		job_ptr->job_id, step_ptr->step_id.step_id, step_ptr->host);
							//}

						} else {
							error("connection not found for job %d step %d",
									job_ptr->job_id, step_ptr->step_id.step_id);
						}
						dynpm_unlock_client_connections();
					}
				}
				list_iterator_destroy(step_iterator);


				info("offer sent to job %d; %d malleable job steps",
						job_ptr->job_id, malleable_step_count);
			}
		}
		list_iterator_destroy(job_iterator);

		if(bit_set_count(node_time[nt].free_nodes) > 0){ // don't print capped entry
			slurm_make_time_str(&node_time[nt].start_time, begin_buf, sizeof(begin_buf));
			slurm_make_time_str(&node_time[nt].end_time, end_buf, sizeof(end_buf));
			node_list = bitmap2node_name(node_time[nt].free_nodes);
			info("Begin:%10s End:%10s Nodes:%s", begin_buf, end_buf, node_list);
			xfree(node_list);
		}

		if ((node_time[nt].is_last)) break;
		else nt++;
	}
	info("==================================================================");

}

static void _simulate_malleable_updates(node_time_gap_t *node_time,
        List malleable_jobs, time_t now, bitstr_t **free_nodes, int malleable_factor){
    // TODO take a look at:
    // src/plugins/select/cons_common/cons_common.c
    // select_p_job_expand
    // select_p_job_rezized

	ListIterator job_iterator;
	job_record_t *job_ptr = NULL;
	int nt = 0, no = 0, last_offer = 0;
	char begin_buf[32], end_buf[32], *node_list;

	verbose("==================================================================");
    verbose("%d nodes are free before malleable updates", bit_set_count(*free_nodes) );
	while (!last_offer) {
		job_iterator = list_iterator_create(malleable_jobs);
		while ((job_ptr = list_next(job_iterator)) && !last_offer) {
			xassert(IS_JOB_RUNNING(job_ptr)); // must be running

			if(job_ptr->end_time > 0 &&
                    job_ptr->end_time <= node_time[nt].end_time &&
					bit_set_count(node_time[nt].free_nodes) > 0){ // not capped
				verbose("--- YES: job %d fits; remaining: %d", job_ptr->job_id, job_ptr->end_time - now);
                int current_node_count = bit_set_count(job_ptr->node_bitmap);
                int max_nodes_to_add = (malleable_factor * current_node_count)
                    - current_node_count;
                int nodes_added;
                bitoff_t offset = bit_ffs(*free_nodes);
                bitoff_t size = bit_size(*free_nodes);
                for(nodes_added = 0;
                        nodes_added < max_nodes_to_add && offset < size;){
                    if(bit_test(*free_nodes, offset)){
                        //_print_bitfield("before:", *free_nodes);
                        bit_clear(*free_nodes, offset);
                        //_print_bitfield("after:", *free_nodes);
                        nodes_added++;
                    }
                    offset++;
                }
                verbose("job %d had %d nodes, now %d nodes", job_ptr->job_id,
                        current_node_count, current_node_count + nodes_added);
			} else {
				verbose("--- NO: job %d does not fit; remaining: %d", job_ptr->job_id, job_ptr->end_time - now);
            }
		}
		list_iterator_destroy(job_iterator);

		if(bit_set_count(node_time[nt].free_nodes) > 0){ // don't print capped entry
			slurm_make_time_str(&node_time[nt].start_time, begin_buf, sizeof(begin_buf));
			slurm_make_time_str(&node_time[nt].end_time, end_buf, sizeof(end_buf));
			node_list = bitmap2node_name(node_time[nt].free_nodes);
			verbose("Begin:%10s End:%10s Nodes:%s", begin_buf, end_buf, node_list);
			xfree(node_list);
		}

		if ((node_time[nt].is_last)) break;
		else nt++;
	}
    verbose("%d nodes are free after malleable updates", bit_set_count(*free_nodes) );
	verbose("==================================================================");
    //exit(0);
}

extern void stop_backfill_agent(void);

static job_desc_msg_t *_simulator_create_job_desc(time_t now) {

	job_desc_msg_t *job_desc = xmalloc_nz(sizeof(*job_desc));

	slurm_init_job_desc_msg(job_desc);

    // -- begin of SWF --
    // TODO there is a potential issue to address here:
    // The actual end_time of the job is set on its completion,
    // but we get it from the SWF file; need to observe if there
    // are any side effects from setting this value before so
    // that we can track it and know when to end a job in the
    // simulation

    // these are read from the SWF instead; never set here
    // 2 (column) in SWF
	//job_desc->begin_time = now; // is this really submission time?

    // 4 (column) in SWF
    // this is the actual runtime of the job as recorded in the SWF log
	//job_desc->end_time = now + 1; //

    // 8 (column) in SWF
    // this is the requested number of processors
    // in general, these should match the number of allocated processors,
    // that is column 5 in SWF
	//job_desc->min_cpus = job_desc->min_cpus = 0;

    // 9 (column) in SWF
    // this is the wallclock time provided by users, and used by the scheduler,
    // for example, during backfilling
    // this is in seconds in SWF, so it needs to be converted to minutes for Slurm
	//job_desc->time_limit = 1; // in minutes

    // the number of nodes is not specified in SWF
    // we need t oderive the node counts based on the cores per node
    // of the partition.
    // TODO in the future, we need to set these separately to simulate
    // moldability
	//job_desc->min_nodes = job_desc->max_nodes = 1;

    // other SWF relevant entries but that are not set by us

    // 12 (column) in SWF
    // this is the (unix) user ID who submitted the job; currently we
    // don't track these
	job_desc->user_id = 1000; // TODO careful not to specify root
    // 13 (column) in SWF
    // this is the group ID of the job; currently we don't track these
	job_desc->group_id = 0; // TODO check if needs to be a valid one
    // 16 (column) in SWF
    // this is the partition number in SWF; needs to be converted from a
    // number to a portition pointer for Slurm.  We don't simulate multiple
    // partitions for now.
	job_desc->partition = NULL; // string
    // 17 (column) in SWF
    // preceeding job number in SWF, indicated which job needs to complete
    // first before this one can start.  We are currently not using this
	job_desc->dependency = NULL;

    // -- end of SWF --

    // The rest of these are not covered in SWF

	job_desc->account = NULL; //string
	job_desc->acctg_freq = NULL; //string

	/* admin_comment not filled in here */
	/* alloc_node not filled in here */
	/* alloc_resp_port not filled in here */
	/* alloc_sid not filled in here */
	/* arg[c|v] not filled in here */
	/* array_inx not filled in here */
	/* array_bitmap not filled in here */
	/* batch_features not filled in here */

	job_desc->bitflags = 0;
	job_desc->burst_buffer = NULL; // string
	job_desc->clusters = NULL; //string
	job_desc->cluster_features = NULL; //string
	job_desc->comment = NULL; //string
	job_desc->req_context = NULL; //string

	job_desc->contiguous = NO_VAL16;
	job_desc->core_spec = NO_VAL16;

	/* cpu_bind not filled in here */
	/* cpu_bind_type not filled in here */

	job_desc->cpu_freq_min = 0;
	job_desc->cpu_freq_max = 0;
	job_desc->cpu_freq_gov = 0;

	job_desc->cpus_per_tres = "";

	/* crontab_entry not filled in here */

    // the deadline is the actual required completion time of the job
    // SWF has no entry (column) for this
	job_desc->deadline = 0; // we don't do deadlines in the traces

	job_desc->delay_boot = 0;

	/* end_time not filled in here */

    // this is expected to exist by the different processing passes
    // in Slurm; not actually used in the simulator, but prevents
    // segfaults
	job_desc->environment = xmalloc(2*sizeof(char*));
	job_desc->environment[0] = "dummy0=0";
	job_desc->environment[1] = "dummy1=1";
	job_desc->env_size = 2;

	job_desc->extra = NULL; // string
	job_desc->exc_nodes = NULL; // string
	job_desc->features = NULL; // string
	job_desc->prefer = NULL; // string

	/* fed_siblings_active not filled in here */
	/* fed_siblings_viable not filled in here */

	/* het_job_offset not filled in here */

	job_desc->immediate = 0;

	/* job_id not filled in here */
	/* job_id_str not filled in here */

	job_desc->kill_on_node_fail = 0;

	job_desc->licenses = NULL; // string

	job_desc->mail_type = 0;

	job_desc->mail_user = NULL; // string

	job_desc->mcs_label = NULL; // string

	job_desc->mem_bind = NULL; // string
	job_desc->mem_bind_type = 0;

	job_desc->mem_per_tres = NULL; // string

	job_desc->name = NULL; // string

	job_desc->network = NULL; // string

	job_desc->nice = NICE_OFFSET;

	/* origin_cluster is not filled in here */
	/* other_port not filled in here */

    // TODO we may use this to simulate power strategies
	job_desc->power_flags = 0; // TODO need to check this

	job_desc->priority = NO_VAL;

	job_desc->profile = 0;

	job_desc->qos = NULL; // string

	/* resp_host not filled in here */
	/* restart_cnt not filled in here */

	/* requeue not filled in here */

	job_desc->reservation = NULL; // string
	job_desc->script = "";
	/* script_buf not filled in here */

	/* site_factor not filled in here */

	job_desc->submit_line = xstrdup("submit_line");
	job_desc->task_dist = SLURM_DIST_UNKNOWN;

	job_desc->tres_bind = NULL; // string
	job_desc->tres_freq = NULL; // string
	job_desc->tres_per_job = NULL; // string
	job_desc->tres_per_node = NULL; // string
	job_desc->tres_per_socket = NULL; // string
	job_desc->tres_per_task = NULL; // string

	/* wait_all_nodes not filled in here */

	job_desc->warn_flags = 0;
	job_desc->warn_signal = 0;
	job_desc->warn_time = 0;

	job_desc->work_dir = NULL; // string

	/* max_cpus not filled in here */

	/* boards_per_node not filled in here */
	/* sockets_per_board not filled in here */

	/* ntasks_per_board not filled in here */

	/* select_jobinfo not filled in here */
	/* desc->std_[err|in|out] not filled in here */
	/* tres_req_cnt not filled in here */

	job_desc->wckey = NULL; // string

	job_desc->x11 = 0;

	return job_desc;
}

/*
 * Read jobs at second _now_ from SWF file
 */
static job_desc_msg_t *_simulator_read_next_job_desc(time_t now, FILE *swf_file,
        char **current_line, int *end_of_file_reached){
    ssize_t read;
    char *line = NULL;
    size_t line_length;
    char *tokenize_line;
    char *token;
    char delimiter[] = " \t\r\n\v\f"; // columns are separated by space
    int allocated_processors;
    int allocated_nodes;

    // line buffering
    static time_t next_submission_time = 0; // job_desc->begin_time
    static time_t next_run_time = 0; // job_desc->time_limit but in seconds
    static time_t next_min_nodes = 0; // job_desc->min_nodes
    static time_t next_max_nodes = 0; // job_desc->max_nodes
    static int current_line_ready = 0;
    static int end_reached = 0;

    if(end_reached){
        *end_of_file_reached = 1;
        return NULL;
    }

    if(*(current_line)[0] == ';') {
        error("found comment line in the middle of the SWF job history");
        error("%s", *current_line);
        exit(-1);
    } else if (!current_line_ready){ // need to parse the current line
        debug2("processing: %s", *current_line);
        tokenize_line = malloc(sizeof(char)*strlen(*current_line)+64);
        strcpy(tokenize_line, *current_line);
        // the job number, job_id in Slurm, can be ignored here, since
        // it is set by Slurm internally; it will match always since
        // we perform them in sequence (no threading)
        token = strtok(tokenize_line, delimiter);
        info("reading job record: %s", token);
        // refer to:
        // https://www.cs.huji.ac.il/labs/parallel/workload/swf.html
        token = strtok(NULL, delimiter); // column 2 Submit Time
        next_submission_time = atoi(token);
        debug3("Submit Time (seconds): %s, parsed: %d", token, next_submission_time);
        token = strtok(NULL, delimiter); // column 3 Wait Time
        token = strtok(NULL, delimiter); // column 4 Run Time
        next_run_time = atoi(token);
        debug3("Run Time (seconds): %s, parsed: %d", token, next_run_time);
        token = strtok(NULL, delimiter); // column 5 Number of Allocated Processors
        // unfortunately, some SWF files use this value as node count, others as
        // requested processors; this became an issue once SMP nodes emerged
        // TODO double check this is compatible with the input SWF
        allocated_processors = atoi(token);
        if(allocated_processors % node_record_table_ptr[0]->cpus){
            allocated_nodes = (allocated_processors)/node_record_table_ptr[0]->cpus + 1;
        } else {
            allocated_nodes = (allocated_processors)/node_record_table_ptr[0]->cpus;
        }
        next_min_nodes = next_max_nodes = allocated_nodes;
        debug("Number of Allocated Processors: %s, parsed: %d", token, next_min_nodes);
        token = strtok(NULL, delimiter); // column 6 Average CPU Time Used
        token = strtok(NULL, delimiter); // column 7 Used Memory
        token = strtok(NULL, delimiter); // column 8 Requested Number of Processors
        // TODO requested time is the walltime provided by the user as a guess
        // unfortunately, our modeling code does not model the difference with
        // this value and the actual runtime, just the actual runtime
        // is generated by the model (i.e. Run Time in column 4)
        // in the future, we need to add this to the model and parse it here
        token = strtok(NULL, delimiter); // column 9 Requested Time
        token = strtok(NULL, delimiter); // column 10 Requested Memory
        token = strtok(NULL, delimiter); // column 11 Status
        token = strtok(NULL, delimiter); // column 12 User ID
        token = strtok(NULL, delimiter); // column 13 Group ID
        token = strtok(NULL, delimiter); // column 14 Executable (Application) Number
        token = strtok(NULL, delimiter); // column 15 Queue Number
        token = strtok(NULL, delimiter); // column 16 Partition Number
        token = strtok(NULL, delimiter); // column 17 Preceding Job Number
        token = strtok(NULL, delimiter); // column 18 Think Time from Preceding Job
        token = strtok(NULL, delimiter); // last one should be NULL
        if(token != NULL) {
            error("SWF line contains more than 18 columns; malformed");
            exit(-1);
        }
        current_line_ready = 1;
        // TODO why does this fail with the KIT input? malformed delimeters?
        //free(tokenize_line);
    }
    xassert(current_line_ready); // should always be ready here

    if(now < next_submission_time){
        // now has not reached the next submission time
        // therefore, there is no job arrival yet
        debug2("now = %d; next submission at: %d", now, next_submission_time);
        return NULL;
    } else if (now == next_submission_time){
        debug3("now = %d; next submission at: %d", now, next_submission_time);
        debug3("next_run_time = %d; next_min_nodes: %d",
                next_run_time, next_min_nodes);

        job_desc_msg_t *job_desc = _simulator_create_job_desc(now);

        //job_desc->begin_time = now; // is this really submission time?
        //job_desc->end_time = next_run_time; // these are absolute (not incremental)
        // TODO currently we based a simulator on MaxNodes (whole nodes)
        // so this value is ignored, but in the future we may want to
        // change this, to beter moder SMP nodes with the MaxProcs SWF parameter
        job_desc->min_cpus = job_desc->min_cpus = 0;
        if(next_run_time > 60){
            job_desc->time_limit = (next_run_time)/60; // in minutes
        } else { // the scheduler operates with a 1 minute resolution for deadlines
            job_desc->time_limit = 1;
        }
        // TODO in the future, these need to be separate values to
        // moldability in the model/simulation
        job_desc->min_nodes = job_desc->max_nodes = next_min_nodes;

        // now has reached the next submission time at the SWF
        if((read = getline(&(*current_line), &line_length, swf_file)) != -1) {
            current_line_ready = 0;
        } else {
            // when we reach the end of the SWF, the simulation loop contiues
            // until it reaches the end of the simulated time specified
            // in the header of the SWF
            info("end of SWF reached");
            *end_of_file_reached = 1;
            end_reached = 1;
        }
        return job_desc;
    } else {
        error("now is greater than next_submission_time");
        exit(-1);
    }
}

// based on internal _job_complete in src/slurmctld/job_mgr.c
static void _simulator_job_complete(job_record_t *job_ptr, uid_t uid, bool requeue,
			 bool node_fail, uint32_t job_return_code) {
	node_record_t *node_ptr;
	time_t now = time(NULL);
	uint32_t job_comp_flag = 0;
	bool suspended = false;
	int i;
	int use_cloud = false;
	uint16_t over_time_limit;

	//xassert(verify_lock(JOB_LOCK, WRITE_LOCK));
	//xassert(verify_lock(FED_LOCK, READ_LOCK));

	//if (IS_JOB_FINISHED(job_ptr)) {
	//	if (job_ptr->exit_code == 0)
	//		job_ptr->exit_code = job_return_code;
	//	return ESLURM_ALREADY_DONE;
	//}

	//if (IS_JOB_COMPLETING(job_ptr))
	//	return SLURM_SUCCESS;	/* avoid replay */

    // TODO this could be set on higher debug mode, instead of comment
	//if ((job_return_code & 0xff) == SIG_OOM) {
	//	info("%s: %pJ OOM failure",  __func__, job_ptr);
	//} else if (WIFSIGNALED(job_return_code)) {
	//	info("%s: %pJ WTERMSIG %d",
	//			__func__, job_ptr, WTERMSIG(job_return_code));
	//} else if (WIFEXITED(job_return_code)) {
	//	info("%s: %pJ WEXITSTATUS %d",
	//			__func__, job_ptr, WEXITSTATUS(job_return_code));
	//}

	if (IS_JOB_RUNNING(job_ptr))
		job_comp_flag = JOB_COMPLETING;
	//else if (IS_JOB_PENDING(job_ptr)) {
	//	job_return_code = NO_VAL;
	//	fed_mgr_job_revoke_sibs(job_ptr);
	//}

	//if ((job_return_code == NO_VAL) &&
	//		(IS_JOB_RUNNING(job_ptr) || IS_JOB_PENDING(job_ptr))) {
	//	if (node_fail) {
	//		info("%s: %pJ cancelled by node failure",
	//				__func__, job_ptr);
	//	} else {
	//		info("%s: %pJ cancelled by interactive user",
	//				__func__, job_ptr);
	//	}
	//}

	//if (IS_JOB_SUSPENDED(job_ptr)) {
	//	uint32_t suspend_job_state = job_ptr->job_state;
	//	/*
	//	 * we can't have it as suspended when we call the
	//	 * accounting stuff.
	//	 */
	//	job_ptr->job_state = JOB_CANCELLED;
	//	jobacct_storage_g_job_suspend(acct_db_conn, job_ptr);
	//	job_ptr->job_state = suspend_job_state;
	//	job_comp_flag = JOB_COMPLETING;
	//	suspended = true;
	//}

	//if (job_comp_flag && (job_ptr->node_cnt == 0)) {
	//	/*
	//	 * Job has no resources left (used to expand another job).
	//	 * Avoid duplicate run of epilog and underflow in CPU count.
	//	 */
	//	job_comp_flag = 0;
	//}

	//	if (requeue && job_ptr->details && job_ptr->batch_flag) {
	//		/*
	//		 * We want this job to look like it was terminated in the
	//		 * accounting logs. Set a new submit time so the restarted
	//		 * job looks like a new job.
	//		 */
	//		job_ptr->end_time = now;
	//		job_ptr->job_state  = JOB_NODE_FAIL;
	//		job_completion_logger(job_ptr, true);
	//		/*
	//		 * Do this after the epilog complete.
	//		 * Setting it here is too early.
	//		 */
	//		//job_ptr->db_index = 0;
	//		//job_ptr->details->submit_time = now + 1;
	//		if (job_ptr->node_bitmap) {
	//			i = bit_ffs(job_ptr->node_bitmap);
	//			if (i >= 0) {
	//				node_ptr = node_record_table_ptr[i];
	//				if (IS_NODE_CLOUD(node_ptr))
	//					use_cloud = true;
	//			}
	//		}
	//		if (!use_cloud)
	//			job_ptr->batch_flag++;	/* only one retry */
	//		job_ptr->restart_cnt++;
	//
	//		/* clear signal sent flag on requeue */
	//		job_ptr->warn_flags &= ~WARN_SENT;
	//
	//		job_ptr->job_state = JOB_PENDING | job_comp_flag;
	//		/*
	//		 * Since the job completion logger removes the job submit
	//		 * information, we need to add it again.
	//		 */
	//		acct_policy_add_job_submit(job_ptr);
	//		if (node_fail) {
	//			info("%s: requeue %pJ due to node failure",
	//					__func__, job_ptr);
	//		} else {
	//			info("%s: requeue %pJ per user/system request",
	//					__func__, job_ptr);
	//		}
	//		/*
	//		 * We have reached the maximum number of requeue
	//		 * attempts hold the job with HoldMaxRequeue reason.
	//		 */
	//		if (job_ptr->batch_flag > MAX_BATCH_REQUEUE) {
	//			job_ptr->job_state |= JOB_REQUEUE_HOLD;
	//			job_ptr->state_reason = WAIT_MAX_REQUEUE;
	//			job_ptr->batch_flag = 1;
	//			debug("%s: Holding %pJ, repeated requeue failures",
	//					__func__, job_ptr);
	//			job_ptr->priority = 0;
	//		}
	//	} else if (IS_JOB_PENDING(job_ptr) && job_ptr->details &&
	//			job_ptr->batch_flag) {
	//		/*
	//		 * Possible failure mode with DOWN node and job requeue.
	//		 * The DOWN node might actually respond to the cancel and
	//		 * take us here.  Don't run job_completion_logger here since
	//		 * this is here to catch duplicate cancels from slowly
	//		 * responding slurmds
	//		 */
	//		return SLURM_SUCCESS;
	//	} else {
	//if (job_ptr->part_ptr &&
	//		(job_ptr->part_ptr->over_time_limit != NO_VAL16)) {
	//	over_time_limit = job_ptr->part_ptr->over_time_limit;
	//} else {
	//	over_time_limit = slurm_conf.over_time_limit;
	//}

	//if (node_fail) {
	//	job_ptr->job_state = JOB_NODE_FAIL | job_comp_flag;
	//	job_ptr->requid = uid;
	//} else if (job_return_code == NO_VAL) {
	//	job_ptr->job_state = JOB_CANCELLED | job_comp_flag;
	//	job_ptr->requid = uid;
	//} else if ((job_return_code & 0xff) == SIG_OOM) {
	//	job_ptr->job_state = JOB_OOM | job_comp_flag;
	//	job_ptr->exit_code = job_return_code;
	//	job_ptr->state_reason = FAIL_OOM;
	//	xfree(job_ptr->state_desc);
	//} else if (WIFEXITED(job_return_code) &&
	//		WEXITSTATUS(job_return_code)) {
	//	job_ptr->job_state = JOB_FAILED   | job_comp_flag;
	//	job_ptr->exit_code = job_return_code;
	//	job_ptr->state_reason = FAIL_EXIT_CODE;
	//	xfree(job_ptr->state_desc);
	//} else if (WIFSIGNALED(job_return_code)) {
	//	job_ptr->job_state = JOB_FAILED | job_comp_flag;
	//	job_ptr->exit_code = job_return_code;
	//	job_ptr->state_reason = FAIL_LAUNCH;
	//} else if (job_comp_flag
	//		&& ((job_ptr->end_time
	//				+ over_time_limit * 60) < now)) {
	//	/*
	//	 * Test if the job has finished before its allowed
	//	 * over time has expired.
	//	 */
	//	job_ptr->job_state = JOB_TIMEOUT  | job_comp_flag;
	//	job_ptr->state_reason = FAIL_TIMEOUT;
	//	xfree(job_ptr->state_desc);
	//} else {
	job_ptr->job_state = JOB_COMPLETE | job_comp_flag;
	job_ptr->exit_code = job_return_code;
	//if (nonstop_ops.job_fini)
		//(nonstop_ops.job_fini)(job_ptr);
	//}

	//if (suspended) {
	//	job_ptr->end_time = job_ptr->suspend_time;
	//	job_ptr->tot_sus_time +=
	//		difftime(now, job_ptr->suspend_time);
	//} else
	job_ptr->end_time = now;
	job_completion_logger(job_ptr, false);
	//}

	last_job_update = now;
	job_ptr->time_last_active = now;   /* Timer for resending kill RPC */
	//if (job_comp_flag) {	/* job was running */
	//	build_cg_bitmap(job_ptr);
	//	deallocate_nodes(job_ptr, false, suspended, false);
	//}

	// these calls replace the call to deallocate nodes above
	if (select_g_job_fini(job_ptr) != SLURM_SUCCESS)
		error("select_g_job_fini(%pJ): %m", job_ptr);

	if (!job_ptr->node_bitmap_cg) build_cg_bitmap(job_ptr);

	int i_first = bit_ffs(job_ptr->node_bitmap_cg);
	int i_last;
	if (i_first >= 0) i_last = bit_fls(job_ptr->node_bitmap_cg);
	else i_last = i_first - 1;

	for (i = i_first; i <= i_last; i++) {
		if (!bit_test(job_ptr->node_bitmap_cg, i)) continue;

		node_ptr = node_record_table_ptr[i];

		// original call makes it completing
		// we do not need this, just make it available inmediately
		// TODO a more detailed simulator in the future, could include a
		// model for node completion overheads here
		// make_node_comp(node_ptr, job_ptr, suspended);
		make_node_avail(node_ptr);
	}

	info("%pJ done", job_ptr);
}

static void _simulate_job_arrivals(time_t now, FILE *swf_file, char **current_line, int *end_reached){
	uint32_t submit_uid = -1;
	char **err_msg = NULL;
	job_record_t *job_ptr = NULL;
	int protocol_version = 0;
	int error_code;
    job_desc_msg_t *job_desc_msg;
    int fifo_scheduled_now;

    // how many created at time now
    int created_now = 0;

	while(job_desc_msg = _simulator_read_next_job_desc(now, swf_file,
                current_line, end_reached)){
		// this is the message that is usually provided by sbatch
		// in the simulator case, we need to construct it ourselves
        // from the SWF file in _simulator_read_next_job_desc

        // Note: more than 1 job may exist in the same submission second, therefore
        // we use a loop here, instead of a condition

		/*
		 * job_allocate - create job_records for the supplied job specification and
		 *	allocate nodes for it.
		 * IN job_specs - job specifications
		 * IN immediate - if set then either initiate the job immediately or fail
		 * IN will_run - don't initiate the job if set, just test if it could run
		 *	now or later
		 * OUT resp - will run response (includes start location, time, etc.)
		 * IN allocate - resource allocation request only if set, batch job if zero
		 * IN submit_uid -uid of user issuing the request
		 * OUT job_pptr - set to pointer to job record
		 * OUT err_msg - Custom error message to the user, caller to xfree results
		 * IN protocol_version - version of the code the caller is using
		 * RET 0 or an error code. If the job would only be able to execute with
		 *	some change in partition configuration then
		 *	ESLURM_REQUESTED_PART_CONFIG_UNAVAILABLE is returned
		 * globals: job_list - pointer to global job list
		 *	list_part - global list of partition info
		 *	default_part_loc - pointer to default partition
		 */
        error_code = job_allocate(
                job_desc_msg,
                job_desc_msg->immediate, // always false here
                false, // always false here; the scheduler determines this
                NULL,  // will run resp (a message?)
                0, // allocation only if set true (srun?) otherwise batch job
                submit_uid, // user id; we don't track different users
                false, // not documented
                &job_ptr, // the created job record
                err_msg, // error for the user; we ignore this
                protocol_version); // protocol to communicate with; ignored
        if(error_code) {
            error("could not add job to queue");
            exit(-1);
        }

        created_now++;
        xfree(job_desc_msg);

        // TODO do we care about these extra times?
        // check src/slurmctld/slurmctld.h for these data structures
        //job_ptr->end_time = -1; // actual end, set at time of completion
        //job_ptr->end_time_exp = -1; // expected end (by the scheduler?)
        //job_ptr->last_sched_eval = -1; // last time considered by the scheduler
        //job_ptr->resize_time = -1; // last time it got "resized"
    }

    if(created_now){
        debug("%d jobs were created at time %d; triggering an immediate FIFO pass",
                created_now, now);
        fifo_pass_immediate(now);
    }
}

static void _simulate_job_completions(time_t now){
	ListIterator job_iterator;
	job_record_t *job_ptr = NULL;

	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = list_next(job_iterator))) {
		if(IS_JOB_RUNNING(job_ptr)){
			if((job_ptr->time_limit*60) < (now - job_ptr->start_time)){
				debug("job %d completed; start: %d, now: %d, limit: %ds(%dm)",
						job_ptr->job_id, job_ptr->start_time,
						now, job_ptr->time_limit*60,
						job_ptr->time_limit);
				_simulator_job_complete(job_ptr, 1000, false, false, 0);
			}
		}
	}
}

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

void *malleable_scheduler_simulator_thread(){
	time_t simulator_start = time(0); // simulator real starting time
	int minutes_run = 0;
	int hours_run = 0;
	int days_run = 0;
	char working_dir[2048];
	DIR *directory_pointer;
	struct dirent *entry;
    FILE *swf_file = NULL;
    char *swf_file_name = NULL;
    FILE *plot_file = NULL;
    char *plot_file_name = NULL;
    char *plot_file_name_extension = NULL;
    char *swf_current_line = NULL;
	uint32_t simulator_now;
    int end_reached; // end of SWF input reached

    // SWF header variables, required
    int max_nodes = -1;   // SWF MaxNodes parameter
    int max_jobs = -1;    // SWF MaxJobs parameter
    int max_records = -1; // SWF MaxRecords parameter
    int max_procs = -1;   // SWF MaxProcs parameter
	int max_runtime = -1; // SWF MaxRuntime parameter

    // Malleability options not part of SWF, required
	int mall_prob = -1; // probability of a malleable job arrival
	int mall_scal = -1; // scalability potential of malleable jobs

    // malleable timing
	time_t window_end; // time_t values are in seconds
	int malleable_window = 60*60*24; // 1 day

    // metric computation variables
    bitstr_t *free_nodes;
    int free_nodes_now;
	uint32_t metric_capture_start;
	uint32_t metric_capture_end;
    double utilization_now;
    double utilization_minimum = 1.0f;
    double utilization_maximum = 0.0f;
    double utilization_accumulated = 0.0f;
    double utilization_accumulated_squares = 0.0f;

	info("Malleable Scheduler Simulator STARTING");
	if (getcwd(working_dir, sizeof(working_dir)) != NULL) {
		info("running under: %s", working_dir);
	} else {
		error("could not determine current working directory");
		exit(-1);
	}

	directory_pointer = opendir("./");
	if (directory_pointer != NULL) {
		while ((entry = readdir (directory_pointer)) != NULL){
			if(string_ends_with(entry->d_name, ".swf")){
				//info("found SWF file: %s", entry->d_name);
                swf_file_name = xstrdup(entry->d_name);
                swf_file = fopen(entry->d_name, "r");
			}
		}
		closedir(directory_pointer);
	} else { error("opendir failed"); }

    if(swf_file == NULL){
        error("this simulator requires an .swf file to exist in the working directory");
        exit(-1);
    }

    _retrieve_simulation_parameters(swf_file, &max_runtime,
            &max_nodes, &max_jobs, &max_records,
            &mall_prob, &mall_scal, &swf_current_line);
    if (max_nodes != node_record_count){
        error("SWF MaxNodes (%d) does not match node count(%d)",
                max_nodes, node_record_count);
        error("update slurm.conf to match SWF MaxNodes");
        exit(-1);
    }

    plot_file_name = malloc(strlen(swf_file_name) + 128);
    plot_file_name_extension = malloc(128);
    strcpy(plot_file_name, swf_file_name);
    // output file name based on factor and probability
    sprintf(plot_file_name_extension, ".%d.%d.out", mall_scal, mall_prob);
    strcat(plot_file_name, plot_file_name_extension);
    plot_file = fopen(plot_file_name, "w");

    info("Simulation parameters:");
    info("SWF file: %s",   swf_file_name);
    info("plot file: %s",  plot_file_name);
    info("MaxRuntime: %d", max_runtime);
    info("MaxNodes: %d",   max_nodes);
    info("MaxJobs: %d",    max_jobs);
    info("MaxRecords: %d", max_records);
    info("MallProb: %d",   mall_prob);
    info("MallScal: %d",   mall_scal);
	info("Slurm scheduler parameters: %s", slurm_conf.sched_params);

    if( //max_runtime == -1 || we don't require MaxRuntime
            max_nodes == -1 ||
            max_jobs == -1 ||
            max_records == -1 ){
        error("missing required parameters in SWF header");
        exit(-1);
    }

    if( mall_prob == -1 || mall_scal == -1){
        error("MallProb or MallScal not set; Malleability support disabled");
    }

    // time 0 is 1970 1 1 00:00:00
    simulator_now = 0; // simulation starts at t=0

	_initialize_nodes(simulator_now);
	backfill_init();
    srand(0); // we prefer a deterministic sequence

    // immediate fifo pass before starting the time loop
    // this is necessary to allow the scheduler to configure
    // itself before we start the simulation
    fifo_pass_immediate(simulator_now);

    end_reached = 0;
    metric_capture_end = 0;
	while(1){
		debug("####################################################");
		debug("### performing a simulator pass at second: %d ", simulator_now);

        // we perform completions first, to release nodes and allow
        // new job arrivals to start in the same step
        // since we simulate at second resolution, the penalty is not
        // high regardless of this order
		_simulate_job_completions(simulator_now);
		_simulate_job_arrivals(simulator_now, swf_file, &swf_current_line, &end_reached);

        if(simulator_now % 60){
            _print_running_and_pending_jobs();
        }

		fifo_pass(simulator_now);
		backfill_pass(simulator_now);

		// this is the malleable pass
        // note that the malleable pass operates at a second rate
        // this is because it is very fast, and therefore we don't
        // need to compensate for its added latency, like in the case
        // of the FIFO or backfill passes

        window_end = simulator_now + malleable_window;
        List malleable_jobs = list_create(NULL);
        free_nodes = bit_copy(avail_node_bitmap);
        _simulate_scan_running_jobs(&malleable_jobs, &free_nodes, simulator_now, mall_prob);
        if(bit_set_count(free_nodes) > 0 && list_count(malleable_jobs) > 0 ){
            _init_node_time_gap(&node_time, simulator_now, window_end, free_nodes);
            _build_node_time_gap(&node_time, window_end, free_nodes);
            // parameter: malleability enabled
            // comment here to disable malleability,
            // or set malleable job probability to 0%
            _simulate_malleable_updates(node_time, malleable_jobs, simulator_now, &free_nodes, mall_scal);
        }
		list_destroy(malleable_jobs);

        // end of the malleable pass

		simulator_now++; // advance 1 second of simulation
		minutes_run = simulator_now/60;
		hours_run = simulator_now/60/60;
		days_run = simulator_now/60/60/24;
		debug("### simulated time: d: %d, h: %d; m: %d: s: %d;",
				days_run, hours_run, minutes_run, simulator_now);

        // update metrics
        free_nodes_now = bit_set_count(free_nodes);
        bit_free(free_nodes);
        utilization_now = (double)(node_record_count - free_nodes_now)/node_record_count;

        fprintf(plot_file, "%d %2.2f\n",
                simulator_now,
                utilization_now * 100);

        if(utilization_now < utilization_minimum) utilization_minimum = utilization_now;
        if(utilization_now > utilization_maximum) utilization_maximum = utilization_now;
        utilization_accumulated += utilization_now;
        utilization_accumulated_squares += utilization_now * utilization_now;
        if(utilization_accumulated_squares > (DBL_MAX/2)){
            error("utilization_accumulated_squares is about to overflow");
            exit(-1);
        }

        if(end_reached && utilization_now < 0.0001f){
            // we finish when the SWF file has no more records
            // and the nodes are drained (0% utilization now)

            double utilization_mean =
                (double)(utilization_accumulated / (double) simulator_now);
            double utilization_variance =
                (double)((utilization_accumulated_squares /(double) simulator_now)
                    - (utilization_mean * utilization_mean));

            info("SWF input: %s", swf_file_name);
            info("simulated %d seconds in %d real seconds",
                    simulator_now, time(0)-simulator_start);
            info("utilization minimum: %2.2f %%", utilization_minimum * 100);
            info("utilization maximum: %2.2f %%", utilization_maximum * 100);
            info("utilization accumulated: %f", utilization_accumulated);
            info("utilization accumulated squares: %f", utilization_accumulated_squares);
            info("utilization mean: %2.2f %%", utilization_mean * 100);
            info("utilization variance: %2.2f %%", utilization_variance * 100);

            // TODO unfortunately, these stats are not accurately tracked
            // in the original Slurm implementation; commented for now
            //info("backfilled: %d", slurmctld_diag_stats.backfilled_jobs);

            backfill_fini();
			exit(0);
		}
	}
}

static void *_server_handler(dynpm_server_to_client_connection_t *connection, const void *read_buffer){
	//TODO add a mutex to control malleable pm registrations
	//pthread_mutex_lock(&malleable_job_list_mutex);
	info("================================================================================");

	dynpm_header_t *header;
	dynpm_deserialize_header(&header, &read_buffer);
	info("header->source_type=%d", header->source_type);
	info("header->message_type=%d", header->message_type);

	if(header->message_type == (dynpm_message_type)REGISTER_PROCESS_MANAGER){
		info("received REGISTER_PROCESS_MANAGER");
	} else if(header->message_type == (dynpm_message_type)DEREGISTER_PROCESS_MANAGER){
		info("received DEREGISTER_PROCESS_MANAGER");
	} else {
		error("message not supported in _server_handler\n%s",
				(char*)read_buffer);
	}

	dynpm_free_header(&header);

	info("================================================================================");
	//pthread_mutex_unlock(&malleable_job_list_mutex);

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

	deepsea_process_conf_entry_t *config_entry;

	if(deepsea_conf_get_process_config(&config_entry, "slurmctld")){
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

	/*
	 * On each malleable scheduler pass:
	 * 1.- Scan running jobs for candidates and free nodes.
	 * 2.- Build an node-time gap data structure.
	 * 3.- Rank the malleable jobs based on quality metrics.
	 * 4.- Split the list in expanding and shrinking candidates.
	 * 5.- Setup offers/requests and book keeping metadata.
	 * 6.- Send a batch of resource offers/requests.
	 *
	 * The information protocol is separate from this algorithm.
	 */
	while(1) {
		lock_slurmctld(job_write_lock);

		List malleable_jobs = list_create(NULL);

		free_nodes = bit_copy(avail_node_bitmap);
		_scan_running_jobs(&malleable_jobs, &free_nodes);

		// give up if nodes available are already zero
		if(bit_set_count(free_nodes) > 0 // at least one free node
				&& list_count(malleable_jobs) > 0 // at least one malleable job
		  ){
            info("free nodes: %d; malleable jobs: %d",
                    bit_set_count(free_nodes), list_count(malleable_jobs));
			_print_bitfield("free nodes: ", free_nodes);

			// TODO check if we need to adjust to time resolution (see backfill.c)
			now = time(NULL);
			window_end = now + malleable_window;

			pthread_mutex_lock(&node_time_mutex);

			_init_node_time_gap(&node_time, now, window_end, free_nodes);
			_build_node_time_gap(&node_time, window_end, free_nodes);

			_generate_offers(node_time, malleable_jobs, now);

			pthread_mutex_unlock(&node_time_mutex);
		} else {
			dynpm_lock_client_connections();

			dynpm_print_client_connections();
			_print_bitfield("free nodes: ", free_nodes);

			// if there are no malleable jobs running, then all
			// connection data is stale from missing dereg messages
			if(list_count(malleable_jobs) == 0) dynpm_clear_client_connections();

			dynpm_unlock_client_connections();
		}

		list_destroy(malleable_jobs);
        bit_free(free_nodes);

		// TODO confirm we don't need a thread yield strategy under high loads
		// if this value is lower than the backfiller period, then malleable applications
		// get resources first, if it's higher, small applications are backfilled first

		unlock_slurmctld(job_write_lock);

		// TODO make this configurable, or reuse a bf_* setting
		sleep(10);
	}

}

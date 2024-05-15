/*
 * Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *
 * Additional copyrights may follow
 *
 */

#include "mrun.h"

mrun_tbon_data_t slurmd_tbon_data;
mrun_tbon_data_t slurmstepd_tbon_data;
mrun_step_data_t step_data;
pthread_mutex_t mrun_io_out_mutex = PTHREAD_MUTEX_INITIALIZER;

// from our copy of srun.c; was static in original
extern srun_job_t *job;
// used to be a local variable insider srun() from srun.c
// now we make it a global in our copy
extern List srun_job_list;

// type from:     slurm/slurm.h
// instance from: src/common/read_config.c
extern slurm_conf_t slurm_conf;

char const plugin_type[] = "launch";
extern int srun(int argc, char **argv);
static pthread_mutex_t mrun_full_mutex   = PTHREAD_MUTEX_INITIALIZER;

void _test_serialization_deserialization(){

	void *buffer = malloc(sizeof(char) * DYNPM_MAX_MESSAGE_SIZE);

	info("tesing register ser/deser");
	// if the tbon is ready, we can register our app with the scheduler
	info("allocating header");
	dynpm_header_t *register_pm_header = malloc(sizeof(dynpm_header_t));
	register_pm_header->source_type =  (dynpm_source_type)LAUNCHER;
	register_pm_header->message_type = (dynpm_message_type)REGISTER_PROCESS_MANAGER;

	info("allocating message");
	dynpm_register_process_manager_msg_t *register_pm =
		malloc(sizeof(dynpm_register_process_manager_msg_t));
	register_pm->pid = getpid();
	char hostname[128];
	if(gethostname(hostname, 128)) { error("error getting hostname"); }
	register_pm->hostname = strdup(hostname);
	register_pm->job_index = 0;
	register_pm->application_count = 1;
	info("allocating %d applications", register_pm->application_count);
	register_pm->applications =
		malloc(register_pm->application_count * sizeof(dynpm_application_t*));
	info("iterating %d applications", register_pm->application_count);
	for(int app = 0; app < register_pm->application_count; app++){
		info("at application %d", app);
		register_pm->applications[app] = malloc(sizeof(dynpm_application_t));
		register_pm->applications[app]->application_index = 0;
		register_pm->applications[app]->malleability_mode = DYNPM_PASSIVE_FLAG;
		// step_resp may not be available
		//register_pm->applications[app]->node_count = job->step_ctx->step_resp->step_layout->node_cnt;
		register_pm->applications[app]->node_count = 1;
		// step_resp may not be available
		//register_pm->applications[app]->nodes = strdup(job->step_ctx->step_resp->step_layout->node_list);
		register_pm->applications[app]->nodes = strdup("fake_host01");
	}

	info("serializing");
	dynpm_serialize_register_process_manager_msg(register_pm_header, register_pm, &buffer);
	info("freeing");
	dynpm_free_register_process_manager_msg(&register_pm);
	info("deserialize");
	dynpm_deserialize_register_process_manager_msg(&register_pm, (const void**)&buffer);
	info("free again");
	dynpm_free_register_process_manager_msg(&register_pm);
	info("free header");
	dynpm_free_header(&register_pm_header);
	info("done tesing register ser/deser");

	// testing deregister
	info("tesing deregister ser/deser");
	dynpm_header_t *deregister_pm_header = malloc(sizeof(dynpm_header_t));
	deregister_pm_header->source_type =  (dynpm_source_type)LAUNCHER;
	deregister_pm_header->message_type = (dynpm_message_type)DEREGISTER_PROCESS_MANAGER;

	dynpm_deregister_process_manager_msg_t *deregister_pm =
		malloc(sizeof(dynpm_deregister_process_manager_msg_t));
	deregister_pm->pid = getpid();
	deregister_pm->hostname = strdup(hostname);
	deregister_pm->error_code = 0;

	dynpm_serialize_deregister_process_manager_msg(deregister_pm_header, deregister_pm, &buffer);
	dynpm_free_deregister_process_manager_msg(&deregister_pm);
	dynpm_deserialize_deregister_process_manager_msg(&deregister_pm, (const void**)&buffer);
	dynpm_free_deregister_process_manager_msg(&deregister_pm);
	dynpm_free_header(&deregister_pm_header);
	info("done tesing deregister ser/deser");

	// allocation request
	info("tesing allocation request ser/deser");
	dynpm_header_t *alloc_req_header = malloc(sizeof(dynpm_header_t));
	alloc_req_header->source_type =  (dynpm_source_type)LAUNCHER;
	alloc_req_header->message_type = (dynpm_message_type)ALLOCATION_REQUEST;

	dynpm_allocation_request_msg_t *allocation_req =
		malloc(sizeof(dynpm_allocation_request_msg_t));
	allocation_req->application_index = 0;
	allocation_req->partition_index = 0;
	allocation_req->type = EXPAND;
	allocation_req->node_count = 1;

	dynpm_serialize_allocation_request_msg(alloc_req_header, allocation_req, &buffer);
	dynpm_free_allocation_request_msg(&allocation_req);
	dynpm_deserialize_allocation_request_msg(&allocation_req, (const void**)&buffer);
	dynpm_free_allocation_request_msg(&allocation_req);
	dynpm_free_header(&alloc_req_header);
	info("done tesing allocation request ser/deser");

	// reallocation start
	info("tesing reallocation start ser/deser");
	dynpm_header_t *realloc_start_header = malloc(sizeof(dynpm_header_t));
	realloc_start_header->source_type =  (dynpm_source_type)LAUNCHER;
	realloc_start_header->message_type = (dynpm_message_type)REALLOCATION_START;

	dynpm_reallocation_start_msg_t *realloc_start_msg =
		malloc(sizeof(dynpm_reallocation_start_msg_t));
	realloc_start_msg->application_index = 0;
	realloc_start_msg->partition_index = 0;
	realloc_start_msg->type = EXPAND;
	realloc_start_msg->current_node_count = 1;
	realloc_start_msg->current_nodes = strdup("fake_host01");
	realloc_start_msg->delta_node_count = 1;
	realloc_start_msg->delta_nodes = strdup("fake_host01");
	realloc_start_msg->updated_node_count = 1;
	realloc_start_msg->updated_nodes = strdup("fake_host01");
	// step_resp may not be available yet
	//realloc_start_msg->nodes = strdup(job->step_ctx->step_resp->step_layout->node_list);

	dynpm_serialize_reallocation_start_msg(realloc_start_header, realloc_start_msg, &buffer);
	dynpm_free_reallocation_start_msg(&realloc_start_msg);
	dynpm_deserialize_reallocation_start_msg(&realloc_start_msg, (const void**)&buffer);
	dynpm_free_reallocation_start_msg(&realloc_start_msg);
	dynpm_free_header(&realloc_start_header);
	info("done tesing reallocation start ser/deser");

	// reallocation complete
	info("tesing reallocation complete ser/deser");
	dynpm_header_t *realloc_complete_header = malloc(sizeof(dynpm_header_t));
	realloc_complete_header->source_type =  (dynpm_source_type)LAUNCHER;
	realloc_complete_header->message_type = (dynpm_message_type)REALLOCATION_COMPLETE;

	dynpm_reallocation_complete_msg_t *realloc_complete_msg =
		malloc(sizeof(dynpm_reallocation_complete_msg_t));
	realloc_complete_msg->application_index = 0;
	realloc_complete_msg->partition_index = 0;
	realloc_complete_msg->type = EXPAND;
	realloc_complete_msg->node_count = 1;
	// step_resp may not be available
	//realloc_complete_msg->nodes = strdup(job->step_ctx->step_resp->step_layout->node_list);
	realloc_complete_msg->nodes = strdup("fake_host01");
	realloc_complete_msg->status_code = 0;

	dynpm_serialize_reallocation_complete_msg(realloc_complete_header, realloc_complete_msg, &buffer);
	dynpm_free_reallocation_complete_msg(&realloc_complete_msg);
	dynpm_deserialize_reallocation_complete_msg(&realloc_complete_msg, (const void**)&buffer);
	dynpm_free_reallocation_complete_msg(&realloc_complete_msg);
	dynpm_free_header(&realloc_complete_header);
	info("done tesing reallocation complete ser/deser");

	free(buffer);
}

static void _print_pmr(dynpm_register_process_manager_msg_t *registration){
}

void _dstepd_client_handler(void *conn, const void *read_buffer){
	pthread_mutex_lock(&mrun_full_mutex);

	dynpm_client_to_server_connection_t *connection = (dynpm_client_to_server_connection_t *)conn;

	dynpm_header_t *header;
	dynpm_deserialize_header(&header, (const void**)&read_buffer);
	char output_buffer[4096];

	//info("================================================================================");
	//info("received header: source: %d, message type: %d", header->source_type, header->message_type);
	//info("bytes: %s", (char*)read_buffer);

	if(header->source_type == PROCESS_MANAGER){
		if(header->message_type == READY_TBON){
			info("READY_TBON from PROCESS_MANAGER/DSTEPD master node");

			assert(slurmd_tbon_data.started == 1);

			// the tbon is now ready to broadcast
			pthread_mutex_lock(&(slurmstepd_tbon_data.mutex));
			slurmstepd_tbon_data.ready = 1;
			pthread_cond_signal(&(slurmstepd_tbon_data.cond));
			pthread_mutex_unlock(&(slurmstepd_tbon_data.mutex));

			if(dynpm_send_header_to_server_connection((dynpm_source_type)LAUNCHER,
						(dynpm_message_type)READY_TBON_ACK, connection)){
				error("failed to send READY_TBON_ACK from LAUNCHER");
			}
		} else if(header->message_type == DYNAMIC_IO_OUT){
			dynpm_dynamic_io_out_msg_t *dynamic_io_out;
			dynpm_deserialize_dynamic_io_out_msg(&dynamic_io_out, (const void**)&read_buffer);
			//info("DYNAMIC_IO_OUT from host %s (%d bytes; id %d)",
			//		dynamic_io_out->host, dynamic_io_out->payload_bytes, dynamic_io_out->id);

			// TODO this does not seem to help with the artifacting
			pthread_mutex_lock(&mrun_io_out_mutex);
			memcpy(output_buffer, dynamic_io_out->payload, dynamic_io_out->payload_bytes);
			for(int i = 0; i < dynamic_io_out->payload_bytes; i++){
				printf("%c", output_buffer[i]);
			}
			fflush(stdout);
			pthread_mutex_unlock(&mrun_io_out_mutex);

			if(dynpm_send_header_to_server_connection((dynpm_source_type)LAUNCHER,
						(dynpm_message_type)DYNAMIC_IO_OUT_ACK, connection)){
				error("failed to send DYNAMIC_IO_OUT_ACK from LAUNCHER");
			}
			// TODO need to free the IO message; done at the caller?
		} else if(header->message_type == PROCESSES_DONE){
			info("master node indicates PROCESSES_DONE");

			if(dynpm_send_header_to_server_connection((dynpm_source_type)LAUNCHER,
						(dynpm_message_type)PROCESSES_DONE_ACK, connection)){
				error("failed to send PROCESSES_DONE_ACK from LAUNCHER");
			}
			info("wrote to master node PROCESSES_DONE_ACK");
			// TODO need to match expected protocol with the scheduler
			sleep(1);
			// TODO instead of exiting here, generate a cond signal to the main() thread
			exit(0);

		} else if(header->message_type == REPORT_PROCESS_DATA){
			// TODO add logic here, to limit polling to one outstanding one
			// regardless of the polling rate; this should limit the polling rate to
			// what the process manager can handle
			info("received REPORT_PROCESS_DATA from master node");
			dynpm_report_process_data_msg_t *report;
			dynpm_deserialize_report_process_data_msg(&report,
					(const void**)&read_buffer);
			info("report->malleability_mode: %d", report->malleability_mode);
			// if the malleability mode has changed, we need to update the registration
			// we also need to set the registration if we have not yet
			// registered with the scheduler

			pthread_mutex_lock(&(step_data.mutex));

			// TODO a process report will include more than just the registration
			// in the future; rework handling here accordingly when ready
			// e.g. we may receive PM specific monitor data as the process report

			// perform the registration when the malleability mode changes, or
			// if we have not registered yet.  Registration requires the first
			// registration message and the matching ack from the scheduler.
			if(step_data.malleability_mode != report->malleability_mode
					|| !step_data.registered){

				step_data.malleability_mode = report->malleability_mode;

				info("updated step_data's malleability mode to: %d",
						step_data.malleability_mode);

				if(step_data.malleability_mode){
					info("setting or updating the registration...");

					dynpm_header_t *registration_header = malloc(sizeof(dynpm_header_t));
					registration_header->source_type =  (dynpm_source_type)LAUNCHER;
					registration_header->message_type = (dynpm_message_type)REGISTER_PROCESS_MANAGER;

					// TODO store the hostname during init in mrun
					char hostname[128];
					if(gethostname(hostname, 128)) { error("error getting hostname"); }

					dynpm_register_process_manager_msg_t *registration =
						malloc(sizeof(dynpm_register_process_manager_msg_t));

					// TODO store the pid during init in mrun
					registration->pid = getpid();
					registration->hostname = strdup(hostname);
					registration->job_index = step_data.job_index;
					// TODO update this for multiple steps, application_count > 1
					registration->application_count = 1;
					registration->applications = malloc(sizeof(dynpm_application_t*));

					// TODO update this for multiple steps (hetjobs) along with step_data
					*(registration->applications) = malloc(sizeof(dynpm_application_t));
					registration->applications[0]->application_index = step_data.application_index;
					registration->applications[0]->malleability_mode = step_data.malleability_mode;
					registration->applications[0]->node_count = step_data.node_count;
					registration->applications[0]->nodes = strdup(step_data.nodes);

					// send the process manager registration to the scheduler
					//
					// this provides the scheduler with the knowledge of which applications
					// managed by this process manager have malleability support, and which
					// resources (nodes) of the job it is using; this is necessary since
					// some other applications may be running on the job, and may not be
					// necessarily malleable or even mpi/parallel/distributed
					// TODO we may need to keep a scheduler tracker, instead of asuming index 0
					if(dynpm_send_message_to_server(&registration_header, (void**)&registration,
								0)){ // the scheduler is always at 0
						error("error sending process manager registration to the scheduler");
					} else {
						info("sent process manager registration to the scheduler");
						_print_pmr(registration);
					}

				} else {
					info("disabling malleability for this step");
					// TODO send a process manager deregistration message here
					// this way we free the scheduler from tracking this process manager
					// and its applications
				}
			} else { // malleability is up to date and already registered
				info("registered and malleability mode is up to date");
			}

			pthread_mutex_unlock(&(step_data.mutex));

			dynpm_free_report_process_data_msg(&report);
		} else {
			error("unknown or unsupported message type stepd tbon child listener");
		}
	} else {
		error("received tbon child message from a process other than PROCESS_MANAGER/SLURMSTEPD");
	}
	//info("================================================================================");

	dynpm_free_header(&header);
	pthread_mutex_unlock(&mrun_full_mutex);
}

void _slurmd_client_handler(void *conn, const void *read_buffer){
	pthread_mutex_lock(&mrun_full_mutex);
	dynpm_client_to_server_connection_t *connection = (dynpm_client_to_server_connection_t *)conn;

	dynpm_header_t *header;
	dynpm_deserialize_header(&header, (const void**)&read_buffer);

	info("================================================================================");
	info("received header: source: %d, message type: %d", header->source_type, header->message_type);
	info("bytes: %s", (char*)read_buffer);

	if(header->source_type == NODE_MANAGER){
		info("from: NODE_MANAGER/SLURMD");
		if(header->message_type == READY_TBON){
			info("READY_TBON from master node");
			assert(slurmd_tbon_data.started == 1);

			pthread_mutex_lock(&(slurmd_tbon_data.mutex));
			slurmd_tbon_data.ready = 1;
			pthread_cond_signal(&(slurmd_tbon_data.cond));
			pthread_mutex_unlock(&(slurmd_tbon_data.mutex));

		} else if(header->message_type == READY_DYNAMIC_PM){
			info("READY_DYNAMIC_PM from master node");

			pthread_mutex_lock(&(step_data.mutex));
			step_data.ready = 1;
			pthread_cond_signal(&(step_data.cond));
			pthread_mutex_unlock(&(step_data.mutex));

		} else {
			error("unknown or unsupported message type tbon child listener");
		}
	} else {
		error("received tbon child message from a process other than NODE_MANAGER/SLURMD");
	}

	info("================================================================================");
	dynpm_free_header(&header);
	pthread_mutex_unlock(&mrun_full_mutex);
}

void _scheduler_client_handler(void *conn, const void *read_buffer) {
	pthread_mutex_lock(&mrun_full_mutex);

	dynpm_client_to_server_connection_t *connection = (dynpm_client_to_server_connection_t *)conn;

	dynpm_header_t *header;
	dynpm_deserialize_header(&header, (const void**)&read_buffer);

	info("================================================================================");
	info("received header: source: %d, message type: %d", header->source_type, header->message_type);
	info("bytes: %s", (char*)read_buffer);

	if(header->source_type == SCHEDULER){
		info("from: SCHEDULER/SLURMCTLD");
		if(header->message_type == OFFER_RESOURCES){

			// the tbon is only started once we know our srun step is
			// up and running and tracket at the scheduler; an offer can only arrive
			// after it is so
			info("================================ begin state ===================================");
			if(srun_job_list){
				info("running a hetjob");
				// TODO do hetjob multicast; instead of job below, must be an entry from the job list
				//pthread_mutex_lock(&(job->state_mutex));
				//pthread_mutex_unlock(&(job->state_mutex));
			//} else if (job->step_ctx->step_resp->job_id > 0){
			// in 22.05 the job id is in the context and not on the resp
			} else if (job->step_ctx->job_id > 0){
				pthread_mutex_lock(&(job->state_mutex));
				info("running a regular launch");
				info("nodes: %d ", job->step_ctx->step_resp->step_layout->node_cnt);
				info("%s ", job->step_ctx->step_resp->step_layout->node_list);
				//char *name = NULL;
				//hostlist_t *hl = hostlist_create(job->step_ctx->step_resp->step_layout->node_list);
				//int i = 0;
				//while ((name = hostlist_shift(hl))) {
				//	info("%s ", name);
				//	free(name);
				//}
				//hostlist_destroy(hl);
				//info("cred time: %s", ctime(&(job->step_ctx->step_resp->cred->ctime)));
				info("total tasks: %d", job->step_ctx->step_resp->step_layout->task_cnt);
				info("task layout:");
				int node_index;
				for(node_index = 0;
						node_index < job->step_ctx->step_resp->step_layout->node_cnt;
						node_index++){
					info("node index: %d; tasks: %d", node_index,
							job->step_ctx->step_resp->step_layout->tasks[node_index]);
				}
				info("================================== end state ===================================");
				pthread_mutex_unlock(&(job->state_mutex));
			} else {
				error("step context still not available (waiting for scheduler)");
			}


			info("================================= begin offer ==================================");
			//dynpm_resource_offer_msg_t *offer = malloc(sizeof(dynpm_resource_offer_msg_t));
			dynpm_resource_offer_msg_t *offer = NULL;
			dynpm_deserialize_resource_offer_msg(&offer, (const void**)&read_buffer);
			info("resource offer: app_idx: %d; prt_idx: %d; nodes: %d;",
					offer->application_index,
					offer->partition_index,
					offer->node_count);
			int node_count = offer->node_count;
			info("created: %d:%s", offer->creation_time, ctime(&(offer->creation_time)));
			info("ends: %d:%s", offer->end_time, ctime(&(offer->end_time)));

			pthread_mutex_lock(&slurmstepd_tbon_data.mutex);

			info("==================================  OFFER BCAST  ===================================");
			if (!slurmstepd_tbon_data.started){
				error("stepd tbon is not available; cannot send offer");
			} else {
				dynpm_header_t *offer_header = malloc(sizeof(dynpm_header_t));
				offer_header->source_type =  (dynpm_source_type)LAUNCHER;
				offer_header->message_type = (dynpm_message_type)OFFER_RESOURCES;

				// single message to the node master; it should broadcast over the tbon from there
				// dynpm_send_message_to_server frees the offer message
				if(dynpm_send_message_to_server(&offer_header, (void**)&offer,
							slurmstepd_tbon_data.master_host_server_index)){
					error("error sending offer to master host %s",
							slurmstepd_tbon_data.master_host, slurmstepd_tbon_data.master_host_server_index);
				} else {
					info("sent offer to master host %s index %d",
							slurmstepd_tbon_data.master_host, slurmstepd_tbon_data.master_host_server_index);
				}
			}

			info("=============================== END OFFER BCAST  ===================================");

			info("================================== end offer ===================================");

			info("================================== generatinng request  ===================================");
			// this is for the hackaton at BSC; the actual implementation needs to generate
			// allocation requests based on interactions with the application via PMIx at
			// the compute nodes

			dynpm_header_t *request_header = malloc(sizeof(dynpm_header_t));
			request_header->source_type =  (dynpm_source_type)LAUNCHER;
			request_header->message_type = (dynpm_message_type)ALLOCATION_REQUEST;

			dynpm_allocation_request_msg_t *request = malloc(sizeof(dynpm_allocation_request_msg_t));
			request->application_index = step_data.application_index;
			request->partition_index = 0;
			request->type = (dynpm_allocation_operation_types)EXPAND;
			request->node_count = node_count; // match the request to the offered node count

			if(dynpm_send_message_to_server(&request_header, (void**)&request, 0)){
				error("error sending request to scheduler");
			} else { info("sent request to scheduler"); }

			info("==================================  request done  ===================================");

			pthread_mutex_unlock(&slurmstepd_tbon_data.mutex);

		} else if(header->message_type == REGISTER_PROCESS_MANAGER_ACK){
			info("received REGISTER_PROCESS_MANAGER_ACK from the scheduler");
			pthread_mutex_lock(&(step_data.mutex));
			step_data.registered = 1;
			pthread_mutex_unlock(&(step_data.mutex));
		} else if(header->message_type == REALLOCATION_START){
			info("received REALLOCATION_START from the scheduler");

			dynpm_reallocation_start_msg_t *realloc = NULL;
			dynpm_deserialize_reallocation_start_msg(&realloc, (const void**)&read_buffer);
			info("reallocation start: app_idx: %d; prt_idx: %d; type: %d;",
					realloc->application_index, realloc->partition_index, realloc->type);
			info("current nodes %d; %s", realloc->current_node_count, realloc->current_nodes);
			info("delta nodes   %d; %s", realloc->delta_node_count, realloc->delta_nodes);
			info("updated nodes %d; %s", realloc->updated_node_count, realloc->updated_nodes);

			// first tell the scheduler that we have received the message
			if(dynpm_send_header_to_server_connection((dynpm_source_type)LAUNCHER,
						(dynpm_message_type)REALLOCATION_START_ACK, connection)){
				error("failed to send READY_TBON_ACK from LAUNCHER");
			}
			// need to lock step data to start a reallocation
			pthread_mutex_lock(&(step_data.mutex));
			// TODO perform the reallocation based on the command
			pthread_mutex_unlock(&(step_data.mutex));
		} else {
			error("unknown or unsupported message type in scheduler client");
		}
	} else {
		error("source not SCHEDULER in scheduler client");
	}

	dynpm_free_header(&header);
	pthread_mutex_unlock(&mrun_full_mutex);
}

static void *_srun_runner_thread(void *input) {
	int srun_return_code;
	mrun_args_t *mrunargs = (mrun_args_t*)input;

	srun_return_code = srun(mrunargs->argc, mrunargs->argv);
	info("completed srun with code: %d", srun_return_code);

	int argi;
	for (argi = 0; argi < mrunargs->argc; argi++){
		free(mrunargs->argv[argi]);
	}
	free(mrunargs->argv);
	free(mrunargs);
}

static void *_process_data_thread(void *input) {
	while(1){
		// TODO need to make the polling rate dynamic, based on feedback and
		// possibly frequency analyses
		sleep(10);
		pthread_mutex_lock(&slurmstepd_tbon_data.mutex);
		if (slurmstepd_tbon_data.started){
			info("polling process data from dstepds");
			if(dynpm_send_header_to_server((dynpm_source_type)LAUNCHER,
						(dynpm_message_type)POLL_PROCESS_DATA,
						slurmstepd_tbon_data.master_host_server_index)){
				error("failed to send POLL_PROCESS_DATA from LAUNCHER");
			}
		} else {
			info("dstepd tbon not started yet; not polling process data");
		}
		pthread_mutex_unlock(&slurmstepd_tbon_data.mutex);
	}
}

int main(int argc, char **argv) {


	dynpm_client_init();
	dynpm_init_logger("./mrun.out", "./mrun.err");

	// the test needs the logger to be ready
	_test_serialization_deserialization();

	if(pthread_mutex_init(&(slurmd_tbon_data.mutex), NULL)){
		error("could not initialize slurmd_tbon_data mutex");
		return -1;
	}
	pthread_mutex_lock(&(slurmd_tbon_data.mutex));
	pthread_cond_init(&(slurmd_tbon_data.cond), NULL);
	slurmd_tbon_data.started = 0;
	slurmd_tbon_data.ready = 0;
	slurmd_tbon_data.master_host = NULL;
	slurmd_tbon_data.node_count = 0;
	slurmd_tbon_data.nodes = NULL;
	slurmd_tbon_data.port = NULL;
	pthread_mutex_unlock(&(slurmd_tbon_data.mutex));

	if(pthread_mutex_init(&(slurmstepd_tbon_data.mutex), NULL)){
		error("could not initialize slurmstepd_tbon_data mutex");
		return -1;
	}
	pthread_mutex_lock(&(slurmstepd_tbon_data.mutex));
	pthread_cond_init(&(slurmstepd_tbon_data.cond), NULL);
	slurmstepd_tbon_data.started = 0;
	slurmstepd_tbon_data.ready = 0;
	slurmstepd_tbon_data.master_host = NULL;
	slurmstepd_tbon_data.node_count = 0;
	slurmstepd_tbon_data.nodes = NULL;
	slurmstepd_tbon_data.port = NULL;
	pthread_mutex_unlock(&(slurmstepd_tbon_data.mutex));

	if(pthread_mutex_init(&(step_data.mutex), NULL)){
		error("could not initialize slurmstepd_tbon_data mutex");
		return -1;
	}
	pthread_mutex_lock(&(step_data.mutex));
	pthread_cond_init(&(step_data.cond), NULL);
	step_data.launched = 0;
	step_data.ready = 0;
	step_data.registered = 0;
	step_data.job_index = -1;
	step_data.application_index = -1;
	step_data.malleability_mode = 0;
	step_data.node_count = 0;
	step_data.nodes = NULL;
	step_data.process_count = 0;
	pthread_mutex_unlock(&(step_data.mutex));

	// run the original srun code in a thread, so that we can peek at state separately
	mrun_args_t *mrunargs = malloc(sizeof(mrun_args_t));
	mrunargs->argc = argc;
	mrunargs->argv = malloc((argc+1)*sizeof(char*));
	int argi;
	for (argi = 0; argi < argc; argi++){
		mrunargs->argv[argi] = strdup(argv[argi]);
	}
	mrunargs->argv[argc] = NULL;

	// identify the host and port of the dynpm server in slurmctld
	dynpm_process_conf_entry_t *ctld_config_entry;
	if(dynpm_conf_get_process_config(&ctld_config_entry, "slurmctld" )){
		error("could not find controller's configuration");
		return -1;
	}

	slurm_conf_init(NULL);

	char port[6];
	sprintf(port, "%d", ctld_config_entry->port);
	int scheduler_server_index;
	// TODO scheduler index should always be 0 for now, but nevertheless, we should have it stored instead
	// look at the tbon master connection as well
	if(dynpm_connect_to_server(*(slurm_conf.control_machine), port,
				_scheduler_client_handler, LAUNCHER, NULL, &scheduler_server_index)) return -1;
	xassert(scheduler_server_index == 0); // should always be 0 since we it's our first connection

	pthread_t srun_runner;
	if(pthread_create(&srun_runner, NULL, _srun_runner_thread, (void *) mrunargs)){
		error("failed to create srun runner thread");
		return -1;
	}
	if(pthread_setname_np(srun_runner, "srunrnr")){
		error("failed to set name of srun runner");
		return -1;
	}

	pthread_t process_data;
	if(pthread_create(&process_data, NULL, _process_data_thread, (void *) NULL)){
		error("failed to create the process data thread");
		return -1;
	}
	if(pthread_setname_np(process_data, "processdt")){
		error("failed to set name of the process data thread");
		return -1;
	}

	// let srun run to completion
	if(pthread_join(srun_runner, NULL)) {
		error("error joining srun runner");
	}

	// if srun is run without a thread
	//srun_return_code = srun(argc, argv);

	// TODO remove the constant and instead store the scheduler's
	// server index to be passed here instead of 0
	if(dynpm_disconnect_from_server(0)){
		info("non zero return while disconnectino from 0");
	}

	if(slurmd_tbon_data.started){
		info("slurmd tbon was started; disconnection from master");
		if(dynpm_disconnect_from_server( slurmd_tbon_data.master_host_server_index)) {
			info("non zero return while disconnecting from %d", slurmd_tbon_data.master_host_server_index);
		}
	} else {
		info("slurmd tbon was NOT started; NOT disconnection from master");
	}

	if(slurmstepd_tbon_data.started){
		info("stepd tbon was started; disconnection from master");
		if(dynpm_disconnect_from_server(
					slurmstepd_tbon_data.master_host_server_index)) {
			info("non zero return while disconnecting from %d", slurmd_tbon_data.master_host_server_index);
		}
	} else {
		info("stepd tbon was NOT started; NOT disconnection from master");
	}

	return 0;
}

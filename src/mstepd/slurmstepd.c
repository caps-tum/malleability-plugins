/*****************************************************************************\
 *  src/slurmd/slurmstepd/slurmstepd.c - Slurm job-step manager.
 *****************************************************************************
 *  Copyright (C) 2002-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2009 Lawrence Livermore National Security.
 *  Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Danny Auble <da@llnl.gov>
 *  and Christopher Morrone <morrone2@llnl.gov>.
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of Slurm, a resource management program.
 *  For details, see <https://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  Slurm is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  Slurm is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with Slurm; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include "config.h"

#include <signal.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <poll.h>

#include "src/common/assoc_mgr.h"
#include "src/common/cpu_frequency.h"
#include "src/common/run_command.h"
#include "src/common/setproctitle.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_pack.h"
#include "src/common/slurm_rlimits_info.h"
#include "src/common/spank.h"
#include "src/common/stepd_api.h"
#include "src/common/xmalloc.h"
#include "src/common/xsignal.h"
#include "src/common/xstring.h"

#include "src/interfaces/acct_gather_energy.h"
#include "src/interfaces/acct_gather_profile.h"
#include "src/interfaces/auth.h"
#include "src/interfaces/cgroup.h"
#include "src/interfaces/core_spec.h"
#include "src/interfaces/gpu.h"
#include "src/interfaces/gres.h"
#include "src/interfaces/hash.h"
#include "src/interfaces/job_container.h"
#include "src/interfaces/mpi.h"
#include "src/interfaces/proctrack.h"
#include "src/interfaces/switch.h"
#include "src/interfaces/task.h"

#include "src/slurmd/common/set_oomadj.h"
#include "src/slurmd/common/slurmstepd_init.h"
#include "src/slurmd/slurmd/slurmd.h"
#include "src/slurmd/slurmstepd/container.h"
#include "src/slurmd/slurmstepd/mgr.h"
#include "src/slurmd/slurmstepd/req.h"
#include "src/slurmd/slurmstepd/slurmstepd.h"
#include "src/slurmd/slurmstepd/slurmstepd_job.h"

// added in deep-sea
char const plugin_type[] = "launch";
// end of deep-sea changes

// deep-sea headers
#include "dynpm_network.h"
#include "dynpm_shm.h"
#include "dynpm_config.h"

// TODO the mpi/deepsea plugin is work in progress
// we need to load it here, instead of via the mpi_g
// interfaces, since we add non-slurm calls to it
// the original slurm interfaces will not allow us to
// reconfigure based on allocation updates at runtime
typedef struct slurm_mpi_ops {
	uint32_t (*plugin_id);
	int (*client_fini)(mpi_plugin_client_state_t *state);
	mpi_plugin_client_state_t *(*client_prelaunch)(
		const mpi_step_info_t *mpi_step, char ***env);
	s_p_hashtbl_t *(*conf_get)(void);
	List (*conf_get_printable)(void);
	void (*conf_options)(s_p_options_t **full_options,
			     int *full_options_cnt);
	void (*conf_set)(s_p_hashtbl_t *tbl);
	int (*slurmstepd_prefork)(const stepd_step_rec_t *step, char ***env);
	int (*slurmstepd_task)(const mpi_task_info_t *mpi_task, char ***env);
	// deep-sea added interface:
	int (*mpi_p_deepsea_disable_wrapping)(void);
} slurm_mpi_ops_t;

static const char *syms[] = {
	"plugin_id",
	"mpi_p_client_fini",
	"mpi_p_client_prelaunch",
	"mpi_p_conf_get",
	"mpi_p_conf_get_printable",
	"mpi_p_conf_options",
	"mpi_p_conf_set",
	"mpi_p_slurmstepd_prefork",
	"mpi_p_slurmstepd_task",
	"mpi_p_deepsea_disable_wrapping"
};

slurm_mpi_ops_t deepsea_pmix_ops;
static plugin_context_t	*g_context = NULL;

// TODO these were moved from the mpi/deepsea plugin
// since these fit better here, and the design was only on the
// plugin due to our try to avoid producing a new slurmstepd binary
// this was impossible, but a possitive result is that we can
// handle the server properly here
// For the slurmd daemons we still use a plugin to inject our server:
// currently switch/deepsea

dynpm_tbon_vertex_t *tbon_vertex = NULL;
static int process_data_reports = 0;
pthread_mutex_t tbon_initialization_mutex = PTHREAD_MUTEX_INITIALIZER;
int tbon_initialized = 0;
pthread_cond_t tbon_initialization_cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t dstepd_full_mutex = PTHREAD_MUTEX_INITIALIZER;

void _dstepd_tbon_child_handler(void *conn, const void *read_buffer){
	pthread_mutex_lock(&dstepd_full_mutex);
	dynpm_client_to_server_connection_t *connection = (dynpm_client_to_server_connection_t *)conn;

	dynpm_header_t *header;
	dynpm_deserialize_header(&header, (const void **)&read_buffer);

	info("=================================================");
	//info("connection: %x", (char*)conn);
	//info("received header: source: %d, message type: %d", header->source_type, header->message_type);

	if(header->source_type == PROCESS_MANAGER){
		info("from: PROCESS_MANAGER/DSTEPD");
		if(header->message_type == READY_TBON){
			pthread_mutex_lock(&tbon_initialization_mutex);
			assert(tbon_vertex != NULL);
			pthread_mutex_lock(&(tbon_vertex->mutex));

			tbon_vertex->ready_count++;
			info("%d/%d ready children at %s",
					tbon_vertex->ready_count,
					tbon_vertex->total_children,
					tbon_vertex->local_host);

			if(tbon_vertex->ready_count > tbon_vertex->total_children){
				error("received more READY_TBON messages than children");
			}

			if(tbon_vertex->ready_count == tbon_vertex->total_children){
				dynpm_header_t *ready_tbon_header = malloc(sizeof(dynpm_header_t));
				ready_tbon_header->source_type =  (dynpm_source_type)PROCESS_MANAGER;
				ready_tbon_header->message_type = (dynpm_message_type)READY_TBON;

				dynpm_ready_tbon_msg_t *ready_tbon = malloc(sizeof(dynpm_ready_tbon_msg_t));
				// TODO use id in the future, to support multiple tbon vertices at a single host
				// this is better to postpone until this code lives is libdynpm
				ready_tbon->id = tbon_vertex->id;
				ready_tbon->subtree_ready_count = tbon_vertex->ready_count;
				ready_tbon->error_code = 0;

				if(dynpm_send_message_to_client_connection(&ready_tbon_header, (void**)&ready_tbon,
							tbon_vertex->parent_connection)){
					error("error sending READY_TBON to parent connection");
				} else {
					info("READY_TBON sent to parent connection %x", tbon_vertex->parent_connection);
				}
				tbon_initialized = 1;
				pthread_cond_signal(&tbon_initialization_cond);
			} else {
				info("still waiting for some children READY_TBON");
			}

			pthread_mutex_unlock(&(tbon_vertex->mutex));
			pthread_mutex_unlock(&tbon_initialization_mutex);

		} else if(header->message_type == DYNAMIC_IO_OUT){
			// TODO check IO message; may need a free here
			info("from TBON child: DYNAMIC_IO_OUT");
			dynpm_dynamic_io_out_msg_t *dynamic_io_out;
			dynpm_deserialize_dynamic_io_out_msg(&dynamic_io_out,
					(const void **)&read_buffer);
			info("DYNAMIC_IO_OUT from host %s (%d bytes; id %d)",
					dynamic_io_out->host, dynamic_io_out->payload_bytes,
					dynamic_io_out->id);

			if(dynpm_send_buffer_to_client_connection(read_buffer,
						tbon_vertex->parent_connection)){
				error("error sending DYNAMIC_IO_OUT to parent connection");
			} else {
				info("DYNAMIC_IO_OUT sent to parent connection %x",
						tbon_vertex->parent_connection);
			}

			if(dynpm_send_header_to_server(PROCESS_MANAGER, DYNAMIC_IO_OUT_ACK,
						connection->index)){
				error("failed to send DYNAMIC_IO_OUT_ACK to child");
			}
		} else if(header->message_type == PROCESSES_DONE){
			pthread_mutex_lock(&(tbon_vertex->mutex));
			tbon_vertex->children_done++;
			info("from TBON child: PROCESSES_DONE (%d/%d)",
					tbon_vertex->children_done, tbon_vertex->total_children);
			if(tbon_vertex->children_done == tbon_vertex->total_children){
				pthread_cond_signal(&(tbon_vertex->children_done_cond));
				tbon_vertex->children_done = 0;
			}
			pthread_mutex_unlock(&(tbon_vertex->mutex));
		} else if(header->message_type == REPORT_PROCESS_DATA){
			// TODO may need to add the report logic into the
			// tbon data, since a reduction may be started before
			// an existing one completes
			pthread_mutex_lock(&(tbon_vertex->mutex));
			info("from TBON child: REPORT_PROCESS_DATA (%d/%d)",
					process_data_reports + 1, tbon_vertex->total_children);

			dynpm_report_process_data_msg_t *report;
			dynpm_deserialize_report_process_data_msg(&report,
					(const void **)&read_buffer);
			info("current report malleability mode: %d", report->malleability_mode);
			info("current report payload bytes: %d", report->payload_bytes);
			memcpy(&(tbon_vertex->process_data_reports[process_data_reports]),
					report, sizeof(dynpm_report_process_data_msg_t));
			if(report->payload_bytes > 0){
				assert(tbon_vertex->process_data_reports[process_data_reports].payload == NULL);
				tbon_vertex->process_data_reports[process_data_reports].payload =
					malloc(sizeof(char) * report->payload_bytes);
				memcpy(tbon_vertex->process_data_reports[process_data_reports].payload,
						report->payload, report->payload_bytes);
			} else {
				if(tbon_vertex->process_data_reports[process_data_reports].payload)
					free(tbon_vertex->process_data_reports[process_data_reports].payload);
				tbon_vertex->process_data_reports[process_data_reports].payload = NULL;
			}

			dynpm_free_report_process_data_msg((dynpm_report_process_data_msg_t**)&report);
			process_data_reports++;

			if(process_data_reports == tbon_vertex->total_children){
				info("all reports received; preparing vertex reduction");
				uint32_t malleability_mode =
					tbon_vertex->process_data_reports[0].malleability_mode;
				for(int i = 1; i < tbon_vertex->total_children; i++){
					if(malleability_mode !=
							tbon_vertex->process_data_reports[i].malleability_mode){
						malleability_mode = 0; // disabled malleability on disagreement
						info("disagreement on malleability mode: disabling (may be transient)");
						break;
					}
				}
				info("malleability mode: %d", malleability_mode);
				// TODO the reduction is process manager specific; think
				// about how to better support use cases via libdynpm
				info("process data reduction not implemented: skipping");

				dynpm_header_t *report_header = malloc(sizeof(dynpm_header_t));
				report_header->source_type =  (dynpm_source_type)PROCESS_MANAGER;
				report_header->message_type = (dynpm_message_type)REPORT_PROCESS_DATA;

				dynpm_report_process_data_msg_t *report =
					malloc(sizeof(dynpm_report_process_data_msg_t));
				report->id = tbon_vertex->id;
				report->job_index = 0;
				report->application_index = 0;
				report->malleability_mode = malleability_mode;
				report->payload_bytes = 0;
				report->payload = NULL;
				// TODO can include monitor data here in the future;
				// PM and launcher need to agree on a format, then pack into
				// report->payload, and set report->payload_bytes

				if(dynpm_send_message_to_client_connection(&report_header,
							(void**)&report, tbon_vertex->parent_connection)){
					error("error sending REPORT_PROCESS_DATA to parent connection");
				} else {
					info("REPORT_PROCESS_DATA sent to parent connection");
				}

				for(int i = 0; i < tbon_vertex->total_children; i++){
					if(tbon_vertex->process_data_reports[i].payload)
						free(tbon_vertex->process_data_reports[i].payload);
					tbon_vertex->process_data_reports[i].payload = NULL;
				}
				process_data_reports = 0;
			} else {
				info("waiting for remaining REPORT_PROCESS_DATA messages");
			}
			pthread_mutex_unlock(&(tbon_vertex->mutex));
		} else {
			error("unknown or unsupported message type tbon child listener");
		}
	} else {
		error("received tbon child message from a process other than PROCESS_MANAGER/SLURMSTEPD");
	}
	info("=================================================");

	dynpm_free_header(&header);
	pthread_mutex_unlock(&dstepd_full_mutex);
}

// TODO this is duplicade in switch/deepsea
static int _initialize_tbon_vertex(dynpm_server_to_client_connection_t *connection,
		dynpm_create_tbon_msg_t *create_tbon){
	if(tbon_vertex) {
		// TODO for node sharing scenarios, there may be more than 1
		info("existing tbon_vertex while trying to create a new one");
		dynpm_free_tbon_vertex(&tbon_vertex);
	} else {
		info("tbon_vertex pointer was clean; creating a new one");
	}

	char **host_array = malloc(create_tbon->total * sizeof(char*));
	char *host = NULL;
	int node_id = 0;
	hostlist_t *hl = hostlist_create(create_tbon->nodes);
	while ((host = hostlist_shift(hl))) {
		host_array[node_id] = strdup(host);
		node_id++;
		free(host);
	}
	assert(create_tbon->total == node_id);

	dynpm_initialize_tbon_vertex(connection, create_tbon, host_array, &tbon_vertex);

	return 0;
_initialize_tbon_vertex_error:
	return -1;
}

// TODO this is duplicade in mpi/deepsea ; need to extract/modularize
// TODO use the tbon vertex mutex here, or in the dynpm call?
static void _create_tbon (dynpm_server_to_client_connection_t *parent_connection, const void *read_buffer){
	info("_create_tbon");
	pthread_mutex_lock(&tbon_initialization_mutex);

	dynpm_create_tbon_msg_t *create_tbon;
	dynpm_deserialize_create_tbon_msg(&create_tbon, (const void **)&read_buffer);
	info("received: degree: %d; nodes: %s; parent connection: %x",
			create_tbon->degree, create_tbon->nodes, parent_connection);

	// first, initialize the local vertex (incl. children)
	if(_initialize_tbon_vertex(parent_connection, create_tbon)){
		error("failed to initialize tbon_vertex");
		return;
	}

	// first connect to the stepd at the master host
	dynpm_process_conf_entry_t *stepd_config_entry;
	if(dynpm_conf_get_process_config(&stepd_config_entry, "slurmstepd" )){
		error("could not find stepd's configuration");
		return;
	}
	char stepd_port[32];
	sprintf(stepd_port, "%d", stepd_config_entry->port);

	for(int child_id = 0; child_id < tbon_vertex->degree &&
			tbon_vertex->children_ids[child_id] != -1; child_id++){
		info("sending tbon create to child %d", child_id);
		// create a child connection with a dependency to trigger cascading
		// disconnections when the root of the tbon (master node)
		// receives a disconnection from parent
		if(dynpm_connect_to_server(
					tbon_vertex->children_hosts[child_id],
					stepd_port,
					_dstepd_tbon_child_handler,
					PROCESS_MANAGER,
					parent_connection,
					&(tbon_vertex->children_connection_indexes[child_id]))){
			error("could not connect to child host %s",
					tbon_vertex->children_hosts[child_id]);
			return;
		}
	}

	dynpm_send_message_to_tbon_children(parent_connection, read_buffer,
				PROCESS_MANAGER, CREATE_TBON, tbon_vertex);

	if(tbon_vertex->total_children == 0){
		info("this is a final tbon_vertex; READY, notifying parent...");
		dynpm_header_t *ready_tbon_header = malloc(sizeof(dynpm_header_t));
		ready_tbon_header->source_type =  (dynpm_source_type)PROCESS_MANAGER;
		ready_tbon_header->message_type = (dynpm_message_type)READY_TBON;

		dynpm_ready_tbon_msg_t *ready_tbon = malloc(sizeof(dynpm_ready_tbon_msg_t));
		//ready_tbon->id = create_tbon->id;
		ready_tbon->id = 0;
		ready_tbon->subtree_ready_count = 0; // no children to track
		ready_tbon->error_code = 0; // no children errors possible

		if( dynpm_send_message_to_client_connection(&ready_tbon_header, (void**)&ready_tbon, parent_connection)){
			error("error sending READY_TBON to parent connection");
		} else {
			info("READY_TBON sent to parent connection");
		}

		// need to signal owrselves that we are ready to continue
		tbon_initialized = 1;
		pthread_cond_signal(&tbon_initialization_cond);
	}

	dynpm_free_create_tbon_msg(&create_tbon);

	pthread_mutex_unlock(&tbon_initialization_mutex);
}

static void _update_offer_data(dynpm_server_to_client_connection_t *connection, const void *read_buffer){
	// TODO after sending, then setup the data for the local processes at this daemon here
	// for example, this could be exposed as shared memory or via PMIx mechanisms
	// for the local slurmstepd/dstepd or any other PM instance
	//info("nodes available: %d", offer->node_count);
	//dynpm_free_resource_offer_msg(offer);

	// need to send after processing, since read_buffer is freed inside
	dynpm_send_message_to_tbon_children(connection, read_buffer,
			PROCESS_MANAGER, OFFER_RESOURCES, tbon_vertex);

	dynpm_resource_offer_msg_t *offer = NULL;
	dynpm_deserialize_resource_offer_msg(&offer, (const void **)&read_buffer);
	info("resource offer: app_idx: %d; prt_idx: %d; nodes: %d;",
			offer->application_index,
			offer->partition_index,
			offer->node_count);
	info("created: %d:%s", offer->creation_time, ctime(&(offer->creation_time)));
	info("ends: %d:%s", offer->end_time, ctime(&(offer->end_time)));

	// TODO add the offer to a location in the server shared memory for task access
	dynpm_free_resource_offer_msg(&offer);
}

void *_dstepd_server_handler(dynpm_server_to_client_connection_t *connection, const void *read_buffer){
	pthread_mutex_lock(&dstepd_full_mutex);
	info("====================");
	dynpm_header_t *header;
	dynpm_deserialize_header(&header, (const void **)&read_buffer);

	if(header->message_type == (dynpm_message_type)CREATE_TBON){
		info("received CREATE_TBON");
		_create_tbon(connection, read_buffer);
	} else if(header->message_type == (dynpm_message_type)OFFER_RESOURCES){
		info("received OFFER_RESOURCES");
		_update_offer_data(connection, read_buffer);
	} else if(header->message_type == (dynpm_message_type)DYNAMIC_IO_OUT_ACK){
		info("received DYNAMIC_IO_ACK");
		// TODO handle when buffers are filled, without brute forcing with large buffers
	} else if(header->message_type == (dynpm_message_type)PROCESSES_DONE_ACK){
		info("received PROCESSES_DONE_ACK");
		pthread_mutex_lock(&(tbon_vertex->mutex));
		if(tbon_vertex->total_children > 0){
			dynpm_send_message_to_tbon_children(connection, read_buffer,
					PROCESS_MANAGER, PROCESSES_DONE_ACK, tbon_vertex);
		}
		tbon_vertex->finalized = 1;
		pthread_cond_signal(&(tbon_vertex->finalized_cond));
		pthread_mutex_unlock(&(tbon_vertex->mutex));
	} else if(header->message_type == (dynpm_message_type)READY_TBON_ACK){
		info("received READY_TBON_ACK");
		pthread_mutex_lock(&(tbon_vertex->mutex));
		if(tbon_vertex->total_children > 0){
			dynpm_send_message_to_tbon_children(connection, read_buffer,
					PROCESS_MANAGER, READY_TBON_ACK, tbon_vertex);
		}
		tbon_vertex->initialized = 1;
		pthread_cond_signal(&(tbon_vertex->initialized_cond));
		pthread_mutex_unlock(&(tbon_vertex->mutex));
	} else if(header->message_type == (dynpm_message_type)POLL_PROCESS_DATA){
		info("received POLL_PROCESS_DATA");
		pthread_mutex_lock(&(tbon_vertex->mutex));
		if(tbon_vertex->total_children > 0){
			info("requesting process data from %d children", tbon_vertex->total_children);
			process_data_reports = 0;
			dynpm_send_message_to_tbon_children(connection, read_buffer,
					PROCESS_MANAGER, POLL_PROCESS_DATA, tbon_vertex);
		} else {
			info("leaf node: reporting process data");
			uint32_t mode=0, status=0;
			if(dynpm_shm_manager_get_malleability_mode(&mode, &status)){
				error("could not get malleability mode: status: %d");
			} else {
				info("got malleability mode: %d; status: %d", mode, status);
				dynpm_header_t *report_header = malloc(sizeof(dynpm_header_t));
				report_header->source_type =  (dynpm_source_type)PROCESS_MANAGER;
				report_header->message_type = (dynpm_message_type)REPORT_PROCESS_DATA;

				dynpm_report_process_data_msg_t *report =
					malloc(sizeof(dynpm_report_process_data_msg_t));
				report->id = tbon_vertex->id;
				report->job_index = 0;
				report->application_index = 0;
				report->malleability_mode = mode;
				report->payload_bytes = 0;
				report->payload = NULL;
				// TODO can include monitor data here in the future;
				// PM and launcher need to agree on a format, then pack into
				// report->payload, and set report->payload_bytes

				if(dynpm_send_message_to_client_connection(&report_header,
							(void**)&report, connection)){
					error("error sending REPORT_PROCESS_DATA to parent connection");
				} else {
					info("REPORT_PROCESS_DATA sent to parent connection");
				}
			}
		}
		pthread_mutex_unlock(&(tbon_vertex->mutex));
	} else {
		error("message not supported in _dstepd_server_handler");
	}
	// we need to free the header in this case, since we don't send it
	dynpm_free_header(&header);
	info("====================");
	pthread_mutex_unlock(&dstepd_full_mutex);
}


static int _init_from_slurmd(int sock, char **argv, slurm_addr_t **_cli,
			     slurm_addr_t **_self, slurm_msg_t **_msg);

static stepd_step_rec_t *_step_setup(slurm_addr_t *cli, slurm_addr_t *self,
				     slurm_msg_t *msg);
static void _process_cmdline(int argc, char **argv);

static pthread_mutex_t cleanup_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool cleanup = false;

/*
 *  List of signals to block in this process
 */
int slurmstepd_blocked_signals[] = {
	SIGINT,  SIGTERM, SIGTSTP,
	SIGQUIT, SIGPIPE, SIGUSR1,
	SIGUSR2, SIGALRM, SIGHUP, 0
};

/* global variable */
slurmd_conf_t * conf;
extern char  ** environ;

void notify_slurmd_dynamic_pm_ready(){
	int dynamic_pm_ready = 9999;
	safe_write(STDOUT_FILENO, &dynamic_pm_ready, sizeof(int));
	return;
rwfail:
	error("failed to notify via notify_slurmd_dynamic_pm_ready");
}

int main (int argc, char **argv) {

	log_options_t lopts = LOG_OPTS_INITIALIZER;
	slurm_addr_t *cli;
	slurm_addr_t *self;
	slurm_msg_t *msg;
	stepd_step_rec_t *step;
	int rc = 0;

	_process_cmdline(argc, argv);

	run_command_init();

	xsignal_block(slurmstepd_blocked_signals);
	conf = xmalloc(sizeof(*conf));
	conf->argv = &argv;
	conf->argc = &argc;
	init_setproctitle(argc, argv);

	log_init(argv[0], lopts, LOG_DAEMON, NULL);

	/* Receive job parameters from the slurmd */
	_init_from_slurmd(STDIN_FILENO, argv, &cli, &self, &msg);
	info("DSTEPD starting...");

	// TODO deep-sea:
	// this is a safe location to start the server threads
	// need to verify of any issues due to some subsystem not being ready

	dynpm_plugin_conf_entry_t *config_entry;
	if(dynpm_conf_get_plugin_config(&config_entry, "mpi")){
		error("could not find mpi plugin configuration");
		return SLURM_ERROR;
	}

	info("running from dstepd; setting up server...");
	dynpm_process_conf_entry_t *slurmstepd_config_entry;

	if(dynpm_conf_get_process_config(&slurmstepd_config_entry, "slurmstepd")){
		error("could not find slurmstepd configuration");
		return SLURM_ERROR;
	}

	dynpm_client_init();

	// TODO this attempts to create the server with the specified port, and
	// therefore can only be one instance of a stepd in the node
	// we have a port range version, but this would require some extra communication
	// to pass the port number back to drun via the slumd tbon, but unfortunately
	// the stepdaemon detaches from its parent slurmd completely
	// a solution to this will most likely not be simple
	// therefore:
	// for now we assume exclusive node access and attempt to create
	// the server in a specific port...
	// TODO to allow for node sharing of parallel launches, the
	// step id can be used to create a port offset from the base port
	if(dynpm_server_init(slurmstepd_config_entry->port, // + step id offset possble
				slurmstepd_config_entry->threads,
				_dstepd_server_handler,
				"/home/mpiuser/dstep.out", "/home/mpiuser/dstep.err")){
		error("error setting up listener threads");
		exit(-1);
		//return SLURM_ERROR;
	} else {
		debug("listener threads created in slurmstepd");
	}

	// deep-sea:
	// at this point, the mpi plugin has been initialized, and
	// our mpi/deepsea plugin server is available
	// TODO notify SLURMD about this
	notify_slurmd_dynamic_pm_ready();

	pthread_mutex_lock(&tbon_initialization_mutex);
	if(tbon_initialized == 0){
		info("waiting for tbon to be initialized");

		struct timespec tbon_wait_timeout;
		struct timeval now;
		gettimeofday(&now, NULL);
		tbon_wait_timeout.tv_sec = now.tv_sec + 10;
		tbon_wait_timeout.tv_nsec = now.tv_usec;

		pthread_cond_timedwait(&(tbon_initialization_cond), &(tbon_initialization_mutex), &tbon_wait_timeout);

	} else {
		info("tbon was initialized before we blocked");
	}
	pthread_mutex_unlock(&tbon_initialization_mutex);
	info("tbon initialized...");

	dynpm_tbon_initialized_wait_timeout(tbon_vertex, 10, 0);

	/* Create the stepd_step_rec_t, mostly from info in a
	 * launch_tasks_request_msg_t or a batch_job_launch_msg_t */
	if (!(step = _step_setup(cli, self, msg))) {
		rc = SLURM_ERROR;
		goto ending;
	}
	info("after _step_setup");

	/* fork handlers cause mutexes on some global data structures
	 * to be re-initialized after the fork. */
	//slurm_conf_install_fork_handlers();
	info("after slurm_conf_install_fork_handlers");

	if (step->step_id.step_id != SLURM_EXTERN_CONT){
		info("step->step_id.step_id != SLURM_EXTERN_CONT");
		close_slurmd_conn();
	}

	acct_gather_energy_g_set_data(ENERGY_DATA_STEP_PTR, step);
	info("after acct_gather_energy_g_set_data");

	/* This does most of the stdio setup, then launches all the tasks,
	 * and blocks until the step is complete */
	info("calling job_manager");
	rc = job_manager(step);
	info("after job_manager");

	return stepd_cleanup(msg, step, cli, self, rc, 0);
ending:
	return stepd_cleanup(msg, step, cli, self, rc, 1);
}

extern int stepd_cleanup(slurm_msg_t *msg, stepd_step_rec_t *step,
			 slurm_addr_t *cli, slurm_addr_t *self,
			 int rc, bool only_mem)
{
	slurm_mutex_lock(&cleanup_mutex);

	if (cleanup)
		goto done;

	if (!step) {
		error("%s: step is NULL, skipping cleanup", __func__);
		goto done;
	}

	//if (!only_mem) {
	//	if (step->batch)
	//		//batch_finish(step, rc); /* sends batch complete message */

	//	/* signal the message thread to shutdown, and wait for it */
	//	if (step->msg_handle)
	//		eio_signal_shutdown(step->msg_handle);
	//	//pthread_join(step->msgid, NULL);
	//}

	mpi_fini();

	/*
	 * This call is only done once per step since stepd_cleanup is protected
	 * agains multiple and concurrent calls.
	 */
	proctrack_g_destroy(step->cont_id);

	if (conf->hwloc_xml)
		(void)remove(conf->hwloc_xml);

	if (step->container)
		cleanup_container(step);

	run_command_shutdown();

	if (step->step_id.step_id == SLURM_EXTERN_CONT) {
		uint32_t jobid;
		jobid = step->step_id.job_id;
		if (container_g_stepd_delete(jobid))
			error("container_g_stepd_delete(%u): %m", jobid);
	}

	cleanup = true;
done:
	slurm_mutex_unlock(&cleanup_mutex);
	info("done with job");
	return rc;
}

extern void close_slurmd_conn(void)
{
	/* Fancy way of closing stdin that keeps STDIN_FILENO from being
	 * allocated to any random file.  The slurmd already opened /dev/null
	 * on STDERR_FILENO for us. */
	dup2(STDERR_FILENO, STDIN_FILENO);

	/* Fancy way of closing stdout that keeps STDOUT_FILENO from being
	 * allocated to any random file.  The slurmd already opened /dev/null
	 * on STDERR_FILENO for us. */
	dup2(STDERR_FILENO, STDOUT_FILENO);
}

static slurmd_conf_t *_read_slurmd_conf_lite(int fd)
{
	int rc;
	int len;
	buf_t *buffer = NULL;
	slurmd_conf_t *confl, *local_conf = NULL;
	int tmp_int = 0;
	List tmp_list = NULL;
	assoc_mgr_lock_t locks = { .tres = WRITE_LOCK };

	/*  First check to see if we've already initialized the
	 *   global slurmd_conf_t in 'conf'. Allocate memory if not.
	 */
	if (conf) {
		confl = conf;
	} else {
		local_conf = xmalloc(sizeof(slurmd_conf_t));
		confl = local_conf;
	}

	safe_read(fd, &len, sizeof(int));

	buffer = init_buf(len);
	safe_read(fd, buffer->head, len);

	rc = unpack_slurmd_conf_lite_no_alloc(confl, buffer);
	if (rc == SLURM_ERROR)
		fatal("slurmstepd: problem with unpack of slurmd_conf");

	rc = unpack_slurm_conf_lite_no_alloc(buffer);
	if (rc == SLURM_ERROR)
		fatal("slurmstepd: problem with unpack of slurm_conf");
	slurm_conf_init_stepd();

	if (slurm_unpack_list(&tmp_list,
			      slurmdb_unpack_tres_rec,
			      slurmdb_destroy_tres_rec,
			      buffer, SLURM_PROTOCOL_VERSION)
	    != SLURM_SUCCESS)
		fatal("slurmstepd: problem with unpack of tres list");

	FREE_NULL_BUFFER(buffer);

	confl->log_opts.prefix_level = 1;
	confl->log_opts.logfile_level = confl->debug_level;

	if (confl->daemonize)
		confl->log_opts.stderr_level = LOG_LEVEL_QUIET;
	else
		confl->log_opts.stderr_level = confl->debug_level;

	if (confl->syslog_debug != LOG_LEVEL_END) {
		confl->log_opts.syslog_level = confl->syslog_debug;
	} else if (!confl->daemonize) {
		confl->log_opts.syslog_level = LOG_LEVEL_QUIET;
	} else if ((confl->debug_level > LOG_LEVEL_QUIET) && !confl->logfile) {
		confl->log_opts.syslog_level = confl->debug_level;
	} else
		confl->log_opts.syslog_level = LOG_LEVEL_FATAL;

	/*
	 * LOGGING BEFORE THIS WILL NOT WORK!  Only afterwards will it show
	 * up in the log.
	 */
	log_alter(confl->log_opts, SYSLOG_FACILITY_DAEMON, confl->logfile);
	log_set_timefmt(slurm_conf.log_fmt);
	debug2("debug level read from slurmd is '%s'.",
		log_num2string(confl->debug_level));

	confl->acct_freq_task = NO_VAL16;
	tmp_int = acct_gather_parse_freq(PROFILE_TASK,
					 slurm_conf.job_acct_gather_freq);
	if (tmp_int != -1)
		confl->acct_freq_task = tmp_int;

	xassert(tmp_list);

	assoc_mgr_lock(&locks);
	assoc_mgr_post_tres_list(tmp_list);
	debug2("%s: slurmd sent %u TRES.", __func__, g_tres_count);
	/* assoc_mgr_post_tres_list destroys tmp_list */
	tmp_list = NULL;
	assoc_mgr_unlock(&locks);

	return (confl);

rwfail:
	FREE_NULL_BUFFER(buffer);
	xfree(local_conf);
	return (NULL);
}

static int _get_jobid_uid_gid_from_env(uint32_t *jobid, uid_t *uid, gid_t *gid)
{
	const char *val;
	char *p;

	if (!(val = getenv("SLURM_JOBID")))
		return error("Unable to get SLURM_JOBID in env!");

	*jobid = (uint32_t) strtoul(val, &p, 10);
	if (*p != '\0')
		return error("Invalid SLURM_JOBID=%s", val);

	if (!(val = getenv("SLURM_UID")))
		return error("Unable to get SLURM_UID in env!");

	*uid = (uid_t) strtoul(val, &p, 10);
	if (*p != '\0')
		return error("Invalid SLURM_UID=%s", val);

	if (!(val = getenv("SLURM_JOB_GID")))
		return error("Unable to get SLURM_JOB_GID in env!");

	*gid = (gid_t) strtoul(val, &p, 10);
	if (*p != '\0')
		return error("Invalid SLURM_JOB_GID=%s", val);

	return SLURM_SUCCESS;
}

static int _handle_spank_mode(int argc, char **argv)
{
	char *prefix = NULL;
	const char *mode = argv[2];
	uid_t uid = (uid_t) -1;
	gid_t gid = (gid_t) -1;
	uint32_t jobid = (uint32_t) -1;
	log_options_t lopts = LOG_OPTS_INITIALIZER;

	/*
	 *  Not necessary to log to syslog
	 */
	lopts.syslog_level = LOG_LEVEL_QUIET;

	/*
	 *  Make our log prefix into spank-prolog: or spank-epilog:
	 */
	xstrfmtcat(prefix, "spank-%s", mode);
	log_init(prefix, lopts, LOG_DAEMON, NULL);
	xfree(prefix);

	/*
	 *  When we are started from slurmd, a lightweight config is
	 *   sent over the stdin fd. If we are able to read this conf
	 *   use it to reinitialize the log.
	 *  It is not a fatal error if we fail to read the conf file.
	 *   This could happen if slurmstepd is run standalone for
	 *   testing.
	 */
	conf = _read_slurmd_conf_lite(STDIN_FILENO);
	close(STDIN_FILENO);

	if (_get_jobid_uid_gid_from_env(&jobid, &uid, &gid))
		return error("spank environment invalid");

	debug("Running spank/%s for jobid [%u] uid [%u] gid [%u]",
	      mode, jobid, uid, gid);

	if (!xstrcmp(mode, "prolog")) {
		if (spank_job_prolog(jobid, uid, gid) < 0)
			return -1;
	} else if (!xstrcmp(mode, "epilog")) {
		if (spank_job_epilog(jobid, uid, gid) < 0)
			return -1;
	} else {
		error("Invalid mode %s specified!", mode);
		return -1;
	}

	return 0;
}

/*
 *  Process special "modes" of slurmstepd passed as cmdline arguments.
 */
static void _process_cmdline(int argc, char **argv)
{
	if ((argc == 2) && !xstrcmp(argv[1], "getenv")) {
		print_rlimits();
		for (int i = 0; environ[i]; i++)
			printf("%s\n", environ[i]);
		exit(0);
	}
	if ((argc == 2) && !xstrcmp(argv[1], "infinity")) {
		set_oom_adj(-1000);
		(void) poll(NULL, 0, -1);
		exit(0);
	}
	if ((argc == 3) && !xstrcmp(argv[1], "spank")) {
		if (_handle_spank_mode(argc, argv) < 0)
			exit(1);
		exit(0);
	}
}

static void _set_job_log_prefix(slurm_step_id_t *step_id)
{
	char *buf;
	char tmp_char[64];

	log_build_step_id_str(step_id, tmp_char, sizeof(tmp_char),
			      STEP_ID_FLAG_NO_PREFIX);
	buf = xstrdup_printf("[%s]", tmp_char);

	setproctitle("%s", buf);
	/* note: will claim ownership of buf, do not free */
	xstrcat(buf, " ");
	log_set_prefix(&buf);
}

/*
 *  This function handles the initialization information from slurmd
 *  sent by _send_slurmstepd_init() in src/slurmd/slurmd/req.c.
 */
static int
_init_from_slurmd(int sock, char **argv,
		  slurm_addr_t **_cli, slurm_addr_t **_self, slurm_msg_t **_msg)
{
	char *incoming_buffer = NULL;
	buf_t *buffer;
	int step_type;
	int len;
	uint16_t proto;
	slurm_addr_t *cli = NULL;
	slurm_addr_t *self = NULL;
	slurm_msg_t *msg = NULL;
	slurm_step_id_t step_id = {
		.job_id = 0,
		.step_id = NO_VAL,
		.step_het_comp = NO_VAL,
	};

	/* receive conf from slurmd */
	if (!(conf = _read_slurmd_conf_lite(sock)))
		fatal("Failed to read conf from slurmd");

	slurm_conf.slurmd_port = conf->port;
	slurm_conf.slurmd_syslog_debug = conf->syslog_debug;

	setenvf(NULL, "SLURMD_NODENAME", "%s", conf->node_name);

	/* receive conf_hashtbl from slurmd */
	read_conf_recv_stepd(sock);

	/* receive job type from slurmd */
	safe_read(sock, &step_type, sizeof(int));
	debug3("step_type = %d", step_type);

	// in deep-sea, we build our own tree
	// this one is disabled (matches slurmd write)
	/* receive reverse-tree info from slurmd */
	slurm_mutex_lock(&step_complete.lock);

	// TODO these need to match in switch_deepsea/send_slurmstepd_init
	// we will disable this once mpi/dpmix is ready
	//safe_read(sock, &step_complete.rank, sizeof(int));
	//safe_read(sock, &step_complete.parent_rank, sizeof(int));
	//safe_read(sock, &step_complete.children, sizeof(int));
	//safe_read(sock, &step_complete.depth, sizeof(int));
	//safe_read(sock, &step_complete.max_depth, sizeof(int));
	//safe_read(sock, &step_complete.parent_addr, sizeof(slurm_addr_t));
	//if (step_complete.children)
		//step_complete.bits = bit_alloc(step_complete.children);
	slurm_mutex_unlock(&step_complete.lock);

	//debug3("slurmstepd rank %d, parent = %pA",
	       //step_complete.rank, &step_complete.parent_addr);

	/* receive cli from slurmd */
	safe_read(sock, &len, sizeof(int));
	incoming_buffer = xmalloc(len);
	safe_read(sock, incoming_buffer, len);
	buffer = create_buf(incoming_buffer,len);
	cli = xmalloc(sizeof(slurm_addr_t));
	if (slurm_unpack_addr_no_alloc(cli, buffer) == SLURM_ERROR)
		fatal("slurmstepd: problem with unpack of slurmd_conf");
	FREE_NULL_BUFFER(buffer);

	/* receive self from slurmd */
	safe_read(sock, &len, sizeof(int));
	if (len > 0) {
		/* receive packed self from main slurmd */
		incoming_buffer = xmalloc(len);
		safe_read(sock, incoming_buffer, len);
		buffer = create_buf(incoming_buffer,len);
		self = xmalloc(sizeof(slurm_addr_t));
		if (slurm_unpack_addr_no_alloc(self, buffer)
		    == SLURM_ERROR) {
			fatal("slurmstepd: problem with unpack of "
			      "slurmd_conf");
		}
		FREE_NULL_BUFFER(buffer);
	}

	/* Grab the slurmd's spooldir. Has %n expanded. */
	cpu_freq_init(conf);

	/* Receive cpu_frequency info from slurmd */
	cpu_freq_recv_info(sock);

	/* get the protocol version of the srun */
	safe_read(sock, &proto, sizeof(uint16_t));

	/* receive req from slurmd */
	safe_read(sock, &len, sizeof(int));
	incoming_buffer = xmalloc(len);
	safe_read(sock, incoming_buffer, len);
	buffer = create_buf(incoming_buffer,len);

	msg = xmalloc(sizeof(slurm_msg_t));
	slurm_msg_t_init(msg);
	/* Always unpack as the current version. */
	msg->protocol_version = SLURM_PROTOCOL_VERSION;

	switch (step_type) {
	case LAUNCH_BATCH_JOB:
		info("received REQUEST_BATCH_JOB_LAUNCH from slurmd");
		msg->msg_type = REQUEST_BATCH_JOB_LAUNCH;
		break;
	case LAUNCH_TASKS:
		info("received REQUEST_LAUNCH_TASKS from slurmd");
		msg->msg_type = REQUEST_LAUNCH_TASKS;
		break;
	default:
		fatal("%s: Unrecognized launch RPC (%d)", __func__, step_type);
		break;
	}

	/* Init switch before unpack_msg to only init the default */
	if (switch_init(1) != SLURM_SUCCESS)
		fatal( "failed to initialize authentication plugin" );

	if (gres_init() != SLURM_SUCCESS)
		fatal("failed to initialize gres plugins");

	if (unpack_msg(msg, buffer) == SLURM_ERROR)
		fatal("slurmstepd: we didn't unpack the request correctly");
	FREE_NULL_BUFFER(buffer);

	switch (step_type) {
	case LAUNCH_BATCH_JOB:
		info("handling LAUNCH_BATCH_JOB");
		step_id.job_id = ((batch_job_launch_msg_t *)msg->data)->job_id;
		step_id.step_id = SLURM_BATCH_SCRIPT;
		step_id.step_het_comp = NO_VAL;
		break;
	case LAUNCH_TASKS:
		info("handling LAUNCH_TASKS");
		memcpy(&step_id,
		       &((launch_tasks_request_msg_t *)msg->data)->step_id,
		       sizeof(step_id));
		break;
	default:
		fatal("%s: Unrecognized launch RPC (%d)", __func__, step_type);
		break;
	}

	_set_job_log_prefix(&step_id);

	/*
	 * Init all plugins after receiving the slurm.conf from the slurmd.
	 */
	if ((auth_g_init() != SLURM_SUCCESS) ||
	    (cgroup_g_init() != SLURM_SUCCESS) ||
	    (hash_g_init() != SLURM_SUCCESS) ||
	    (acct_gather_conf_init() != SLURM_SUCCESS) ||
	    (core_spec_g_init() != SLURM_SUCCESS) ||
	    (proctrack_g_init() != SLURM_SUCCESS) ||
	    (slurmd_task_init() != SLURM_SUCCESS) ||
	    (acct_gather_profile_init() != SLURM_SUCCESS) ||
	    (cred_g_init() != SLURM_SUCCESS) ||
	    (job_container_init() != SLURM_SUCCESS))
		fatal("Couldn't load all plugins");

	info("loaded plugins");

	/*
	 * Receive all secondary conf files from the slurmd.
	 */

	/* receive cgroup conf from slurmd */
	if (cgroup_read_conf(sock) != SLURM_SUCCESS)
		fatal("Failed to read cgroup conf from slurmd");

	/* receive acct_gather conf from slurmd */
	if (acct_gather_read_conf(sock) != SLURM_SUCCESS)
		fatal("Failed to read acct_gather conf from slurmd");

	/* Receive job_container information from slurmd */
	if (container_g_recv_stepd(sock) != SLURM_SUCCESS)
		fatal("Failed to read job_container.conf from slurmd.");

	/* Receive GRES information from slurmd */
	if (gres_g_recv_stepd(sock, msg) != SLURM_SUCCESS)
		fatal("Failed to read gres.conf from slurmd.");

	info("all plugins initialized");

	/* Receive mpi.conf from slurmd */
	//if ((step_type == LAUNCH_TASKS) &&
	//    (step_id.step_id != SLURM_EXTERN_CONT) &&
	//    (step_id.step_id != SLURM_INTERACTIVE_STEP) &&
	//    (mpi_conf_recv_stepd(sock) != SLURM_SUCCESS))
	//	fatal("Failed to read MPI conf from slurmd");

	//info("received MPI configuration");

	char plugin_type[CONFIG_MAX_STR_LEN*2] = "mpi/deepsea";
	info("loading %s", plugin_type);

	// init() is called inside:
	// ->plugin_load_and_link->plugin_load_from_file()->init()
	g_context = plugin_context_create("mpi",
			plugin_type,
			(void **) &deepsea_pmix_ops, syms, sizeof(syms));

	if (!g_context) {
		error("cannot create %s context for %s", "mpi", plugin_type);
	}

	if((*(deepsea_pmix_ops.mpi_p_deepsea_disable_wrapping))()){
		error("failed to disable wrapping in mpi/deepsea");
	} else {
		info("disabled wrapping; we prodive PMIx support directly");
	}

	if (!conf->hwloc_xml) {
		conf->hwloc_xml = xstrdup_printf("%s/hwloc_topo_%u.%u",
						 conf->spooldir,
						 step_id.job_id,
						 step_id.step_id);
		if (step_id.step_het_comp != NO_VAL)
			xstrfmtcat(conf->hwloc_xml, ".%u",
				   step_id.step_het_comp);
		xstrcat(conf->hwloc_xml, ".xml");
	}
	/*
	 * Swap the field to the srun client version, which will eventually
	 * end up stored as protocol_version in srun_info_t. It's a hack to
	 * pass it in-band, while still using the correct version to unpack
	 * the launch request message above.
	 */
	msg->protocol_version = proto;

	*_cli = cli;
	*_self = self;
	*_msg = msg;

	return 1;

rwfail:
	fatal("Error reading initialization data from slurmd");
	exit(1);
}

static stepd_step_rec_t *
_step_setup(slurm_addr_t *cli, slurm_addr_t *self, slurm_msg_t *msg)
{
	stepd_step_rec_t *step = NULL;

	switch (msg->msg_type) {
	case REQUEST_BATCH_JOB_LAUNCH:
		debug2("setup for a batch_job");
		step = mgr_launch_batch_job_setup(msg->data, cli);
		break;
	case REQUEST_LAUNCH_TASKS:
		debug2("setup for a launch_task");
		step = mgr_launch_tasks_setup(msg->data, cli, self,
					      msg->protocol_version);
		break;
	default:
		fatal("handle_launch_message: Unrecognized launch RPC");
		break;
	}

	if (!step) {
		error("_step_setup: no job returned");
		return NULL;
	}

	if (step->container) {
		int rc = setup_container(step);

		if (rc == ESLURM_CONTAINER_NOT_CONFIGURED) {
			debug2("%s: container %s requested but containers are not configured on this node",
			       __func__, step->container->bundle);
		} else if (rc) {
			error("%s: container setup failed: %s",
			      __func__, slurm_strerror(rc));
			stepd_step_rec_destroy(step);
			return NULL;
		} else {
			debug2("%s: container %s successfully setup",
			       __func__, step->container->bundle);
		}
	}

	step->jmgr_pid = getpid();

	/* Establish GRES environment variables */
	if (slurm_conf.debug_flags & DEBUG_FLAG_GRES) {
		gres_job_state_log(step->job_gres_list,
				   step->step_id.job_id);
		gres_step_state_log(step->step_gres_list,
				    step->step_id.job_id,
				    step->step_id.step_id);
	}
	if (step->batch || (step->step_id.step_id == SLURM_INTERACTIVE_STEP)) {
		gres_g_job_set_env(step, 0);
	} else if (msg->msg_type == REQUEST_LAUNCH_TASKS) {
		gres_g_step_set_env(step);
	}

	/*
	 * Add slurmd node topology informations to job env array
	 */
	env_array_overwrite(&step->env,"SLURM_TOPOLOGY_ADDR",
			    conf->node_topo_addr);
	env_array_overwrite(&step->env,"SLURM_TOPOLOGY_ADDR_PATTERN",
			    conf->node_topo_pattern);
	/*
	 * Reset address for cloud nodes
	 */
	if (step->alias_list && set_nodes_alias(step->alias_list)) {
		error("%s: set_nodes_alias failed: %s", __func__,
		      step->alias_list);
		stepd_step_rec_destroy(step);
		return NULL;
	}

	set_msg_node_id(step);

	return step;
}

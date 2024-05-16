/*
 * Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *
 * Additional copyrights may follow
 *
 */

#include <signal.h>
#include <sys/types.h>

#include "slurm/slurm_errno.h"
#include "src/common/slurm_xlator.h"

#include "src/common/log.h"
#include "src/common/plugrack.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_pack.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/slurmd/slurmstepd/slurmstepd_job.h"
#include "src/slurmctld/slurmctld.h"

// deep-sea headers
#include "dynpm_network.h"
#include "dynpm_config.h"

//#include "zlib.h"
#include <bzlib.h>
//#include "src/slurmd/slurmd/req.h"

// duplicaded code from slurmd
// TODO need to minimize or eliminate these

#include "src/slurmd/slurmd/slurmd.h"
// after 22.05
#include "src/interfaces/job_container.h"
#include "src/interfaces/gres.h"
//#include "src/interfaces/task.h"
//#include "src/interfaces/node_features.h"
// in 22.05 it is under common
//#include "src/common/gres.h"
//#include "src/common/task.h"
//#include "src/common/node_features.h"
#include "src/slurmd/common/slurmstepd_init.h"
#include "src/common/reverse_tree.h"
#include "src/common/cpu_frequency.h"
#include "src/interfaces/cgroup.h"
#include "src/interfaces/acct_gather_energy.h"
//#include "src/interfaces/mpi.h"
#include "src/common/fd.h"

typedef struct {
	uint64_t job_mem;
	slurm_step_id_t step_id;
	uint64_t step_mem;
} job_mem_limits_t;

typedef struct {
	bool batch_step;
	uint32_t job_id;
} active_job_t;

#define JOB_STATE_CNT 64
static active_job_t active_job_id[JOB_STATE_CNT] = {{0}};
static pthread_mutex_t prolog_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t job_limits_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t job_state_mutex   = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  job_state_cond    = PTHREAD_COND_INITIALIZER;

static pthread_mutex_t slurmd_full_mutex   = PTHREAD_MUTEX_INITIALIZER;

slurmd_conf_t *conf;
int devnull;
extern int send_slurmd_conf_lite(int fd, slurmd_conf_t *cf);

// TODO there seems to be memory corruption when our plugin does nothing on any of the callbacks,
// other than print some info string; investigate the issue

// TODO the switch/none plugin no longer exists, and the switch framework generates buffers
// for the messages from the scheduler to srun/drun.  We may need to replace the switch plugin
// with another one, to enable our extensions in slurmd, and avoid unexpected issues related
// to empty buffers (e.g. segfaults)

// copied from src/common/switch.c
// unfortunately, not exposed through a header
typedef struct slurm_switch_ops {
	uint32_t     (*plugin_id);
	int          (*state_save)        ( char *dir_name );
	int          (*state_restore)     ( char *dir_name, bool recover );

	int          (*alloc_jobinfo)     ( switch_jobinfo_t **jobinfo,
			uint32_t job_id, uint32_t step_id );
	int          (*build_jobinfo)     ( switch_jobinfo_t *jobinfo,
			slurm_step_layout_t *step_layout,
			step_record_t *step_ptr );
	int          (*duplicate_jobinfo) ( switch_jobinfo_t *source,
			switch_jobinfo_t **dest);
	void         (*free_jobinfo)      ( switch_jobinfo_t *jobinfo );
	int          (*pack_jobinfo)      ( switch_jobinfo_t *jobinfo,
			buf_t *buffer,
			uint16_t protocol_version );
	int          (*unpack_jobinfo)    ( switch_jobinfo_t **jobinfo,
			buf_t *buffer,
			uint16_t protocol_version );
	int          (*get_jobinfo)       ( switch_jobinfo_t *switch_job,
			int key, void *data);
	int          (*job_preinit)       ( stepd_step_rec_t *job );
	int          (*job_init)          ( stepd_step_rec_t *job );
	int          (*job_suspend_test)  ( switch_jobinfo_t *jobinfo );
	void         (*job_suspend_info_get)( switch_jobinfo_t *jobinfo,
			void *suspend_info );
	void         (*job_suspend_info_pack)( void *suspend_info,
			buf_t *buffer,
			uint16_t protocol_version );
	int          (*job_suspend_info_unpack)( void **suspend_info,
			buf_t *buffer,
			uint16_t protocol_version );
	void         (*job_suspend_info_free)( void *suspend_info );
	int          (*job_suspend)       ( void *suspend_info,
			int max_wait );
	int          (*job_resume)        ( void *suspend_info,
			int max_wait );
	int          (*job_fini)          ( switch_jobinfo_t *jobinfo );
	int          (*job_postfini)      ( stepd_step_rec_t *job);
	int          (*job_attach)        ( switch_jobinfo_t *jobinfo,
			char ***env, uint32_t nodeid,
			uint32_t procid, uint32_t nnodes,
			uint32_t nprocs, uint32_t rank);
	int          (*step_complete)     ( switch_jobinfo_t *jobinfo,
			char *nodelist );
	int          (*step_allocated)    ( switch_jobinfo_t *jobinfo,
			char *nodelist );
	int          (*state_clear)       ( void );
	int          (*reconfig)          ( void );
	int          (*job_step_pre_suspend)( stepd_step_rec_t *job );
	int          (*job_step_post_suspend)( stepd_step_rec_t *job );
	int          (*job_step_pre_resume)( stepd_step_rec_t *job );
	int          (*job_step_post_resume)( stepd_step_rec_t *job );
	void         (*job_complete)      ( uint32_t job_id );
} slurm_switch_ops_t;

static const char *syms[] = {
	"plugin_id",
	"switch_p_libstate_save",
	"switch_p_libstate_restore",
	"switch_p_alloc_jobinfo",
	"switch_p_build_jobinfo",
	"switch_p_duplicate_jobinfo",
	"switch_p_free_jobinfo",
	"switch_p_pack_jobinfo",
	"switch_p_unpack_jobinfo",
	"switch_p_get_jobinfo",
	"switch_p_job_preinit",
	"switch_p_job_init",
	"switch_p_job_suspend_test",
	"switch_p_job_suspend_info_get",
	"switch_p_job_suspend_info_pack",
	"switch_p_job_suspend_info_unpack",
	"switch_p_job_suspend_info_free",
	"switch_p_job_suspend",
	"switch_p_job_resume",
	"switch_p_job_fini",
	"switch_p_job_postfini",
	"switch_p_job_attach",
	"switch_p_job_step_complete",
	"switch_p_job_step_allocated",
	"switch_p_libstate_clear",
	"switch_p_reconfig",
	"switch_p_job_step_pre_suspend",
	"switch_p_job_step_post_suspend",
	"switch_p_job_step_pre_resume",
	"switch_p_job_step_post_resume",
	"switch_p_job_complete",
};

static slurm_switch_ops_t ops;
static plugin_context_t	*g_context = NULL;

const char plugin_name[] = "DEEP-SEA Switch plugin";
const char plugin_type[] = "switch/deepsea";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;
const uint32_t plugin_id      = 200;

dynpm_tbon_vertex_t *tbon_vertex = NULL;
int current_dynamic_pm_ready_count;

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

// this handler processes any incomming messages from down the hierarchy;
// any leaf process message (matching the tree degree at its max)
// is handled here
void _tbon_child_handler(void *conn, const void *read_buffer){
	pthread_mutex_lock(&slurmd_full_mutex);

	dynpm_client_to_server_connection_t *connection = (dynpm_client_to_server_connection_t *)conn;

	dynpm_header_t *header;
	dynpm_deserialize_header(&header, &read_buffer);

	info("====================");
	info("received header: source: %d, message type: %d", header->source_type, header->message_type);
	//info("bytes: %s", (const char*)read_buffer);

	if(header->source_type == NODE_MANAGER){
		info("from: NODE_MANAGER/SLURMD"); // currently only peers are leafs down the three
		if(header->message_type == READY_TBON){
			info("READY_TBON");

			assert(tbon_vertex != NULL);
			pthread_mutex_lock(&(tbon_vertex->mutex));
			tbon_vertex->ready_count++;
			info("%d/%d ready children at %s", tbon_vertex->ready_count,
					tbon_vertex->total_children, tbon_vertex->local_host);

			if(tbon_vertex->ready_count > tbon_vertex->total_children){
				error("received more READY_TBON messages than children");
			}

			if(tbon_vertex->ready_count == tbon_vertex->total_children){
				dynpm_header_t *ready_tbon_header = malloc(sizeof(dynpm_header_t));
				ready_tbon_header->source_type =  (dynpm_source_type)NODE_MANAGER;
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
			} else {
				info("still waiting for some children READY_TBON");
			}

			pthread_mutex_unlock(&(tbon_vertex->mutex));

		} else if(header->message_type == READY_DYNAMIC_PM){
			info("READY_DYNAMIC_PM");
			assert(tbon_vertex != NULL);

			pthread_mutex_lock(&(tbon_vertex->mutex));

			current_dynamic_pm_ready_count++;
			info("%d/%d ready dynamic pms", current_dynamic_pm_ready_count, tbon_vertex->total_children);

			if(current_dynamic_pm_ready_count == tbon_vertex->total_children){
				dynpm_header_t *ready_dynamic_pm_header = malloc(sizeof(dynpm_header_t));
				ready_dynamic_pm_header->source_type =  (dynpm_source_type)NODE_MANAGER;
				ready_dynamic_pm_header->message_type = (dynpm_message_type)READY_DYNAMIC_PM;

				dynpm_ready_dynamic_pm_msg_t *ready_dynamic_pm = malloc(sizeof(dynpm_ready_dynamic_pm_msg_t));
				ready_dynamic_pm->id = 0; // TODO this IDs need to be set properly, based on the initial launch
				ready_dynamic_pm->subtree_ready_count = current_dynamic_pm_ready_count;
				// TODO handle errors properly
				ready_dynamic_pm->error_code = 0;

				if(dynpm_send_message_to_client_connection(&ready_dynamic_pm_header, (void**)&ready_dynamic_pm,
							tbon_vertex->parent_connection)){
					error("error sending READY_DYNAMIC_PM to parent connection");
				} else {
					info("READY_DYNAMIC_PM sent to parent connection %x", tbon_vertex->parent_connection);
				}

				// resetting the dynamic launch counter
				current_dynamic_pm_ready_count = 0;
			} else {
				info("still waiting for some children READY_DYNAMIC_PM");
			}

			pthread_mutex_unlock(&(tbon_vertex->mutex));

		} else {
			error("unknown or unsupported message type tbon child listener");
		}
	} else {
		error("received tbon child message from a process other than NODE_MANAGER/SLURMD");
	}
	info("====================");

	dynpm_free_header(&header);
	pthread_mutex_unlock(&slurmd_full_mutex);
}

// TODO this is duplicade in mpi/deepsea
// TODO this needs to be extracted into libdynpm once we implement hostfile processing
static int _initialize_tbon_vertex(dynpm_server_to_client_connection_t *connection, dynpm_create_tbon_msg_t *create_tbon){
	if(tbon_vertex) {
		// TODO for node sharing scenarios, there may be more than 1
		info("existing tbon_vertex while trying to create a new one");
		dynpm_free_tbon_vertex(&tbon_vertex);
	} else {
		//info("tbon_vertex pointer was clean; creating a new one");
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
	dynpm_create_tbon_msg_t *create_tbon;
	dynpm_deserialize_create_tbon_msg(&create_tbon, &read_buffer);
	info("degree: %d; nodes: %s; parent connection: %x",
			create_tbon->degree, create_tbon->nodes, parent_connection);

	// first, initialize the local vertex (incl. children)
	if(_initialize_tbon_vertex(parent_connection, create_tbon)){
		error("failed to initialize tbon_vertex");
		return;
	}

	// first connect to the slurmd at the master host
	dynpm_process_conf_entry_t *slurmd_config_entry;
	if(dynpm_conf_get_process_config(&slurmd_config_entry, "slurmd" )){
		error("could not find slurmd's configuration");
		return;
	}
	char slurmd_port[32];
	sprintf(slurmd_port, "%d", slurmd_config_entry->port);

	for(int child_id = 0; child_id < tbon_vertex->degree &&
			tbon_vertex->children_ids[child_id] != -1; child_id++){
		info("sending tbon create to child %d", child_id);
		// create a child connection with a dependency to trigger cascading
		// disconnections when the root of the tbon (master node)
		// receives a disconnection from parent
		if(dynpm_connect_to_server(
					tbon_vertex->children_hosts[child_id],
					slurmd_port,
					_tbon_child_handler,
					NODE_MANAGER,
					parent_connection,
					&(tbon_vertex->children_connection_indexes[child_id]))){
			error("could not connect to child host %s",
					tbon_vertex->children_hosts[child_id]);
			return;
		}
	}

	dynpm_send_message_to_tbon_children(parent_connection, read_buffer,
				NODE_MANAGER, CREATE_TBON, tbon_vertex);

	if(tbon_vertex->total_children == 0){
		dynpm_header_t *ready_tbon_header = malloc(sizeof(dynpm_header_t));
		ready_tbon_header->source_type =  (dynpm_source_type)NODE_MANAGER;
		ready_tbon_header->message_type = (dynpm_message_type)READY_TBON;

		dynpm_ready_tbon_msg_t *ready_tbon = malloc(sizeof(dynpm_ready_tbon_msg_t));
		ready_tbon->id = create_tbon->id;
		ready_tbon->subtree_ready_count = 0; // no children to track
		ready_tbon->error_code = 0; // no children errors possible

		if( dynpm_send_message_to_client_connection(&ready_tbon_header, (void**)&ready_tbon, parent_connection)){
			error("error sending READY_TBON to parent connection");
		} else {
			info("READY_TBON sent to parent connection %x", parent_connection);
		}
	}

	dynpm_free_create_tbon_msg(&create_tbon);
}

static int _compare_starting_steps(void *listentry, void *key)
{
	slurm_step_id_t *step0 = (slurm_step_id_t *)listentry;
	slurm_step_id_t *step1 = (slurm_step_id_t *)key;

	/* If step1->step_id is NO_VAL then return for any step */
	if ((step1->step_id == NO_VAL) &&
	    (step0->job_id == step1->job_id)) {
		return 1;
	} else if (memcmp(step0, step1, sizeof(*step0)))
		return 0;
	else
		return 1;
}

static int
_remove_starting_step(uint16_t type, void *req)
{
	slurm_step_id_t starting_step;
	int rc = SLURM_SUCCESS;

	switch(type) {
	case LAUNCH_BATCH_JOB:
		starting_step.job_id =
			((batch_job_launch_msg_t *)req)->job_id;
		starting_step.step_id = SLURM_BATCH_SCRIPT;
		starting_step.step_het_comp = NO_VAL;
		break;
	case LAUNCH_TASKS:
		memcpy(&starting_step,
		       &((launch_tasks_request_msg_t *)req)->step_id,
		       sizeof(starting_step));
		break;
	default:
		error("%s called with an invalid type: %u", __func__, type);
		rc = SLURM_ERROR;
		goto fail;
	}

	if (!list_delete_all(conf->starting_steps,
			     _compare_starting_steps,
			     &starting_step)) {
		error("%s: %ps not found", __func__, &starting_step);
		rc = SLURM_ERROR;
	}
	slurm_cond_broadcast(&conf->starting_steps_cond);
fail:
	return rc;
}

static int
_add_starting_step(uint16_t type, void *req)
{
	slurm_step_id_t *starting_step;

	/* Add the step info to a list of starting processes that
	   cannot reliably be contacted. */
	starting_step = xmalloc(sizeof(slurm_step_id_t));

	switch (type) {
	case LAUNCH_BATCH_JOB:
		starting_step->job_id =
			((batch_job_launch_msg_t *)req)->job_id;
		starting_step->step_id = SLURM_BATCH_SCRIPT;
		starting_step->step_het_comp = NO_VAL;
		break;
	case LAUNCH_TASKS:
		memcpy(starting_step,
		       &((launch_tasks_request_msg_t *)req)->step_id,
		       sizeof(*starting_step));
		break;
	case REQUEST_LAUNCH_PROLOG:
		starting_step->job_id  = ((prolog_launch_msg_t *)req)->job_id;
		starting_step->step_id = SLURM_EXTERN_CONT;
		starting_step->step_het_comp = NO_VAL;
		break;
	default:
		error("%s called with an invalid type: %u", __func__, type);
		xfree(starting_step);
		return SLURM_ERROR;
	}

	list_append(conf->starting_steps, starting_step);

	return SLURM_SUCCESS;
}

// needs to be kept up to date with the version in:
// src/slurmd/slurmd/req.c
static int
_send_slurmstepd_init(int fd, int type, void *req,
		      slurm_addr_t *cli, slurm_addr_t *self,
		      hostlist_t *step_hset, uint16_t protocol_version)
{
	int len = 0;
	buf_t *buffer = NULL;
	slurm_msg_t msg;

	int rank;
	int parent_rank, children, depth, max_depth;
	char *parent_alias = NULL;
	slurm_addr_t parent_addr = {0};

	slurm_msg_t_init(&msg);

	/* send conf over to slurmstepd */
	if (send_slurmd_conf_lite(fd, conf) < 0)
		goto rwfail;

	/* send conf_hashtbl */
	// not found in 22.05
	//if (read_conf_send_stepd(fd))
		//goto rwfail;

	/* send type over to slurmstepd */
	safe_write(fd, &type, sizeof(int));

	/* step_hset can be NULL for batch scripts OR if the job was submitted
	 * by SlurmUser or root using the --no-allocate/-Z option and the job
	 * job credential validation by _check_job_credential() failed. If the
	 * job credential did not validate, then it did not come from slurmctld
	 * and there is no reason to send step completion messages to slurmctld.
	 */
	// TODO there seems to be an error in setup here, where the target
	// mpi/pmix plugin does not work with our drun/dstepd combination;
	// may skip this entirely once we add the dpmix plugin
	if (step_hset == NULL) {
		bool send_error = false;
		if (type == LAUNCH_TASKS) {
			launch_tasks_request_msg_t *launch_req = req;
			info("launch_req->step_id.step_id = %d", launch_req->step_id.step_id);
			info("launch_req->step_id.job_id = %d", launch_req->step_id.job_id);
			if (launch_req->step_id.step_id != SLURM_EXTERN_CONT)
				send_error = true;
		}
		if (send_error) {
			info("task rank unavailable due to invalid job "
			     "credential, step completion RPC impossible");
		}
		rank = -1;
		parent_rank = -1;
		children = 0;
		depth = 0;
		max_depth = 0;
	} else {
#ifndef HAVE_FRONT_END
		int count;
		count = hostlist_count(step_hset);
		rank = hostlist_find(step_hset, conf->node_name);
		reverse_tree_info(rank, count, REVERSE_TREE_WIDTH,
				  &parent_rank, &children,
				  &depth, &max_depth);

		if (children == -1) {
			error("reverse_tree_info: Sanity check fail, can't start job");
			goto rwfail;
		}
		/*
		 * rank 0 always talks directly to the slurmctld. If
		 * parent_rank = -1, all nodes talk to the slurmctld
		 */
		if (rank > 0 && parent_rank != -1) {
			int rc;
			/* Find the slurm_addr_t of this node's parent slurmd
			 * in the step host list */
			parent_alias = hostlist_nth(step_hset, parent_rank);
			rc = slurm_conf_get_addr(parent_alias, &parent_addr, 0);
			if (rc != SLURM_SUCCESS)
				error("%s: failed getting address for parent NodeName %s (parent rank %d)",
				      __func__, parent_alias, parent_rank);
		}
#else
		/* In FRONT_END mode, one slurmd pretends to be all
		 * NodeNames, so we can't compare conf->node_name
		 * to the NodeNames in step_hset.  Just send step complete
		 * RPC directly to the controller.
		 */
		rank = 0;
		parent_rank = -1;
		children = 0;
		depth = 0;
		max_depth = 0;
#endif
	}
	// TODO is this where we break mpi/pmix?
	info("--- dstepd rank %d (%s), parent rank %d (%s), "
	       "children %d, depth %d, max_depth %d",
	       rank, conf->node_name,
	       parent_rank, parent_alias ? parent_alias : "NONE",
	       children, depth, max_depth);
	if (parent_alias)
		free(parent_alias);

	// in deep-sea, we build our own tree
	// this one is disabled (matches dstepd read)
	/* send reverse-tree info to the slurmstepd */
	// TODO disable this once dpmix is up
	//safe_write(fd, &rank, sizeof(int));
	//safe_write(fd, &parent_rank, sizeof(int));
	//safe_write(fd, &children, sizeof(int));
	//safe_write(fd, &depth, sizeof(int));
	//safe_write(fd, &max_depth, sizeof(int));
	//safe_write(fd, &parent_addr, sizeof(slurm_addr_t));

	/* send cli address over to slurmstepd */
	buffer = init_buf(0);
	slurm_pack_addr(cli, buffer);
	len = get_buf_offset(buffer);
	safe_write(fd, &len, sizeof(int));
	safe_write(fd, get_buf_data(buffer), len);
	FREE_NULL_BUFFER(buffer);

	/* send self address over to slurmstepd */
	if (self) {
		buffer = init_buf(0);
		slurm_pack_addr(self, buffer);
		len = get_buf_offset(buffer);
		safe_write(fd, &len, sizeof(int));
		safe_write(fd, get_buf_data(buffer), len);
		FREE_NULL_BUFFER(buffer);

	} else {
		len = 0;
		safe_write(fd, &len, sizeof(int));
	}

	/* send cpu_frequency info to slurmstepd */
	cpu_freq_send_info(fd);

	/* send req over to slurmstepd */
	switch (type) {
	case LAUNCH_BATCH_JOB:
		msg.msg_type = REQUEST_BATCH_JOB_LAUNCH;
		break;
	case LAUNCH_TASKS:
		info("about to instruct REQUEST_LAUNCH_TASKS to dstepd");
		msg.msg_type = REQUEST_LAUNCH_TASKS;
		break;
	default:
		error("Was sent a task I didn't understand");
		break;
	}
	buffer = init_buf(0);
	msg.data = req;

	/* always force the RPC format to the latest */
	msg.protocol_version = SLURM_PROTOCOL_VERSION;
	pack_msg(&msg, buffer);
	len = get_buf_offset(buffer);

	/* send the srun protocol_version over, which may be older */
	safe_write(fd, &protocol_version, sizeof(uint16_t));
	info("WROTE to dstepd");

	safe_write(fd, &len, sizeof(int));
	safe_write(fd, get_buf_data(buffer), len);
	FREE_NULL_BUFFER(buffer);

	/*
	 * Send all secondary conf files to the stepd.
	 */

	/* send cgroup conf over to slurmstepd */
	if (cgroup_write_conf(fd) < 0)
		goto rwfail;

	/* send acct_gather.conf over to slurmstepd */
	if (acct_gather_write_conf(fd) < 0)
		goto rwfail;

	/* Send job_container information to slurmstepd */
	if (container_g_send_stepd(fd) != SLURM_SUCCESS)
		goto rwfail;

	/* Send GRES information to slurmstepd */
	gres_g_send_stepd(fd, &msg);

	/* Send mpi.conf over to slurmstepd */
	//if (type == LAUNCH_TASKS) {
	//	launch_tasks_request_msg_t *job = req;
	//	if ((job->step_id.step_id != SLURM_EXTERN_CONT) &&
	//	    (job->step_id.step_id != SLURM_INTERACTIVE_STEP)) {
	//		info("sending mpi_conf_send_stepd");
	//		if (mpi_conf_send_stepd(fd, job->mpi_plugin_id) !=
	//		    SLURM_SUCCESS)
	//			goto rwfail;
	//	}
	//}

	return 0;

rwfail:
	FREE_NULL_BUFFER(buffer);
	error("_send_slurmstepd_init failed");
	return errno;
}

static void _launch_complete_log(char *type, uint32_t job_id)
{
#if 0
	int j;

	info("active %s %u", type, job_id);
	slurm_mutex_lock(&job_state_mutex);
	for (j = 0; j < JOB_STATE_CNT; j++) {
		if (active_job_id[j].job_id != 0) {
			info("active_job_id[%d]=%u", j,
			     active_job_id[j].job_id);
		}
	}
	slurm_mutex_unlock(&job_state_mutex);
#endif
}

static uint32_t _kill_job(uint32_t job_id)
{
	return slurm_kill_job(job_id, SIGKILL, 0);
}


static int
_launch_job_fail(uint32_t job_id, uint32_t slurm_rc)
{
	struct requeue_msg req_msg = {0};
	slurm_msg_t resp_msg;
	int rc = 0, rpc_rc;

	slurm_msg_t_init(&resp_msg);

	if (slurm_rc == ESLURMD_CREDENTIAL_REVOKED)
		return _kill_job(job_id);

	/* Try to requeue the job. If that doesn't work, kill the job. */
	req_msg.job_id = job_id;
	req_msg.job_id_str = NULL;
	req_msg.flags = JOB_LAUNCH_FAILED;
	resp_msg.msg_type = REQUEST_JOB_REQUEUE;
	resp_msg.data = &req_msg;
	rpc_rc = slurm_send_recv_controller_rc_msg(&resp_msg, &rc,
						   working_cluster_rec);

	if ((rc == ESLURM_DISABLED) || (rc == ESLURM_BATCH_ONLY)) {
		info("Could not launch job %u and not able to requeue it, "
		     "cancelling job", job_id);

		if (slurm_rc == ESLURMD_PROLOG_FAILED) {
			/*
			 * Send the job's stdout a message, whether or not it's
			 * a batch job. ESLURM_DISABLED can take priority over
			 * ESLURM_BATCH_ONLY so we have no way to tell if it's
			 * a batch job or not.
			 */
			char *buf = NULL;
			xstrfmtcat(buf, "Prolog failure on node %s",
				   conf->node_name);
			slurm_notify_job(job_id, buf);
			xfree(buf);
		}
		rpc_rc = _kill_job(job_id);
	}

	return rpc_rc;
}

static void _launch_complete_add(uint32_t job_id, bool batch_step)
{
	int j, empty;

	slurm_mutex_lock(&job_state_mutex);
	empty = -1;
	for (j = 0; j < JOB_STATE_CNT; j++) {
		if (job_id == active_job_id[j].job_id) {
			if (batch_step)
				active_job_id[j].batch_step = batch_step;
			break;
		}
		if ((active_job_id[j].job_id == 0) && (empty == -1))
			empty = j;
	}
	if (j >= JOB_STATE_CNT || job_id != active_job_id[j].job_id) {
		if (empty == -1)	/* Discard oldest job */
			empty = 0;
		for (j = empty + 1; j < JOB_STATE_CNT; j++) {
			active_job_id[j - 1] = active_job_id[j];
		}
		active_job_id[JOB_STATE_CNT - 1].job_id = 0;
		active_job_id[JOB_STATE_CNT - 1].batch_step = false;
		for (j = 0; j < JOB_STATE_CNT; j++) {
			if (active_job_id[j].job_id == 0) {
				active_job_id[j].job_id = job_id;
				active_job_id[j].batch_step = batch_step;
				break;
			}
		}
	}
	slurm_cond_signal(&job_state_cond);
	slurm_mutex_unlock(&job_state_mutex);
	_launch_complete_log("job add", job_id);
}


// deep-sea: copied and modified from:
//  /src/slurmd/slurmd/req.c
//  original comments:
/*
 * Fork and exec the slurmstepd, then send the slurmstepd its
 * initialization data.  Then wait for slurmstepd to send an "ok"
 * message before returning.  When the "ok" message is received,
 * the slurmstepd has created and begun listening on its unix
 * domain socket.
 *
 * Note that this code forks twice and it is the grandchild that
 * becomes the slurmstepd process, so the slurmstepd's parent process
 * will be init, not slurmd.
 */
static int
_forkexec_slurmstepd(uint16_t type, void *req,
		     slurm_addr_t *cli, slurm_addr_t *self,
		     hostlist_t *step_hset, uint16_t protocol_version,
			 dynpm_server_to_client_connection_t *parent_connection)
{
	pid_t pid;
	int to_stepd[2] = {-1, -1};
	int to_slurmd[2] = {-1, -1};

	if (pipe(to_stepd) < 0 || pipe(to_slurmd) < 0) {
		error("%s: pipe failed: %m", __func__);
		return SLURM_ERROR;
	}

	if (_add_starting_step(type, req)) {
		error("%s: failed in _add_starting_step: %m", __func__);
		return SLURM_ERROR;
	}

	if ((pid = fork()) < 0) {
		error("%s: fork: %m", __func__);
		close(to_stepd[0]);
		close(to_stepd[1]);
		close(to_slurmd[0]);
		close(to_slurmd[1]);
		_remove_starting_step(type, req);
		return SLURM_ERROR;
	} else if (pid > 0) {
		int rc = SLURM_SUCCESS;
#if (SLURMSTEPD_MEMCHECK == 0)
		int i;
		time_t start_time = time(NULL);
#endif
		/*
		 * Parent sends initialization data to the slurmstepd
		 * over the to_stepd pipe, and waits for the return code
		 * reply on the to_slurmd pipe.
		 */
		if (close(to_stepd[0]) < 0)
			error("Unable to close read to_stepd in parent: %m");
		if (close(to_slurmd[1]) < 0)
			error("Unable to close write to_slurmd in parent: %m");

		if ((rc = _send_slurmstepd_init(to_stepd[1], type,
						req, cli, self,
						step_hset,
						protocol_version)) != 0) {
			error("Unable to init slurmstepd");
			goto done;
		}

		// deep-sea:
		// this tells us that the dstepd has started, and the mpi plugin
		// is ready (including its server)
		// it would have been nicer to read at the caller, after return,
		// but the pipes are closed by then
		int dynamic_pm_confirmation = 0;
		safe_read(to_slurmd[0], &dynamic_pm_confirmation, sizeof(int));
		if(dynamic_pm_confirmation == 9999){
			info("confirmed dstepd started");
		} else {
			info("dstepd NOT started; received: %d", dynamic_pm_confirmation);
			return SLURM_ERROR;
		}

		// deep-sea:
		// now that we are ready, notify the launcher
		if(tbon_vertex->total_children == 0){
			dynpm_header_t *ready_dynamic_pm_header = malloc(sizeof(dynpm_header_t));
			ready_dynamic_pm_header->source_type =  (dynpm_source_type)NODE_MANAGER;
			ready_dynamic_pm_header->message_type = (dynpm_message_type)READY_DYNAMIC_PM;

			dynpm_ready_dynamic_pm_msg_t *ready_dynamic_pm = malloc(sizeof(dynpm_ready_dynamic_pm_msg_t));
			ready_dynamic_pm->id = 0; // TODO this is NOT zero necessarily; the ID is received from the launch message
			ready_dynamic_pm->subtree_ready_count = 0; // no children to track
			ready_dynamic_pm->error_code = 0; // no children errors possible

			if( dynpm_send_message_to_client_connection(&ready_dynamic_pm_header, (void**)&ready_dynamic_pm, parent_connection)){
				error("error sending READY_DYNAMIC_PM to parent connection");
			} else {
				info("READY_DYNAMIC_PM sent to parent connection");
			}
		} else {
			info("not a leaf vertex; waiting before sending READY_DYNAMIC_PM");
		}

done:
		if (_remove_starting_step(type, req))
			error("Error cleaning up starting_step list");

		/* Reap child */
		if (waitpid(pid, NULL, 0) < 0)
			error("Unable to reap slurmd child process");
		if (close(to_stepd[1]) < 0)
			error("close write to_stepd in parent: %m");
		if (close(to_slurmd[0]) < 0)
			error("close read to_slurmd in parent: %m");
		return rc;

	} else {
		// deep-sea:
		// there was a bunch of debugging cruft that makes the code hard
		// to understand;  this has been removed
		/* no memory checking, default */

		// original: char *const argv[2] = { (char *)conf->stepd_loc, NULL};
		// this is where we exec into dstepd, instead of slurmstepd
		// the above string was the original
		char *const argv[2] = { "dstepd", NULL};
		int i;
		int failed = 0;

		/*
		 * Child forks and exits
		 */
		if (setsid() < 0) {
			error("%s: setsid: %m", __func__);
			failed = 1;
		}
		if ((pid = fork()) < 0) {
			error("%s: Unable to fork grandchild: %m", __func__);
			failed = 2;
		} else if (pid > 0) { /* child */
			_exit(0);
		}

		/*
		 * Just in case we (or someone we are linking to)
		 * opened a file and didn't do a close on exec.  This
		 * is needed mostly to protect us against libs we link
		 * to that don't set the flag as we should already be
		 * setting it for those that we open.  The number 256
		 * is an arbitrary number based off test7.9.
		 */
		for (i=3; i<256; i++) {
			(void) fcntl(i, F_SETFD, FD_CLOEXEC);
		}

		/*
		 * Grandchild exec's the slurmstepd
		 *
		 * If the slurmd is being shutdown/restarted before
		 * the pipe happens the old conf->lfd could be reused
		 * and if we close it the dup2 below will fail.
		 */
		if ((to_stepd[0] != conf->lfd)
		    && (to_slurmd[1] != conf->lfd))
			close(conf->lfd);

		if (close(to_stepd[1]) < 0)
			error("close write to_stepd in grandchild: %m");
		if (close(to_slurmd[0]) < 0)
			error("close read to_slurmd in parent: %m");

		(void) close(STDIN_FILENO); /* ignore return */
		if (dup2(to_stepd[0], STDIN_FILENO) == -1) {
			error("dup2 over STDIN_FILENO: %m");
			_exit(1);
		}
		fd_set_close_on_exec(to_stepd[0]);
		(void) close(STDOUT_FILENO); /* ignore return */
		if (dup2(to_slurmd[1], STDOUT_FILENO) == -1) {
			error("dup2 over STDOUT_FILENO: %m");
			_exit(1);
		}
		fd_set_close_on_exec(to_slurmd[1]);
		(void) close(STDERR_FILENO); /* ignore return */
		if (dup2(devnull, STDERR_FILENO) == -1) {
			error("dup2 /dev/null to STDERR_FILENO: %m");
			_exit(1);
		}
		fd_set_noclose_on_exec(STDERR_FILENO);
		log_fini();
		if (!failed) {
			execvp(argv[0], argv);
			error("exec of slurmstepd failed: %m");
		}
		_exit(2);
	}
rwfail:
	error("rwfail");
	return SLURM_ERROR;
}

static void _rpc_launch_tasks(slurm_msg_t *msg, dynpm_server_to_client_connection_t *parent_connection) {
	int      errnum = SLURM_SUCCESS;
	uint16_t port;
	char     host[HOST_NAME_MAX];
	launch_tasks_request_msg_t *req = msg->data;
	bool     mem_sort = false;
	bool     first_job_run;
	char *errmsg = NULL;

	slurm_addr_t self;
	slurm_addr_t *cli = &msg->orig_addr;
	hostlist_t *step_hset = NULL;
	job_mem_limits_t *job_limits_ptr;
	int node_id = 0;
	bitstr_t *numa_bitmap = NULL;

	node_id = nodelist_find(req->complete_nodelist, conf->node_name);
	memcpy(&req->orig_addr, &msg->orig_addr, sizeof(slurm_addr_t));

	if (req->step_id.step_id == SLURM_INTERACTIVE_STEP) {
		req->cpu_bind_type = CPU_BIND_NONE;
		xfree(req->cpu_bind);
		req->mem_bind_type = MEM_BIND_NONE;
		xfree(req->mem_bind);
	}

	if (node_id < 0) {
		info("%s: Invalid node list (%s not in %s)", __func__,
		     conf->node_name, req->complete_nodelist);
		errnum = ESLURM_INVALID_NODE_NAME;
		goto done;
	}

	if (first_job_run) {
		int rc;
		//job_env_t job_env;
		list_t *job_gres_list, *gres_prep_env_list;
		uint32_t jobid;

		slurm_cred_insert_jobid(conf->vctx, req->step_id.job_id);

		jobid = req->step_id.job_id;
		if (container_g_create(jobid, req->uid))
			error("container_g_create(%u): %m", req->step_id.job_id);
	}

	info("%s: call to _forkexec_slurmstepd", __func__);
	errnum = _forkexec_slurmstepd(LAUNCH_TASKS, (void *)req, cli, &self,
				      step_hset, msg->protocol_version, parent_connection);
	info("%s: return from _forkexec_slurmstepd", __func__);

	_launch_complete_add(req->step_id.job_id, false);

done:
	FREE_NULL_HOSTLIST(step_hset);
}

static void _dynamic_launch (dynpm_server_to_client_connection_t *parent_connection, const void *read_buffer){
	// TODO safely free memory here
	dynpm_dynamic_pm_launch_msg_t *dynamic_launch;

	// this is a broadcasted message
	dynpm_send_message_to_tbon_children(parent_connection, read_buffer,
				NODE_MANAGER, DYNAMIC_PM_LAUNCH, tbon_vertex);

	dynpm_deserialize_dynamic_pm_launch_msg(&dynamic_launch, &read_buffer);
	info("received: id: %d; type: %d; payload_bytes: %d;",
			dynamic_launch->id, dynamic_launch->type,
			dynamic_launch->payload_bytes);

	//for(int i = 0; i < 10; i++){ info("%2x", ((char*)(dynamic_launch->payload))[i]); }
	//info("%2x", ((char*)(dynamic_launch->payload))[dynamic_launch->payload_bytes -1]);

	current_dynamic_pm_ready_count = 0;
	//info("initialized current_dynamic_pm_ready_count = %d", current_dynamic_pm_ready_count);

	char *decompressed_buffer = malloc(64000);
	unsigned dlen = 64000;
	int error_code;
	//if(error_code = uncompress(decompressed_buffer, &dlen, (char*)dynamic_launch->payload,
	if(error_code = BZ2_bzBuffToBuffDecompress(decompressed_buffer, &dlen, (char*)dynamic_launch->payload,
				(unsigned long)dynamic_launch->payload_bytes, 0, 0)){
		error("could not uncompress: dlen = %d, error_code = %d", dlen, error_code);
		return;
	} else {
		info("uncompress success: dlen: %d", dlen);
	}

	// these are Slurm types
	buf_t *buffer;
	slurm_msg_t *msg = xmalloc(sizeof(slurm_msg_t));
	slurm_msg_t_init(msg);
	msg->msg_type = REQUEST_LAUNCH_TASKS;

	buffer = create_buf(decompressed_buffer, dlen);
	if(unpack_msg(msg, buffer) != SLURM_SUCCESS){
		error("failed to unpack slurm launch tasks message");
		return;
	}

	_rpc_launch_tasks(msg, parent_connection);

	//free(decompressed_buffer);
	//free_buf(buffer);
	//free(msg);
}

static void *_server_handler(dynpm_server_to_client_connection_t *connection, const void *read_buffer){
	pthread_mutex_lock(&slurmd_full_mutex);
	info("================================================================================");
	dynpm_header_t *header;
	dynpm_deserialize_header(&header, &read_buffer);
	info("header->source_type=%d", header->source_type);
	info("header->message_type=%d", header->message_type);
	//info("buffer: %x", (char*)read_buffer);

	if(header->message_type == (dynpm_message_type)CREATE_TBON){
		info("received CREATE_TBON");
		_create_tbon(connection, read_buffer);
	} else if(header->message_type == (dynpm_message_type)DYNAMIC_PM_LAUNCH){
		info("received DYNAMIC_PM_LAUNCH");
		_dynamic_launch(connection, read_buffer);
	} else {
		error("message not supported in _server_handler\n%s",
				(char*)read_buffer);
	}
	// we need to free the header in this case, since we don't send it
	dynpm_free_header(&header);
	info("================================================================================");
	pthread_mutex_unlock(&slurmd_full_mutex);

	return NULL;
}

extern int init(void) {
	int retval = SLURM_SUCCESS;
	// double the size here, since it's a concatenation of 2
	char plugin_type[CONFIG_MAX_STR_LEN*2] = "switch/";
	debug("%s loaded", plugin_name);

	// deep-sea change: slurmd initializes this plugin after xdaemon()
	if(!strcmp("slurmd", program_invocation_short_name)) {
		info("running from slurmd/switch; setting up server...");
		dynpm_process_conf_entry_t *slurmd_config_entry;

		if(dynpm_conf_get_process_config(&slurmd_config_entry, "slurmd")){
			error("could not find slurmd configuration");
			return SLURM_ERROR;
		}

		dynpm_client_init(slurm_info, slurm_verbose);

		if(dynpm_server_init(slurmd_config_entry->port,
					slurmd_config_entry->threads,
					_server_handler,
					"/var/log/slurmd.out", "/var/log/slurmd.err")){
			error("error setting up listener threads");
			return SLURM_ERROR;
		} else {
			debug("listener threads created in slurmd");
		}

	} else {
		info("slurmd/switch; loaded at %s", program_invocation_short_name);
	}

	// TODO need to check that there are no side effects of not loading anything
	// update from slurm-23.02 branch:
	// there is no longer a 'none' switch plugin, therefore, we don't wrap anything.

	//deepsea_plugin_conf_entry_t *config_entry;
	//if(deepsea_conf_get_plugin_config(&config_entry, "switch")){
	//	error("could not find switch plugin configuration");
	//	return SLURM_ERROR;
	//}
	//strcat(plugin_type, config_entry->plugin_name);
	//debug("wrapping %s", plugin_type);

	//// init() is called inside:
	//// ->plugin_load_and_link->plugin_load_from_file()->init()
	//g_context = plugin_context_create("switch",
	//		plugin_type,
	//		(void **) &ops, syms, sizeof(syms));

	//if (!g_context) {
	//	error("cannot create %s context for %s", "switch", plugin_type);
	//	retval = SLURM_ERROR;
	//	goto done;
	//}

done:
	return retval;
}

extern void fini(void) {
	// fini() on the loaded plugin should be called there
	// need init() about 23.02 series
	//if(plugin_context_destroy(g_context))
	//	error("while unloading switch/none plugin from wrapper");
}

extern int switch_p_reconfig ( void ) {
	//info("switch_p_reconfig");
	return SLURM_SUCCESS;
	return (*(ops.reconfig))();
}

int switch_p_libstate_save ( char * dir_name ) {
	//info("switch_p_libstate_save");
	return SLURM_SUCCESS;
	return (*(ops.state_save))(dir_name);
}

int switch_p_libstate_restore ( char * dir_name, bool recover ) {
	//info("switch_p_libstate_restore");
	return SLURM_SUCCESS;
	return (*(ops.state_restore))(dir_name, recover);
}

int switch_p_libstate_clear ( void ) {
	//info("switch_p_libstate_clear");
	return SLURM_SUCCESS;
	return (*(ops.state_clear))();
}

int switch_p_alloc_jobinfo (switch_jobinfo_t **switch_job,
		uint32_t job_id, uint32_t step_id ) {
	//info("switch_p_alloc_jobinfo");
	return SLURM_SUCCESS;
	return (*(ops.alloc_jobinfo))(switch_job, job_id, step_id);
}

extern int switch_p_build_jobinfo(switch_jobinfo_t *switch_job,
		slurm_step_layout_t *step_layout,
		step_record_t *step_ptr) {
	//info("switch_p_build_jobinfo");
	return SLURM_SUCCESS;
	return (*(ops.build_jobinfo))(switch_job, step_layout, step_ptr);
}

int switch_p_duplicate_jobinfo (switch_jobinfo_t *tmp,
		switch_jobinfo_t **dest) {
	//info("switch_p_duplicate_jobinfo");
	return SLURM_SUCCESS;
	return (*(ops.duplicate_jobinfo))(tmp, dest);
}

void switch_p_free_jobinfo ( switch_jobinfo_t *switch_job ) {
	//info("switch_p_free_jobinfo");
	return;
	(*(ops.free_jobinfo))(switch_job);
}

int switch_p_pack_jobinfo(switch_jobinfo_t *switch_job, buf_t *buffer,
		uint16_t protocol_version) {
	// TODO a simple print makes the system crash at slurmd; why?
	//info("switch_p_pack_jobinfo");
	return SLURM_SUCCESS;
	return (*(ops.pack_jobinfo))(switch_job, buffer, protocol_version);
}

int switch_p_unpack_jobinfo(switch_jobinfo_t **switch_job, buf_t *buffer,
		uint16_t protocol_version) {
	// TODO a simple print makes the system crash at slurmd; why?
	//info("switch_p_unpack_jobinfo");
	return SLURM_SUCCESS;
	return (*(ops.unpack_jobinfo))(switch_job, buffer, protocol_version);
}

int switch_p_job_preinit(stepd_step_rec_t *job) {
	//info("switch_p_job_preinit");
	return SLURM_SUCCESS;
	return (*(ops.job_preinit))(job);
}

extern int switch_p_job_init (stepd_step_rec_t *job) {
	//info("switch_p_job_init");
	return SLURM_SUCCESS;
	return (*(ops.job_init))(job);
}

extern int switch_p_job_suspend_test(switch_jobinfo_t *jobinfo) {
	//info("switch_p_job_suspend_test");
	return SLURM_SUCCESS;
	return (*(ops.job_suspend_test))(jobinfo);
}

extern void switch_p_job_suspend_info_get(switch_jobinfo_t *jobinfo,
		void **suspend_info) {
	//info("switch_p_job_suspend_info_get");
	return;
	(*(ops.job_suspend_info_get))(jobinfo, suspend_info);
}

extern void switch_p_job_suspend_info_pack(void *suspend_info, buf_t *buffer,
		uint16_t protocol_version) {
	//info("switch_p_job_suspend_info_pack");
	return;
	(*(ops.job_suspend_info_pack))(suspend_info, buffer, protocol_version);
}

extern int switch_p_job_suspend_info_unpack(void **suspend_info, buf_t *buffer,
		uint16_t protocol_version) {
	//info("switch_p_job_suspend_info_unpack");
	return SLURM_SUCCESS;
	return (*(ops.job_suspend_info_unpack))(suspend_info,
			buffer,protocol_version);
}

extern void switch_p_job_suspend_info_free(void *suspend_info) {
	//info("switch_p_job_suspend_info_free");
	return;
	(*(ops.job_suspend_info_free))(suspend_info);
}

extern int switch_p_job_suspend(void *suspend_info, int max_wait) {
	//info("switch_p_job_suspend");
	return SLURM_SUCCESS;
	return (*(ops.job_suspend))(suspend_info, max_wait);
}

extern int switch_p_job_resume(void *suspend_info, int max_wait) {
	//info("switch_p_job_resume");
	return SLURM_SUCCESS;
	return (*(ops.job_resume))(suspend_info, max_wait);
}

int switch_p_job_fini ( switch_jobinfo_t *jobinfo ) {
	//info("switch_p_job_fini");
	return SLURM_SUCCESS;
	return (*(ops.job_fini))(jobinfo);
}

int switch_p_job_postfini (stepd_step_rec_t *job) {
	//info("switch_p_job_postfini");
	return SLURM_SUCCESS;
	return (*(ops.job_postfini))(job);
}

int switch_p_job_attach ( switch_jobinfo_t *jobinfo, char ***env,
		uint32_t nodeid, uint32_t procid, uint32_t nnodes,
		uint32_t nprocs, uint32_t rank ) {
	//info("switch_p_job_attach");
	return SLURM_SUCCESS;
	return (*(ops.job_attach))(jobinfo, env, nodeid, procid,
			nnodes, nprocs, rank);
}

extern int switch_p_get_jobinfo(switch_jobinfo_t *switch_job,
		int key, void *resulting_data) {
	//info("switch_p_get_jobinfo");
	return SLURM_SUCCESS;
	return (*(ops.get_jobinfo))(switch_job, key, resulting_data);
}

extern int switch_p_job_step_complete(switch_jobinfo_t *jobinfo,
		char *nodelist) {
	//info("switch_p_job_step_complete");
	return SLURM_SUCCESS;
	return (*(ops.step_complete))(jobinfo, nodelist);
}

extern int switch_p_job_step_allocated(switch_jobinfo_t *jobinfo,
		char *nodelist) {
	//info("switch_p_job_step_allocated");
	return SLURM_SUCCESS;
	return (*(ops.step_allocated))(jobinfo, nodelist);
}

extern int switch_p_job_step_pre_suspend( stepd_step_rec_t *job ) {
	//info("switch_p_job_step_pre_suspend");
	return SLURM_SUCCESS;
	return (*(ops.job_step_pre_suspend))(job);
}

extern int switch_p_job_step_post_suspend( stepd_step_rec_t *job ) {
	//info("switch_p_job_step_post_suspend");
	return SLURM_SUCCESS;
	return (*(ops.job_step_post_suspend))(job);
}

extern int switch_p_job_step_pre_resume( stepd_step_rec_t *job ) {
	//info("switch_p_job_step_pre_resume");
	return SLURM_SUCCESS;
	return (*(ops.job_step_pre_resume))(job);
}

extern int switch_p_job_step_post_resume( stepd_step_rec_t *job ) {
	//info("switch_p_job_step_post_resume");
	return SLURM_SUCCESS;
	return (*(ops.job_step_post_resume))(job);
}

extern void switch_p_job_complete(uint32_t job_id) {
	//info("switch_p_job_complete");
	return;
	(*(ops.job_complete))(job_id);
}

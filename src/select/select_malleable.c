/*
 * Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *
 * Additional copyrights may follow
 *
 */

// slurm headers
#include "src/common/log.h"
#include "src/common/plugrack.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
// after 22.05
#include "src/interfaces/select.h"
// on 22.05 it is under common
//#include "src/common/select.h"

// deep-sea headers
#include "dynpm_config.h"

// copied from src/common/select.c
// unfortunately, not located in the header
static const char *syms[] = {
	"plugin_id",
	"select_p_state_save",
	"select_p_state_restore",
	"select_p_job_init",
	"select_p_node_init",
	"select_p_job_test",
	"select_p_job_begin",
	"select_p_job_ready",
	"select_p_job_expand",
	"select_p_job_resized",
	"select_p_job_fini",
	"select_p_job_suspend",
	"select_p_job_resume",
	"select_p_step_pick_nodes",
	"select_p_step_start",
	"select_p_step_finish",
	"select_p_select_nodeinfo_pack",
	"select_p_select_nodeinfo_unpack",
	"select_p_select_nodeinfo_alloc",
	"select_p_select_nodeinfo_free",
	"select_p_select_nodeinfo_set_all",
	"select_p_select_nodeinfo_set",
	"select_p_select_nodeinfo_get",
	"select_p_select_jobinfo_alloc",
	"select_p_select_jobinfo_free",
	"select_p_select_jobinfo_set",
	"select_p_select_jobinfo_get",
	"select_p_select_jobinfo_copy",
	"select_p_select_jobinfo_pack",
	"select_p_select_jobinfo_unpack",
	"select_p_get_info_from_plugin",
	"select_p_reconfigure",
	"select_p_resv_test",
};

// from src/common/select.h
static slurm_select_ops_t ops;
static plugin_context_t	*g_context = NULL;

const char plugin_name[] = "DEEP-SEA Selection plugin";
const char plugin_type[] = "select/deepsea";
const uint32_t plugin_id      = 200;
const uint32_t plugin_version = SLURM_VERSION_NUMBER;
const uint32_t pstate_version = 7;	/* version control on saved state */
const uint16_t nodeinfo_magic = 0x8a5d;

extern int init(void) {
	int retval = SLURM_SUCCESS;
	// double the size here, since it's a concatenation of 2
	char plugin_type[CONFIG_MAX_STR_LEN*2] = "select/";
	dynpm_plugin_conf_entry_t *config_entry;
	if(dynpm_conf_get_plugin_config(&config_entry, "select")){
		error("could not find select plugin configuration");
		return SLURM_ERROR;
	}
	strcat(plugin_type, config_entry->plugin_name);
	debug("wrapping %s", plugin_type);

	// init() is called inside:
	// ->plugin_load_and_link->plugin_load_from_file()->init()
	g_context = plugin_context_create("select",
			                  plugin_type,
					  (void **) &ops, syms, sizeof(syms));

	if (!g_context) {
		error("cannot create %s context for %s", "select", plugin_type);
		retval = SLURM_ERROR;
		goto done;
	}

done:
	return retval;
}

extern void fini(void) {
	// fini() on the loaded plugin should be called there
	if(plugin_context_destroy(g_context))
		error("while unloading select plugin from wrapper");
}

extern int select_p_state_save(char *dir_name) {
	return (*(ops.state_save))(dir_name);
}

extern int select_p_state_restore(char *dir_name) {
	return (*(ops.state_restore))(dir_name);
}

extern int select_p_job_init(List job_list) {
	return (*(ops.job_init))(job_list);
}

extern int select_p_node_init() {
	return (*(ops.node_init))();
}

extern int select_p_job_test(job_record_t *job_ptr,
		             bitstr_t *node_bitmap,
			     uint32_t min_nodes,
			     uint32_t max_nodes,
			     uint32_t req_nodes,
			     uint16_t mode,
			     List preemptee_candidates,
			     List *preemptee_job_list,
			     bitstr_t *exc_core_bitmap){
	return (*(ops.job_test))(job_ptr, node_bitmap, min_nodes,
			         max_nodes, req_nodes, mode,
				 preemptee_candidates, preemptee_job_list,
				 exc_core_bitmap);
}

extern int select_p_job_begin(job_record_t *job_ptr) {
	return (*(ops.job_begin))(job_ptr);
}

extern int select_p_job_ready(job_record_t *job_ptr) {
	return (*(ops.job_ready))(job_ptr);
}

extern int select_p_job_expand(job_record_t *from_job_ptr,
			       job_record_t *to_job_ptr) {
	return (*(ops.job_expand))(from_job_ptr, to_job_ptr);
}

extern int select_p_job_resized(job_record_t *job_ptr, node_record_t *node_ptr) {
	return (*(ops.job_resized))(job_ptr, node_ptr);
}

//extern int select_p_job_signal(job_record_t *job_ptr, int signal) {
//	return (*(ops.job_signal))(job_ptr, signal);
//}

extern int select_p_job_fini(job_record_t *job_ptr) {
	return (*(ops.job_fini))(job_ptr);
}

extern int select_p_job_suspend(job_record_t *job_ptr, bool indf_susp) {
	return (*(ops.job_suspend))(job_ptr, indf_susp);
}

extern int select_p_job_resume(job_record_t *job_ptr, bool indf_susp) {
	return (*(ops.job_resume))(job_ptr, indf_susp);
}

extern bitstr_t *select_p_step_pick_nodes(job_record_t *job_ptr,
					  select_jobinfo_t *jobinfo,
					  uint32_t node_count,
					  bitstr_t **avail_nodes) {
	return (*(ops.step_pick_nodes))(job_ptr,
					jobinfo,
					node_count,
					avail_nodes);
}

extern int select_p_step_start(step_record_t *step_ptr) {
	return (*(ops.step_start))(step_ptr);
}

extern int select_p_step_finish(step_record_t *step_ptr, bool killing_step) {
	return (*(ops.step_finish))(step_ptr, killing_step);
}

extern int select_p_select_nodeinfo_pack(select_nodeinfo_t *nodeinfo,
					 buf_t *buffer,
					 uint16_t protocol_version) {
	return (*(ops.nodeinfo_pack))(nodeinfo, buffer, protocol_version);
}

extern select_nodeinfo_t *select_p_select_nodeinfo_alloc(void) {
	return (*(ops.nodeinfo_alloc))();
}

extern int select_p_select_nodeinfo_free(select_nodeinfo_t *nodeinfo) {
	return (*(ops.nodeinfo_free))(nodeinfo);
}

extern int select_p_select_nodeinfo_unpack(select_nodeinfo_t **nodeinfo,
					   buf_t *buffer,
					   uint16_t protocol_version) {
	return (*(ops.nodeinfo_unpack))(nodeinfo, buffer, protocol_version);
}

extern int select_p_select_nodeinfo_set_all(void) {
	return (*(ops.nodeinfo_set_all))();
}

extern int select_p_select_nodeinfo_set(job_record_t *job_ptr) {
	return (*(ops.nodeinfo_set))(job_ptr);
}

extern int select_p_select_nodeinfo_get(select_nodeinfo_t *nodeinfo,
					enum select_nodedata_type dinfo,
					enum node_states state,
					void *data) {
	return (*(ops.nodeinfo_get))(nodeinfo, dinfo, state, data);
}

extern select_jobinfo_t *select_p_select_jobinfo_alloc(void) {
	return (*(ops.jobinfo_alloc))();
}

extern int select_p_select_jobinfo_free(select_jobinfo_t *jobinfo) {
	return (*(ops.jobinfo_free))(jobinfo);
}

extern int select_p_select_jobinfo_set(select_jobinfo_t *jobinfo,
				       enum select_jobdata_type data_type,
				       void *data) {
	return (*(ops.jobinfo_set))(jobinfo, data_type, data);
}

extern int select_p_select_jobinfo_get(select_jobinfo_t *jobinfo,
				       enum select_jobdata_type data_type,
				       void *data) {
	return (*(ops.jobinfo_set))(jobinfo, data_type, data);
}

extern select_jobinfo_t *select_p_select_jobinfo_copy(select_jobinfo_t *jobinfo) {
	return (*(ops.jobinfo_copy))(jobinfo);
}

extern int select_p_select_jobinfo_pack(select_jobinfo_t *jobinfo,
					buf_t *buffer,
					uint16_t protocol_version) {
	return (*(ops.jobinfo_pack))(jobinfo, buffer, protocol_version);
}

extern int select_p_select_jobinfo_unpack(select_jobinfo_t **jobinfo,
					  buf_t *buffer,
					  uint16_t protocol_version) {
	return (*(ops.jobinfo_unpack))(jobinfo, buffer, protocol_version);
}

//extern char *select_p_select_jobinfo_sprint(select_jobinfo_t *jobinfo,
//					    char *buf, size_t size, int mode) {
//	return (*(ops.jobinfo_sprint))(jobinfo, buf, size, mode);
//}

//extern char *select_p_select_jobinfo_xstrdup(select_jobinfo_t *jobinfo,
//					     int mode) {
//	return (*(ops.jobinfo_xstrdup))(jobinfo, mode);
//}

extern int select_p_get_info_from_plugin(enum select_plugindata_info info,
					 job_record_t *job_ptr,
					 void *data) {
	return (*(ops.get_info_from_plugin))(info, job_ptr, data);
}

//extern int select_p_update_node_config(int index) {
//	return (*(ops.update_node_config))(index);
//}

extern int select_p_reconfigure(void) {
	return (*(ops.reconfigure))();
}

extern bitstr_t *select_p_resv_test(resv_desc_msg_t *resv_desc_ptr,
				    uint32_t node_cnt,
				    bitstr_t *avail_node_bitmap,
				    bitstr_t **core_bitmap) {
	return (*(ops.resv_test))(resv_desc_ptr, node_cnt,
			          avail_node_bitmap, core_bitmap);
}

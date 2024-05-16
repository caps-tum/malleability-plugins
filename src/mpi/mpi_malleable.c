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
//#include "src/interfaces/mpi.h"
#include "src/api/step_launch.h"
#include "src/common/parse_config.h"
#include "src/slurmctld/slurmctld.h"

// deep-sea headers
#include "dynpm_network.h"
#include "dynpm_config.h"

// from src/common/slurm_mpi.c
// unfortunately, not exposed via a header
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
	"mpi_p_slurmstepd_task"
};

static slurm_mpi_ops_t ops;
static plugin_context_t	*g_context = NULL;

const char plugin_name[] = "DEEP-SEA PMIx plugin";
const char plugin_type[] = "mpi/deepsea";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;
const uint32_t plugin_id = 200;

static int wrapping_disabled = 0;
static pthread_mutex_t configuration_mutex = PTHREAD_MUTEX_INITIALIZER;

extern int init(void) {
	char plugin_type[CONFIG_MAX_STR_LEN*2] = "mpi/pmix";
	info("loading %s", plugin_type);

	pthread_mutex_lock(&configuration_mutex);

	// init() is called inside:
	// ->plugin_load_and_link->plugin_load_from_file()->init()
	g_context = plugin_context_create("mpi",
			plugin_type,
			(void **) &ops, syms, sizeof(syms));

	if (!g_context) {
		error("cannot create %s context for %s", "mpi", plugin_type);
	}

	pthread_mutex_unlock(&configuration_mutex);

	int retval = SLURM_SUCCESS;
done:
	return retval;
}

extern void fini(void) {
}

extern int mpi_p_slurmstepd_prefork(const stepd_step_rec_t *job, char ***env) {
	if(wrapping_disabled) {
		info("disabled");
		return 0;
	} else {
		info("wrapping");
		return (*(ops.slurmstepd_prefork))(job, env);
	}
}

extern int mpi_p_slurmstepd_task(const mpi_task_info_t *job, char ***env) {
	info("mpi_p_slurmstepd_task");
	if(wrapping_disabled) return 0;
	else return (*(ops.slurmstepd_task))(job, env);
}

extern mpi_plugin_client_state_t *mpi_p_client_prelaunch(
		const mpi_step_info_t *mpi_step, char ***env) {
	if(wrapping_disabled){
		info("disabled");
		return (void*)0xff; // the API expect non-null, but does not use the object...
	} else{
		info("wrapping");
		return (*(ops.client_prelaunch))(mpi_step, env);
	}
}

extern int mpi_p_client_fini(mpi_plugin_client_state_t *state) {
	if(wrapping_disabled){
		info("disabled");
		return 0;
	} else {
		info("wrapping");
		return (*(ops.client_fini))(state);
	}
}

extern void mpi_p_conf_options(s_p_options_t **full_options, int *full_opt_cnt) {
	info("mpi_p_conf_options");
	if(wrapping_disabled) return;
	else return (*(ops.conf_options))(full_options, full_opt_cnt);
}

extern void mpi_p_conf_set(s_p_hashtbl_t *tbl) {
	info("mpi_p_conf_set");
	if(wrapping_disabled) return;
	else return (*(ops.conf_set))(tbl);
}

extern s_p_hashtbl_t *mpi_p_conf_get(void) {
	info("mpi_p_conf_get");
	if(wrapping_disabled) return NULL;
	else return (*(ops.conf_get))();
}

extern List mpi_p_conf_get_printable(void) {
	info("mpi_p_conf_get_printable");
	if(wrapping_disabled) return NULL;
	else return (*(ops.conf_get_printable))();
}

// TODO this is not used, but leaving it here as reference to future
// extensions that do not belong to the slurm plugin api
extern int mpi_p_deepsea_disable_wrapping() {
	info("mpi_p_deepsea_disable_wrapping: extended deep-sea call");
	pthread_mutex_lock(&configuration_mutex);
	wrapping_disabled = 1;
	pthread_mutex_unlock(&configuration_mutex);
	info("mpi_p_deepsea_disable_wrapping: DISABLED PMI WRAPPING");
	return 0;
}

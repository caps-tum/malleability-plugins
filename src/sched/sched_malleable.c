/*
 * Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *
 * Additional copyrights may follow
 *
 */

#define _GNU_SOURCE
#include <pthread.h>

// slurm headers
#include "src/common/log.h"
#include "src/common/plugrack.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

// deep-sea headers
#include "malleability.h"

typedef struct {
	int (*reconfig)(void);
} plugin_ops_t;

static const char *syms[] = {
	"sched_p_reconfig",
};

static plugin_ops_t ops;
static plugin_context_t	*g_context = NULL;

const char plugin_name[] = "DEEP-SEA Scheduler plugin";
const char plugin_type[] = "sched/malleable";
const uint32_t plugin_version = SLURM_VERSION_NUMBER;

static pthread_t malleable_scheduler;

extern int init(void) {
	int retval = SLURM_SUCCESS;
	char thread_name[16]; // pthread limits this to 16
	// double the size here, since it's a concatenation of 2
	char plugin_type[CONFIG_MAX_STR_LEN*2] = "sched/";

	dynpm_plugin_conf_entry_t *config_entry;
	if(dynpm_conf_get_plugin_config(&config_entry, "sched")){
		error("could not find sched plugin configuration");
		return SLURM_ERROR;
	}
	strcat(plugin_type, config_entry->plugin_name);
	debug("wrapping %s", plugin_type);

	// init() is called inside: ->plugin_load_and_link->plugin_load_from_file()->init()
	g_context = plugin_context_create("sched",
			plugin_type,
			(void **) &ops, syms, sizeof(syms));

	if (!g_context) {
		error("cannot create %s context for %s", "sched", plugin_type);
		retval = SLURM_ERROR;
		goto done;
	}

	if(pthread_create(&malleable_scheduler, NULL, malleable_scheduler_thread, (void *) NULL)){
		error("failed to create malleable scheduler thread");
	}
	sprintf(thread_name, "mallsched");
	if(pthread_setname_np(malleable_scheduler, thread_name)){
		error("failed to set name of malleable scheduler thread");
	}
	if(pthread_detach(malleable_scheduler)){
		error("failed to detach malleable scheduler thread");
	}

done:
	return retval;
}

extern void fini(void) { }

extern int sched_p_reconfig(void) {
	return (*(ops.reconfig))();
}

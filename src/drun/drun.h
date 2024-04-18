/*
 * Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *
 * Additional copyrights may follow
 *
 */

#define _GNU_SOURCE
#include <pthread.h>

#include "slurm/slurm.h"
#include "src/common/read_config.h"
#include "src/srun/srun_job.h"
#include "src/interfaces/cred.h"

#include "dynpm_network.h"
#include "dynpm_config.h"

typedef struct drun_args {
	int argc;
	char **argv;
} drun_args_t;

// TODO needs to be an array or dynamic list
// since we will have multiple tbons during a transition
typedef struct drun_tbon_data_t {
	int started;
	int ready;
	char *master_host;
	char *port;
	int master_host_server_index;
	int node_count;
	char *nodes;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
} drun_tbon_data_t;

typedef struct drun_step_data {
	int launched; // TODO may need a list instead for hetjobs
	int ready;
	int registered;
	int job_index;
	int application_index;
	int malleability_mode;
	int node_count;
	int process_count;
	char *nodes;
	pthread_mutex_t mutex;
	pthread_cond_t  cond;
} drun_step_data_t;

void _slurmd_client_handler(void *, const void *);
void _dstepd_client_handler(void*, const void *);

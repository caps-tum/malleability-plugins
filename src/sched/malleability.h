/*
 * Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *
 * Additional copyrights may follow
 *
 */

#ifndef _PLUGINS_MALLEABILITY_H
#define _PLUGINS_MALLEABILITY_H

#include <float.h>

// slurm headers
// slurmctld
#include "src/slurmctld/locks.h"
#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/job_scheduler.h"
#include "src/slurmctld/acct_policy.h"
#include "src/slurmctld/gang.h"
#include "src/slurmctld/gres_ctld.h"
#include "src/slurmctld/reservation.h"
#include "src/slurmctld/fed_mgr.h"
#include "src/slurmctld/agent.h"
#include "src/slurmctld/node_scheduler.h"
// common
#include "src/common/parse_time.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/assoc_mgr.h"
#include "src/common/tres_bind.h"
#include "src/common/tres_frequency.h"
#include "src/common/uid.h"
// interfaces
//#include "src/interfaces/select.h"
#include "src/common/select.h"
//#include "src/interfaces/job_submit.h"
#include "src/slurmctld/job_submit.h"
//#include "src/interfaces/node_features.h"
#include "src/common/node_features.h"

// slurm headers added for simulation
//#include "src/slurmctld/job_mgr.h"

// deep-sea headers
#include "dynpm_network.h"
#include "dynpm_config.h"

#define MAX_OFFERS 100

// similar to node_space_map_t from backfill.c
// but handled with a different algorithm,
// and only needs to consider from time now, to the
// first reservation front (and not for the
// full backfilling time window).
//
// TODO need to include partition data here
// for T5.2 due M18 we will assume a single partition
// the partition can also be determined with the
// node records, instead of specified directly here
typedef struct node_time_gap {
	time_t start_time; // always now, needed?
	time_t end_time;
	bitstr_t *free_nodes;
	int is_last;
} node_time_gap_t;

// TODO we need to identify the minimum set of parameters here:
// 1.- Remove any gres/tres entries that are used during node selection:
//       We do not need to worry about the node selection process,
//       since we will scale node counts on partitions where the job
//       is already running, and therefore the gres/tres have already been
//       matched
// 2.- Keep any paraemters related to the mapping of tasks to resources:
//       These should be re-mappable via malleability operations, and
//       therefore shourd be a captured by the operation target
// 3.- Limit details to the job relevant parameters here:
//       Slurm captures too much data for a malleable implementation,
//       since this is more or less acceptable when jobs are launched
//       once, but with malleability, this can overload the scheduler
//       and should instead be delegated to the launcher and process
//       manager.
//
typedef struct malleable_operation_target {
	int started;
	int completed;
	uint16_t ntasks_per_node; // these need NO_VAL16
	uint16_t ntasks_per_socket;
	uint16_t threads_per_core;
	uint32_t req_switch; // as expected in job records (can be NO_VAL)
	uint32_t wait4switch;
	uint32_t time_limit;
	bitstr_t *node_bitmap;
	// TODO may need cond and/or mutex here
} malleable_operation_target_t;

/*
 * Node offers are rectangles, where the node count
 * is the base, and the time is implicit between
 * now and the job's own maximum runtime (as
 * specified by the user.
 *
 * This may change in the future, if we see the need
 * for node-time rectangles that are shorter in the
 * time dimension.  Such jobs should be capable of
 * using nodes opportunistically, able to take them
 * and release them in time.  Wether such use cases
 * are important or not, should be decided in WP1.
 * This flexibility will not be added until this
 * is clear.
 */
typedef struct {
	time_t end_time; // only for book-keeping for now
			 // end_time could be a part of
			 // the negotiation protocols
			 // in the final prototype
	int node_count;
	int job_id;
} deepsea_offer_t;

// extern data from the slurmctld host process
// from src/common/node_conf.c
extern node_record_t **node_record_table_ptr;
extern int node_record_count;
extern xhash_t* node_hash_table;
extern int node_record_table_size;
extern List config_list;

// from src/slurmctld/node_mgr.c
extern bitstr_t *avail_node_bitmap;
extern bitstr_t *up_node_bitmap;
extern bitstr_t *booting_node_bitmap;

// from src/slurmctld/partition_mgr.c
extern List part_list;

// from src/common/slurm_jobcomp.c
extern List job_list;

// from src/common/node_conf.c
extern List config_list;

extern pthread_rwlock_t slurmctld_locks[5];
extern pthread_mutex_t state_mutex;
extern pthread_mutex_t thread_flag_mutex;
extern pthread_mutex_t check_bf_running_lock;
extern time_t last_node_update;

void print_running_jobs();
void scan_running_jobs(List *, bitstr_t **);
void build_node_time_gap(node_time_gap_t **, time_t , bitstr_t *);
void generate_offers(deepsea_offer_t *, node_time_gap_t *, List, time_t);
void *malleable_scheduler_thread();
void *malleable_scheduler_simulator_thread();

// simulator routines
void backfill_init();
void backfill_fini();
void backfill_pass(time_t now);
void fifo_pass(time_t now);
void fifo_pass_immediate(time_t now);

// TODO
// check if we need this to sort the job queue,
// in the event that job_queues are necessary,
// instead of the job list directly
//
// refer to: src/slurmctld/job_scheduler.c
//extern void sort_job_queue(List job_queue);
//extern int sort_job_queue2(void *x, void *y);

#endif

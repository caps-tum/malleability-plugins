/*****************************************************************************\
 *  src/slurmd/slurmstepd/mgr.c - step manager functions for slurmstepd
 *****************************************************************************
 *  Copyright (C) 2002-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2009 Lawrence Livermore National Security.
 *  Copyright (C) 2010-2016 SchedMD LLC.
 *  Copyright (C) 2013      Intel, Inc.
 *  Copyright (c) 2022-2024 Technical University of Munich (TUM)
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Mark Grondona <mgrondona@llnl.gov>.
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

#define _GNU_SOURCE

#include "config.h"

#if HAVE_SYS_PRCTL_H
#  include <sys/prctl.h>
#endif

#include <grp.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#ifdef HAVE_PTY_H
#  include <pty.h>
#  ifdef HAVE_UTMP_H
#    include <utmp.h>
#  endif
#endif

#ifdef WITH_SELINUX
#include <selinux/selinux.h>
#endif

#include "slurm/slurm_errno.h"

#include "src/common/cbuf.h"
#include "src/common/cpu_frequency.h"
#include "src/common/env.h"
#include "src/common/fd.h"
#include "src/common/forward.h"
#include "src/common/hostlist.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/reverse_tree.h"
#include "src/common/spank.h"
#include "src/common/strlcpy.h"
#include "src/common/tres_frequency.h"
#include "src/common/util-net.h"
#include "src/common/x11_util.h"
#include "src/common/xmalloc.h"
#include "src/common/xsignal.h"
#include "src/common/xstring.h"

#include "src/interfaces/acct_gather_profile.h"
#include "src/interfaces/core_spec.h"
#include "src/interfaces/cred.h"
#include "src/interfaces/gres.h"
#include "src/interfaces/job_container.h"
#include "src/interfaces/jobacct_gather.h"
#include "src/interfaces/mpi.h"
#include "src/interfaces/proctrack.h"
#include "src/interfaces/switch.h"
#include "src/interfaces/task.h"

#include "src/slurmd/common/fname.h"
#include "src/slurmd/common/set_oomadj.h"
#include "src/slurmd/common/slurmd_cgroup.h"
#include "src/slurmd/common/xcpuinfo.h"

#include "src/slurmd/slurmd/slurmd.h"
#include "src/slurmd/slurmstepd/io.h"
#include "src/slurmd/slurmstepd/mgr.h"
#include "src/slurmd/slurmstepd/pam_ses.h"
#include "src/slurmd/slurmstepd/pdebug.h"
#include "src/slurmd/slurmstepd/req.h"
#include "src/slurmd/slurmstepd/slurmstepd.h"
#include "src/slurmd/slurmstepd/step_terminate_monitor.h"
#include "src/slurmd/slurmstepd/task.h"
#include "src/slurmd/slurmstepd/ulimits.h"
#include "src/slurmd/slurmstepd/x11_forwarding.h"

#define RETRY_DELAY 15		/* retry every 15 seconds */
#define MAX_RETRY   240		/* retry 240 times (one hour max) */

// deep-sea headers
#include "dynpm_network.h"
#include "dynpm_shm.h"
#include "pmix.h"
#include "dynpm_config.h"

// declared in slurmstepd.c
void *_slurmstepd_server_handler(dynpm_server_to_client_connection_t *connection, void *read_buffer);
extern dynpm_tbon_vertex_t *tbon_vertex;
extern int tbon_initialized;
extern pthread_mutex_t tbon_initialization_mutex;

struct priv_state {
	uid_t	saved_uid;
	gid_t	saved_gid;
	gid_t *	gid_list;
	int	ngids;
	char saved_cwd[PATH_MAX];
};

struct eio_handle_components {
	int  magic;
	int  fds[2];
	pthread_mutex_t shutdown_mutex;
	time_t shutdown_time;
	uint16_t shutdown_wait;
	List obj_list;
	List new_objs;
};

step_complete_t step_complete = {
	PTHREAD_COND_INITIALIZER,
	PTHREAD_MUTEX_INITIALIZER,
	-1,
	-1,
	-1,
	{},
	-1,
	-1,
	true,
	(bitstr_t *)NULL,
	0,
	NULL
};

typedef struct kill_thread {
	pthread_t thread_id;
	int       secs;
} kill_thread_t;

static pthread_t x11_signal_handler_thread = 0;
static int sig_array[] = {SIGTERM, 0};

/*
 * Prototypes
 */

/*
 * step manager related prototypes
 */
static bool _access(const char *path, int modes, uid_t uid,
		    int ngids, gid_t *gids);
static void _send_launch_failure(launch_tasks_request_msg_t *,
				 slurm_addr_t *, int, uint16_t);
static int  _fork_all_tasks(stepd_step_rec_t *step, bool *io_initialized);
static int  _become_user(stepd_step_rec_t *step, struct priv_state *ps);
static void  _set_prio_process (stepd_step_rec_t *step);
static int  _setup_normal_io(stepd_step_rec_t *step);
static int  _drop_privileges(stepd_step_rec_t *step, bool do_setuid,
			     struct priv_state *state, bool get_list);
static int  _reclaim_privileges(struct priv_state *state);
static void _send_launch_resp(stepd_step_rec_t *step, int rc);
static int  _slurmd_job_log_init(stepd_step_rec_t *step);
static void _wait_for_io(stepd_step_rec_t *step);
static int  _send_exit_msg(stepd_step_rec_t *step, uint32_t *tid, int n,
			   int status);
static void _wait_for_all_tasks(stepd_step_rec_t *step);
static int  _wait_for_any_task(stepd_step_rec_t *step, bool waitflag);

static void _random_sleep(stepd_step_rec_t *step);
static int  _run_script_as_user(const char *name, const char *path,
				stepd_step_rec_t *step,
				int max_wait, char **env);
static void _unblock_signals(void);
static void *_x11_signal_handler(void *arg);

/*
 * Batch step management prototypes:
 */
static char * _make_batch_dir(stepd_step_rec_t *step);
static int _make_batch_script(batch_job_launch_msg_t *msg,
			      stepd_step_rec_t *step);

/*
 * Launch an step step on the current node
 */
extern stepd_step_rec_t *
mgr_launch_tasks_setup(launch_tasks_request_msg_t *msg, slurm_addr_t *cli,
		       slurm_addr_t *self, uint16_t protocol_version)
{
	stepd_step_rec_t *step = NULL;

	if (!(step = stepd_step_rec_create(msg, protocol_version))) {
		/*
		 * We want to send back to the slurmd the reason we
		 * failed so keep track of it since errno could be
		 * reset in _send_launch_failure.
		 */
		int fail = errno;
		_send_launch_failure(msg, cli, errno, protocol_version);
		errno = fail;
		return NULL;
	}

	step->envtp->cli = cli;
	step->envtp->self = self;
	step->accel_bind_type = msg->accel_bind_type;
	step->tres_bind = xstrdup(msg->tres_bind);
	step->tres_freq = xstrdup(msg->tres_freq);

	return step;
}

inline static int
_send_srun_resp_msg(slurm_msg_t *resp_msg, uint32_t nnodes)
{
	int rc = SLURM_ERROR, retry = 0, max_retry = 0;
	unsigned long delay = 100000;

	/* NOTE: Wait until suspended job step is resumed or the RPC
	 * authentication credential from Munge may expire by the time
	 * it is resumed */
	wait_for_resumed(resp_msg->msg_type);
	while (1) {
		if (resp_msg->protocol_version >= SLURM_MIN_PROTOCOL_VERSION) {
			int msg_rc = 0;
			msg_rc = slurm_send_recv_rc_msg_only_one(resp_msg,
								 &rc, 0);
			/* Both must be zero for a successful transmission. */
			if (!msg_rc && !rc)
				break;
		} else {
			rc = SLURM_ERROR;
			break;
		}

		if (!max_retry)
			max_retry = (nnodes / 1024) + 5;

		debug("%s: %d/%d failed to send msg type %s: %m",
		      __func__, retry, max_retry,
		      rpc_num2string(resp_msg->msg_type));

		if (retry >= max_retry)
			break;

		usleep(delay);
		if (delay < 800000)
			delay *= 2;
		retry++;
	}
	return rc;
}

/* Find the maximum task return code */
static uint32_t _get_exit_code(stepd_step_rec_t *step) {
	uint32_t i;
	uint32_t step_rc = NO_VAL;

	/* We are always killing/cancelling the extern_step so don't
	 * report that.
	 */
	if (step->step_id.step_id == SLURM_EXTERN_CONT)
		return 0;

	for (i = 0; i < step->node_tasks; i++) {
		/* if this task was killed by cmd, ignore its
		 * return status as it only reflects the fact
		 * that we killed it
		 */
		if (step->task[i]->killed_by_cmd) {
			debug("get_exit_code task %u killed by cmd", i);
			continue;
		}
		/* if this task called PMI_Abort or PMI2_Abort,
		 * then we let it define the exit status
		 */
		if (step->task[i]->aborted) {
			step_rc = step->task[i]->estatus;
			debug("get_exit_code task %u called abort", i);
			break;
		}
		/* If signaled we need to cycle thru all the
		 * tasks in case one of them called abort
		 */
		if (WIFSIGNALED(step->task[i]->estatus)) {
			info("get_exit_code task %u died by signal: %d",
			     i, WTERMSIG(step->task[i]->estatus));
			step_rc = step->task[i]->estatus;
			break;
		}
		if ((step->task[i]->estatus & 0xff) == SIG_OOM) {
			step_rc = step->task[i]->estatus;
		} else if ((step_rc  & 0xff) != SIG_OOM) {
			step_rc = MAX(step_complete.step_rc,
				      step->task[i]->estatus);
		}
	}
	/* If we killed all the tasks by cmd give at least one return
	   code. */
	if (step_rc == NO_VAL && step->task[0])
		step_rc = step->task[0]->estatus;

	return step_rc;
}

static char *_batch_script_path(stepd_step_rec_t *step)
{
	return xstrdup_printf("%s/%s", step->batchdir, "slurm_script");
}


/*
 * Launch a batch job script on the current node
 */
stepd_step_rec_t *
mgr_launch_batch_job_setup(batch_job_launch_msg_t *msg, slurm_addr_t *cli)
{
	stepd_step_rec_t *step = NULL;

	if (!(step = batch_stepd_step_rec_create(msg))) {
		error("batch_stepd_step_rec_create() failed for job %u on %s: %s",
		      msg->job_id, conf->hostname, slurm_strerror(errno));
		return NULL;
	}

	if ((step->batchdir = _make_batch_dir(step)) == NULL) {
		goto cleanup;
	}

	xfree(step->argv[0]);

	if (_make_batch_script(msg, step))
		goto cleanup;

	env_array_for_batch_job(&step->env, msg, conf->node_name);

	return step;

cleanup:
	error("batch script setup failed for job %u on %s: %s",
	      msg->job_id, conf->hostname, slurm_strerror(errno));

	if (step->aborted)
		verbose("job %u abort complete", step->step_id.job_id);

	/* Do not purge directory until slurmctld is notified of batch job
	 * completion to avoid race condition with slurmd registering missing
	 * batch job. */
	if (step->batchdir && (rmdir(step->batchdir) < 0))
		error("rmdir(%s): %m",  step->batchdir);
	xfree(step->batchdir);

	errno = ESLURMD_CREATE_BATCH_DIR_ERROR;

	return NULL;
}

static int
_setup_normal_io(stepd_step_rec_t *step)
{
	// TODO need to replace the whole thing with a dynamic way
	int rc = 0, ii = 0;
	struct priv_state sprivs;

	//info("Entering _setup_normal_io");

	/*
	 * Temporarily drop permissions, initialize task stdio file
	 * descriptors (which may be connected to files), then
	 * reclaim privileges.
	 */
	if (_drop_privileges(step, true, &sprivs, true) < 0)
		return ESLURMD_SET_UID_OR_GID_ERROR;

	info("calling io_init_tasks_stdio");
	if (io_init_tasks_stdio(step) != SLURM_SUCCESS) {
		rc = ESLURMD_IO_ERROR;
		//info("going to claim");
		goto claim;
	}
	info("after io_init_tasks_stdio");

	/*
	 * MUST create the initial client object before starting
	 * the IO thread, or we risk losing stdout/err traffic.
	 */
	if (!step->batch) {
		//info("!step->batch");
		// TODO this is called in both batch and interactive launches
		// why?
		srun_info_t *srun = list_peek(step->sruns);

		/* local id of task that sends to srun, -1 for all tasks,
		   any other value for no tasks */
		int srun_stdout_tasks = -1;
		int srun_stderr_tasks = -1;

		xassert(srun != NULL);

		/* If I/O is labelled with task num, and if a separate file is
		   written per node or per task, the I/O needs to be sent
		   back to the stepd, get a label appended, and written from
		   the stepd rather than sent back to srun or written directly
		   from the node.  When a task has ofname or efname == NULL, it
		   means data gets sent back to the client. */
		if (step->flags & LAUNCH_LABEL_IO) {
			// deep-sea: we never observe this branch taken;
			// TODO safe to delete this and simplify?
			//info("step->flags & LAUNCH_LABEL_IO");
			slurmd_filename_pattern_t outpattern, errpattern;
			bool same = false;
			int file_flags;

			io_find_filename_pattern(step, &outpattern, &errpattern,
						 &same);
			file_flags = io_get_file_flags(step);

			/* Make eio objects to write from the slurmstepd */
			if (outpattern == SLURMD_ALL_UNIQUE) {
				/* Open a separate file per task */
				for (ii = 0; ii < step->node_tasks; ii++) {
					rc = io_create_local_client(
						step->task[ii]->ofname,
						file_flags, step, 1,
						step->task[ii]->id,
						same ? step->task[ii]->id : -2);
					if (rc != SLURM_SUCCESS) {
						error("Could not open output file %s: %m",
						      step->task[ii]->ofname);
						rc = ESLURMD_IO_ERROR;
						goto claim;
					}
				}
				srun_stdout_tasks = -2;
				if (same)
					srun_stderr_tasks = -2;
			} else if (outpattern == SLURMD_ALL_SAME) {
				/* Open a file for all tasks */
				rc = io_create_local_client(
					step->task[0]->ofname, file_flags,
					step, 1, -1, same ? -1 : -2);
				if (rc != SLURM_SUCCESS) {
					error("Could not open output file %s: %m",
					      step->task[0]->ofname);
					rc = ESLURMD_IO_ERROR;
					goto claim;
				}
				srun_stdout_tasks = -2;
				if (same)
					srun_stderr_tasks = -2;
			}

			if (!same) {
				if (errpattern == SLURMD_ALL_UNIQUE) {
					/* Open a separate file per task */
					for (ii = 0;
					     ii < step->node_tasks; ii++) {
						rc = io_create_local_client(
							step->task[ii]->efname,
							file_flags, step, 1,
							-2, step->task[ii]->id);
						if (rc != SLURM_SUCCESS) {
							error("Could not open error file %s: %m",
							      step->task[ii]->
							      efname);
							rc = ESLURMD_IO_ERROR;
							goto claim;
						}
					}
					srun_stderr_tasks = -2;
				} else if (errpattern == SLURMD_ALL_SAME) {
					/* Open a file for all tasks */
					rc = io_create_local_client(
						step->task[0]->efname,
						file_flags, step, 1, -2, -1);
					if (rc != SLURM_SUCCESS) {
						error("Could not open error file %s: %m",
						      step->task[0]->efname);
						rc = ESLURMD_IO_ERROR;
						goto claim;
					}
					srun_stderr_tasks = -2;
				}
			}
		}
	}

claim:
	if (_reclaim_privileges(&sprivs) < 0) {
		error("sete{u/g}id(%lu/%lu): %m",
		      (u_long) sprivs.saved_uid, (u_long) sprivs.saved_gid);
	}

	// deep-sea:
	// we are replacing the thread start with our own thread
	if (!rc && !step->batch)
		io_thread_start(step);

	// TODO maybe we do this at the caller
	//info("missing IO thread start");

	//info("Leaving  _setup_normal_io");
	return rc;
}

static void
_random_sleep(stepd_step_rec_t *step)
{
#if !defined HAVE_FRONT_END
	long int delay = 0;
	long int max = (slurm_conf.tcp_timeout * step->nnodes);

	max = MIN(max, 5000);
	srand48((long int) (step->step_id.job_id + step->nodeid));

	delay = lrand48() % ( max + 1 );
	debug3("delaying %ldms", delay);
	if (poll(NULL, 0, delay) == -1)
		error("%s: poll(): %m", __func__);
#endif
}

/*
 * Send task exit message for n tasks. tid is the list of _global_
 * task ids that have exited
 */
static int
_send_exit_msg(stepd_step_rec_t *step, uint32_t *tid, int n, int status)
{
	slurm_msg_t     resp;
	task_exit_msg_t msg;
	ListIterator    i       = NULL;
	srun_info_t    *srun    = NULL;

	debug3("sending task exit msg for %d tasks status %d oom %d",
	       n, status, step->oom_error);

	memset(&msg, 0, sizeof(msg));
	msg.task_id_list	= tid;
	msg.num_tasks		= n;
	if (step->oom_error)
		msg.return_code = SIG_OOM;
	else if (WIFSIGNALED(status) && (step->flags & LAUNCH_NO_SIG_FAIL))
		msg.return_code = SLURM_SUCCESS;
	else
		msg.return_code = status;

	memcpy(&msg.step_id, &step->step_id, sizeof(msg.step_id));

	slurm_msg_t_init(&resp);
	resp.data		= &msg;
	resp.msg_type		= MESSAGE_TASK_EXIT;

	/*
	 *  Hack for TCP timeouts on exit of large, synchronized step
	 *  termination. Delay a random amount if step->nnodes > 500
	 */
	if (step->nnodes > 500)
		_random_sleep(step);

	/*
	 * Notify each srun and sattach.
	 * No message for poe or batch steps
	 */
	i = list_iterator_create(step->sruns);
	while ((srun = list_next(i))) {
		resp.address = srun->resp_addr;
		if (slurm_addr_is_unspec(&resp.address))
			continue;	/* no srun or sattach here */

		/* This should always be set to something else we have a bug. */
		xassert(srun->protocol_version);
		resp.protocol_version = srun->protocol_version;
		slurm_msg_set_r_uid(&resp, srun->uid);

		if (_send_srun_resp_msg(&resp, step->nnodes) != SLURM_SUCCESS)
			error("Failed to send MESSAGE_TASK_EXIT: %m");
	}
	list_iterator_destroy(i);

	return SLURM_SUCCESS;
}

extern void stepd_wait_for_children_slurmstepd(stepd_step_rec_t *step)
{
	int left_bit = 0;
	int rc;
	struct timespec ts = {0, 0};

	slurm_mutex_lock(&step_complete.lock);

	/* wait an extra 3 seconds for every level of tree below this level */
	if (step_complete.bits && (step_complete.children > 0)) {
		ts.tv_sec += 3 * (step_complete.max_depth-step_complete.depth);
		ts.tv_sec += time(NULL) + REVERSE_TREE_CHILDREN_TIMEOUT;

		while((left_bit = bit_clear_count(step_complete.bits)) > 0) {
			debug3("Rank %d waiting for %d (of %d) children",
			       step_complete.rank, left_bit,
			       step_complete.children);
			rc = pthread_cond_timedwait(&step_complete.cond,
						    &step_complete.lock, &ts);
			if (rc == ETIMEDOUT) {
				debug2("Rank %d timed out waiting for %d"
				       " (of %d) children", step_complete.rank,
				       left_bit, step_complete.children);
				break;
			}
		}
		if (left_bit == 0) {
			debug2("Rank %d got all children completions",
			       step_complete.rank);
		}
	} else {
		debug2("Rank %d has no children slurmstepd",
		       step_complete.rank);
	}

	step_complete.step_rc = _get_exit_code(step);
	step_complete.wait_children = false;

	slurm_mutex_unlock(&step_complete.lock);
}

/*
 * Send a single step completion message, which represents a single range
 * of complete job step nodes.
 */
/* caller is holding step_complete.lock */
static void
_one_step_complete_msg(stepd_step_rec_t *step, int first, int last)
{
	slurm_msg_t req;
	step_complete_msg_t msg;
	int rc = -1;
	int retcode;
	int i;
	static bool acct_sent = false;

	debug2("_one_step_complete_msg: first=%d, last=%d", first, last);

	if (step->batch) {	/* Nested batch step anomalies */
		if (first == -1)
			first = 0;
		if (last == -1)
			last = 0;
	}
	memset(&msg, 0, sizeof(msg));

	memcpy(&msg.step_id, &step->step_id, sizeof(msg.step_id));

	msg.range_first = first;
	msg.range_last = last;
	if (step->oom_error)
		msg.step_rc = SIG_OOM;
	else
		msg.step_rc = step_complete.step_rc;
	msg.jobacct = jobacctinfo_create(NULL);
	slurm_msg_t_init(&req);
	slurm_msg_set_r_uid(&req, slurm_conf.slurmd_user_id);
	req.msg_type = REQUEST_STEP_COMPLETE;
	req.data = &msg;
	req.address = step_complete.parent_addr;

	/* Do NOT change this check to "step_complete.rank != 0", because
	 * there are odd situations where SlurmUser or root could
	 * craft a launch without a valid credential, and no tree information
	 * can be built with out the hostlist from the credential.
	 */
	if (step_complete.parent_rank != -1) {
		debug3("Rank %d sending complete to rank %d, range %d to %d",
		       step_complete.rank, step_complete.parent_rank,
		       first, last);
		/* On error, pause then try sending to parent again.
		 * The parent slurmstepd may just not have started yet, because
		 * of the way that the launch message forwarding works.
		 */
		for (i = 0; i < REVERSE_TREE_PARENT_RETRY; i++) {
			if (i)
				sleep(1);
			retcode = slurm_send_recv_rc_msg_only_one(&req, &rc, 0);
			if ((retcode == 0) && (rc == 0))
				goto finished;
		}
		/*
		 * On error AGAIN, send to the slurmctld instead.
		 * This is useful if parent_rank gave up waiting for us
		 * on stepd_wait_for_children_slurmstepd.
		 * If it's just busy handeling our prev messages we'll need
		 * to handle duplicated messages in both the parent and
		 * slurmctld.
		 */
		debug3("Rank %d sending complete to slurmctld instead, range "
		       "%d to %d", step_complete.rank, first, last);
	}  else {
		/* this is the base of the tree, its parent is slurmctld */
		debug3("Rank %d sending complete to slurmctld, range %d to %d",
		       step_complete.rank, first, last);
	}

	/* Retry step complete RPC send to slurmctld indefinitely.
	 * Prevent orphan job step if slurmctld is down */
	i = 1;
	while (slurm_send_recv_controller_rc_msg(&req, &rc,
						 working_cluster_rec) < 0) {
		if (i++ == 1) {
			error("Rank %d failed sending step completion message directly to slurmctld, retrying",
			      step_complete.rank);
		}
		sleep(60);
	}
	if (i > 1) {
		info("Rank %d sent step completion message directly to slurmctld",
		     step_complete.rank);
	}

finished:
	jobacctinfo_destroy(msg.jobacct);
}

/*
 * Given a starting bit in the step_complete.bits bitstring, "start",
 * find the next contiguous range of set bits and return the first
 * and last indices of the range in "first" and "last".
 *
 * caller is holding step_complete.lock
 */
static int
_bit_getrange(int start, int size, int *first, int *last)
{
	int i;
	bool found_first = false;

	if (!step_complete.bits)
		return 0;

	for (i = start; i < size; i++) {
		if (bit_test(step_complete.bits, i)) {
			if (found_first) {
				*last = i;
				continue;
			} else {
				found_first = true;
				*first = i;
				*last = i;
			}
		} else {
			if (!found_first) {
				continue;
			} else {
				*last = i - 1;
				break;
			}
		}
	}

	if (found_first)
		return 1;
	else
		return 0;
}

/*
 * Send as many step completion messages as necessary to represent
 * all completed nodes in the job step.  There may be nodes that have
 * not yet signaled their completion, so there will be gaps in the
 * completed node bitmap, requiring that more than one message be sent.
 */
extern void stepd_send_step_complete_msgs(stepd_step_rec_t *step)
{
	int start, size;
	int first = -1, last = -1;
	bool sent_own_comp_msg = false;

	slurm_mutex_lock(&step_complete.lock);
	start = 0;
	if (step_complete.bits)
		size = bit_size(step_complete.bits);
	else
		size = 0;

	/* If no children, send message and return early */
	if (size == 0) {
		_one_step_complete_msg(step, step_complete.rank,
				       step_complete.rank);
		slurm_mutex_unlock(&step_complete.lock);
		return;
	}

	while (_bit_getrange(start, size, &first, &last)) {
		/* THIS node is not in the bit string, so we need to prepend
		 * the local rank */
		if (start == 0 && first == 0) {
			sent_own_comp_msg = true;
			first = -1;
		}

		_one_step_complete_msg(step, (first + step_complete.rank + 1),
	      			       (last + step_complete.rank + 1));
		start = last + 1;
	}

	if (!sent_own_comp_msg) {
		_one_step_complete_msg(step, step_complete.rank,
				       step_complete.rank);
	}

	slurm_mutex_unlock(&step_complete.lock);
}

extern void set_job_state(stepd_step_rec_t *step, slurmstepd_state_t new_state)
{
	slurm_mutex_lock(&step->state_mutex);
	step->state = new_state;
	slurm_cond_signal(&step->state_cond);
	slurm_mutex_unlock(&step->state_mutex);
}

static bool _need_join_container()
{
	/*
	 * To avoid potential problems with the job_container/tmpfs and
	 * home_xauthority, don't join the container to create the xauthority
	 * file when it is set.
	 */
	if ((xstrcasestr(slurm_conf.job_container_plugin, "tmpfs")) &&
	    (!xstrcasestr(slurm_conf.x11_params, "home_xauthority"))) {
		return true;
	}

	return false;
}

static void _shutdown_x11_forward(stepd_step_rec_t *step)
{
	struct priv_state sprivs = { 0 };

	if (_drop_privileges(step, true, &sprivs, false) < 0) {
		error("%s: Unable to drop privileges", __func__);
		return;
	}

	if (shutdown_x11_forward(step) != SLURM_SUCCESS)
		error("%s: x11 forward shutdown failed", __func__);

	if (_reclaim_privileges(&sprivs) < 0)
		error("%s: Unable to reclaim privileges", __func__);
}

static void *_x11_signal_handler(void *arg)
{
	stepd_step_rec_t *step = (stepd_step_rec_t *) arg;
	int sig, status;
	sigset_t set;
	pid_t cpid, pid;

	(void) pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	(void) pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

	while (1) {
		xsignal_sigset_create(sig_array, &set);
		if (sigwait(&set, &sig) == EINTR)
			continue;

		switch (sig) {
		case SIGTERM:	/* kill -15 */
			debug("Terminate signal (SIGTERM) received");
			if (!_need_join_container()) {
				_shutdown_x11_forward(step);
				return NULL;
			}
			if ((cpid = fork()) == 0) {
				if (container_g_join(step->step_id.job_id,
						     step->uid) !=
				    SLURM_SUCCESS) {
					error("%s: cannot join container",
					      __func__);
					_exit(1);
				}
				_shutdown_x11_forward(step);
				_exit(0);
			} else if (cpid < 0) {
				error("%s: fork: %m", __func__);
			} else {
				pid = waitpid(cpid, &status, 0);
				if (pid < 0)
					error("%s: waitpid failed: %m",
					      __func__);
				else if (!WIFEXITED(status))
					error("%s: child terminated abnormally",
					      __func__);
				else if (WEXITSTATUS(status))
					error("%s: child returned non-zero",
					      __func__);
			}
			return NULL;
		default:
			error("Invalid signal (%d) received", sig);
		}
	}
}

static int _set_xauthority(stepd_step_rec_t *step)
{
	struct priv_state sprivs = { 0 };

	if (_drop_privileges(step, true, &sprivs, false) < 0) {
		error("%s: Unable to drop privileges before xauth", __func__);
		return SLURM_ERROR;
	}

	if (x11_set_xauth(step->x11_xauthority, step->x11_magic_cookie,
			  step->x11_display)) {
		error("%s: failed to run xauth", __func__);
		return SLURM_ERROR;
	}

	if (_reclaim_privileges(&sprivs) < 0) {
		error("%s: Unable to reclaim privileges after xauth", __func__);
		return SLURM_ERROR;
	}

	return SLURM_SUCCESS;
}

/*
 * Executes the functions of the slurmd job manager process,
 * which runs as root and performs shared memory and interconnect
 * initialization, etc.
 *
 * Returns 0 if job ran and completed successfully.
 * Returns errno if job startup failed. NOTE: This will DRAIN the node.
 */
int job_manager(stepd_step_rec_t *step) {

	int  rc = SLURM_SUCCESS;
	bool io_initialized = false;

	debug3("Entered job_manager for %ps pid=%d", &step->step_id, step->jmgr_pid);

	debug2("Before call to spank_init()");
	if (spank_init(step) < 0) {
		error ("Plugin stack initialization failed.");
		rc = SLURM_PLUGIN_NAME_INVALID;
		goto fail1;
	}
	debug2("After call to spank_init()");

	info("calling _fork_all_tasks");
	if ((rc = _fork_all_tasks(step, &io_initialized)) < 0) {
		info("_fork_all_tasks failed");
		rc = ESLURMD_EXECVE_FAILED;
		goto fail3;
	}
	info("after _fork_all_tasks");

	/*
	 * If IO initialization failed, return SLURM_SUCCESS (on a
	 * batch step) or the node will be drain otherwise.  Regular
	 * srun needs the error sent or it will hang waiting for the
	 * launch to happen.
	 */
	if ((rc != SLURM_SUCCESS) || !io_initialized){
		info("going to fail 3");
		goto fail3;
	}

	io_close_task_fds(step);

	set_job_state(step, SLURMSTEPD_STEP_RUNNING);

	info("before _wait_for_all_tasks");
	_wait_for_all_tasks(step);
	// TODO this is where dstepd waits for its local tasks
	info("after _wait_for_all_tasks");
	set_job_state(step, SLURMSTEPD_STEP_ENDING);

fail3:

fail2:
	set_job_state(step, SLURMSTEPD_STEP_ENDING);

	debug2("Before call to spank_fini()");
	if (spank_fini (step)  < 0) {
		error ("spank_fini failed");
	}
	debug2("After call to spank_fini()");

fail1:
	set_job_state(step, SLURMSTEPD_STEP_ENDING);
	if (rc != 0) {
		error("%s: exiting abnormally: %s",
		      __func__, slurm_strerror(rc));
		//_send_launch_resp(step, rc);
	}

	dynpm_tbon_children_done_wait_timeout(tbon_vertex, 10, 0);

	slurm_mutex_lock(&step->eio->shutdown_mutex);
	step->eio->shutdown_time = time(NULL);
	step->eio->shutdown_wait = 10; // TODO is no delay OK?
	//info("shutdown time: %d", step->eio->shutdown_time);
	slurm_mutex_unlock(&step->eio->shutdown_mutex);

	if (!step->batch && io_initialized) _wait_for_io(step);

	dynpm_header_t *processes_done_header = malloc(sizeof(dynpm_header_t));
	processes_done_header->source_type =  (dynpm_source_type)PROCESS_MANAGER;
	processes_done_header->message_type = (dynpm_message_type)PROCESSES_DONE;

	dynpm_ready_tbon_msg_t *processes_done = malloc(sizeof(dynpm_processes_done_msg_t));
	// TODO use id in the future, to support multiple tbon vertices at a single host
	// this is better to postpone until this code lives is libdynpm
	processes_done->id = tbon_vertex->id;
	processes_done->subtree_ready_count = tbon_vertex->ready_count;
	processes_done->error_code = 0;

	if(dynpm_send_message_to_client_connection(&processes_done_header, (void**)&processes_done,
				tbon_vertex->parent_connection)){
		error("error sending PROCESSES_DONE to parent connection %x", tbon_vertex->parent_connection);
	} else {
		info("PROCESSES_DONE sent to parent connection %x", tbon_vertex->parent_connection);
	}

	dynpm_tbon_finalized_wait_timeout(tbon_vertex, 10, 0);
	info("parent confirms. exiting...");

	if(dynpm_shm_manager_finalize()){
		error("failed to close shm");
	} else {
		info("shm closed");
	}

	info("finalize server...");
	if(dynpm_server_finalize()) error("dynpm_server_finalize failed");

	info("returning from job manager...");
	return(rc);
}

static int _pre_task_child_privileged(
	stepd_step_rec_t *step, int taskid, struct priv_state *sp)
{
	int setwd = 0; /* set working dir */
	int rc = 0;

	if (_reclaim_privileges(sp) < 0)
		return SLURM_ERROR;

	set_oom_adj(0); /* the tasks may be killed by OOM */

#ifndef HAVE_NATIVE_CRAY
	if (!(step->flags & LAUNCH_NO_ALLOC)) {
		/* Add job's pid to job container, if a normal job */
		if (container_g_join(step->step_id.job_id, step->uid)) {
			error("container_g_join failed: %u",
			      step->step_id.job_id);
			exit(1);
		}

		/*
		 * tmpfs job container plugin changes the working directory
		 * back to root working directory, so change it back to users
		 * but after dropping privillege
		 */
		setwd = 1;
	}
#endif

	if (spank_task_privileged(step, taskid) < 0)
		return error("spank_task_init_privileged failed");

	/* sp->gid_list should already be initialized */
	rc = _drop_privileges(step, true, sp, false);
	if (rc) {
		error ("_drop_privileges: %m");
		return rc;
	}

	if (step->container) {
		/* Container jobs must start in the correct directory */
		if (chdir(step->cwd) < 0) {
			error("couldn't chdir to `%s': %m", step->cwd);
			return errno;
		}
		debug2("%s: chdir(%s) success", __func__, step->cwd);
	} else if (setwd) {
		if (chdir(step->cwd) < 0) {
			error("couldn't chdir to `%s': %m: going to /tmp instead",
			      step->cwd);
			if (chdir("/tmp") < 0) {
				error("couldn't chdir to /tmp either. dying.");
				return SLURM_ERROR;
			}
		}
	}

	return rc;

}

struct exec_wait_info {
	int id;
	pid_t pid;
	int parentfd;
	int childfd;
};

static struct exec_wait_info * _exec_wait_info_create (int i)
{
	int fdpair[2];
	struct exec_wait_info * e;

	if (pipe2(fdpair, O_CLOEXEC) < 0) {
		error ("_exec_wait_info_create: pipe: %m");
		return NULL;
	}

	e = xmalloc (sizeof (*e));
	e->childfd = fdpair[0];
	e->parentfd = fdpair[1];
	e->id = i;
	e->pid = -1;

	return (e);
}

static void _exec_wait_info_destroy (struct exec_wait_info *e)
{
	if (e == NULL)
		return;

	if (e->parentfd >= 0) {
		close (e->parentfd);
		e->parentfd = -1;
	}
	if (e->childfd >= 0) {
		close (e->childfd);
		e->childfd = -1;
	}
	e->id = -1;
	e->pid = -1;
	xfree(e);
}

static pid_t _exec_wait_get_pid (struct exec_wait_info *e)
{
	if (e == NULL)
		return (-1);
	return (e->pid);
}

static struct exec_wait_info * _fork_child_with_wait_info (int id)
{
	struct exec_wait_info *e;

	if (!(e = _exec_wait_info_create (id)))
		return (NULL);

	if ((e->pid = fork ()) < 0) {
		_exec_wait_info_destroy (e);
		return (NULL);
	}
	/*
	 *  Close parentfd in child, and childfd in parent:
	 */
	if (e->pid == 0) {
		close (e->parentfd);
		e->parentfd = -1;
	} else {
		close (e->childfd);
		e->childfd = -1;
	}
	return (e);
}

static int _exec_wait_child_wait_for_parent (struct exec_wait_info *e)
{
	char c;

	if (read (e->childfd, &c, sizeof (c)) != 1)
		return error ("_exec_wait_child_wait_for_parent: failed: %m");

	return (0);
}

static int exec_wait_signal_child (struct exec_wait_info *e)
{
	char c = '\0';

	if (write (e->parentfd, &c, sizeof (c)) != 1)
		return error ("write to unblock task %d failed: %m", e->id);

	return (0);
}

static int exec_wait_signal (struct exec_wait_info *e, stepd_step_rec_t *step)
{
	debug3 ("Unblocking %ps task %d, writefd = %d",
		&step->step_id, e->id, e->parentfd);
	exec_wait_signal_child (e);
	return (0);
}

/*
 *  Send SIGKILL to child in exec_wait_info 'e'
 *  Returns 0 for success, -1 for failure.
 */
static int exec_wait_kill_child (struct exec_wait_info *e)
{
	if (e->pid < 0)
		return (-1);
	if (kill (e->pid, SIGKILL) < 0)
		return (-1);
	e->pid = -1;
	return (0);
}

/*
 *  Send all children in exec_wait_list SIGKILL.
 *  Returns 0 for success or  < 0 on failure.
 */
static int exec_wait_kill_children (List exec_wait_list)
{
	int rc = 0;
	int count;
	struct exec_wait_info *e;
	ListIterator i;

	if ((count = list_count (exec_wait_list)) == 0)
		return (0);

	verbose ("Killing %d remaining child%s",
		 count, (count > 1 ? "ren" : ""));

	i = list_iterator_create (exec_wait_list);
	if (i == NULL)
		return error ("exec_wait_kill_children: iterator_create: %m");

	while ((e = list_next (i)))
		rc += exec_wait_kill_child (e);
	list_iterator_destroy (i);
	return (rc);
}

static void _unblock_signals (void) {
	sigset_t set;
	int i;

	for (i = 0; slurmstepd_blocked_signals[i]; i++) {
		/* eliminate pending signals, then set to default */
		xsignal(slurmstepd_blocked_signals[i], SIG_IGN);
		xsignal(slurmstepd_blocked_signals[i], SIG_DFL);
	}
	sigemptyset(&set);
	xsignal_set_mask (&set);
}

static char *_build_path(char *fname, char **prog_env)
{
	char *path_env = NULL, *dir = NULL;
	char *file_name, *last = NULL;
	struct stat stat_buf;
	int len = PATH_MAX;

	if (!fname)
		return NULL;

	file_name = (char *) xmalloc(len);

	/* check if already absolute path */
	if (fname[0] == '/') {
		/* copy and ensure null termination */
		strlcpy(file_name, fname, len);
		return file_name;
	}

	if (fname[0] == '.') {
		dir = xmalloc(len);
		if (!getcwd(dir, len))
			error("getcwd failed: %m");
		snprintf(file_name, len, "%s/%s", dir, fname);
		xfree(dir);
		return file_name;
	}

	/* search for the file using PATH environment variable */
	path_env = xstrdup(getenvp(prog_env, "PATH"));
	if (path_env)
		dir = strtok_r(path_env, ":", &last);
	while (dir) {
		snprintf(file_name, len, "%s/%s", dir, fname);
		if ((stat(file_name, &stat_buf) == 0)
		    && (! S_ISDIR(stat_buf.st_mode)))
			break;
		dir = strtok_r(NULL, ":", &last);
	}
	if (dir == NULL)	/* not found */
		strlcpy(file_name, fname, len);

	xfree(path_env);
	return file_name;
}

static void _exec_task(stepd_step_rec_t *step, int local_proc_id) {
	int fd, j;
	stepd_step_task_info_t *task = step->task[local_proc_id];
	char **tmp_env;
	int saved_errno, status;
	uint32_t node_offset = 0, task_offset = 0;


	if (step->het_job_node_offset != NO_VAL)
		node_offset = step->het_job_node_offset;
	if (step->het_job_task_offset != NO_VAL)
		task_offset = step->het_job_task_offset;

	for (j = 0; j < step->node_tasks; j++)
		xstrfmtcat(step->envtp->sgtids, "%s%u", j ? "," : "",
			   step->task[j]->gtid + task_offset);

	if (step->het_job_id != NO_VAL)
		step->envtp->jobid = step->het_job_id;
	else
		step->envtp->jobid = step->step_id.job_id;
	step->envtp->stepid = step->step_id.step_id;
	step->envtp->nodeid = step->nodeid + node_offset;
	step->envtp->cpus_on_node = step->cpus;
	step->envtp->procid = task->gtid + task_offset;
	step->envtp->localid = task->id;
	step->envtp->task_pid = getpid();
	step->envtp->distribution = step->task_dist;
	step->envtp->cpu_bind = xstrdup(step->cpu_bind);
	step->envtp->cpu_bind_type = step->cpu_bind_type;
	step->envtp->cpu_freq_min = step->cpu_freq_min;
	step->envtp->cpu_freq_max = step->cpu_freq_max;
	step->envtp->cpu_freq_gov = step->cpu_freq_gov;
	step->envtp->mem_bind = xstrdup(step->mem_bind);
	step->envtp->mem_bind_type = step->mem_bind_type;
	step->envtp->distribution = -1;
	step->envtp->batch_flag = step->batch;
	step->envtp->uid = step->uid;
	step->envtp->job_end_time = step->job_end_time;
	step->envtp->job_licenses = xstrdup(step->job_licenses);
	step->envtp->job_start_time = step->job_start_time;
	step->envtp->user_name = xstrdup(step->user_name);

	step->envtp->env = env_array_copy((const char **) step->env);
	setup_env(step->envtp, false);
	setenvf(&step->envtp->env, "SLURM_JOB_GID", "%u", step->gid);
	setenvf(&step->envtp->env, "SLURMD_NODENAME", "%s", conf->node_name);
	if (step->tres_bind) {
		setenvf(&step->envtp->env, "SLURMD_TRES_BIND", "%s",
			step->tres_bind);
	}
	if (step->tres_freq) {
		setenvf(&step->envtp->env, "SLURMD_TRES_FREQ", "%s",
			step->tres_freq);
	}
	tmp_env = step->env;
	step->env = step->envtp->env;
	env_array_free(tmp_env);
	step->envtp->env = NULL;

	xfree(step->envtp->task_count);

	auth_setuid_unlock();
	if (spank_user_task(step, local_proc_id) < 0) {
		error("Failed to invoke spank plugin stack");
		_exit(1);
	}
	auth_setuid_lock();

	if (task->argv[0] == NULL) { error("No executable program specified for this task"); _exit(2); }

	if (*task->argv[0] != '/') { task->argv[0] = _build_path(task->argv[0], step->env); }

	/* Do this last so you don't worry too much about the users limits including the slurmstepd in with it. */
	set_user_limits(step, 0);

	execve(task->argv[0], task->argv, step->env);
	saved_errno = errno;

	/*
	 * print error message and clean up if execve() returns:
	 */
	if ((errno == ENOENT) &&
	    ((fd = open(task->argv[0], O_RDONLY)) >= 0)) {
		char buf[256], *eol;
		int sz;
		sz = read(fd, buf, sizeof(buf));
		if ((sz >= 3) && (xstrncmp(buf, "#!", 2) == 0)) {
			buf[sizeof(buf)-1] = '\0';
			eol = strchr(buf, '\n');
			if (eol)
				eol[0] = '\0';
			slurm_seterrno(saved_errno);
			error("execve(): bad interpreter(%s): %m", buf+2);
			_exit(errno);
		}
	}
	slurm_seterrno(saved_errno);
	error("execve(): %s: %m", task->argv[0]);
	_exit(errno);
}

/* fork and exec N tasks */
static int _fork_all_tasks(stepd_step_rec_t *step, bool *io_initialized) {
	int rc = SLURM_SUCCESS;
	int i;
	struct priv_state sprivs;
	List exec_wait_list = NULL;
	uint32_t jobid;
	uint32_t node_offset = 0, task_offset = 0;

	if (task_g_pre_setuid(step)) {
		error("Failed to invoke task plugins: one of task_p_pre_setuid functions returned error");
		return SLURM_ERROR;
	}

	set_umask(step);		/* set umask for stdout/err files */

	// TODO this IO routines needs a full replacement
	rc = _setup_normal_io(step);

	if (!rc) rc = _slurmd_job_log_init(step);

	if (rc) { error("%s: IO setup failed: %s", __func__, slurm_strerror(rc));
		if (step->batch) rc = SLURM_SUCCESS;	/* drains node otherwise */ goto fail1;
	} else { *io_initialized = true; }

	if (_drop_privileges (step, true, &sprivs, true) < 0) { error ("_drop_privileges: %m"); rc = SLURM_ERROR;
		goto fail2; }

	if (chdir(step->cwd) < 0) {
		error("couldn't chdir to `%s': %m: going to /tmp instead", step->cwd);
		if (chdir("/tmp") < 0) { error("couldn't chdir to /tmp either. dying."); rc = SLURM_ERROR; goto fail3; }
	}

	if (spank_user (step) < 0) { error("spank_user failed."); rc = SLURM_ERROR; goto fail4; }

	exec_wait_list = list_create ((ListDelF) _exec_wait_info_destroy);

	/* Fork all of the task processes. */
	verbose("starting %u tasks", step->node_tasks);
	for (i = 0; i < step->node_tasks; i++) {
		char time_stamp[256];
		pid_t pid;
		struct exec_wait_info *ei;

		acct_gather_profile_g_task_start(i);
		if ((ei = _fork_child_with_wait_info(i)) == NULL) { /* fail */
			error("child fork: %m");
			exec_wait_kill_children(exec_wait_list);
			rc = SLURM_ERROR;
			goto fail4;
		} else if ((pid = _exec_wait_get_pid(ei)) == 0) { /* child */

			if(dynpm_server_finalize()) error("dynpm_server_finalize failed");

			FREE_NULL_LIST(exec_wait_list);
			if (slurm_conf.propagate_prio_process) _set_prio_process(step);

			if (_pre_task_child_privileged(step, i, &sprivs) < 0) _exit(1);

 			if (_become_user(step, &sprivs) < 0) { error("_become_user failed: %m"); _exit(1); }

			_unblock_signals();

			io_dup_stdio(step->task[i]);

			// this blocks until the parent allows this to continue
			if (_exec_wait_child_wait_for_parent(ei) < 0) _exit(1);

			_exec_task(step, i);
		}

		/* Parent continues: */

		list_append(exec_wait_list, ei);

		log_timestamp(time_stamp, sizeof(time_stamp));
		verbose("task %lu (%lu) started %s",
			(unsigned long) step->task[i]->gtid + task_offset, (unsigned long) pid, time_stamp);

		step->task[i]->pid = pid;
		if (i == 0) step->pgid = pid;
	}

	/* All tasks are now forked and running as the user, but will wait for our signal before calling exec. */

	// begin deep-sea shm setup

	// we need to build a pid array
	info("creating pid table for %d tasks in this node", step->node_tasks);
	uint32_t *pid_array = malloc(sizeof(uint32_t)*(step->node_tasks));
	for(int i = 0; i < step->node_tasks; i++){
		pid_array[i] = step->task[i]->pid;
	}

	int shm_rt = dynpm_shm_manager_init(step->step_id.job_id, step->step_id.step_id,
			1, 1, pid_array, step->node_tasks);
	if(shm_rt){
		error("failed to initialize dynpm shared memory: %d", shm_rt);
		exit(-1);
	} else {
		info("shared memory created in slurmstepd");
	}
	free(pid_array);

	char *hello = "hello dynpm\n\0  ";

	info("about to write to shm");
	if(dynpm_shm_manager_write((const void *)hello, strlen(hello))){
		info("unable to write the first time");
	} else info("wrote to shm");
	if(dynpm_shm_manager_write((const void *)hello, strlen(hello))){
		info("unable to write the second time");
	} else info("wrote repeat to shm");
	// end of deep-sea shm setup

	int pmix_error = dynpm_pmix_server_init();
	if(pmix_error){
		info("error initializing the pmix server: %d (%s)", pmix_error, PMIx_Error_string(pmix_error));
	} else info("pmix server initialized");

	if (_reclaim_privileges(&sprivs) < 0) { error ("Unable to reclaim privileges"); }

	if (chdir(sprivs.saved_cwd) < 0) { error ("Unable to return to working directory"); }

	for (i = 0; i < step->node_tasks; i++) {
		if (((step->flags & LAUNCH_PTY) == 0) && (setpgid (step->task[i]->pid, step->pgid) < 0)) {
			error("Unable to put task %d (pid %d) into pgrp %d: %m", i, step->task[i]->pid, step->pgid);
		}

		if (spank_task_post_fork (step, i) < 0) {
			error ("spank task %d post-fork failed", i);
			rc = SLURM_ERROR;
			goto fail2;
		}
	}

	jobid = step->step_id.job_id;

	/* Now it's ok to unblock the tasks, so they may call exec. */
	info("dstepd done setting up; UNBLOCKING the tasks...");
	list_for_each (exec_wait_list, (ListForF) exec_wait_signal, step);
	FREE_NULL_LIST (exec_wait_list);

	return rc;

fail4:
	if (chdir (sprivs.saved_cwd) < 0) {
		error ("Unable to return to working directory");
	}
fail3:
	_reclaim_privileges (&sprivs);
fail2:
	FREE_NULL_LIST(exec_wait_list);
	io_close_task_fds(step);
fail1:
	return rc;
}

/*
 * Loop once through tasks looking for all tasks that have exited with
 * the same exit status (and whose statuses have not been sent back to
 * the client) Aggregate these tasks into a single task exit message.
 *
 */
extern int stepd_send_pending_exit_msgs(stepd_step_rec_t *step)
{
	int  i;
	int  nsent  = 0;
	int  status = 0;
	bool set    = false;
	uint32_t *tid;

	/*
	 * Collect all exit codes with the same status into a
	 * single message.
	 */
	tid = xmalloc(sizeof(uint32_t) * step->node_tasks);
	for (i = 0; i < step->node_tasks; i++) {
		stepd_step_task_info_t *t = step->task[i];

		if (!t->exited || t->esent)
			continue;

		if (!set) {
			status = t->estatus;
			set    = true;
		} else if (status != t->estatus)
			continue;

		tid[nsent++] = t->gtid;
		t->esent = true;
	}

	if (nsent) {
		debug2("Aggregated %d task exit messages", nsent);
		_send_exit_msg(step, tid, nsent, status);
	}
	xfree(tid);

	return nsent;
}

static inline void
_log_task_exit(unsigned long taskid, unsigned long pid, int status)
{
	/*
	 *  Print a nice message to the log describing the task exit status.
	 *
	 *  The final else is there just in case there is ever an exit status
	 *   that isn't WIFEXITED || WIFSIGNALED. We'll probably never reach
	 *   that code, but it is better than dropping a potentially useful
	 *   exit status.
	 */
	if ((status & 0xff) == SIG_OOM) {
		verbose("task %lu (%lu) Out Of Memory (OOM)",
			taskid, pid);
	} else if (WIFEXITED(status)) {
		verbose("task %lu (%lu) exited with exit code %d.",
			taskid, pid, WEXITSTATUS(status));
	} else if (WIFSIGNALED(status)) {
		/* WCOREDUMP isn't available on AIX */
		verbose("task %lu (%lu) exited. Killed by signal %d%s.",
			taskid, pid, WTERMSIG(status),
#ifdef WCOREDUMP
			WCOREDUMP(status) ? " (core dumped)" : ""
#else
			""
#endif
			);
	} else {
		verbose("task %lu (%lu) exited with status 0x%04x.",
			taskid, pid, status);
	}
}

/*
 * If waitflag is true, perform a blocking wait for a single process
 * and then return.
 *
 * If waitflag is false, do repeated non-blocking waits until
 * there are no more processes to reap (waitpid returns 0).
 *
 * Returns the number of tasks for which a wait3() was successfully
 * performed, or -1 if there are no child tasks.
 */
static int
_wait_for_any_task(stepd_step_rec_t *step, bool waitflag)
{
	stepd_step_task_info_t *t = NULL;
	int rc, status = 0;
	pid_t pid;
	int completed = 0;
	struct rusage rusage;
	char **tmp_env;
	uint32_t task_offset = 0;

	if (step->het_job_task_offset != NO_VAL)
		task_offset = step->het_job_task_offset;
	do {
		pid = wait3(&status, waitflag ? 0 : WNOHANG, &rusage);
		if (pid == -1) {
			if (errno == ECHILD) {
				debug("No child processes");
				if (completed == 0)
					completed = -1;
				break;
			} else if (errno == EINTR) {
				debug("wait3 was interrupted");
				continue;
			} else {
				debug("Unknown errno %d", errno);
				continue;
			}
		} else if (pid == 0) { /* WNOHANG and no pids available */
			break;
		}

		if ((t = job_task_info_by_pid(step, pid))) {
			completed++;
			_log_task_exit(t->gtid + task_offset, pid, status);
			t->exited  = true;
			t->estatus = status;
			step->envtp->procid = t->gtid + task_offset;
			step->envtp->localid = t->id;
			step->envtp->distribution = -1;
			step->envtp->batch_flag = step->batch;
			step->envtp->uid = step->uid;
			step->envtp->user_name = xstrdup(step->user_name);
			step->envtp->nodeid = step->nodeid;

			/*
			 * Modify copy of job's environment. Do not alter in
			 * place or concurrent searches of the environment can
			 * generate invalid memory references.
			 */
			step->envtp->env =
				env_array_copy((const char **) step->env);
			setup_env(step->envtp, false);
			tmp_env = step->env;
			step->env = step->envtp->env;
			env_array_free(tmp_env);

			setenvf(&step->env, "SLURM_SCRIPT_CONTEXT",
				"epilog_task");
			setenvf(&step->env, "SLURMD_NODENAME", "%s",
				conf->node_name);

			if (step->task_epilog) {
				rc = _run_script_as_user("user task_epilog",
							 step->task_epilog,
							 step, 5, step->env);
				if (rc)
					error("TaskEpilog failed status=%d",
					      rc);
			}
			if (slurm_conf.task_epilog) {
				rc = _run_script_as_user("slurm task_epilog",
							 slurm_conf.task_epilog,
							 step, -1, step->env);
				if (rc)
					error("--task-epilog failed status=%d",
					      rc);
			}

			if (spank_task_exit(step, t->id) < 0) {
				error ("Unable to spank task %d at exit",
				       t->id);
			}
			rc = task_g_post_term(step, t);
			if (rc == ENOMEM)
				step->oom_error = true;
		}

	} while ((pid > 0) && !waitflag);

	return completed;
}

static void
_wait_for_all_tasks(stepd_step_rec_t *step)
{
	int tasks_left = 0;
	int i;

	for (i = 0; i < step->node_tasks; i++) {
		if (step->task[i]->state < STEPD_STEP_TASK_COMPLETE) {
			tasks_left++;
		}
	}
	if (tasks_left < step->node_tasks)
		verbose("Only %d of %d requested tasks successfully launched",
			tasks_left, step->node_tasks);

	for (i = 0; i < tasks_left; ) {
		int rc;
		rc = _wait_for_any_task(step, true);
		if (rc != -1) {
			i += rc;
			if (i < tasks_left) {
				/* To limit the amount of traffic back
				 * we will sleep a bit to make sure we
				 * have most if not all the tasks
				 * completed before we return */
				usleep(100000);	/* 100 msec */
				rc = _wait_for_any_task(step, false);
				if (rc != -1)
					i += rc;
			}
		}

		if (i < tasks_left) {
			/* Send partial completion message only.
			 * The full completion message can only be sent
			 * after resetting CPU frequencies */
			while (stepd_send_pending_exit_msgs(step)) {;}
		}
	}
}

/*
 * Wait for IO
 */
static void
_wait_for_io(stepd_step_rec_t *step)
{
	debug("Waiting for IO");
	io_close_all(step);

	slurm_mutex_lock(&step->io_mutex);
	if (step->io_running) {
		/*
		 * Give the I/O thread up to 300 seconds to cleanup before
		 * continuing with shutdown. Note that it is *not* safe to
		 * try to kill that thread if it's still running - it could
		 * be holding some internal locks which could lead to deadlock
		 * on step teardown, which is infinitely worse than letting
		 * that thread attempt to continue as we quickly head towards
		 * the process exiting anyways.
		 */
		struct timespec ts = { 0, 0 };
		ts.tv_sec = time(NULL) + 300;
		slurm_cond_timedwait(&step->io_cond, &step->io_mutex, &ts);
	}
	slurm_mutex_unlock(&step->io_mutex);

	/* Close any files for stdout/stderr opened by the stepd */
	io_close_local_fds(step);

	return;
}


static char *
_make_batch_dir(stepd_step_rec_t *step)
{
	char path[PATH_MAX];

	if (step->step_id.step_id == SLURM_BATCH_SCRIPT)
		snprintf(path, sizeof(path), "%s/job%05u",
			 conf->spooldir, step->step_id.job_id);
	else {
		snprintf(path, sizeof(path), "%s/job%05u.%05u",
			 conf->spooldir, step->step_id.job_id,
			 step->step_id.step_id);
	}

	if ((mkdir(path, 0750) < 0) && (errno != EEXIST)) {
		error("mkdir(%s): %m", path);
		if (errno == ENOSPC)
			stepd_drain_node("SlurmdSpoolDir is full");
		goto error;
	}

	if (chown(path, (uid_t) -1, (gid_t) step->gid) < 0) {
		error("chown(%s): %m", path);
		goto error;
	}

	if (chmod(path, 0750) < 0) {
		error("chmod(%s, 750): %m", path);
		goto error;
	}

	return xstrdup(path);

error:
	return NULL;
}

static int _make_batch_script(batch_job_launch_msg_t *msg,
			      stepd_step_rec_t *step)
{
	int flags = O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC;
	int fd, length;
	char *script = NULL;
	char *output;

	if (msg->script == NULL) {
		error("%s: called with NULL script", __func__);
		return SLURM_ERROR;
	}

	/* note: should replace this with a length as part of msg */
	if ((length = strlen(msg->script)) < 1) {
		error("%s: called with empty script", __func__);
		return SLURM_ERROR;
	}

	script = _batch_script_path(step);

	if ((fd = open(script, flags, S_IRWXU)) < 0) {
		error("couldn't open `%s': %m", script);
		goto error;
	}

	if (ftruncate(fd, length) == -1) {
		error("%s: ftruncate to %d failed on `%s`: %m",
		      __func__, length, script);
		close(fd);
		goto error;
	}

	output = mmap(0, length, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
	if (output == MAP_FAILED) {
		error("%s: mmap failed", __func__);
		close(fd);
		goto error;
	}

	(void) close(fd);

	memcpy(output, msg->script, length);

	munmap(output, length);

	if (chown(script, (uid_t) msg->uid, (gid_t) -1) < 0) {
		error("chown(%s): %m", script);
		goto error;
	}

	step->argv[0] = script;
	return SLURM_SUCCESS;

error:
	(void) unlink(script);
	xfree(script);
	return SLURM_ERROR;
}

extern int stepd_drain_node(char *reason)
{
	slurm_msg_t req_msg;
	update_node_msg_t update_node_msg;

	slurm_init_update_node_msg(&update_node_msg);
	update_node_msg.node_names = conf->node_name;
	update_node_msg.node_state = NODE_STATE_DRAIN;
	update_node_msg.reason = reason;
	slurm_msg_t_init(&req_msg);
	req_msg.msg_type = REQUEST_UPDATE_NODE;
	req_msg.data = &update_node_msg;

	if (slurm_send_only_controller_msg(&req_msg, working_cluster_rec) < 0)
		return SLURM_ERROR;

	return SLURM_SUCCESS;
}

static void
_send_launch_failure(launch_tasks_request_msg_t *msg, slurm_addr_t *cli, int rc,
		     uint16_t protocol_version)
{
	slurm_msg_t resp_msg;
	launch_tasks_response_msg_t resp;
	int nodeid;
	char *name = NULL;

	/*
	 * The extern step can get here if something goes wrong starting the
	 * step.  If this does happen we don't have to contact the srun since
	 * there isn't one, just return.
	 */
	if ((msg->step_id.step_id == SLURM_EXTERN_CONT) ||
	    !msg->resp_port || !msg->num_resp_port) {
		debug2("%s: The extern step has nothing to send a launch failure to",
		       __func__);
		return;
	}

#ifndef HAVE_FRONT_END
	nodeid = nodelist_find(msg->complete_nodelist, conf->node_name);
	name = xstrdup(conf->node_name);
#else
	nodeid = 0;
	name = xstrdup(msg->complete_nodelist);
#endif
	debug ("sending launch failure message: %s", slurm_strerror (rc));

	slurm_msg_t_init(&resp_msg);
	memcpy(&resp_msg.address, cli, sizeof(slurm_addr_t));
	slurm_set_port(&resp_msg.address,
		       msg->resp_port[nodeid % msg->num_resp_port]);
	resp_msg.data = &resp;
	resp_msg.msg_type = RESPONSE_LAUNCH_TASKS;
	resp_msg.protocol_version = protocol_version;
	slurm_msg_set_r_uid(&resp_msg, msg->uid);

	memcpy(&resp.step_id, &msg->step_id, sizeof(resp.step_id));

	resp.node_name     = name;
	resp.return_code   = rc ? rc : -1;
	resp.count_of_pids = 0;

	if (_send_srun_resp_msg(&resp_msg, msg->nnodes) != SLURM_SUCCESS)
		error("%s: Failed to send RESPONSE_LAUNCH_TASKS: %m", __func__);
	xfree(name);
	return;
}

static void
_send_launch_resp(stepd_step_rec_t *step, int rc)
{
	int i;
	slurm_msg_t resp_msg;
	launch_tasks_response_msg_t resp;
	srun_info_t *srun = list_peek(step->sruns);

	if (step->batch)
		return;

	info("Sending launch resp rc=%d", rc);

	slurm_msg_t_init(&resp_msg);
	resp_msg.address	= srun->resp_addr;
	slurm_msg_set_r_uid(&resp_msg, srun->uid);
	resp_msg.protocol_version = srun->protocol_version;
	resp_msg.data		= &resp;
	resp_msg.msg_type	= RESPONSE_LAUNCH_TASKS;

	memcpy(&resp.step_id, &step->step_id, sizeof(resp.step_id));

	resp.node_name		= xstrdup(step->node_name);
	resp.return_code	= rc;
	resp.count_of_pids	= step->node_tasks;

	resp.local_pids = xmalloc(step->node_tasks * sizeof(*resp.local_pids));
	resp.task_ids = xmalloc(step->node_tasks * sizeof(*resp.task_ids));
	for (i = 0; i < step->node_tasks; i++) {
		resp.local_pids[i] = step->task[i]->pid;
		/*
		 * Don't add offset here, this represents a bit on the other
		 * side.
		 */
		resp.task_ids[i] = step->task[i]->gtid;
	}

	if (_send_srun_resp_msg(&resp_msg, step->nnodes) != SLURM_SUCCESS)
		error("%s: Failed to send RESPONSE_LAUNCH_TASKS: %m", __func__);

	xfree(resp.local_pids);
	xfree(resp.task_ids);
	xfree(resp.node_name);
}


/* If get_list is false make sure ps->gid_list is initialized before
 * hand to prevent xfree.
 */
static int
_drop_privileges(stepd_step_rec_t *step, bool do_setuid,
		 struct priv_state *ps, bool get_list)
{
	auth_setuid_lock();
	ps->saved_uid = getuid();
	ps->saved_gid = getgid();

	if (!getcwd (ps->saved_cwd, sizeof (ps->saved_cwd))) {
		error ("Unable to get current working directory: %m");
		strlcpy(ps->saved_cwd, "/tmp", sizeof(ps->saved_cwd));
	}

	ps->ngids = getgroups(0, NULL);
	if (ps->ngids == -1) {
		error("%s: getgroups(): %m", __func__);
		return -1;
	}
	if (get_list) {
		ps->gid_list = xcalloc(ps->ngids, sizeof(gid_t));

		if (getgroups(ps->ngids, ps->gid_list) == -1) {
			error("%s: couldn't get %d groups: %m",
			      __func__, ps->ngids);
			xfree(ps->gid_list);
			return -1;
		}
	}

	/*
	 * No need to drop privileges if we're not running as root
	 */
	if (getuid() != (uid_t) 0)
		return SLURM_SUCCESS;

	if (setegid(step->gid) < 0) {
		error("setegid: %m");
		return -1;
	}

	if (setgroups(step->ngids, step->gids) < 0) {
		error("setgroups: %m");
		return -1;
	}

	if (do_setuid && seteuid(step->uid) < 0) {
		error("seteuid: %m");
		return -1;
	}

	return SLURM_SUCCESS;
}

static int
_reclaim_privileges(struct priv_state *ps)
{
	int rc = SLURM_SUCCESS;

	/*
	 * No need to reclaim privileges if our uid == step->uid
	 */
	if (geteuid() == ps->saved_uid)
		goto done;
	else if (seteuid(ps->saved_uid) < 0) {
		error("seteuid: %m");
		rc = -1;
	} else if (setegid(ps->saved_gid) < 0) {
		error("setegid: %m");
		rc = -1;
	} else if (setgroups(ps->ngids, ps->gid_list) < 0) {
		error("setgroups: %m");
		rc = -1;
	}

done:
	auth_setuid_unlock();
	xfree(ps->gid_list);

	return rc;
}


static int
_slurmd_job_log_init(stepd_step_rec_t *step)
{
	char argv0[64];

	conf->log_opts.buffered = 1;

	/*
	 * Reset stderr logging to user requested level
	 * (Logfile and syslog levels remain the same)
	 *
	 * The maximum stderr log level is LOG_LEVEL_DEBUG2 because
	 * some higher level debug messages are generated in the
	 * stdio code, which would otherwise create more stderr traffic
	 * to srun and therefore more debug messages in an endless loop.
	 */
	conf->log_opts.stderr_level = LOG_LEVEL_ERROR;
	if (step->debug > LOG_LEVEL_ERROR) {
		if ((step->uid == 0) || (step->uid == slurm_conf.slurm_user_id))
			conf->log_opts.stderr_level = step->debug;
		else
			error("Use of --slurmd-debug is allowed only for root and SlurmUser(%s), ignoring it",
			      slurm_conf.slurm_user_name);
	}
	if (conf->log_opts.stderr_level > LOG_LEVEL_DEBUG2)
		conf->log_opts.stderr_level = LOG_LEVEL_DEBUG2;

#if defined(MULTIPLE_SLURMD)
	snprintf(argv0, sizeof(argv0), "slurmstepd-%s", conf->node_name);
#else
	snprintf(argv0, sizeof(argv0), "slurmstepd");
#endif
	/*
	 * reinitialize log
	 */

	log_alter(conf->log_opts, 0, NULL);
	log_set_argv0(argv0);

	/*
	 *  Connect slurmd stderr to stderr of job
	 */
	if (step->flags & LAUNCH_PTY)
		fd_set_nonblocking(STDERR_FILENO);
	if (step->task != NULL) {
		if (dup2(step->task[0]->stderr_fd, STDERR_FILENO) < 0) {
			error("job_log_init: dup2(stderr): %m");
			return ESLURMD_IO_ERROR;
		}
	}

	verbose("debug levels are stderr='%s', logfile='%s', syslog='%s'",
		log_num2string(conf->log_opts.stderr_level),
		log_num2string(conf->log_opts.logfile_level),
		log_num2string(conf->log_opts.syslog_level));

	return SLURM_SUCCESS;
}

/*
 * Set the priority of the job to be the same as the priority of
 * the process that launched the job on the submit node.
 * In support of the "PropagatePrioProcess" config keyword.
 */
static void _set_prio_process (stepd_step_rec_t *step)
{
	char *env_name = "SLURM_PRIO_PROCESS";
	char *env_val;
	int prio_daemon, prio_process;

	if (!(env_val = getenvp( step->env, env_name ))) {
		error( "Couldn't find %s in environment", env_name );
		prio_process = 0;
	} else {
		/* Users shouldn't get this in their environment */
		unsetenvp( step->env, env_name );
		prio_process = atoi( env_val );
	}

	if (slurm_conf.propagate_prio_process == PROP_PRIO_NICER) {
		prio_daemon = getpriority( PRIO_PROCESS, 0 );
		prio_process = MAX( prio_process, (prio_daemon + 1) );
	}

	if (setpriority( PRIO_PROCESS, 0, prio_process ))
		error( "setpriority(PRIO_PROCESS, %d): %m", prio_process );
	else {
		debug2( "_set_prio_process: setpriority %d succeeded",
			prio_process);
	}
}

static int
_become_user(stepd_step_rec_t *step, struct priv_state *ps)
{
	/*
	 * First reclaim the effective uid and gid
	 */
	if (geteuid() == ps->saved_uid)
		return SLURM_SUCCESS;

	if (seteuid(ps->saved_uid) < 0) {
		error("_become_user seteuid: %m");
		return SLURM_ERROR;
	}

	if (setegid(ps->saved_gid) < 0) {
		error("_become_user setegid: %m");
		return SLURM_ERROR;
	}

	/*
	 * Now drop real, effective, and saved uid/gid
	 */
	if (setregid(step->gid, step->gid) < 0) {
		error("setregid: %m");
		return SLURM_ERROR;
	}

	if (setreuid(step->uid, step->uid) < 0) {
		error("setreuid: %m");
		return SLURM_ERROR;
	}

	return SLURM_SUCCESS;
}

/*
 * Check this user's access rights to a file
 * path IN: pathname of file to test
 * modes IN: desired access
 * uid IN: user ID to access the file
 * gid IN: group ID to access the file
 * RET true on success, false on failure
 */
static bool _access(const char *path, int modes, uid_t uid,
		    int ngids, gid_t *gids)
{
	struct stat buf;
	int f_mode, i;

	if (!gids)
		return false;

	if (stat(path, &buf) != 0)
		return false;

	if (buf.st_uid == uid)
		f_mode = (buf.st_mode >> 6) & 07;
	else {
		for (i=0; i < ngids; i++)
			if (buf.st_gid == gids[i])
				break;
		if (i < ngids)	/* one of the gids matched */
			f_mode = (buf.st_mode >> 3) & 07;
		else		/* uid and gid failed, test against all */
			f_mode = buf.st_mode & 07;
	}

	if ((f_mode & modes) == modes)
		return true;

	return false;
}

/*
 * Run a script as a specific user, with the specified uid, gid, and
 * extended groups.
 *
 * name IN: class of program (task prolog, task epilog, etc.),
 * path IN: pathname of program to run
 * job IN: slurd job structue, used to get uid, gid, and groups
 * max_wait IN: maximum time to wait in seconds, -1 for no limit
 * env IN: environment variables to use on exec, sets minimal environment
 *	if NULL
 *
 * RET 0 on success, -1 on failure.
 */
int
_run_script_as_user(const char *name, const char *path, stepd_step_rec_t *step,
		    int max_wait, char **env)
{
	int status, rc, opt;
	pid_t cpid;
	struct exec_wait_info *ei;

	xassert(env);
	if (path == NULL || path[0] == '\0')
		return 0;

	debug("[job %u] attempting to run %s [%s]",
	      step->step_id.job_id, name, path);

	if (!_access(path, 5, step->uid, step->ngids, step->gids)) {
		error("Could not run %s [%s]: access denied", name, path);
		return -1;
	}

	if ((ei = _fork_child_with_wait_info(0)) == NULL) {
		error ("executing %s: fork: %m", name);
		return -1;
	}
	if ((cpid = _exec_wait_get_pid (ei)) == 0) {
		struct priv_state sprivs;
		char *argv[2];
		uint32_t jobid;

#ifdef HAVE_NATIVE_CRAY
		if (step->het_job_id && (step->het_job_id != NO_VAL))
			jobid = step->het_job_id;
		else
			jobid = step->step_id.job_id;
#else
		jobid = step->step_id.job_id;
#endif
		/* container_g_join needs to be called in the
		   forked process part of the fork to avoid a race
		   condition where if this process makes a file or
		   detacts itself from a child before we add the pid
		   to the container in the parent of the fork.
		*/
		if ((jobid != 0) &&	/* Ignore system processes */
		    !(step->flags & LAUNCH_NO_ALLOC) &&
		    (container_g_join(jobid, step->uid) != SLURM_SUCCESS))
			error("container_g_join(%u): %m", step->step_id.job_id);

		argv[0] = (char *)xstrdup(path);
		argv[1] = NULL;

#ifdef WITH_SELINUX
		if (setexeccon(step->selinux_context)) {
			error("Failed to set SELinux context to %s: %m",
			      step->selinux_context);
			_exit(1);
		}
#else
		if (step->selinux_context) {
			error("Built without SELinux support but context was specified");
			_exit(1);
		}
#endif

		sprivs.gid_list = NULL;	/* initialize to prevent xfree */
		if (_drop_privileges(step, true, &sprivs, false) < 0) {
			error("run_script_as_user _drop_privileges: %m");
			/* child process, should not return */
			exit(127);
		}

		if (_become_user(step, &sprivs) < 0) {
			error("run_script_as_user _become_user failed: %m");
			/* child process, should not return */
			exit(127);
		}

		if (chdir(step->cwd) == -1)
			error("run_script_as_user: couldn't "
			      "change working dir to %s: %m", step->cwd);
		setpgid(0, 0);
		/*
		 *  Wait for signal from parent
		 */
		_exec_wait_child_wait_for_parent (ei);

		while (1) {
			execve(path, argv, env);
			error("execve(%s): %m", path);
			if ((errno == ENFILE) || (errno == ENOMEM)) {
				/* System limit on open files or memory reached,
				 * retry after short delay */
				sleep(1);
			} else {
				break;
			}
		}
		_exit(127);
	}

	if (exec_wait_signal_child (ei) < 0)
		error ("run_script_as_user: Failed to wakeup %s", name);
	_exec_wait_info_destroy (ei);

	if (max_wait < 0)
		opt = 0;
	else
		opt = WNOHANG;

	while (1) {
		rc = waitpid(cpid, &status, opt);
		if (rc < 0) {
			if (errno == EINTR)
				continue;
			error("waitpid: %m");
			status = 0;
			break;
		} else if (rc == 0) {
			sleep(1);
			if ((--max_wait) <= 0) {
				killpg(cpid, SIGKILL);
				opt = 0;
			}
		} else  {
			/* spawned process exited */
			break;
		}
	}
	/* Ensure that all child processes get killed, one last time */
	killpg(cpid, SIGKILL);

	return status;
}

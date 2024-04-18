/*****************************************************************************\
 * src/slurmd/slurmstepd/io.c - Standard I/O handling routines for slurmstepd
 *****************************************************************************
 *  Copyright (C) 2002 The Regents of the University of California.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Copyright (c) 2022-2024 Technical University of Munich (TUM)
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

#include "config.h"

#define _GNU_SOURCE /* for setresuid(3) */

#ifdef HAVE_PTY_H
#  include <pty.h>
#endif

#ifdef HAVE_UTMP_H
#  include <utmp.h>
#endif

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <termios.h>
#include <unistd.h>

#include "src/common/cbuf.h"
#include "src/common/eio.h"
#include "src/common/fd.h"
#include "src/common/io_hdr.h"
#include "src/common/list.h"
#include "src/common/log.h"
#include "src/common/macros.h"
#include "src/common/net.h"
#include "src/common/read_config.h"
#include "src/common/write_labelled_message.h"
#include "src/common/xmalloc.h"
#include "src/common/xsignal.h"
#include "src/common/xstring.h"

#include "src/slurmd/common/fname.h"
#include "src/slurmd/slurmd/slurmd.h"
#include "src/slurmd/slurmstepd/io.h"
#include "src/slurmd/slurmstepd/slurmstepd.h"

#include "src/interfaces/mpi.h"

// deep-sea headers
#include "dynpm_network.h"
#include "dynpm_config.h"

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
	int (*deepsea_get_tbon_vertex)(dynpm_tbon_vertex_t **tv);
} slurm_mpi_ops_t;

extern dynpm_tbon_vertex_t *tbon_vertex;
extern slurm_mpi_ops_t deepsea_pmix_ops;
cbuf_t *multi_task_cbuf = NULL;
char *work_buffer = NULL;
//static pthread_mutex_t io_send_lock = PTHREAD_MUTEX_INITIALIZER

struct eio_handle_components {
	int  magic;
	int  fds[2];
	pthread_mutex_t shutdown_mutex;
	time_t shutdown_time;
	uint16_t shutdown_wait;
	List obj_list;
	List new_objs;
};

typedef struct {
	eio_obj_t **map;
	unsigned int *nfds_ptr;
	struct pollfd *pfds;
} foreach_pollfd_t;

struct cbuf {

#ifndef NDEBUG
    unsigned long       magic;          /* cookie for asserting validity     */
#endif /* !NDEBUG */

    pthread_mutex_t     mutex;          /* mutex to protect access to cbuf   */

    int                 alloc;          /* num bytes xmalloc'd/xrealloc'd      */
    int                 minsize;        /* min bytes of data to allocate     */
    int                 maxsize;        /* max bytes of data to allocate     */
    int                 size;           /* num bytes of data allocated       */
    int                 used;           /* num bytes of unread data          */
    cbuf_overwrite_t    overwrite;      /* overwrite option behavior         */
    int                 got_wrap;       /* true if data has wrapped          */
    int                 i_in;           /* index to where data is written in */
    int                 i_out;          /* index to where data is read out   */
    int                 i_rep;          /* index to where data is replayable */
    unsigned char      *data;           /* ptr to circular buffer of data    */
};

/**********************************************************************
 * Task read declarations
 **********************************************************************/
static bool _task_readable(eio_obj_t *);
static int  _task_read(eio_obj_t *, List);

struct io_operations task_read_ops = {
	.readable = &_task_readable,
	.handle_read = &_task_read,
};

#define TASK_OUT_MAGIC  0x10103
struct task_read_info {
	int              magic;
	uint16_t         type;           /* type of IO object          */
	uint16_t         gtaskid;
	uint16_t         ltaskid;
	stepd_step_rec_t *step; /* pointer back to step data */
	cbuf_t          *buf;
	bool		 eof;
	bool		 eof_msg_sent;
};

static void *_io_thr(void *);
static void _flush_multi_task_cbuf();

/**********************************************************************
 * Task read functions
 **********************************************************************/
/*
 * Create an eio_obj_t for handling a task's stdout or stderr traffic
 */
static eio_obj_t * _create_task_out_eio(int fd, uint16_t type,
		stepd_step_rec_t *step, stepd_step_task_info_t *task) {

	struct task_read_info *out = xmalloc(sizeof(*out));
	eio_obj_t *eio = NULL;

	out->magic = TASK_OUT_MAGIC;
	out->type = type;
	out->gtaskid = task->gtid;
	out->ltaskid = task->id;
	out->step = step;
	out->buf = cbuf_create(MAX_MSG_LEN, MAX_MSG_LEN*4);
	out->eof = false;
	out->eof_msg_sent = false;
	if (cbuf_opt_set(out->buf, CBUF_OPT_OVERWRITE, CBUF_NO_DROP) == -1)
		error("setting cbuf options");

	eio = eio_obj_create(fd, &task_read_ops, (void *)out);

	return eio;
}

static bool _task_readable(eio_obj_t *obj) {

	struct task_read_info *out = (struct task_read_info *)obj->arg;

	debug5("Called _task_readable, task %d, %s", out->gtaskid,
	       out->type == SLURM_IO_STDOUT ? "STDOUT" : "STDERR");

	if (out->eof_msg_sent) {
		debug5("  false, eof message sent");
		return false;
	}
	if (cbuf_free(out->buf) > 0) {
		debug5("  cbuf_free = %d", cbuf_free(out->buf));
		return true;
	}

	debug5("  false");
	return false;
}

/*
 * Read output (stdout or stderr) from a task into a cbuf.  The cbuf
 * allows whole lines to be packed into messages if line buffering
 * is requested.
 */
static int _task_read(eio_obj_t *obj, List objs) {

	struct task_read_info *out = (struct task_read_info *)obj->arg;
	int len;
	int rc = -1;
	int overwritten = 0;

	xassert(out->magic == TASK_OUT_MAGIC);

	len = cbuf_free(out->buf);
	if (len > 0 && !out->eof) {
again:
		if ((rc = cbuf_write_from_fd(out->buf, obj->fd, len, &overwritten))
		    < 0) {
			if (errno == EINTR)
				goto again;
			if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
				error("_task_read returned EAGAIN");
				return SLURM_SUCCESS;
			}
			//error("  error in _task_read: %m");
		}
		if (rc <= 0) {  /* got eof */
			info("  got eof on task");
			out->eof = true;
		}
	}

	if(overwritten){ error("%d bytes were overwritten before these could be captured", overwritten); }

	if(cbuf_used(out->buf) > 0){
		if(strncmp("dstepd", out->buf->data, 6)
				&& strncmp("slurmstepd", out->buf->data, 10)){
			//info("************************ %d bytes read from task %s", rc,
			//		out->type == SLURM_IO_STDOUT ? "STDOUT" : "STDERR");
			//info("%s", out->buf->data);

			if(tbon_vertex == NULL ){
				info("tbon_vertex == NULL; cannot flush at the moment");
				return SLURM_SUCCESS;
			}

			pthread_mutex_lock(&(tbon_vertex->mutex));
			pthread_mutex_lock(&(tbon_vertex->io_mutex));
			if(tbon_vertex->ready_count < tbon_vertex->total_children){
				info("tbon_vertex not ready; cannot flush at the moment");
				pthread_mutex_unlock(&(tbon_vertex->mutex));
				return SLURM_SUCCESS;
			} else {
				//info("updating multi_task_cbuf");
				xassert(multi_task_cbuf != NULL);
				int lines_available = cbuf_lines_used(out->buf);
				if(lines_available > 0){
					//info("%d lines available", lines_available);
					int bytes_read = cbuf_read_line(out->buf, work_buffer, 4096, lines_available);
					//info("%d bytes read", bytes_read);
					int dropped;
					int bytes_written = cbuf_write_line(multi_task_cbuf, work_buffer, &dropped);
					//info("%d written to multi_task_cbuf; %d dropped; used: %d",
							//bytes_written, dropped, cbuf_used(multi_task_cbuf));
					cbuf_flush(out->buf);
				} else {
					//info("no lines available in current task cbuf");
				}
			}
			pthread_mutex_unlock(&(tbon_vertex->io_mutex));
			pthread_mutex_unlock(&(tbon_vertex->mutex));
		} else {
			info("************************ %d bytes read from dstepd %s",
					rc, out->type == SLURM_IO_STDOUT ? "STDOUT" : "STDERR");
			info("%s", out->buf->data);
			cbuf_flush(out->buf);
		}
	}

	return SLURM_SUCCESS;
}

static int _init_task_stdio_fds(stepd_step_task_info_t *task, stepd_step_rec_t *step) {

	int file_flags = io_get_file_flags(step);

	/*
	 *  Initialize stdout
	 */
#ifdef HAVE_PTY_H
	if (step->flags & LAUNCH_PTY) {
		if (task->gtid == 0) {
			task->stdout_fd = dup(task->stdin_fd);
			fd_set_close_on_exec(task->stdout_fd);
			task->from_stdout = dup(task->to_stdin);
			fd_set_close_on_exec(task->from_stdout);
			fd_set_nonblocking(task->from_stdout);
			task->out = _create_task_out_eio(task->from_stdout,
						 SLURM_IO_STDOUT, step, task);
			list_append(step->stdout_eio_objs, (void *)task->out);
			eio_new_initial_obj(step->eio, (void *)task->out);
		} else {
			xfree(task->ofname);
			task->ofname = xstrdup("/dev/null");
			task->stdout_fd = open("/dev/null", O_RDWR, O_CLOEXEC);
			task->from_stdout = -1;  /* not used */
		}
	} else if ((task->ofname != NULL) &&
		   (((step->flags & LAUNCH_LABEL_IO) == 0) ||
		    (xstrcmp(task->ofname, "/dev/null") == 0))) {
#else
	if (task->ofname != NULL &&
	    (((step->flags & LAUNCH_LABEL_IO) == 0) ||
	     xstrcmp(task->ofname, "/dev/null") == 0)) {
#endif
		int count = 0;
		/* open file on task's stdout */
		debug5("  stdout file name = %s", task->ofname);
		do {
			task->stdout_fd = open(task->ofname,
					       file_flags | O_CLOEXEC, 0666);
			if (!count && (errno == ENOENT)) {
				mkdirpath(task->ofname, 0755, false);
				errno = EINTR;
			}
			++count;
		} while (task->stdout_fd == -1 && errno == EINTR && count < 10);
		if (task->stdout_fd == -1) {
			error("Could not open stdout file %s: %m",
			      task->ofname);
			return SLURM_ERROR;
		}
		task->from_stdout = -1; /* not used */
	} else {
		/* create pipe and eio object */
		int pout[2];
#if HAVE_PTY_H
		struct termios tio;
		if (!(step->flags & LAUNCH_BUFFERED_IO)) {
#if HAVE_SETRESUID
			if (setresuid(geteuid(), geteuid(), 0) < 0)
				error("%s: %u setresuid() %m",
				      __func__, geteuid());
#endif
			if (openpty(pout, pout + 1, NULL, NULL, NULL) < 0) {
				error("%s: stdout openpty: %m", __func__);
				return SLURM_ERROR;
			}
			memset(&tio, 0, sizeof(tio));
			if (tcgetattr(pout[1], &tio) == 0) {
				tio.c_oflag &= ~OPOST;
				if (tcsetattr(pout[1], 0, &tio) != 0)
					error("%s: tcsetattr: %m", __func__);
			}
#if HAVE_SETRESUID
			if (setresuid(0, getuid(), 0) < 0)
				error("%s 0 setresuid() %m", __func__);
#endif
		} else {
			debug5("  stdout uses an eio object");
			if (pipe(pout) < 0) {
				error("stdout pipe: %m");
				return SLURM_ERROR;
			}
		}
#else
		debug5("  stdout uses an eio object");
		if (pipe(pout) < 0) {
			error("stdout pipe: %m");
			return SLURM_ERROR;
		}
#endif
		task->stdout_fd = pout[1];
		fd_set_close_on_exec(task->stdout_fd);
		task->from_stdout = pout[0];
		fd_set_close_on_exec(task->from_stdout);
		fd_set_nonblocking(task->from_stdout);
		task->out = _create_task_out_eio(task->from_stdout,
						 SLURM_IO_STDOUT, step, task);
		list_append(step->stdout_eio_objs, (void *)task->out);
		eio_new_initial_obj(step->eio, (void *)task->out);
	}

	/*
	 *  Initialize stderr
	 */
#ifdef HAVE_PTY_H
	if (step->flags & LAUNCH_PTY) {
		if (task->gtid == 0) {
			/* Make a file descriptor for the task to write to, but
			   don't make a separate one read from, because in pty
			   mode we can't distinguish between stdout and stderr
			   coming from the remote shell.  Both streams from the
			   shell will go to task->stdout_fd, which is okay in
			   pty mode because any output routed through the stepd
			   will be displayed. */
			task->stderr_fd = dup(task->stdin_fd);
			fd_set_close_on_exec(task->stderr_fd);
			task->from_stderr = -1;
		} else {
			xfree(task->efname);
			task->efname = xstrdup("/dev/null");
			task->stderr_fd = open("/dev/null", O_RDWR | O_CLOEXEC);
			task->from_stderr = -1;  /* not used */
		}

	} else if ((task->efname != NULL) &&
		   (((step->flags & LAUNCH_LABEL_IO) == 0) ||
		    (xstrcmp(task->efname, "/dev/null") == 0))) {
#else
	if ((task->efname != NULL) &&
	    (((step->flags & LAUNCH_LABEL_IO) == 0) ||
	     (xstrcmp(task->efname, "/dev/null") == 0))) {
#endif
		int count = 0;
		/* open file on task's stdout */
		debug5("  stderr file name = %s", task->efname);
		do {
			task->stderr_fd = open(task->efname,
					       file_flags | O_CLOEXEC, 0666);
			if (!count && (errno == ENOENT)) {
				mkdirpath(task->efname, 0755, false);
				errno = EINTR;
			}
			++count;
		} while (task->stderr_fd == -1 && errno == EINTR && count < 10);
		if (task->stderr_fd == -1) {
			error("Could not open stderr file %s: %m",
			      task->efname);
			return SLURM_ERROR;
		}
		task->from_stderr = -1; /* not used */
	} else {
		/* create pipe and eio object */
		int perr[2];
		debug5("  stderr uses an eio object");
		if (pipe(perr) < 0) {
			error("stderr pipe: %m");
			return SLURM_ERROR;
		}
		task->stderr_fd = perr[1];
		fd_set_close_on_exec(task->stderr_fd);
		task->from_stderr = perr[0];
		fd_set_close_on_exec(task->from_stderr);
		fd_set_nonblocking(task->from_stderr);
		task->err = _create_task_out_eio(task->from_stderr,
						 SLURM_IO_STDERR, step, task);
		list_append(step->stderr_eio_objs, (void *)task->err);
		eio_new_initial_obj(step->eio, (void *)task->err);
	}

	return SLURM_SUCCESS;
}

int
io_init_tasks_stdio(stepd_step_rec_t *step)
{
	int i, rc = SLURM_SUCCESS, tmprc;

	for (i = 0; i < step->node_tasks; i++) {
		tmprc = _init_task_stdio_fds(step->task[i], step);
		if (tmprc != SLURM_SUCCESS)
			rc = tmprc;
	}

	return rc;
}

extern void io_thread_start(stepd_step_rec_t *step)
{
	slurm_mutex_lock(&step->io_mutex);
	pthread_mutex_lock(&(tbon_vertex->io_mutex));

	// let the Slurm runtime system inflate the buffer up to 128 times
	multi_task_cbuf = cbuf_create(4096, 128*4096);
	work_buffer = malloc(4096);

	// TODO replace this once we replace/update steps due to malleability
	slurm_thread_create_detached(_io_thr, step);
	step->io_running = true;

	pthread_mutex_unlock(&(tbon_vertex->io_mutex));
	slurm_mutex_unlock(&step->io_mutex);
}

/* Close I/O file descriptors created by slurmstepd. The connections have
 * all been moved to the spawned tasks stdin/out/err file descriptors. */
extern void io_close_task_fds(stepd_step_rec_t *step) {
	int i;

	for (i = 0; i < step->node_tasks; i++) {
		close(step->task[i]->stdin_fd);
		close(step->task[i]->stdout_fd);
		close(step->task[i]->stderr_fd);
	}

}

void io_close_all(stepd_step_rec_t *step) {
	int devnull;
	/* No more debug info will be received by client after this point */
	debug("Closing debug channel");

	/* Send stderr to /dev/null since debug channel is closing
	 *  and log facility may still try to write to stderr. */
	if ((devnull = open("/dev/null", O_RDWR)) < 0) {
		error("Could not open /dev/null: %m");
	} else {
		if (dup2(devnull, STDERR_FILENO) < 0)
			error("Unable to dup /dev/null onto stderr");
		(void) close(devnull);
	}

	/* Signal IO thread to close appropriate client connections */
	eio_signal_shutdown(step->eio);
}

void io_close_local_fds(stepd_step_rec_t *step) { }

// also from src/custom/eio.c
static bool _is_writable(eio_obj_t *obj) {
	return (obj->ops->writable && (*obj->ops->writable)(obj));
}

static bool _is_readable(eio_obj_t *obj) {
	return (obj->ops->readable && (*obj->ops->readable)(obj));
}

static int _poll_internal(struct pollfd *pfds, unsigned int nfds,
			  time_t shutdown_time)
{
	int n, timeout;

	if (shutdown_time)
		timeout = 1000;	/* Return every 1000 msec during shutdown */
	else
		timeout = -1;

	while ((n = poll(pfds, nfds, timeout)) < 0) {
		switch (errno) {
		case EINTR:
			return 0;
		case EAGAIN:
			continue;
		default:
			error("poll: %m");
			return -1;
		}
	}

	return n;
}

static int _mark_shutdown_true(void *x, void *arg)
{
	eio_obj_t *obj = x;

	obj->shutdown = true;
	return 0;
}

static void _poll_handle_event(short revents, eio_obj_t *obj, List objList)
{
	bool read_called = false;
	bool write_called = false;

	if (revents & (POLLERR|POLLNVAL)) {
		//info("revents & (POLLERR|POLLNVAL)");
		if (obj->ops->handle_error) {
			(*obj->ops->handle_error) (obj, objList);
		} else if (obj->ops->handle_read) {
			(*obj->ops->handle_read) (obj, objList);
		} else if (obj->ops->handle_write) {
			(*obj->ops->handle_write) (obj, objList);
		} else {
			debug("No handler for %s on fd %d",
			      revents & POLLERR ? "POLLERR" : "POLLNVAL",
			      obj->fd);
			obj->shutdown = true;
		}
		return;
	}

	if ((revents & POLLHUP) && ((revents & POLLIN) == 0)) {
		//info("revents & POLLHUP) && ((revents & POLLIN)");
		if (obj->ops->handle_close) {
			(*obj->ops->handle_close) (obj, objList);
		} else if (obj->ops->handle_read) {
			if (!read_called) {
				(*obj->ops->handle_read) (obj, objList);
				read_called = true;
			}
		} else if (obj->ops->handle_write) {
			if (!write_called) {
				(*obj->ops->handle_write) (obj, objList);
				write_called = true;
			}
		} else {
			debug("No handler for POLLHUP");
			obj->shutdown = true;
		}
	}

	if (revents & POLLIN) {
		//info("revents & POLLIN; stdout from a task");
		if (obj->ops->handle_read) {
			//info("obj->ops->handle_read");
			if (!read_called) {
				//info("!read_called");
				(*obj->ops->handle_read ) (obj, objList);
			}
		} else {
			debug("No handler for POLLIN");
			obj->shutdown = true;
		}
	}

	if (revents & POLLOUT) {
		//info("revents & POLLOUT");
		if (obj->ops->handle_write) {
			if (!write_called) {
				(*obj->ops->handle_write) (obj, objList);
			}
		} else {
			debug("No handler for POLLOUT");
			obj->shutdown = true;
		}
	}
}

static void _poll_dispatch(struct pollfd *pfds, unsigned int nfds,
			   eio_obj_t *map[], List objList) {
	int i;
	for (i = 0; i < nfds; i++) {
		if (pfds[i].revents > 0) {
			_poll_handle_event(pfds[i].revents, map[i], objList);
		}
	}
}

static int _eio_wakeup_handler(eio_handle_t *eio) {
	char c = 0;
	int rc = 0;

	while ((rc = (read(eio->fds[0], &c, 1)) > 0)) {
		if (c == 1) list_for_each(eio->obj_list, _mark_shutdown_true, NULL);
	}

	/* move new eio objects from the new_objs to the obj_list */
	list_transfer(eio->obj_list, eio->new_objs);

	if (rc < 0) return error("%s: read: %m", __func__);

	return 0;
}

static int _foreach_helper_setup_pollfds(void *x, void *arg) {
	eio_obj_t *obj = x;
	foreach_pollfd_t *hargs = arg;
	struct pollfd *pfds = hargs->pfds;
	eio_obj_t **map = hargs->map;
	unsigned int nfds = *hargs->nfds_ptr;
	bool readable, writable;

	//writable = _is_writable(obj);
	// wo do not support writes to stdin
	// TODO verify if that there are no side effects elsewhere
	writable = 0;
	readable = _is_readable(obj);
	if (writable && readable) {
		//info("both");
		pfds[nfds].fd     = obj->fd;
		pfds[nfds].events = POLLOUT | POLLIN | POLLHUP | POLLRDHUP;
		map[nfds]         = obj;
	} else if (readable) {
		//info("readable");
		pfds[nfds].fd     = obj->fd;
		pfds[nfds].events = POLLIN | POLLRDHUP;
		map[nfds]         = obj;
	} else if (writable) {
		//info("writable");
		pfds[nfds].fd     = obj->fd;
		pfds[nfds].events = POLLOUT | POLLHUP;
		map[nfds]         = obj;
	} else {
		//info("none");
	}

	if (writable || readable)
		(*hargs->nfds_ptr)++;

	return 0;
}

static unsigned int _poll_setup_pollfds(struct pollfd *pfds, eio_obj_t *map[], List l) {
	unsigned int  nfds = 0;
	foreach_pollfd_t args = {
		.pfds = pfds,
		.map = map,
		.nfds_ptr = &nfds
	};

	if (!pfds) {	/* Fix for CLANG false positive */
		fatal("%s: pollfd data structure is null", __func__);
		return nfds;
	}

	list_for_each(l, _foreach_helper_setup_pollfds, &args);

	return nfds;
}

// replaced eio_handle_mainloop from src/custom/eio.c
int _custom_eio_handle_mainloop(eio_handle_t *eio) {
	int            retval  = 0;
	struct pollfd *pollfds = NULL;
	eio_obj_t    **map     = NULL;
	unsigned int   maxnfds = 0, nfds = 0;
	unsigned int   n       = 0;
	time_t shutdown_time;
	time_t io_epoch_timer = time(0) + 2;
	time_t current_time;

	xassert (eio != NULL);
	xassert (eio->magic == EIO_MAGIC);

	while (1) {
		/* Alloc memory for pfds and map if needed */
		n = list_count(eio->obj_list);
		if (maxnfds < n) {
			maxnfds = n;
			xrealloc(pollfds, (maxnfds+1) * sizeof(struct pollfd));
			xrealloc(map, maxnfds * sizeof(eio_obj_t *));
			/* Note: xrealloc() also handles initial malloc */
		}
		if (!pollfds)  /* Fix for CLANG false positive */ goto done;

		//info("eio: handling events for %d objects", list_count(eio->obj_list));
		nfds = _poll_setup_pollfds(pollfds, map, eio->obj_list);
		if (nfds <= 0) goto done;

		/* Setup eio handle signaling fd */
		pollfds[nfds].fd     = eio->fds[0];
		pollfds[nfds].events = POLLIN;
		nfds++;

		xassert(nfds <= maxnfds + 1);

		/* Get shutdown_time to pass to _poll_internal */
		slurm_mutex_lock(&eio->shutdown_mutex);
		shutdown_time = eio->shutdown_time;
		slurm_mutex_unlock(&eio->shutdown_mutex);

		if (_poll_internal(pollfds, nfds, shutdown_time) < 0) goto error;

		/* See if we've been told to shut down by eio_signal_shutdown */
		if (pollfds[nfds-1].revents & POLLIN){ _eio_wakeup_handler(eio); }

		_poll_dispatch(pollfds, nfds - 1, map, eio->obj_list);

		current_time = time(0);
		if(io_epoch_timer < current_time){
			_flush_multi_task_cbuf();
			io_epoch_timer = time(0) + 1;
		}

		slurm_mutex_lock(&eio->shutdown_mutex);
		shutdown_time = eio->shutdown_time;
		slurm_mutex_unlock(&eio->shutdown_mutex);
		if (shutdown_time &&
		    (difftime(time(NULL), shutdown_time)>=eio->shutdown_wait)) {
			info("%s: finishing IO %d secs after job shutdown initiated", __func__, eio->shutdown_wait);
			break;
		}
	}

error:
	retval = -1;
done:
	xfree(pollfds);
	xfree(map);
	return retval;
}

extern pthread_mutex_t dstepd_full_mutex;

static void _flush_multi_task_cbuf(){
	pthread_mutex_lock(&dstepd_full_mutex);
	pthread_mutex_lock(&(tbon_vertex->io_mutex));

	if(cbuf_used(multi_task_cbuf)){
		dynpm_header_t *dynamic_io_out_header = malloc(sizeof(dynpm_header_t));
		dynamic_io_out_header->source_type =  (dynpm_source_type)PROCESS_MANAGER;
		dynamic_io_out_header->message_type = (dynpm_message_type)DYNAMIC_IO_OUT;

		dynpm_dynamic_io_out_msg_t *dynamic_io_out = malloc(sizeof(dynpm_dynamic_io_out_msg_t));
		dynamic_io_out->payload = malloc(sizeof(char)*3840);

		dynamic_io_out->id = tbon_vertex->id;
		char hostname[128];
		gethostname(hostname, 128);
		dynamic_io_out->host = strdup(hostname);

		if(cbuf_used(multi_task_cbuf) <= 3840){
			dynamic_io_out->payload_bytes = cbuf_used(multi_task_cbuf);
			memcpy((char*)dynamic_io_out->payload, (char*)multi_task_cbuf->data, dynamic_io_out->payload_bytes);
			((char*)dynamic_io_out->payload)[dynamic_io_out->payload_bytes] = '\0';
			cbuf_flush(multi_task_cbuf);
		} else {
			error("exceeded capacity");
		}

		info("sending an IO packet");
		info("header: source: %d; type: %d; payload_bytes: %d",
				dynamic_io_out_header->source_type, dynamic_io_out_header->message_type, dynamic_io_out->payload_bytes);
		//info("payload: %s;", (char*)dynamic_io_out->payload);

		//info("--- locking tbon_vertex->io_mutex");
		//pthread_mutex_lock(&(tbon_vertex->io_mutex));
		if(dynpm_send_message_to_client_connection(&dynamic_io_out_header, (void**)&dynamic_io_out,
					tbon_vertex->parent_connection)){
			error("sending DYNAMIC_IO_OUT to parent connection %x", tbon_vertex->parent_connection);
		} else {
			info("DYNAMIC_IO_OUT sent to parent connection %x", tbon_vertex->parent_connection);
		}
	} else {
		//info("nothing to send");
	}

	pthread_mutex_unlock(&(tbon_vertex->io_mutex));
	pthread_mutex_unlock(&dstepd_full_mutex);
}

static void * _io_thr(void *arg) {
	stepd_step_rec_t *step = (stepd_step_rec_t *) arg;
	sigset_t set;
	int rc;

	/* A SIGHUP signal signals a reattach to the mgr thread.  We need
	 * to block SIGHUP from being delivered to this thread so the mgr
	 * thread will see the signal.
	 */
	sigemptyset(&set);
	sigaddset(&set, SIGHUP);
	sigaddset(&set, SIGPIPE);
	pthread_sigmask(SIG_BLOCK, &set, NULL);

	info("IO handler started pid=%lu", (unsigned long) getpid());
	rc = _custom_eio_handle_mainloop(step->eio);
	info("IO handler exited, rc=%d", rc);

	slurm_mutex_lock(&step->io_mutex);
	step->io_running = false;
	slurm_cond_broadcast(&step->io_cond);
	slurm_mutex_unlock(&step->io_mutex);

	pthread_mutex_lock(&(tbon_vertex->io_mutex));
	if(cbuf_used(multi_task_cbuf) > 0){
		//info("missing output:\n\n%s\n", multi_task_cbuf->data);
		_flush_multi_task_cbuf();
	}
	cbuf_destroy(multi_task_cbuf);
	free(work_buffer);
	pthread_mutex_unlock(&(tbon_vertex->io_mutex));

	return (void *)1;
}

int io_create_local_client(const char *filename, int file_flags, stepd_step_rec_t *step, bool labelio,
		       int stdout_tasks, int stderr_tasks) {
	error("not implemented in dstepd");
	return SLURM_SUCCESS;
}

int io_initial_client_connect(srun_info_t *srun, stepd_step_rec_t *step,
			  int stdout_tasks, int stderr_tasks) {
	error("not implemented in dstepd");
	return SLURM_ERROR;
}

int io_client_connect(srun_info_t *srun, stepd_step_rec_t *step) {
	error("not implemented in dstepd");
	return SLURM_ERROR;
}

/*
 * dup the appropriate file descriptors onto the task's
 * stdin, stdout, and stderr.
 *
 * Close the server's end of the stdio pipes.
 */
int io_dup_stdio(stepd_step_task_info_t *t) {
	info("alloc_io_buf");
	if (dup2(t->stdout_fd, STDOUT_FILENO) < 0) {
		error("dup2(stdout): %m");
		return SLURM_ERROR;
	}
	fd_set_noclose_on_exec(STDOUT_FILENO);

	if (dup2(t->stderr_fd, STDERR_FILENO) < 0) {
		error("dup2(stderr): %m");
		return SLURM_ERROR;
	}
	fd_set_noclose_on_exec(STDERR_FILENO);

	return SLURM_SUCCESS;
}

struct io_buf * alloc_io_buf(void) {
	info("alloc_io_buf");
	struct io_buf *buf = xmalloc(sizeof(*buf));

	buf->ref_count = 0;
	buf->length = 0;
	/* The following "+ 1" is just temporary so I can stick a \0 at
	   the end and do a printf of the data pointer */
	buf->data = xmalloc(MAX_MSG_LEN + io_hdr_packed_size() + 1);

	return buf;
}

void free_io_buf(struct io_buf *buf) {
	info("free_io_buf");
	if (buf) {
		if (buf->data)
			xfree(buf->data);
		xfree(buf);
	}
}

void io_find_filename_pattern( stepd_step_rec_t *step,
			  slurmd_filename_pattern_t *outpattern,
			  slurmd_filename_pattern_t *errpattern,
			  bool *same_out_err_files ) {

	int ii, jj;
	int of_num_null = 0, ef_num_null = 0;
	int of_num_devnull = 0, ef_num_devnull = 0;
	int of_lastnull = -1, ef_lastnull = -1;
	bool of_all_same = true, ef_all_same = true;
	bool of_all_unique = true, ef_all_unique = true;

	*outpattern = SLURMD_UNKNOWN;
	*errpattern = SLURMD_UNKNOWN;
	*same_out_err_files = false;

	for (ii = 0; ii < step->node_tasks; ii++) {
		if (step->task[ii]->ofname == NULL) {
			of_num_null++;
			of_lastnull = ii;
		} else if (xstrcmp(step->task[ii]->ofname, "/dev/null")==0) {
			of_num_devnull++;
		}

		if (step->task[ii]->efname == NULL) {
			ef_num_null++;
			ef_lastnull = ii;
		} else if (xstrcmp(step->task[ii]->efname, "/dev/null")==0) {
			ef_num_devnull++;
		}
	}
	if (of_num_null == step->node_tasks)
		*outpattern = SLURMD_ALL_NULL;

	if (ef_num_null == step->node_tasks)
		*errpattern = SLURMD_ALL_NULL;

	if (of_num_null == 1 && of_num_devnull == step->node_tasks-1)
		*outpattern = SLURMD_ONE_NULL;

	if (ef_num_null == 1 && ef_num_devnull == step->node_tasks-1)
		*errpattern = SLURMD_ONE_NULL;

	if (*outpattern == SLURMD_ALL_NULL && *errpattern == SLURMD_ALL_NULL)
		*same_out_err_files = true;

	if (*outpattern == SLURMD_ONE_NULL && *errpattern == SLURMD_ONE_NULL &&
	    of_lastnull == ef_lastnull)
		*same_out_err_files = true;

	if (*outpattern != SLURMD_UNKNOWN && *errpattern != SLURMD_UNKNOWN)
		return;

	for (ii = 1; ii < step->node_tasks; ii++) {
		if (!step->task[ii]->ofname || !step->task[0]->ofname ||
		    xstrcmp(step->task[ii]->ofname, step->task[0]->ofname) != 0)
			of_all_same = false;

		if (!step->task[ii]->efname || !step->task[0]->efname ||
		    xstrcmp(step->task[ii]->efname, step->task[0]->efname) != 0)
			ef_all_same = false;
	}

	if (of_all_same && *outpattern == SLURMD_UNKNOWN)
		*outpattern = SLURMD_ALL_SAME;

	if (ef_all_same && *errpattern == SLURMD_UNKNOWN)
		*errpattern = SLURMD_ALL_SAME;

	if (step->task[0]->ofname && step->task[0]->efname &&
	    xstrcmp(step->task[0]->ofname, step->task[0]->efname)==0)
		*same_out_err_files = true;

	if (*outpattern != SLURMD_UNKNOWN && *errpattern != SLURMD_UNKNOWN)
		return;

	for (ii = 0; ii < step->node_tasks-1; ii++) {
		for (jj = ii+1; jj < step->node_tasks; jj++) {

			if (!step->task[ii]->ofname ||
			    !step->task[jj]->ofname ||
			    xstrcmp(step->task[ii]->ofname,
				    step->task[jj]->ofname) == 0)
				of_all_unique = false;

			if (!step->task[ii]->efname ||
			    !step->task[jj]->efname ||
			    xstrcmp(step->task[ii]->efname,
				    step->task[jj]->efname) == 0)
				ef_all_unique = false;
		}
	}

	if (of_all_unique)
		*outpattern = SLURMD_ALL_UNIQUE;

	if (ef_all_unique)
		*errpattern = SLURMD_ALL_UNIQUE;

	if (of_all_unique && ef_all_unique) {
		*same_out_err_files = true;
		for (ii = 0; ii < step->node_tasks; ii++) {
			if (step->task[ii]->ofname &&
			    step->task[ii]->efname &&
			    xstrcmp(step->task[ii]->ofname,
				    step->task[ii]->efname) != 0) {
				*same_out_err_files = false;
				break;
			}
		}
	}
}


int
io_get_file_flags(stepd_step_rec_t *step)
{
	int file_flags;

	/* set files for opening stdout/err */
	if (step->open_mode == OPEN_MODE_APPEND)
		file_flags = O_CREAT|O_WRONLY|O_APPEND;
	else if (step->open_mode == OPEN_MODE_TRUNCATE)
		file_flags = O_CREAT|O_WRONLY|O_APPEND|O_TRUNC;
	else {
		slurm_conf_t *conf = slurm_conf_lock();
		if (conf->job_file_append)
			file_flags = O_CREAT|O_WRONLY|O_APPEND;
		else
			file_flags = O_CREAT|O_WRONLY|O_APPEND|O_TRUNC;
		slurm_conf_unlock();
	}
	return file_flags;
}

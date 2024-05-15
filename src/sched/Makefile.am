# scheduler plugin
sched_malleable_la_CPPFLAGS = $(AM_CPPFLAGS) -Isrc/sched/
lib_LTLIBRARIES += sched_malleable.la

sched_malleable_la_SOURCES = src/sched/sched_malleable.c \
						   src/sched/malleability.c

sched_malleable_la_LDFLAGS = -module -avoid-version --export-dynamic \
						   -L${LIBDYNPM_INSTALL}/lib/ -ldynpmnetwork -ldynpmconfig
sched_malleable_la_LIBADD =

# simulator program that mimics the controller and scheduler threads
#simulator_CPPFLAGS = $(AM_CPPFLAGS) -I$(SLURM_SOURCE)/src/slurmctld/ -I$(SLURM_SOURCE)/src/plugins/sched/backfill/
#bin_PROGRAMS += simulator
#
## note that the simulator*.c files replace the original job_scheduler.c file
#simulator_SOURCES = src/sched/simulator.c \
#		    src/sched/simulated_backfill.c \
#		    src/sched/simulated_fifo.c \
#		    $(SLURM_SOURCE)/src/slurmctld/acct_policy.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/agent.c  	\
#		    $(SLURM_SOURCE)/src/slurmctld/backup.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/burst_buffer.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/crontab.c 	\
#		    $(SLURM_SOURCE)/src/slurmctld/fed_mgr.c 	\
#		    $(SLURM_SOURCE)/src/slurmctld/front_end.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/gang.c		\
#		    $(SLURM_SOURCE)/src/slurmctld/gres_ctld.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/groups.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/heartbeat.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/job_mgr.c 	\
#		    $(SLURM_SOURCE)/src/slurmctld/job_submit.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/licenses.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/locks.c   	\
#		    $(SLURM_SOURCE)/src/slurmctld/node_mgr.c 	\
#		    $(SLURM_SOURCE)/src/slurmctld/node_scheduler.c \
#		    $(SLURM_SOURCE)/src/slurmctld/partition_mgr.c \
#		    $(SLURM_SOURCE)/src/slurmctld/ping_nodes.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/port_mgr.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/power_save.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/preempt.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/prep_slurmctld.c \
#		    $(SLURM_SOURCE)/src/slurmctld/proc_req.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/read_config.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/reservation.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/rpc_queue.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/sched_plugin.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/slurmctld_plugstack.c \
#		    $(SLURM_SOURCE)/src/slurmctld/slurmscriptd.c \
#		    $(SLURM_SOURCE)/src/slurmctld/slurmscriptd_protocol_defs.c \
#		    $(SLURM_SOURCE)/src/slurmctld/slurmscriptd_protocol_pack.c \
#		    $(SLURM_SOURCE)/src/slurmctld/srun_comm.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/state_save.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/statistics.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/step_mgr.c	\
#		    $(SLURM_SOURCE)/src/slurmctld/trigger_mgr.c
#
#simulator_LDADD =
#
#simulator_DEPENDENCIES = $(SLURM_BUILD)/src/api/libslurmfull.la \
#						 $(SLURM_BUILD)/src/api/full_version.map
#
#simulator_LDFLAGS = -export-dynamic -ldl -L$(SLURM_BUILD)/src/api/.libs -lslurmfull \
#					-L${LIBDYNPM_INSTALL}/lib/ -ldynpmcommon

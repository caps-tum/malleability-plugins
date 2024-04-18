#drun_CPPFLAGS = $(AM_CPPFLAGS) -I$(SLURM_SOURCE)/src/srun/libsrun/
dstepd_CPPFLAGS = $(AM_CPPFLAGS) -I$(SLURM_SOURCE)/src/

sbin_PROGRAMS += dstepd

# $(SLURM_SOURCE)/src/srun/srun.wrapper.c \ this one is replaced with drun
dstepd_SOURCES = \
			   src/dstepd/slurmstepd.c \
			   src/dstepd/mgr.c \
			   src/dstepd/io.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/container.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/container.h	\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/slurmstepd.h	\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/mgr.h			\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/task.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/task.h			\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/slurmstepd_job.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/slurmstepd_job.h \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/io.h			\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/ulimits.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/ulimits.h     	\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/pdebug.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/pdebug.h		\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/pam_ses.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/pam_ses.h		\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/req.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/req.h			\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/multi_prog.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/multi_prog.h	\
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/step_terminate_monitor.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/step_terminate_monitor.h \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/x11_forwarding.c \
			   $(SLURM_SOURCE)/src/slurmd/slurmstepd/x11_forwarding.h

dstepd_LDADD = $(SLURM_BUILD)/src/api/libslurmfull.la \
			   $(SLURM_BUILD)/src/interfaces/libslurmd_interfaces.la \
			   $(SLURM_BUILD)/src/slurmd/common/libslurmd_common.la

dstepd_LDFLAGS = -export-dynamic -ldl -Wl,-rpath=${exec_prefix}/lib/slurm \
			   -L$(exec_prefix)/lib/ -lslurm \
			   -L${LIBDYNPM_INSTALL}/lib/ -ldynpmnetwork -ldynpmconfig -ldynpmshm

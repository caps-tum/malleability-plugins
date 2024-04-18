#drun_CPPFLAGS = $(AM_CPPFLAGS) -I$(SLURM_SOURCE)/src/srun/libsrun/
drun_CPPFLAGS = $(AM_CPPFLAGS) -I$(SLURM_SOURCE)/src/srun/

bin_PROGRAMS += drun

# $(SLURM_SOURCE)/src/srun/srun.wrapper.c \ this one is replaced with drun
drun_SOURCES = src/drun/drun.c \
			   src/drun/srun.c \
			   src/drun/launch.c \
			   src/drun/step_launch.c \
			   $(SLURM_SOURCE)/src/srun/allocate.c \
			   $(SLURM_SOURCE)/src/srun/allocate.h \
			   $(SLURM_SOURCE)/src/srun/debugger.c \
			   $(SLURM_SOURCE)/src/srun/debugger.h \
			   $(SLURM_SOURCE)/src/srun/fname.c \
			   $(SLURM_SOURCE)/src/srun/fname.h \
			   $(SLURM_SOURCE)/src/srun/launch.h \
			   $(SLURM_SOURCE)/src/srun/multi_prog.c \
			   $(SLURM_SOURCE)/src/srun/multi_prog.h \
			   $(SLURM_SOURCE)/src/srun/opt.c opt.h \
			   $(SLURM_SOURCE)/src/srun/srun_job.c \
			   $(SLURM_SOURCE)/src/srun/srun_job.h \
			   $(SLURM_SOURCE)/src/srun/srun_pty.c \
			   $(SLURM_SOURCE)/src/srun/srun_pty.h \
			   $(SLURM_SOURCE)/src/srun/step_ctx.c \
			   $(SLURM_SOURCE)/src/srun/step_ctx.h \
			   $(SLURM_SOURCE)/src/srun/task_state.c \
			   $(SLURM_SOURCE)/src/srun/task_state.h

drun_LDADD = $(SLURM_BUILD)/src/bcast/libfile_bcast.la

drun_DEPENDENCIES = $(SLURM_BUILD)/src/bcast/libfile_bcast.la \
					$(SLURM_BUILD)/src/api/full_version.map \
					$(SLURM_BUILD)/src/api/libslurmfull.la

drun_LDFLAGS = -export-dynamic -ldl -Wl,-rpath=${exec_prefix}/lib/slurm \
			   -L$(SLURM_BUILD)/src/api/.libs -lslurmfull \
			   -L${LIBDYNPM_INSTALL}/lib/ -ldynpmnetwork -ldynpmconfig -lbz2

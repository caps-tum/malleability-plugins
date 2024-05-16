ACLOCAL_AMFLAGS = -I m4
EXTRA_DIST =
AUTOMAKE_OPTIONS = foreign

AM_CPPFLAGS = -DSLURM_PLUGIN_DEBUG -I$(SLURM_SOURCE) -I$(SLURM_SOURCE)/src/common \
	      -I$(SLURM_BUILD) -Werror -I$(top_srcdir)/. -I$(top_srcdir)/include/ \
	      -I$(PMIX_INSTALL)/include -I$(LIBEVENT_INSTALL)/include \
		  -I$(LIBDYNPM_INSTALL)/include \
	      -I$(prefix)/include
AM_CXXFLAGS = -fexceptions

bin_PROGRAMS =
sbin_PROGRAMS =
lib_LTLIBRARIES =

include src/mpi/Makefile.am
include src/sched/Makefile.am
include src/select/Makefile.am
include src/switch/Makefile.am

# include src/mrun/Makefile.am
# include src/mstepd/Makefile.am

# workaround for seemingly buggy libtool around changes to libdir
install-exec-hook:
	ls ${libdir}/*_malleable*
	mv ${libdir}/*_malleable* ${libdir}/slurm/

uninstall-hook:
	ls ${libdir}/slurm/*_malleable*
	rm -rf ${libdir}/slurm/*_malleable*

AC_INIT(deepsea-slurm-plugins, m4_esyscmd_s([cat VERSION]), isaias.compres@tum.de)
AC_CONFIG_SRCDIR(src/)
AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE(subdir-objects)
AM_SILENT_RULES([yes])
AC_CONFIG_HEADERS([deepsea_plugins_config.h])
AC_LANG([C])

LT_INIT

AM_PROG_LEX
AC_PROG_CC
AC_PROG_YACC

dnl these were copied from Slurm
X_AC_MUNGE
X_AC_PMIX
X_AC_HWLOC

dnl we require a Slurm source tree to build
AC_ARG_WITH([slurmsrc],
	    [AS_HELP_STRING([--with-slurmsrc Slurm source code path (required)])],
			    [slurm_source_tree="${withval}"],
			    [slurm_source_tree=""])
if test "x${slurm_source_tree}" == "x"; then
	echo "Please specify Slurm's source code path with '--with-slurmsrc'"
	echo "ERROR: cannot build without a Slurm source tree."
	exit -1
else
	echo "Using Slurm's source tree in: ${slurm_source_tree}"
fi

dnl we require a Slurm build directory to build
AC_ARG_WITH([slurmbuild],
	    [AS_HELP_STRING([--with-slurmbuild Slurm source code path (required)])],
			    [slurm_builddir="${withval}"],
			    [slurm_builddir=""])
if test "x${slurm_builddir}" == "x"; then
	echo "Please specify Slurm's source code path with '--with-slurmbuild'"
	echo "ERROR: cannot build without a Slurm build directory."
	exit -1
else
	echo "Using Slurm's source tree in: ${slurm_builddir}"
fi

dnl we require Open PMIx to build
AC_ARG_WITH([pmix],
	    [AS_HELP_STRING([--with-pmix Open PMIx path (required)])],
			    [pmix_install="${withval}"],
			    [pmix_install=""])
if test "x${pmix_install}" == "x"; then
	echo "Please specify Open PMIx's path with '--with-pmix'"
	echo "ERROR: cannot build without Open PMIx."
	exit -1
else
	echo "Using Open PMIx's in: ${pmix_install}"
fi

dnl we require libevent to build
AC_ARG_WITH([libevent],
	    [AS_HELP_STRING([--with-libevent libevent path (required)])],
			    [libevent_install="${withval}"],
			    [libevent_install=""])
if test "x${libevent_install}" == "x"; then
	echo "did not specify libevent's path with '--with-libevent'"
else
	echo "Using libevent's in: ${libevent_install}"
fi

dnl we require libdynpm to build
AC_ARG_WITH([libdynpm],
	    [AS_HELP_STRING([--with-libdynpm libdynpm path (required)])],
			    [libdynpm_install="${withval}"],
			    [libdynpm_install=""])
if test "x${libdynpm_install}" == "x"; then
	echo "Please specify libdynpm's path with '--with-libdynpm'"
	echo "ERROR: cannot build without libdynpm."
	exit -1
else
	echo "Using libdynpm's in: ${libdynpm_install}"
fi

AC_SUBST([SLURM_SOURCE],       [${slurm_source_tree}])
AC_SUBST([SLURM_BUILD],        [${slurm_builddir}])
AC_SUBST([PMIX_INSTALL],       [${pmix_install}])
AC_SUBST([LIBEVENT_INSTALL],   [${libevent_install}])
AC_SUBST([LIBDYNPM_INSTALL],   [${libdynpm_install}])

AC_OUTPUT(Makefile)




mpi_malleable_la_CPPFLAGS = $(AM_CPPFLAGS)
lib_LTLIBRARIES += mpi_malleable.la
mpi_malleable_la_SOURCES = src/mpi/mpi_malleable.c
mpi_malleable_la_LDFLAGS = -module -avoid-version --export-dynamic \
						 -L${LIBDYNPM_INSTALL}/lib/ -ldynpmnetwork
mpi_malleable_la_LIBADD =

mpi_deepsea_la_CPPFLAGS = $(AM_CPPFLAGS)
lib_LTLIBRARIES += mpi_deepsea.la
mpi_deepsea_la_SOURCES = src/mpi/mpi_deepsea.c
mpi_deepsea_la_LDFLAGS = -module -avoid-version --export-dynamic \
						 -L${LIBDYNPM_INSTALL}/lib/ -ldynpmnetwork
mpi_deepsea_la_LIBADD =

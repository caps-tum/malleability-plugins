lib_LTLIBRARIES += switch_deepsea.la

switch_deepsea_la_SOURCES = src/switch/switch_deepsea.c
switch_deepsea_la_LDFLAGS = -module -avoid-version --export-dynamic \
							-L${LIBDYNPM_INSTALL}/lib/ -ldynpmnetwork -ldynpmconfig -lbz2
switch_deepsea_la_LIBADD =

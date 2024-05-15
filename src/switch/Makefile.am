lib_LTLIBRARIES += switch_malleable.la

switch_malleable_la_SOURCES = src/switch/switch_malleable.c
switch_malleable_la_LDFLAGS = -module -avoid-version --export-dynamic \
							-L${LIBDYNPM_INSTALL}/lib/ -ldynpmnetwork -ldynpmconfig -lbz2
switch_malleable_la_LIBADD =

MODULE_big = matrixgate

PGFILEDESC = "matrixgate - supportive functions and workers"
OBJS = matrixgate.o warden.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/matrixgate
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

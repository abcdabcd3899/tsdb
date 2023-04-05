MODULE_big = matrixmgr

EXTENSION = matrixmgr
OBJS = matrixmgr.o

DATA = matrixmgr--3.0.sql matrixdb.supervisor.service supervisor.conf alert.yml
DATA += matrixdb-supervisor.centos.sysv
DATA += matrixdb-supervisor.default
DATA += supervisor.mxui.conf
DATA += supervisor.cylinder.conf
DATA += matrixmgr--3.0--3.1.sql

DOCS = dashboard.json alert.json

REGRESS += mxmgr/mxmgr_1_setup
REGRESS += mxmgr/mxmgr_2_metadata
REGRESS += mxmgr/mxmgr_2_1_cmdrun
REGRESS += mxmgr/mxmgr_3_gatecheck
REGRESS += mxmgr/mxmgr_4_telegrafcheck
REGRESS += mxmgr/mxmgr_5_deployall

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/matrixmgr
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

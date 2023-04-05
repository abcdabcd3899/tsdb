MODULE_big = matrixts

EXTENSION = matrixts
SCHEMAS += matrixts--2.0.sql
SCHEMAS += matrixts--2.0--2.1.sql
SCHEMAS += matrixts--2.1--2.2.sql
SCHEMAS += matrixts--2.2--2.3.sql
SCHEMAS += matrixts--2.3--2.4.sql

DATA += $(SCHEMAS)
EXTRA_CLEAN += $(SCHEMAS)

# MXKV is only enabled in enterprise build, but the $(enable_enterprise) is
# not defined yet, which is defined by including Makefile.global, so we have
# to do some dirty works.
top_builddir = ../..
enable_enterprise = $(word 3, $(shell grep '^enable_enterprise' $(top_builddir)/src/Makefile.global))

OBJS = matrixts.o planner.o time_bucket.o agg_bookend.o sort_transform.o func_cache.o estimate.o guc.o utils.o
OBJS += plan_agg_bookend.o

# Toin index
OBJS += toin.o
OBJS += toin_brin.o
OBJS += toin_nbtree.o
OBJS += toin_nbtsearch.o
OBJS += toin_nbtutils.o

# Odin
OBJS += odin.o

# Reorder
OBJS += reorder.o

# Gapfill
OBJS += gapfill/gapfill.o
OBJS += gapfill/gapfill_custom_scan_exec.o
OBJS += gapfill/gapfill_custom_scan_planner.o

# MXKV is only enabled in enterprise build
ifeq ($(enable_enterprise), yes)
OBJS += mxkv/bits_writer.o
OBJS += mxkv/config.o
OBJS += mxkv/delta_coding.o
OBJS += mxkv/delta_decoder.o
OBJS += mxkv/delta_encoder.o
OBJS += mxkv/module.o
OBJS += mxkv/mxkv.o
OBJS += mxkv/mxkv_in.o
OBJS += mxkv/mxkv_iter.o
OBJS += mxkv/types.o
endif # enable_enterprise

# Automated Partition Management
OBJS += apm.o

# SortHeap
OBJS += sortheap_mock.o
# Continuous View
OBJS += continuous_view/continuous_view.o
OBJS += continuous_view/continuous_view_plan.o
OBJS += continuous_view/continuous_view_customscan.o
OBJS += continuous_view/continuous_view_insert.o
OBJS += continuous_view/continuous_view_sortheap.o
OBJS += continuous_view/continuous_view_definition.o
OBJS += continuous_view/continuous_view_helper.o

# Last Not Null
OBJS += last_not_null_kv.o

PG_CPPFLAGS = -I$(libpq_srcdir) -DPG12

CONTRIB_TESTDB = $(EXTENSION)_regression

REGRESS_OPTS += --init-file=init_file

# odin bitmapscan is disabled for now

REGRESS += toin/setup

REGRESS += toin/load_heap
REGRESS += toin/toin_types
REGRESS += toin/btree_1_setup
REGRESS += toin/btree_1_indexscan
REGRESS += toin/btree_2_setup
REGRESS += toin/btree_2_indexscan
REGRESS += toin/odin_1_setup
REGRESS += toin/odin_1_indexscan
REGRESS += toin/odin_1_indexscan_parallel
#REGRESS += toin/odin_1_bitmapscan
REGRESS += toin/odin_2_setup
REGRESS += toin/odin_2_indexscan
REGRESS += toin/odin_2_indexscan_parallel
#REGRESS += toin/odin_2_bitmapscan
REGRESS += toin/brin_1_setup
REGRESS += toin/brin_1_bitmapscan
REGRESS += toin/brin_2_setup
REGRESS += toin/brin_2_bitmapscan
REGRESS += toin/toin_1_setup
REGRESS += toin/toin_1_indexscan
REGRESS += toin/toin_1_bitmapscan
REGRESS += toin/toin_2_setup
REGRESS += toin/toin_2_indexscan
REGRESS += toin/toin_2_bitmapscan

REGRESS += toin/load_heap_parted
REGRESS += toin/toin_types
REGRESS += toin/btree_1_setup
REGRESS += toin/btree_1_indexscan
REGRESS += toin/btree_2_setup
REGRESS += toin/btree_2_indexscan
REGRESS += toin/odin_1_setup
REGRESS += toin/odin_1_indexscan
REGRESS += toin/odin_1_indexscan_parallel
#REGRESS += toin/odin_1_bitmapscan
REGRESS += toin/odin_2_setup
REGRESS += toin/odin_2_indexscan
REGRESS += toin/odin_2_indexscan_parallel
#REGRESS += toin/odin_2_bitmapscan
REGRESS += toin/brin_1_setup
REGRESS += toin/brin_1_bitmapscan
REGRESS += toin/brin_2_setup
REGRESS += toin/brin_2_bitmapscan
REGRESS += toin/toin_1_setup
REGRESS += toin/toin_1_indexscan
REGRESS += toin/toin_1_bitmapscan
REGRESS += toin/toin_2_setup
REGRESS += toin/toin_2_indexscan
REGRESS += toin/toin_2_bitmapscan

REGRESS += toin/load_ao
REGRESS += toin/toin_types
REGRESS += toin/btree_1_setup
REGRESS += toin/btree_1_indexscan
REGRESS += toin/btree_2_setup
REGRESS += toin/btree_2_indexscan
REGRESS += toin/odin_1_setup
REGRESS += toin/odin_1_indexscan
REGRESS += toin/odin_1_indexscan_parallel
#REGRESS += toin/odin_1_bitmapscan
REGRESS += toin/odin_2_setup
REGRESS += toin/odin_2_indexscan
REGRESS += toin/odin_2_indexscan_parallel
#REGRESS += toin/odin_2_bitmapscan
REGRESS += toin/brin_1_setup
REGRESS += toin/brin_1_bitmapscan
REGRESS += toin/brin_2_setup
REGRESS += toin/brin_2_bitmapscan
REGRESS += toin/toin_1_setup
REGRESS += toin/toin_1_indexscan
REGRESS += toin/toin_1_bitmapscan
REGRESS += toin/toin_2_setup
REGRESS += toin/toin_2_indexscan
REGRESS += toin/toin_2_bitmapscan

REGRESS += toin/load_ao_parted
REGRESS += toin/toin_types
REGRESS += toin/btree_1_setup
REGRESS += toin/btree_1_indexscan
REGRESS += toin/btree_2_setup
REGRESS += toin/btree_2_indexscan
REGRESS += toin/odin_1_setup
REGRESS += toin/odin_1_indexscan
REGRESS += toin/odin_1_indexscan_parallel
#REGRESS += toin/odin_1_bitmapscan
REGRESS += toin/odin_2_setup
REGRESS += toin/odin_2_indexscan
REGRESS += toin/odin_2_indexscan_parallel
#REGRESS += toin/odin_2_bitmapscan
REGRESS += toin/brin_1_setup
REGRESS += toin/brin_1_bitmapscan
REGRESS += toin/brin_2_setup
REGRESS += toin/brin_2_bitmapscan
REGRESS += toin/toin_1_setup
REGRESS += toin/toin_1_indexscan
REGRESS += toin/toin_1_bitmapscan
REGRESS += toin/toin_2_setup
REGRESS += toin/toin_2_indexscan
REGRESS += toin/toin_2_bitmapscan

REGRESS += toin/load_aocs
REGRESS += toin/toin_types
REGRESS += toin/btree_1_setup
REGRESS += toin/btree_1_indexscan
REGRESS += toin/btree_2_setup
REGRESS += toin/btree_2_indexscan
REGRESS += toin/odin_1_setup
REGRESS += toin/odin_1_indexscan
REGRESS += toin/odin_1_indexscan_parallel
#REGRESS += toin/odin_1_bitmapscan
REGRESS += toin/odin_2_setup
REGRESS += toin/odin_2_indexscan
REGRESS += toin/odin_2_indexscan_parallel
#REGRESS += toin/odin_2_bitmapscan
REGRESS += toin/brin_1_setup
REGRESS += toin/brin_1_bitmapscan
REGRESS += toin/brin_2_setup
REGRESS += toin/brin_2_bitmapscan
REGRESS += toin/toin_1_setup
REGRESS += toin/toin_1_indexscan
REGRESS += toin/toin_1_bitmapscan
REGRESS += toin/toin_2_setup
REGRESS += toin/toin_2_indexscan
REGRESS += toin/toin_2_bitmapscan

REGRESS += toin/load_aocs_parted
REGRESS += toin/toin_types
REGRESS += toin/btree_1_setup
REGRESS += toin/btree_1_indexscan
REGRESS += toin/btree_2_setup
REGRESS += toin/btree_2_indexscan
REGRESS += toin/odin_1_setup
REGRESS += toin/odin_1_indexscan
REGRESS += toin/odin_1_indexscan_parallel
#REGRESS += toin/odin_1_bitmapscan
REGRESS += toin/odin_2_setup
REGRESS += toin/odin_2_indexscan
REGRESS += toin/odin_2_indexscan_parallel
#REGRESS += toin/odin_2_bitmapscan
REGRESS += toin/brin_1_setup
REGRESS += toin/brin_1_bitmapscan
REGRESS += toin/brin_2_setup
REGRESS += toin/brin_2_bitmapscan
REGRESS += toin/toin_1_setup
REGRESS += toin/toin_1_indexscan
REGRESS += toin/toin_1_bitmapscan
REGRESS += toin/toin_2_setup
REGRESS += toin/toin_2_indexscan
REGRESS += toin/toin_2_bitmapscan

REGRESS += reorder
REGRESS += count
REGRESS += first_last
REGRESS += wide_ao
REGRESS += last_not_null
REGRESS += last_not_null_kv

REGRESS += gapfill
REGRESS += continuous_view
REGRESS += continuous_view/partition_inherit

# MXKV is only enabled in enterprise build
ifeq ($(enable_enterprise), yes)
# setup must be the first
MXKV_REGRESS += mxkv/setup

MXKV_REGRESS += mxkv/syntax
MXKV_REGRESS += mxkv/import_keys
MXKV_REGRESS += mxkv/compress
# mxkv/utils depends on mxkv/syntax
MXKV_REGRESS += mxkv/utils

REGRESS += $(MXKV_REGRESS)
endif # enable_enterprise

# Automated Partition Management
APM_REGRESS += apm/schema
APM_REGRESS += apm/split_cn
APM_REGRESS += apm/split_utc
APM_REGRESS += apm/split_us
APM_REGRESS += apm/add_prt_cn
APM_REGRESS += apm/add_prt_utc
APM_REGRESS += apm/add_prt_us
APM_REGRESS += apm/expire_drop_cn
APM_REGRESS += apm/expire_drop_utc
APM_REGRESS += apm/expire_drop_us
APM_REGRESS += apm/api
APM_REGRESS += apm/utils
APM_REGRESS += apm/mars
REGRESS += $(APM_REGRESS)

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/matrixts
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifeq ($(enable_enterprise), yes)
installcheck-mxkv: override REGRESS = $(MXKV_REGRESS)
installcheck-mxkv: installcheck
endif # enable_enterprise

installcheck-apm: override REGRESS = $(APM_REGRESS)
installcheck-apm: installcheck

ifneq ($(wildcard extra/),)
# Install out-of-tree components
install: install-extra
.PHONY: install-extra
install-extra:
	+test -f extra/bin/mxgate && extra/bin/mxgate --version
	+test -f extra/bin/mxgated && extra/bin/mxgated --version
	+test -f extra/bin/supervisord && extra/bin/supervisord version
	+test -f extra/bin/telegraf && extra/bin/telegraf --version
	+test -f extra/bin/mxctl && extra/bin/mxctl --version
	+test -f extra/bin/mxui && extra/bin/mxui --version
	+test -f extra/bin/cylinder && extra/bin/cylinder --version
	+test -f extra/bin/mxbench && extra/bin/mxbench --version
	+test -f extra/bin/mxbackup && extra/bin/mxbackup --version
	+test -f extra/bin/mxrestore && extra/bin/mxrestore --version
	+test -f extra/bin/mxbackup_helper && extra/bin/mxbackup_helper --version
	+tar -cf - -C extra/ . | tar -xf - -C $(prefix)/
	+ln -sf mxctl $(prefix)/bin/mxmgr_ctrl
endif

all: $(SCHEMAS)
install: $(SCHEMAS)

# the order is important, schema/matrixts--2.0.sql must be the first
matrixts--2.0.sql: schema/matrixts--2.0.sql

# MXKV is only enabled in enterprise build
ifeq ($(enable_enterprise), yes)
matrixts--2.0--2.1.sql: schema/mxkv--2.0--2.1.sql
endif # enable_enterprise
matrixts--2.0--2.1.sql: schema/apm--2.0--2.1.sql

matrixts--2.1--2.2.sql: schema/apm--2.1--2.2.sql

matrixts--2.2--2.3.sql: schema/sortheap--2.2--2.3.sql schema/continuous_view--2.2--2.3.sql
matrixts--2.2--2.3.sql: schema/last-not-null--2.2--2.3.sql

matrixts--2.3--2.4.sql: schema/apm--2.3--2.4.sql
matrixts--2.3--2.4.sql: schema/last-not-null--2.3--2.4.sql

matrixts--%.sql:
	cat $^ > $@

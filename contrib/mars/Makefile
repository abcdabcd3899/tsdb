MODULE_big = mars

EXTENSION = mars
DATA = mars--0.1.sql
DATA += mars--1.0.sql
DATA += mars--1.0--1.1.sql
DATA += mars--1.1--1.2.sql
DATA += mars--1.2--1.3.sql
DATA += mars--1.3--1.4.sql
DATA += mars--1.4--1.5.sql

OBJS = mars.o marsam.o FileManager.o ParquetWriter.o ParquetReader.o MarsFileSerializer.o tools.o
OBJS += guc.o
OBJS += utils.o
OBJS += InsertOp.o
OBJS += MarsRowGroupSerializer.o
OBJS += Footer.o
OBJS += ScanKey.o
OBJS += ColumnStore.o
OBJS += RowStore.o
OBJS += Scanner.o
OBJS += BlockReader.o
OBJS += BlockMerger.o
OBJS += MarsFile.o
OBJS += Tle.o
OBJS += Compare.o
OBJS += PreAgg.o
OBJS += mars_planner.o
OBJS += customnodes/modifytable.o

# mars encoder
OBJS += coder/Coder.o
OBJS += coder/Encoder.o
OBJS += coder/Decoder.o

# mars lz4
OBJS += coder/lz4/lz4.o
OBJS += coder/lz4/lz4_compression.o

OBJS += coder/zstd/zstd_compression.o

# gorilla
OBJS += coder/gorilla/gorilla.o
OBJS += coder/gorilla/delta_delta.o
OBJS += coder/gorilla/bit_writer.o

# agg pushdown
OBJS += marscustomscan.o
OBJS += marscustomaggscan.o
OBJS += marscustomsortscan.o

REGRESS_OPTS += --init-file=$(top_srcdir)/src/test/regress/init_file
REGRESS_OPTS += --init-file=init_file

REGRESS = init
REGRESS += tsbs
REGRESS += createtable
REGRESS += read
REGRESS += upsert
REGRESS += insert
REGRESS += udf
REGRESS += variable_length_type
REGRESS += varlen_type_as_tag
REGRESS += varlen_type_as_globalorder
REGRESS += orderkeys_setup
REGRESS += orderkeys_none
REGRESS += orderkeys_block12
REGRESS += orderkeys_table12
REGRESS += orderkeys_table12_disorder
REGRESS += orderkeys_table12_groups
REGRESS += orderkeys_block12_table1
REGRESS += orderkeys_block12_table12
# REGRESS += orderkeys_block12_table2
# REGRESS += orderkeys_block12_table21
REGRESS += orderkeys_table_float_groups
REGRESS += orderkeys_table_float_onegroup
REGRESS += scandir
REGRESS += timeseries
REGRESS += compression
REGRESS += analyze
REGRESS += wide
REGRESS += pre_agg
REGRESS += gapfill
REGRESS += count
REGRESS += sum
REGRESS += first_last
REGRESS += min_max
REGRESS += join
REGRESS += null
REGRESS += unsupport
REGRESS += xlog
REGRESS += auxtable
REGRESS += compatibility_index_on_order_col
REGRESS += filters
REGRESS += bfv
REGRESS += file_format
REGRESS += preagg_varlen_type
### disable "disordered_insert" because the merge-insert has been replaced by
### the merge-scan.
# REGRESS += disordered_insert
### disable "update" because the feature is disabled, it is replaced by the
### insert+mergescan.
# REGRESS += update
REGRESS += mergescan
REGRESS += multikeys
REGRESS += float
REGRESS += jsonb

REGRESS += gorilla/compress
REGRESS += gorilla/filter
REGRESS += gorilla/delta_delta

PG_CPPFLAGS := $(shell pkg-config --cflags arrow parquet)
SHLIB_LINK := $(shell pkg-config --libs arrow parquet)

PGFILEDESC = "MatrixDB Append-only Resilient Storage"

PRIVATE_LINK_OPTION = -lstdc++ -lzstd
PRIVATE_CPP_FLAGS = -std=c++11 -fPIC

SHLIB_LINK += $(PRIVATE_LINK_OPTION)

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/mars
top_builddir = ../..

include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifeq ($(autodepend)+$(GCC), yes+yes)
%.o: %.cc
	@if test ! -d $(DEPDIR); then mkdir -p $(DEPDIR); fi
	$(CXX) $(PRIVATE_CPP_FLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@ -MMD -MP -MF $(DEPDIR)/$(*F).Po
else
%.o: %.cc
	$(CXX) $(PRIVATE_CPP_FLAGS) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@
endif

installcheck-aux-v1-column-v0.1: override export PGOPTIONS = -c mars.default_auxfile_format=v1 -c mars.default_file_format=v0.1
installcheck-aux-v1-column-v0.1: installcheck

installcheck-aux-v1-column-v1.0: override export PGOPTIONS = -c mars.default_auxfile_format=v1 -c mars.default_file_format=v1.0
installcheck-aux-v1-column-v1.0: installcheck

installcheck-aux-v2-column-v0.1: override export PGOPTIONS = -c mars.default_auxfile_format=v2 -c mars.default_file_format=v0.1
installcheck-aux-v2-column-v0.1: installcheck

installcheck-aux-v2-column-v1.0: override export PGOPTIONS = -c mars.default_auxfile_format=v2 -c mars.default_file_format=v1.0
installcheck-aux-v2-column-v1.0: installcheck

# FIXME: we have to disable the unittests because now we compare the datums
# with the pg functions, which requires a lot of mocking to run in the
# unittests.
.PHONY: unittest
unittest:
	@echo "mars unittest is skipped"
	#$(MAKE) -C unittest $@

ifneq ($(shell which doxygen),)
.PHONY: doc
doc:
	-rm -rf doc
	doxygen
endif

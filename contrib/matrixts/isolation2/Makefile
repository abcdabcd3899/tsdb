ifdef USE_PGXS
$(error MatrixTS isolation2 tests are not available in a PGXS build)
endif # USE_PGXS

subdir = contrib/matrixts/isolation2
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

isolation2dir := $(top_builddir)/src/test/isolation2

all:
	$(MAKE) -C $(isolation2dir) install

clean distclean:
	rm -rf $(pg_regress_clean_files)

installcheck: all
	cd $(isolation2dir); \
	$(pg_isolation2_regress_installcheck) \
		--inputdir=../../../$(subdir) \
		--outputdir=../../../$(subdir) \
		--schedule=../../../$(subdir)/isolation2_schedule

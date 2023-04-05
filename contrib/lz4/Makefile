PG_CONFIG = pg_config

MODULE_big = gp_lz4_compression
OBJS = lz4_compression.o

# lz4.[ch] is copied from https://github.com/lz4/lz4/tree/v1.9.3
OBJS += lz4.o

REGRESS = lz4_column_compression compression_lz4 lz4_abort_leak AOCO_lz4 AORO_lz4

ifdef USE_PGXS
  PGXS := $(shell pg_config --pgxs)
  include $(PGXS)
else
  top_builddir = ../..
  include $(top_builddir)/src/Makefile.global
  include $(top_srcdir)/contrib/contrib-global.mk
endif


# Install into cdb_init.d, so that the catalog changes performed by initdb,
# and the compressor is available in all databases.
.PHONY: install-data
install-data:
	$(INSTALL_DATA) lz4_compression.sql '$(DESTDIR)$(datadir)/cdb_init.d/lz4_compression.sql'

install: install-data

.PHONY: uninstall-data

uninstall-data:
	rm -f '$(DESTDIR)$(datadir)/cdb_init.d/lz4_compression.sql'

uninstall: uninstall-data

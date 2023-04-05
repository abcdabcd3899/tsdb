MODULE_big = telemetry

OBJS = telemetry.o bg_monitor.o monitor.o conn.o conn_plain.o conn_ssl.o http.o http_request.o http_response.o guc.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/telemetry
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

SHLIB_LINK += -lcrypto -lssl

endif

# Install into cdb_init.d, so that the catalog changes performed by initdb,
# and the compressor is available in all databases.
.PHONY: install-data
install-data:
	$(INSTALL_DATA) telemetry.sql '$(DESTDIR)$(datadir)/cdb_init.d/telemetry.sql'

install: install-data

.PHONY: uninstall-data

uninstall-data:
	rm -f '$(DESTDIR)$(datadir)/cdb_init.d/telemetry.sql'

uninstall: uninstall-data

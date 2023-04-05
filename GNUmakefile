#
# PostgreSQL top level makefile
#
# GNUmakefile.in
#

subdir =
top_builddir = .
include $(top_builddir)/src/Makefile.global

$(call recurse,all install,src config)

all:
	$(MAKE) -C contrib/auto_explain all
	$(MAKE) -C contrib/citext all
	$(MAKE) -C contrib/file_fdw all
	$(MAKE) -C contrib/formatter all
	$(MAKE) -C contrib/formatter_fixedwidth all
	$(MAKE) -C contrib/fuzzystrmatch all
	$(MAKE) -C contrib/extprotocol all
	$(MAKE) -C contrib/dblink all
	$(MAKE) -C contrib/indexscan all
	$(MAKE) -C contrib/pageinspect all  # needed by src/test/isolation
	$(MAKE) -C contrib/hstore all
ifeq ($(enable_enterprise), yes)
	$(MAKE) -C contrib/pgcrypto all
endif
	$(MAKE) -C contrib/matrixts $@
	$(MAKE) -C contrib/mars $@
	$(MAKE) -C contrib/matrixmgr $@
	$(MAKE) -C contrib/matrixgate $@
	$(MAKE) -C contrib/telemetry $@
	$(MAKE) -C contrib/lz4 $@
ifeq ($(with_openssl), yes)
	$(MAKE) -C contrib/sslinfo all
endif
	$(MAKE) -C gpAux/extensions all
	$(MAKE) -C gpMgmt all
	$(MAKE) -C gpcontrib all
	+@echo "All of Greenplum Database successfully made. Ready to install."

docs:
	$(MAKE) -C doc all

$(call recurse,world,doc src config contrib gpcontrib,all)
world:
	+@echo "PostgreSQL, contrib, and documentation successfully made. Ready to install."

# build src/ before contrib/
world-contrib-recurse: world-src-recurse

html man:
	$(MAKE) -C doc $@

install:
	$(MAKE) -C contrib/auto_explain $@
	$(MAKE) -C contrib/citext $@
	$(MAKE) -C contrib/file_fdw $@
	$(MAKE) -C contrib/formatter $@
	$(MAKE) -C contrib/formatter_fixedwidth $@
	$(MAKE) -C contrib/fuzzystrmatch $@
	$(MAKE) -C contrib/extprotocol $@
	$(MAKE) -C contrib/dblink $@
	$(MAKE) -C contrib/indexscan $@
	$(MAKE) -C contrib/pageinspect $@  # needed by src/test/isolation
	$(MAKE) -C contrib/hstore $@
ifeq ($(enable_enterprise), yes)
	$(MAKE) -C contrib/pgcrypto $@
endif
	$(MAKE) -C contrib/matrixts $@
	$(MAKE) -C contrib/mars $@
	$(MAKE) -C contrib/matrixmgr $@
	$(MAKE) -C contrib/matrixgate $@
	$(MAKE) -C contrib/telemetry $@
	$(MAKE) -C contrib/lz4 $@
ifeq ($(with_postgresfdw), yes)
	$(MAKE) -C contrib/postgres_fdw $@
endif
ifeq ($(with_mysqlfdw), yes)
	$(MAKE) -C contrib/mysql_fdw $@
endif
ifeq ($(with_mongofdw), yes)
	contrib/mongo_fdw/autogen.sh --with-release
	$(MAKE) -C contrib/mongo_fdw $@
endif
ifeq ($(with_openssl), yes)
	$(MAKE) -C contrib/sslinfo $@
endif
	$(MAKE) -C gpMgmt $@
	$(MAKE) -C gpAux/extensions $@
	$(MAKE) -C gpcontrib $@
	+@echo "Greenplum Database installation complete."

install-docs:
	$(MAKE) -C doc install

$(call recurse,install-world,doc src config contrib gpcontrib,install)
install-world:
	+@echo "PostgreSQL, contrib, and documentation installation complete."

# build src/ before contrib/
install-world-contrib-recurse: install-world-src-recurse

$(call recurse,installdirs uninstall init-po update-po,doc src config gpcontrib)

$(call recurse,distprep coverage,doc src config contrib gpcontrib)

# clean, distclean, etc should apply to contrib too, even though
# it's not built by default
$(call recurse,clean,doc contrib gpcontrib src config)
clean:
	rm -rf tmp_install/
# Garbage from autoconf:
	@rm -rf autom4te.cache/
# leap over gpAux/Makefile into subdirectories to avoid circular dependency.
# gpAux/Makefile is the entry point for the enterprise build, which ends up
# calling top-level configure and this Makefile
	$(MAKE) -C gpAux/extensions $@
	$(MAKE) -C gpMgmt $@

# Important: distclean `src' last, otherwise Makefile.global
# will be gone too soon.
distclean maintainer-clean:
#	$(MAKE) -C doc $@
	$(MAKE) -C gpAux/extensions $@
	$(MAKE) -C contrib $@
	$(MAKE) -C gpcontrib $@
	$(MAKE) -C config $@
	$(MAKE) -C gpMgmt $@
	$(MAKE) -C src $@
	rm -rf tmp_install/
# Garbage from autoconf:
	@rm -rf autom4te.cache/
	rm -f config.cache config.log config.status GNUmakefile

installcheck-resgroup:
	$(MAKE) -C src/test/isolation2 $@

# A full ICW takes quite a long time, we only run a core subset for now.
.PHONY: installcheck-matrixdb
installcheck-matrixdb: submake-generated-headers
	$(MAKE) -C src/test/regress installcheck
	$(MAKE) -C src/test/isolation2 installcheck
	$(MAKE) -C contrib/matrixts installcheck
	$(MAKE) -C contrib/matrixmgr installcheck
	$(MAKE) -C contrib/lz4 installcheck

# Create or destroy a demo cluster.
create-demo-cluster:
	$(MAKE) -C gpAux/gpdemo create-demo-cluster

destroy-demo-cluster:
	$(MAKE) -C gpAux/gpdemo destroy-demo-cluster

check-tests installcheck installcheck-parallel installcheck-tests: CHECKPREP_TOP=src/test/regress
check-tests installcheck installcheck-parallel installcheck-tests: submake-generated-headers
	$(MAKE) -C src/test/regress $@

check:
	if [ ! -f $(prefix)/greenplum_path.sh ]; then \
		$(MAKE) -C $(top_builddir) install; \
	fi
	. $(prefix)/greenplum_path.sh; \
	if pg_isready 1>/dev/null; then \
	  $(MAKE) -C $(top_builddir) installcheck; \
	else \
	  if [ ! -f $(top_builddir)/gpAux/gpdemo/gpdemo-env.sh ]; then \
	    . $(prefix)/greenplum_path.sh && $(MAKE) -C $(top_builddir) create-demo-cluster; \
	  fi; \
	  . $(prefix)/greenplum_path.sh && . $(top_builddir)/gpAux/gpdemo/gpdemo-env.sh && $(MAKE) -C $(top_builddir) installcheck; \
	fi

$(call recurse,check-world,src/test src/pl src/interfaces/ecpg contrib src/bin gpcontrib,check)
$(call recurse,checkprep,  src/test src/pl src/interfaces/ecpg contrib src/bin gpcontrib)

# This is a top-level target that runs "all" regression test suites against
# a running server. This is what the CI pipeline runs.
.PHONY: installcheck-world

# Run all ICW targets in different directories under a recurse call, so
# that make -k works as expected. Order is significant here (for some reason,
# which probably indicates that we're relying on undefined behavior... we should
# probably pull anything order-dependent out of recurse() and back into the
# recipe body).
ICW_TARGETS  = src/test src/pl src/interfaces/gppc
ICW_TARGETS += contrib/auto_explain contrib/citext
ICW_TARGETS += contrib/file_fdw contrib/formatter_fixedwidth
ICW_TARGETS += contrib/extprotocol contrib/dblink
ICW_TARGETS += contrib/indexscan contrib/hstore
ifeq ($(enable_enterprise), yes)
ICW_TARGETS += contrib/pgcrypto
endif
ICW_TARGETS += contrib/matrixts
# sslinfo depends on openssl
ifeq ($(with_openssl), yes)
ICW_TARGETS += contrib/sslinfo
endif
ICW_TARGETS += gpcontrib src/bin gpMgmt/bin

$(call recurse,installcheck-world, $(ICW_TARGETS),installcheck)

# GPDB: Postgres disables the SSL tests during ICW because of the TCP port that
# it opens to other users on the same machine during testing. GPDB makes no such
# security guarantees during tests: currently, it is unsafe to run
# installcheck-world on a machine with untrusted users.
$(call recurse,installcheck-world, \
			   src/test/ssl,check)

.PHONY: installcheck-gpcheckcat
installcheck-world: installcheck-gpcheckcat
installcheck-gpcheckcat:
	gpcheckcat -A
$(call recurse,installcheck-world,gpcontrib/gp_replica_check,installcheck)
$(call recurse,installcheck-world,src/bin/pg_upgrade,check)

# Run mock tests, that don't require a running server. Arguably these should
# be part of [install]check-world, but we treat them more like part of
# compilation than regression testing, in the CI. But they are too heavy-weight
# to put into "make all", either.
.PHONY : unittest-check
unittest-check:
	$(MAKE) CFLAGS=-DUNITTEST -C src/backend unittest-check
	$(MAKE) CFLAGS=-DUNITTEST -C src/bin unittest-check

GNUmakefile: GNUmakefile.in $(top_builddir)/config.status
	./config.status $@


##########################################################################

distdir	= postgresql-$(VERSION)
dummy	= =install=

dist: $(distdir).tar.gz $(distdir).tar.bz2
	rm -rf $(distdir)

$(distdir).tar: distdir
	$(TAR) chf $@ $(distdir)

.INTERMEDIATE: $(distdir).tar

distdir-location:
	@echo $(distdir)

distdir:
	rm -rf $(distdir)* $(dummy)
	for x in `cd $(top_srcdir) && find . \( -name CVS -prune \) -o \( -name .git -prune \) -o -print`; do \
	  file=`expr X$$x : 'X\./\(.*\)'`; \
	  if test -d "$(top_srcdir)/$$file" ; then \
	    mkdir "$(distdir)/$$file" && chmod 777 "$(distdir)/$$file";	\
	  else \
	    ln "$(top_srcdir)/$$file" "$(distdir)/$$file" >/dev/null 2>&1 \
	      || cp "$(top_srcdir)/$$file" "$(distdir)/$$file"; \
	  fi || exit; \
	done
	$(MAKE) -C $(distdir) distprep
	#$(MAKE) -C $(distdir)/doc/src/sgml/ INSTALL
	#cp $(distdir)/doc/src/sgml/INSTALL $(distdir)/
	$(MAKE) -C $(distdir) distclean
	#rm -f $(distdir)/README.git

distcheck: dist
	rm -rf $(dummy)
	mkdir $(dummy)
	$(GZIP) -d -c $(distdir).tar.gz | $(TAR) xf -
	install_prefix=`cd $(dummy) && pwd`; \
	cd $(distdir) \
	&& ./configure --prefix="$$install_prefix"
	$(MAKE) -C $(distdir) -q distprep
	$(MAKE) -C $(distdir)
	$(MAKE) -C $(distdir) install
	$(MAKE) -C $(distdir) uninstall
	@echo "checking whether \`$(MAKE) uninstall' works"
	test `find $(dummy) ! -type d | wc -l` -eq 0
	$(MAKE) -C $(distdir) dist
# Room for improvement: Check here whether this distribution tarball
# is sufficiently similar to the original one.
	rm -rf $(distdir) $(dummy)
	@echo "Distribution integrity checks out."

cpluspluscheck: submake-generated-headers
	$(top_srcdir)/src/tools/pginclude/cpluspluscheck $(top_srcdir) $(abs_top_builddir)

.PHONY: dist distdir distcheck docs install-docs world check-world install-world installcheck-world

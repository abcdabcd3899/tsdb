# contrib/Makefile

subdir = contrib
top_builddir = ..
include $(top_builddir)/src/Makefile.global

# Greenplum specific changes to the targetlist:
# tsearch2 and postgres_fdw are currently disabled due to test failures.
# cube and the related earthdistance are disabled since GPDB define CUBE
# as a keyword. lo is disabled since large objects aren't supported.
# adminpack is disabled since the functionality has been imported into
# GPDB.

SUBDIRS = \
		amcheck		\
		auth_delay	\
		auto_explain	\
		bloom		\
		btree_gin	\
		btree_gist	\
		citext		\
		dblink		\
		dict_int	\
		dict_xsyn	\
		file_fdw	\
		fuzzystrmatch	\
		hstore		\
		intagg		\
		intarray	\
		isn		\
		ltree		\
		oid2name	\
		pageinspect	\
		passwordcheck	\
		pg_buffercache	\
		pg_freespacemap \
		pg_prewarm	\
		pg_standby	\
		pg_stat_statements \
		pg_trgm		\
		pgcrypto	\
		pgrowlocks	\
		pgstattuple	\
		pg_visibility	\
		seg		\
		spi		\
		tablefunc	\
		tcn		\
		test_decoding	\
		tsm_system_rows \
		tsm_system_time \
		unaccent	\
		vacuumlo

# Greenplum-specific additions (to ease merge pain).
SUBDIRS += \
		formatter \
		formatter_fixedwidth \
		extprotocol \
		indexscan

# MatrixDB-specific additions
SUBDIRS += \
		lz4 \
		matrixmgr \
		matrixts \
		telemetry 

ifneq (,$(wildcard postgres_fdw/Makefile))
ifeq ($(with_postgresfdw), yes)
SUBDIRS += postgres_fdw
else
ALWAYS_SUBDIRS += postgres_fdw
endif
endif

ifneq (,$(wildcard mysql_fdw/Makefile))
ifeq ($(with_mysqlfdw), yes)
SUBDIRS += mysql_fdw
else
ALWAYS_SUBDIRS += mysql_fdw
endif
endif

ifneq (,$(wildcard mongo_fdw/Makefile))
ifeq ($(with_mongofdw), yes)
SUBDIRS += mongo_fdw
else
ALWAYS_SUBDIRS += mongo_fdw
endif
endif

ifeq ($(with_openssl),yes)
SUBDIRS += sslinfo
else
ALWAYS_SUBDIRS += sslinfo
endif

ifneq ($(with_uuid),no)
SUBDIRS += uuid-ossp
else
ALWAYS_SUBDIRS += uuid-ossp
endif

ifeq ($(with_libxml),yes)
SUBDIRS += xml2
else
ALWAYS_SUBDIRS += xml2
endif

ifeq ($(with_selinux),yes)
SUBDIRS += sepgsql
else
ALWAYS_SUBDIRS += sepgsql
endif

ifeq ($(with_perl),yes)
SUBDIRS += hstore_plperl jsonb_plperl
else
ALWAYS_SUBDIRS += hstore_plperl jsonb_plperl
endif

ifeq ($(with_python),yes)
SUBDIRS += hstore_plpython jsonb_plpython ltree_plpython
else
ALWAYS_SUBDIRS += hstore_plpython jsonb_plpython ltree_plpython
endif

# Missing:
#		start-scripts	\ (does not have a makefile)


$(recurse)
$(recurse_always)

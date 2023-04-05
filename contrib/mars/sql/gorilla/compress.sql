-- Tests for gorilla compression.
-- start_ignore
create extension if not exists matrixts;
create extension if not exists mars;
-- end_ignore

-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

\set f4_agg_funcs 'count(*) as f4count, min(f4) as f4min, max(f4) as f4max, avg(f4) as f4avg, sum(f4) as f4sum'
\set f8_agg_funcs 'count(*) as f8count, min(f8) as f8min, max(f8) as f8max, avg(f8) as f8avg, sum(f8) as f8sum'

-- lz4 compress
\set compress lz4
\set tname gorilla_marsscan_:compress
set mars.enable_customscan to on;
\i sql/gorilla/with_compress.template

\set tname gorilla_noscan_:compress
set mars.enable_customscan to off;
\i sql/gorilla/with_compress.template

-- zstd compress
\set compress zstd
\set tname gorilla_marsscan_:compress
set mars.enable_customscan to on;
\i sql/gorilla/with_compress.template

\set tname gorilla_noscan_:compress
set mars.enable_customscan to off;
\i sql/gorilla/with_compress.template

-- one column (float-encode)
CREATE TABLE gorilla_mars_1col (f8 float ENCODING (compresstype=:compress))
USING mars DISTRIBUTED masteronly;
insert into gorilla_mars_1col select i/101::float from generate_series(1, 1000000)i;

select count(*) from gorilla_mars_1col;

-- two columns (int, float-encode)
CREATE TABLE aoco_lz4 (float8 float ENCODING (compresstype=lz4))
WITH (APPENDONLY=true, COMPRESSTYPE=lz4, compresslevel=1, ORIENTATION=column)
DISTRIBUTED masteronly;

CREATE TABLE aoco_zstd (float8 float ENCODING (compresstype=zstd))
WITH (APPENDONLY=true, COMPRESSTYPE=zstd, compresslevel=1, ORIENTATION=column)
DISTRIBUTED masteronly;

CREATE TABLE gorilla_mars (float8 float)
USING mars DISTRIBUTED masteronly;

CREATE TABLE gorilla_lz4 (float8 float ENCODING (compresstype=lz4))
USING mars DISTRIBUTED masteronly;

CREATE TABLE gorilla_zstd(float8 float ENCODING (compresstype=zstd))
USING mars DISTRIBUTED masteronly;

\set test_data_set 'select i/101::float from generate_series(1, 1000000)i'
insert into aoco_lz4 :test_data_set;
insert into aoco_zstd :test_data_set;
insert into gorilla_mars :test_data_set;
insert into gorilla_lz4 :test_data_set;
insert into gorilla_zstd :test_data_set;

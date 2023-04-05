DROP DATABASE IF EXISTS testanalyze;
CREATE DATABASE testanalyze;
\c testanalyze
CREATE EXTENSION matrixts;
CREATE EXTENSION mars;
create extension if not exists gp_debug_numsegments;
select gp_debug_set_create_table_default_numsegments(1);
DROP TABLE if exists foo;
CREATE TABLE foo (a int, b int) using mars;
insert into foo select i, i*2 from generate_series(1, 1024) i;
insert into foo select i, i*2 from generate_series(2049, 4096) i;
analyze foo;
select relname, reltuples from pg_class where relname like 'foo';
select * from pg_stats where tablename like 'foo' order by attname;

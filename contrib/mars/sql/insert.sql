-- start_ignore
DROP DATABASE IF EXISTS marsinsert;
CREATE DATABASE marsinsert;
\c marsinsert
CREATE EXTENSION IF NOT EXISTS matrixts;
CREATE EXTENSION IF NOT EXISTS mars;
create or replace function footer_total(relname text) returns float8 as $$
declare
  aosegname text;
  tupcount float8 := 0;
  rc int := 0;
begin

  execute 'select relname from pg_class where oid=(select segrelid from pg_class, mx_mars where relname=''' || relname || ''' and relid = pg_class.oid)' into aosegname;
  if aosegname is not null then
          execute 'select count(*) from gp_dist_random(''pg_aoseg.' || aosegname || ''')' into tupcount;
  end if;
  return tupcount;
end; $$ language plpgsql volatile READS SQL DATA;

create extension if not exists gp_debug_numsegments;

select gp_debug_set_create_table_default_numsegments(1);

-- end_ignore

CREATE TABLE foo (a int, b bigint) USING mars DISTRIBUTED BY(a);

INSERT INTO foo SELECT i, i * 2 FROM GENERATE_SERIES(1, 10) i;

SELECT COUNT(*) FROM foo;

SELECT COUNT(*) FROM mx_mars;

SELECT COUNT(*) FROM ((SELECT segrelid FROM mx_mars WHERE relid = 'foo'::regclass::oid)) AS T;

TRUNCATE foo;

SELECT COUNT(*) FROM foo;

INSERT INTO foo SELECT i, i * 2 FROM GENERATE_SERIES(1, 10) i;

INSERT INTO foo SELECT i, i * 2 FROM GENERATE_SERIES(1, 10) i;

SELECT SUM(a), SUM(b) FROM foo;

-- test each row group package a footer
TRUNCATE foo;

INSERT INTO foo SELECT i, i * 2 FROM GENERATE_SERIES(1, 20480) i;

SELECT footer_total('foo');

-- test null value
TRUNCATE foo;

INSERT INTO foo SELECT * FROM GENERATE_SERIES(1, 5);

SELECT * FROM foo;

DROP TABLE foo;

SELECT COUNT(*) FROM mx_mars;

\d pg_aoseg.*

\c -

create table test (i int, j int) using mars with (group_col_="{i}", global_order_col_="{i}", local_order_col_="{i}") distributed by (i);
insert into test  select a / 10, a % 10 from generate_series(1, 100) as a;
select count(i), sum(i) from test;

CREATE TABLE check_not_null(tag INT, ts TIMESTAMP, val FLOAT4 NOT NULL) USING mars WITH(tagkey=tag, timekey=ts);
INSERT INTO check_not_null VALUES(1, NULL, 4);
INSERT INTO check_not_null VALUES(1, '2021-10-29 10:36:00', NULL);
INSERT INTO check_not_null VALUES(NULL, '2021-10-29 10:36:00', 4);

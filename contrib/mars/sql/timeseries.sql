-- start_ignore
CREATE EXTENSION IF NOT EXISTS mars;

CREATE extension IF NOT EXISTS gp_debug_numsegments;

SELECT gp_debug_set_create_table_default_numsegments(1);

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
-- end_ignore

CREATE TABLE foo(ts timestamp, tag int, a int) using mars with (group_param_col_ = "{ts in 1 mins}");

INSERT INTO foo SELECT '2010-01-01 00:00:00'::TIMESTAMP + ('' || i * 20)::interval, i, i * 2 FROM generate_series(1, 10) i;

SELECT * FROM foo;

SELECT footer_total('foo');

TRUNCATE foo;

INSERT INTO foo SELECT '2010-01-01 00:00:00'::TIMESTAMP + ('' || i)::interval, i, i * 2 FROM generate_series(1, 59) i;

SELECT footer_total('foo');

DROP TABLE foo;

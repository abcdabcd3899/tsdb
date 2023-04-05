-- verify backward compatibility
\set tname test_mars_auxtable_index_compatibility
\set tnamestr '''' :tname ''''

drop table if exists :tname;

-- this test is sensitive to cluster size, so force all the data to be stored
-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

set mars.enable_index_on_auxtable to off;
show mars.enable_index_on_auxtable;

create table :tname (c1 int, c2 real) using mars with (
  group_col_ = "{c1}",
  global_order_col_ = "{c1, c2}"
) distributed by (c1);

insert into :tname select i, i % 4 from generate_series(0, 7) i;

\out /dev/null
select segrelid as aux_table_oid
  from mx_mars
 where relid = :tnamestr::regclass;
\out

\gset

-- verify no index columns in the aux table
select attnum, attname, atttypid::regtype::name from pg_attribute
  where attrelid = :aux_table_oid and attnum > 0;

set mars.enable_index_on_auxtable to on;
show mars.enable_index_on_auxtable;

insert into :tname select i, i % 4 from generate_series(8, 15) i;

\i sql/orderkeys.template

truncate :tname;

insert into :tname
  select i / 10, i / 2.0 from generate_series(0, 20000) i;

select c1, c2 from :tname where c1 = 10;
select c1, c2 from :tname where c1 = 10 and c2 > 51.2;
select c1, c2 from :tname where c1 = 10 and c2 < 43;

select c1, c2 from :tname where 1 >= c1;
select c1, c2 from :tname where 1 >= c1 and c2 > 8;
select c1, c2 from :tname where 100 < c1 and c2 < 515;

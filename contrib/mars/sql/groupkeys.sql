-- start_ignore
create extension if not exists mars;
create extension if not exists gp_debug_numsegments;
drop schema if exists test_groupkeys;
-- end_ignore

-- this test is sensitive to cluster size, so force all the data to be stored
-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

create schema test_groupkeys;
set search_path to test_groupkeys,public;

-- create t1 with the first column "c1" as the group key
create table t1 (c1 int, c2 int)
 using mars
  with (tablegroupkeys = 1)
 distributed by (c1)
;

insert into t1 select i / 4, i from generate_series(0, 9) i;
select ctid, * from t1;

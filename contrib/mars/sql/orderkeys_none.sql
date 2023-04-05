\set tname test_orderkeys.t1_none

-- this test is sensitive to cluster size, so force all the data to be stored
-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

-- create :tname with the first column "c1" as the group key
create table :tname (c1 int, c2 int)
 using mars
 distributed by (c1)
;

-- FIXME: we should also support multiple row groups in one footer
/*
insert into :tname select i / 4, i % 4 from generate_series(0, 15) i;
*/
insert into :tname select i / 4, i % 4 from generate_series(0, 3) i;
insert into :tname select i / 4, i % 4 from generate_series(4, 7) i;
insert into :tname select i / 4, i % 4 from generate_series(8, 11) i;
insert into :tname select i / 4, i % 4 from generate_series(12, 15) i;

\i sql/orderkeys.template

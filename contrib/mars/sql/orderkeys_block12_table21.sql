\set tname test_orderkeys.t1_block12_table21

-- this test is sensitive to cluster size, so force all the data to be stored
-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

-- create :tname with the first column "c1" as the group key
create table :tname (c1 int, c2 int)
 using mars
  with (group_param_col_ = "{c1 in 4}", global_order_col_ = "{c2, c1}" ,local_order_col_ = "{c1, c2}")
 distributed by (c1)
;

insert into :tname select i / 2 - 0, i % 2 + 0 from generate_series(0, 7) i;
insert into :tname select i / 2 - 4, i % 2 + 2 from generate_series(8, 15) i;

\i sql/orderkeys.template

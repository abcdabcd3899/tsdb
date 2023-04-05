\set tname test_orderkeys.t1_block12_table1

-- this test is sensitive to cluster size, so force all the data to be stored
-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

-- create :tname with global order (c1), local order (c1, c2)
create table :tname (c1 int, c2 int, id serial)
 using mars
  with (group_param_col_ = "{c1 in 2, id in 100}", local_order_col_= "{c1, c2}")
 distributed by (c1)
;

insert into :tname select i / 4, i % 4 from generate_series(0, 15) i;

\i sql/orderkeys.template

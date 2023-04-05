\set tname test_orderkeys.t1_floattable_onegroup

-- this test is sensitive to cluster size, so force all the data to be stored
-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

-- create :tname with the first column "c1" as the group key
create table :tname (c1 float, c2 int)
 using mars
  with (global_order_col_= "{c1, c2}", local_order_col_= "{c1}")
 distributed by (c1)
;

insert into :tname select i / 4., i % 4 from generate_series(0, 15) i;

\i sql/orderkeys.template

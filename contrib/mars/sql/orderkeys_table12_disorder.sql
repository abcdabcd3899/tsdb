\set tname test_orderkeys.t1_table12_disorder

-- this test is sensitive to cluster size, so force all the data to be stored
-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

create table :tname (c1 int, c2 int)
 using mars
  with (global_order_col_= "{c2}", local_order_col_= "{c1}")
 distributed by (c1)
;

-- break global order convention
insert into :tname select i / 4, i % 4 from generate_series(0, 15) i;

\i sql/orderkeys.template

\set tname test_orderkeys.t1_block12_table12

-- this test is sensitive to cluster size, so force all the data to be stored
-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

-- create :tname with global order (c1, c2), local order (c1, c2)
create table :tname (c1 int, c2 int)
 using mars
  with (group_col_ = "{c1}", group_param_col_ = "{c2 in 4}",
        local_order_col_ = "{c1, c2}")
 distributed by (c1)
;

insert into :tname select i / 4, i % 4 from generate_series(0, 15) i;

\i sql/orderkeys.template

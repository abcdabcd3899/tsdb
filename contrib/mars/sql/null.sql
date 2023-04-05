--
-- verify NULL handling.
--
-- at the moment we do not allow speciying "NULLS FIRST" or "NULLS LAST" in the
-- order options, the default is "NULLS LAST" as the store order is always ASC
-- in mars.
--

-- start_ignore
create extension if not exists matrixts;
create extension if not exists mars;
create extension if not exists gp_debug_numsegments;
-- end_ignore

--
-- case 1, do not specify the local_order_col option, so reorder is needed in
-- ordered output, we need to verify the null handling here.
--

\set tname test_mars_null_t1

drop table if exists :tname;

select gp_debug_set_create_table_default_numsegments(1);

create table :tname (batch int, c1 int, c2 int, id serial)
 using mars
  with ( group_col_ = "{batch}"
       , group_param_col_ = "{id in 1000}"
       )
 distributed by (batch);

select gp_debug_reset_create_table_default_numsegments();

\i sql/null.template

--
-- case 2, specify the local_order_col option, so block level fast-seeking is
-- used, we need to verify the null handling here.
--

\set tname test_mars_null_t2

drop table if exists :tname;

select gp_debug_set_create_table_default_numsegments(1);

create table :tname (batch int, c1 int, c2 int, id serial)
 using mars
  with ( group_col_ = "{batch}"
       , group_param_col_ = "{id in 1000}"
       , local_order_col_ = "{c1}"
       )
 distributed by (batch);

select gp_debug_reset_create_table_default_numsegments();

\i sql/null.template

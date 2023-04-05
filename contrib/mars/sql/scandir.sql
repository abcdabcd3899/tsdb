--
-- verify the scan in different directions.
--
-- TODO: order on float columns.
--

-- start_ignore
create extension if not exists mars;
-- end_ignore

\set tname test_mars_scandir_t1

drop table if exists :tname;

-- we dispatch by batch, so the tuples of the same batch are on the same seg,
-- this is important to ensure the behavior of this test.
create table :tname (batch int, c1 int)
       using mars
        with (global_order_col_ = "{c1}")
 distributed by (batch);

insert into :tname
     select 0 as batch
          , i as c1
       from generate_series(1, 1000) i;

explain (costs off)
select c1
  from :tname
 order by c1
 limit 1;

select c1
  from :tname
 order by c1
 limit 1;

explain (costs off)
select c1
  from :tname
 order by c1 desc
 limit 1;

select c1
  from :tname
 order by c1 desc
 limit 1;

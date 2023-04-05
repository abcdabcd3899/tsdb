-- start_ignore
create extension if not exists mars;
-- end_ignore

--
-- pushdown of count() aggs.
--

\set tname test_mars_customscan_count_t1
\set tmpname test_mars_customscan_count_t2

drop table if exists :tname;

-- we dispatch by batch, so the tuples of the same batch are on the same seg,
-- this is important to ensure the per-block count.
create table :tname (batch int, c1 int, id serial)
	using mars
	with (group_col_ = "{batch}", group_param_col_ = '{id in 1000}')
	distributed by (batch);

-- count on empty tables should return nothing.
select batch
     , count() as full_count
     , count(c1) as column_count
  from :tname
 group by batch;

insert into :tname values
-- one single non-null value
\set batch 0
  (:batch, 1)

-- one single null value
\set batch 10
, (:batch, null)

-- multiple non-null values
\set batch 20
, (:batch, 1)
, (:batch, 2)
, (:batch, 3)

-- multiple null values
\set batch 30
, (:batch, null)
, (:batch, null)
, (:batch, null)

-- mix of non-null and null values
\set batch 40
, (:batch, 1)
, (:batch, null)
, (:batch, 2)
, (:batch, null)
, (:batch, 3)
, (:batch, null)

-- end of the insert
;

-- first let's do them via the content scan of the custom scan.
select batch
     , count() as full_count
     , count(c1) as column_count
  from :tname
 group by batch;

-- then let's do them with the block scan of the custom scan, we need to insert
-- each batch separately.
create temp table :tmpname (like :tname) distributed by (batch);
insert into :tmpname select * from :tname order by batch;
truncate :tname;

do language plpgsql $$
    declare
        batch_ int;
    begin
        for batch_ in select distinct batch
                        from test_mars_customscan_count_t2
                       order by 1
        loop
            insert into test_mars_customscan_count_t1
                 select *
                   from test_mars_customscan_count_t2
                  where batch = batch_;
        end loop;
    end;
$$;

select batch
     , count() as full_count
     , count(c1) as column_count
  from :tname
 group by batch;

--
-- pushdown of count(1) aggs.
--

\set tname test_mars_customscan_count_1_t
-- start_ignore
drop table if exists :tname;
-- end_ignore
create table :tname (tag_id int, time timestamp, read int)
	using mars
	with (tagkey='tag_id', timekey='time', timebucket='5 mins')
	distributed by (tag_id);

insert into :tname values (1,'2020-01-01 12:00:00',1),
(1,'2020-01-01 12:01:00',1),
(1,'2020-01-01 12:02:00',1),
(1,'2020-01-01 12:03:00',null),
(1,'2020-01-01 12:04:00',1),
(1,'2020-01-01 12:05:00',1),
(1,'2020-01-01 12:06:00',null),
(1,'2020-01-01 12:07:00',1),
(1,'2020-01-01 12:08:00',1),
(1,'2020-01-01 12:09:00',null),
(1,'2020-01-01 12:10:00',1);

explain (costs off) select count() from :tname;
select count() from :tname;

explain (costs off) select count(*) from :tname;
select count(*) from :tname;

explain (costs off) select count(1) from :tname;
select count(1) from :tname;

explain (costs off) select count(2) from :tname;
select count(2) from :tname;

explain (costs off) select count(read) from :tname;
select count(read) from :tname;

explain (costs off) select count(null) from :tname;
select count(null) from :tname;

explain (costs off) select count() from :tname where time <= '2020-01-01 12:07:00';
select count() from :tname where time <= '2020-01-01 12:07:00';

explain (costs off) select count(*) from :tname where time <= '2020-01-01 12:07:00';
select count(*) from :tname where time <= '2020-01-01 12:07:00';

explain (costs off) select count(1) from :tname where time <= '2020-01-01 12:07:00';
select count(1) from :tname where time <= '2020-01-01 12:07:00';

explain (costs off) select count(2) from :tname where time <= '2020-01-01 12:07:00';
select count(2) from :tname where time <= '2020-01-01 12:07:00';

explain (costs off) select count(read) from :tname where time <= '2020-01-01 12:07:00';
select count(read) from :tname where time <= '2020-01-01 12:07:00';

explain (costs off) select count(null) from :tname where time <= '2020-01-01 12:07:00';
select count(null) from :tname where time <= '2020-01-01 12:07:00';

explain (costs off) select count(distinct read) from :tname group by tag_id;
select count(distinct read) from :tname group by tag_id;

explain (costs off) select count(distinct 2) from :tname group by tag_id;
select count(distinct 2) from :tname group by tag_id;

explain (costs off) select count(read) filter (where tag_id = 2) from :tname group by tag_id;
select count(read) filter (where tag_id = 2) from :tname group by tag_id;

explain (costs off) select count(read order by time) from :tname group by tag_id;
select count(read order by time) from :tname group by tag_id;

select count() from :tname where tag_id = 1;
select count(*) from :tname where tag_id = 1;
select count(1) from :tname where tag_id = 1;
select count(2) from :tname where tag_id = 1;
select count(read) from :tname where tag_id = 1;
select count(null) from :tname where tag_id = 1;

select count() from :tname where tag_id = 1 and time <= '2020-01-01 12:07:00';
select count(*) from :tname where tag_id = 1 and time <= '2020-01-01 12:07:00';
select count(1) from :tname where tag_id = 1 and time <= '2020-01-01 12:07:00';
select count(2) from :tname where tag_id = 1 and time <= '2020-01-01 12:07:00';
select count(read) from :tname where tag_id = 1 and time <= '2020-01-01 12:07:00';
select count(null) from :tname where tag_id = 1 and time <= '2020-01-01 12:07:00';

select count(distinct read) from :tname where tag_id = 1;
select count(distinct 2) from :tname where tag_id = 1;
select count(read) filter (where tag_id = 2) from :tname where tag_id = 1;
select count(read order by time) from :tname where tag_id = 1;

drop table :tname;

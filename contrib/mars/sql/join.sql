-- start_ignore
create extension if not exists mars;
create extension if not exists matrixts;

drop schema if exists test_join;
create schema test_join;
-- end_ignore

\set device test_join.device
\set performance test_join.performance

create table :device (tag int, name text, attr1 text) using heap;
create unique index on :device(tag);
create table :performance (
		tag int,
		ts timestamp,
		perf1 int, 
		perf2 float) 
	using mars 
	with (group_col_ = "{tag}", group_param_col_ = "{ts in 1 hour}")
	distributed by (tag);

insert into :device select i, concat('device_', i), 'cpu' from generate_series(1, 10) i;

insert into :performance select i / 50 + 1, '2020-01-01 08:00:00'::timestamp + concat(i, ' mins')::interval, i, i::float/100 from generate_series(0, 249) i;

explain (costs off)
select sum(perf1), max(perf2) from :performance
where tag in (select tag from :device where name in ('device_1', 'device_4'));

select sum(perf1), max(perf2) from :performance
where tag in (select tag from :device where name in ('device_1', 'device_4'));

explain (costs off)
select sum(perf1), max(perf2) from :performance
where tag < (select distinct tag from :device where name in ('device_3'));

select sum(perf1), max(perf2) from :performance
where tag < (select distinct tag from :device where name in ('device_3'));

explain (costs off)
select sum(perf1), max(perf2) from :performance
where tag > (select distinct tag from :device where name in ('device_3'));

select sum(perf1), max(perf2) from :performance
where tag > (select distinct tag from :device where name in ('device_3'));

explain (costs off)
select time_bucket('1 hour', ts) as hour, sum(perf1), max(perf2)
  from :performance
 where tag in (select distinct tag from :device
                where name in ('device_1', 'device_4'))
 group by hour;

select time_bucket('1 hour', ts) as hour, sum(perf1), max(perf2)
  from :performance
 where tag in (select distinct tag from :device
                where name in ('device_1', 'device_4'))
 group by hour;

-- test MARS as sub-partition 

create table part_root (like :performance) partition by range (ts);
alter table part_root attach partition :performance for values from('2020-01-01') to ('2020-02-01');

explain (costs off)
select sum(perf1), max(perf2) from part_root
where tag in (select tag from :device where name in ('device_1', 'device_4'));

select sum(perf1), max(perf2) from part_root
where tag in (select tag from :device where name in ('device_1', 'device_4'));

explain (costs off)
select sum(perf1), max(perf2) from part_root
where tag < (select distinct tag from :device where name in ('device_3'));

select sum(perf1), max(perf2) from part_root
where tag < (select distinct tag from :device where name in ('device_3'));

explain (costs off)
select sum(perf1), max(perf2) from part_root
where tag > (select distinct tag from :device where name in ('device_3'));

select sum(perf1), max(perf2) from part_root
where tag > (select distinct tag from :device where name in ('device_3'));

explain (costs off)
select time_bucket('1 hour', ts) as hour, sum(perf1), max(perf2)
  from part_root
 where tag in (select distinct tag from :device
                where name in ('device_1', 'device_4'))
 group by hour;

select time_bucket('1 hour', ts) as hour, sum(perf1), max(perf2)
  from part_root
 where tag in (select distinct tag from :device
                where name in ('device_1', 'device_4'))
 group by hour;

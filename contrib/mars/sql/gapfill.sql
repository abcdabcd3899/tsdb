-- start_ignore
create extension if not exists mars;
create extension if not exists matrixts;
set enable_partitionwise_join to on;
set enable_partitionwise_aggregate to on;

-- end_ignore
create table gf (tag integer, ts timestamp, col1 integer) using mars with (tagkey = "tag", timekey = "ts", timebucket = "1 hour") distributed by (tag);

insert into gf select i/59, '2020-01-01 08:00:00'::timestamp + i * '1 min'::interval, i from generate_series(1, 59) as i;

insert into gf select i/59, '2020-01-01 10:00:00'::timestamp + i * '1 min'::interval, i from generate_series(1, 59) as i;

insert into gf select i/59, '2020-01-01 12:00:00'::timestamp + i * '1 min'::interval, i from generate_series(1, 59) as i;


explain (costs off) select time_bucket_gapfill('60 mins', ts) as tbg, sum(col1) from gf group by tbg;
select time_bucket_gapfill('60 mins', ts) as tbg, sum(col1) from gf group by tbg;

set mars.enable_customscan to off;
explain (costs off) select time_bucket_gapfill('60 mins', ts) as tbg, sum(col1) from gf group by tbg;
select time_bucket_gapfill('60 mins', ts) as tbg, sum(col1) from gf group by tbg;
reset mars.enable_customscan;

explain (costs off) select time_bucket_gapfill('90 mins', ts) as tbg, sum(col1) from gf group by tbg;
select time_bucket_gapfill('90 mins', ts) as tbg, sum(col1) from gf group by tbg;

set mars.enable_customscan to off;
explain (costs off) select time_bucket_gapfill('90 mins', ts) as tbg, sum(col1) from gf group by tbg;
select time_bucket_gapfill('90 mins', ts) as tbg, sum(col1) from gf group by tbg;
reset mars.enable_customscan;

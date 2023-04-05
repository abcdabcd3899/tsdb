--start ignore
CREATE extension matrixts;
CREATE extension mars;
DROP TABLE IF EXISTS mars.temp_public_foo;
DROP TABLE IF EXISTS foo;
--end ignore
create table foo ( tag int ,tz timestamp(6) , measurement float ) distributed by (tag) partition by range(tz);
select mars.build_timeseries_table('foo','tagkey="tag", timekey="tz", timebucket="1 hour"', true);
select mars.add_partition('foo', '2020-01-08', '2020-01-09','1 day');
insert into foo select i%5, '2020-01-08 08:00:00'::timestamp + concat(i/100, 'mins')::interval, i/100 from generate_series(1, 10000) i;

select sum(measurement), count(measurement) from foo group by tag;

explain select sum(measurement), count(measurement) from foo group by tag;

select * from mars.list_partition('foo');

select mars.compress_partition(inhrelid::regclass) from pg_inherits where inhparent = 'foo'::regclass;

select * from mars.list_partition('foo');

select sum(measurement), count(measurement) from foo group by tag;

explain select sum(measurement), count(measurement) from foo group by tag;

select mars.destroy_timeseries_table('foo');

drop table foo;

create table foo ( tag int ,tz timestamp , measurement float ) distributed by (tag) partition by range(tz);
select mars.build_timeseries_table('foo','tagkey="tag", timekey="tz", timebucket="1 hour"', true);
select mars.add_partition('foo', '2020-01-10', '1 day');
insert into foo select i%5, '2020-01-10 08:00:00'::timestamp + concat(i/100, 'mins')::interval, i/100 from generate_series(1, 10000) i;

select sum(measurement), count(measurement) from foo group by tag;

explain select sum(measurement), count(measurement) from foo group by tag;

select * from mars.list_partition('foo');

select mars.compress_partition(inhrelid::regclass) from pg_inherits where inhparent = 'foo'::regclass;

select * from mars.list_partition('foo');

select sum(measurement), count(measurement) from foo group by tag;

explain select sum(measurement), count(measurement) from foo group by tag;

select mars.destroy_timeseries_table('foo');

drop table foo;

create table foo ( tag int ,tz timestamp , measurement float ) distributed by (tag) partition by range(tz);
select mars.build_timeseries_table('foo','tagkey="tag", timekey="tz", timebucket="1 hour"', true);
select mars.add_partition('foo', '2020-01-01', '2020-01-02','8 hour');
insert into foo select i%5, '2020-01-01 08:00:00'::timestamp + concat(i/100, 'mins')::interval, i/100 from generate_series(1, 10000) i;

select sum(measurement), count(measurement) from foo group by tag;

explain select sum(measurement), count(measurement) from foo group by tag;

select * from mars.list_partition('foo');

select mars.compress_partition(inhrelid::regclass) from pg_inherits where inhparent = 'foo'::regclass;

select * from mars.list_partition('foo');

select sum(measurement), count(measurement) from foo group by tag;

explain select sum(measurement), count(measurement) from foo group by tag;

-- test compression type keep in mars relation
select count(*) from (select unnest(attoptions) as opt from pg_attribute_encoding inner join pg_inherits on inhrelid = attrelid where inhparent = 'foo'::regclass) as foo where opt = 'compresstype=lz4';

select mars.destroy_timeseries_table('foo');

-- failed case
select mars.destroy_timeseries_table('foo');

drop table foo;

create table "FoO_" ( "tag@" int, "timestamp" timestamp, measurement float) distributed by ("tag@") partition by range("timestamp");

select mars.build_timeseries_table('"FoO_"','tagkey="tag@", timekey="timestamp", timebucket="1 hour"', true);

select mars.add_partition('"FoO_"', '2020-01-01', '2020-01-02','8 hour');

insert into "FoO_" select i%5, '2020-01-01 08:00:00'::timestamp + concat(i/100, 'mins')::interval, i/100 from generate_series(1, 10000) i;

select sum(measurement), count(measurement) from "FoO_" group by "tag@";

explain select sum(measurement), count(measurement) from "FoO_" group by "tag@";

select * from mars.list_partition('"FoO_"');

select mars.compress_partition(inhrelid::regclass) from  pg_inherits where inhparent = '"FoO_"'::regclass;

select * from mars.list_partition('"FoO_"');

select sum(measurement), count(measurement) from "FoO_" group by "tag@";

explain select sum(measurement), count(measurement) from "FoO_" group by "tag@";

select mars.destroy_timeseries_table('"FoO_"');

drop table "FoO_";

create table """F'" ( "tag@" int, "timestamp" timestamp, measurement float) distributed by ("tag@") partition by range("timestamp");

select mars.build_timeseries_table('"""F''"','tagkey="tag@", timekey="timestamp", timebucket="1 hour"', true);

select mars.add_partition('"""F''"', '2020-01-01', '2020-01-02','8 hour');

insert into """F'" select i%5, '2020-01-01 08:00:00'::timestamp + concat(i/100, 'mins')::interval, i/100 from generate_series(1, 10000) i;

select sum(measurement), count(measurement) from """F'" group by "tag@";

explain select sum(measurement), count(measurement) from """F'" group by "tag@";

select * from mars.list_partition('"""F''"');

select mars.compress_partition(inhrelid::regclass) from  pg_inherits where inhparent = '"""F''"'::regclass;

select * from mars.list_partition('"""F''"');

select sum(measurement), count(measurement) from """F'" group by "tag@";

explain select sum(measurement), count(measurement) from """F'" group by "tag@";

select * from mars.destroy_timeseries_table('"""F''"');

drop table """F'";

create table constraint_test(tag int NOT NULL, tz timestamp NOT NULL, value1 int DEFAULT(1), primary key(tag, tz)) distributed by (tag) partition by range(tz);

select mars.build_timeseries_table('constraint_test', 'tagkey="tag", timekey="tz", timebucket="1 hour"');

select mars.add_partition('constraint_test', '2020-01-01', '2020-01-02', '12 hour');

insert into constraint_test select i%5, '2020-01-01 00:00:00'::timestamp + concat(i, 'mins')::interval from generate_series(1, 1000) i;

select sum(value1), count(*) from constraint_test group by tag;

explain select sum(value1), count(*) from constraint_test group by tag;

select * from mars.list_partition('constraint_test');

select mars.compress_partition(inhrelid::regclass) from  pg_inherits where inhparent = 'constraint_test'::regclass;

select * from mars.list_partition('constraint_test');

select sum(value1), count(*) from constraint_test group by tag;

explain select sum(value1), count(*) from constraint_test group by tag;

select mars.destroy_timeseries_table('constraint_test');

drop table constraint_test;

CREATE TABLE foo(
    time timestamp(6) with time zone,
    tag_id int not null,
    measurement int,
    unique (tag_id, time)
)
Distributed by (tag_id)
Partition by range(time);

SELECT mars.build_timeseries_table('foo','tagkey="tag_id", timekey="time", timebucket="1 hour"');
SELECT mars.add_partition('foo', '2021-04-01', '1 day');
insert into foo select '2021-04-01 00:00:00'::timestamp + concat(i, 'mins')::interval, i%5, i/100 from generate_series(1, 1000) i;
select sum(measurement), count(measurement) from foo group by tag_id;
explain select sum(measurement), count(measurement) from foo group by tag_id;
select * from mars.list_partition('foo');
select mars.compress_partition(inhrelid::regclass) from pg_inherits where inhparent = 'foo'::regclass;
select * from mars.list_partition('foo');
select sum(measurement), count(measurement) from foo group by tag_id;
explain select sum(measurement), count(measurement) from foo group by tag_id;
select mars.destroy_timeseries_table('foo');
drop table foo;


CREATE TABLE foo_1_mars1 (a int);
CREATE TABLE foo(
    "'Tim""" timestamp(6) with time zone,
    "TAG" int not null,
    measurement int,
    unique ("TAG", "'Tim""")
)
Distributed by ("TAG")
Partition by range("'Tim""");

SELECT mars.build_timeseries_table('foo','tagkey="TAG", timekey="''Tim""", timebucket="1 hour"');
SELECT mars.add_partition('foo', '2021-04-01', '1 day');
insert into foo select '2021-04-01 00:00:00'::timestamp + concat(i, 'mins')::interval, i%5, i/100 from generate_series(1, 1000) i;
select sum(measurement), count(measurement) from foo group by "TAG";
explain select sum(measurement), count(measurement) from foo group by "TAG";
select count(*) from pg_index where indrelid = (select inhrelid from pg_inherits where inhparent = 'foo'::regclass);
select * from mars.list_partition('foo');
select mars.compress_partition(inhrelid::regclass) from pg_inherits where inhparent = 'foo'::regclass;
select * from mars.list_partition('foo');
select count(*) from pg_index where indexrelid = (select inhrelid from pg_inherits where inhparent = 'foo'::regclass);
select sum(measurement), count(measurement) from foo group by "TAG";
explain select sum(measurement), count(measurement) from foo group by "TAG";
select mars.destroy_timeseries_table('foo');
drop table foo;

DROP TABLE foo_1_mars1;



set timezone to 'Asia/Shanghai';
create table foo ( tag int ,tz timestamp(6) , measurement float ) distributed by (tag) partition by range(tz);
select mars.build_timeseries_table('foo','tagkey="tag", timekey="tz", timebucket="1 hour"', true);
select mars.add_partition('foo', '2021-01-01', '2021-01-02','1 day');
select mars.add_partition('foo', '2021-01-01 20:00:00+4', '2021-01-03','1 day');
select mars.add_partition('foo', '2021-01-03', '2021-01-03 21:00:00+5','1 day');
select mars.add_partition('foo', '2021-01-03 22:00:00+6', '2021-01-04 23:00:00+7','1 day');
select mars.add_partition('foo', '2021-01-05', '2021-01-06','1 day');
select mars.destroy_timeseries_table('foo');
drop table foo;

CREATE TABLE foo (
    "Z" timestamp(2)
    , z int
    , ts timestamp
)
DISTRIBUTED BY (z)
PARTITION BY RANGE (ts)
;
SELECT mars.build_timeseries_table('foo','tagkey=z, timekey=ts, timebucket="1 hour"', true);
SELECT mars.destroy_timeseries_table('foo');
DROP TABLE foo;

-- test table merge
create table """F'" ( "tag@" int, "timestamp" timestamptz, measurement float) distributed by ("tag@") partition by range("timestamp");

select mars.build_timeseries_table('"""F''"','tagkey="tag@", timekey="timestamp", timebucket="1 hour"', true);

select mars.add_partition('"""F''"', '2020-01-01', '2020-01-02','8 hour');

insert into """F'" select i%5, '2020-01-01 08:00:00+08'::timestamptz + concat(i/100, 'mins')::interval, i/100 from generate_series(1, 10000) i;

select sum(measurement), count(measurement) from """F'" group by "tag@";

explain select sum(measurement), count(measurement) from """F'" group by "tag@";

select * from mars.list_partition('"""F''"');

select mars.compress_partition(inhrelid::regclass, '1 day') from  pg_inherits where inhparent = '"""F''"'::regclass;

select * from mars.list_partition('"""F''"');

select sum(measurement), count(measurement) from """F'" group by "tag@";

explain select sum(measurement), count(measurement) from """F'" group by "tag@";

select * from mars.destroy_timeseries_table('"""F''"');

drop table """F'";


-- merge by order
create table foo (tag int, ts timestamp, col int) distributed by (tag) partition by range (ts);

select mars.build_timeseries_table('foo', 'tagkey=tag, timekey=ts, timebucket="1 hour"', true);

select mars.add_partition('foo', '2020-01-01', '2020-01-02', '8 hour');

insert into foo select i%5, '2020-01-01 08:00:00'::timestamp + concat(i/100, 'mins')::interval, i/100 from generate_series(1, 10000) i;

select mars.compress_partition(inhrelid::regclass, '1 day') from  pg_inherits where inhparent = 'foo'::regclass order by inhrelid;

select * from mars.list_partition('foo');

select mars.destroy_timeseries_table('foo');

drop table foo;

-- merge by reverse order
create table foo (tag int, ts timestamp, col int) distributed by (tag) partition by range (ts);

select mars.build_timeseries_table('foo', 'tagkey=tag, timekey=ts, timebucket="1 hour"', true);

select mars.add_partition('foo', '2020-01-01', '2020-01-02', '8 hour');

insert into foo select i%5, '2020-01-01 08:00:00'::timestamp + concat(i/100, 'mins')::interval, i/100 from generate_series(1, 10000) i;

select mars.compress_partition(inhrelid::regclass, '1 day') from  pg_inherits where inhparent = 'foo'::regclass order by inhrelid desc;

select * from mars.list_partition('foo');

select mars.destroy_timeseries_table('foo');

drop table foo;

-- has gap
create table foo (tag int, ts timestamp, col int) distributed by (tag) partition by range (ts);

select mars.build_timeseries_table('foo', 'tagkey=tag, timekey=ts, timebucket="1 hour"', true);

select mars.add_partition('foo', '2020-01-01 00:00:00', '2020-01-01 12:00:00', '4 hour');

select mars.add_partition('foo', '2020-01-01 16:00:00', '2020-01-02 0:00:00', '4 hour');

select mars.compress_partition(inhrelid::regclass, '1 day') from  pg_inherits where inhparent = 'foo'::regclass order by inhrelid desc;

select * from mars.list_partition('foo');

select mars.destroy_timeseries_table('foo');

drop table foo;

-- not align
create table foo (tag int, ts timestamp, col int) distributed by (tag) partition by range (ts);

select mars.build_timeseries_table('foo', 'tagkey=tag, timekey=ts, timebucket="1 hour"', true);

select mars.add_partition('foo', '2020-01-01', '2020-01-02', '5 hour');

insert into foo select i%5, '2020-01-01 08:00:00'::timestamp + concat(i/100, 'mins')::interval, i/100 from generate_series(1, 10000) i;

select mars.compress_partition(inhrelid::regclass, '1 day') from  pg_inherits where inhparent = 'foo'::regclass order by inhrelid desc;

select * from mars.list_partition('foo');

select mars.destroy_timeseries_table('foo');

drop table foo;

set timezone to 'UTC';

create table foo (tag int, ts timestamp, col int) distributed by (tag) partition by range (ts);

select mars.build_timeseries_table('foo', 'tagkey=tag, timekey=ts, timebucket="1 hour"', true);

select mars.add_partition('foo', '2020-01-01 00:00:00', '2020-01-02 00:00:00', '3 hour');

insert into foo select i%5, '2020-01-01 00:00:00'::timestamp + concat(i/10, 'mins')::interval, i/10 from generate_series(1, 10000) i;

select mars.compress_partition(inhrelid::regclass, '4 hour') from  pg_inherits where inhparent = 'foo'::regclass order by inhrelid;

select * from mars.list_partition('foo');

select mars.destroy_timeseries_table('foo');

drop table foo;

-- merge not by time series
create table foo (tag int, ts timestamp, col int) distributed by (tag) partition by range (ts);

select mars.build_timeseries_table('foo', 'tagkey=tag, timekey=ts, timebucket="1 hour"', true);

select mars.add_partition('foo', '2020-01-01 00:00:00', '2020-01-02 00:00:00', '8 hour');

insert into foo select i%5, '2020-01-01 00:00:00'::timestamp + concat(i/10, 'mins')::interval, i/10 from generate_series(1, 10000) i;

select mars.compress_partition('foo_1_prt_3', '1 day');

select mars.compress_partition('foo_1_prt_1', '1 day');

select mars.compress_partition('foo_1_prt_2', '1 day');

select * from mars.list_partition('foo');

select mars.destroy_timeseries_table('foo');

drop table foo;

-- partition table with index
create table foo (tag int, ts timestamp, col int, unique(tag,ts)) distributed by (tag) partition by range (ts);

select mars.build_timeseries_table('foo', 'tagkey=tag, timekey=ts, timebucket="1 hour"', true);

select mars.add_partition('foo', '2020-01-01 00:00:00', '2020-01-02 00:00:00', '8 hour');

insert into foo select i%5, '2020-01-01 00:00:00'::timestamp + concat(i/10., 'mins')::interval, i/10 from generate_series(1, 10000) i;

select mars.compress_partition('foo_1_prt_1', '1 day');

select mars.compress_partition('foo_1_prt_2', '1 day');

select mars.compress_partition('foo_1_prt_3', '1 day');

select * from mars.list_partition('foo');

-- test insert failed by UNIQUE check.
select mars.add_partition('foo', '2020-01-02 00:00:00', '2020-01-03 00:00:00', '8 hour');

insert into foo select 2, '2020-01-02 00:00:00'::timestamp + concat(i/10, 'mins')::interval, i/10 from generate_series(1, 10000) i;

explain (costs off) select sum(col), count(col) from foo group by tag;

select sum(col), count(col) from foo group by tag;

-- test upsert in heap table
insert into foo select i%5, '2020-01-02 00:00:00'::timestamp + concat(i/10, 'mins')::interval, i/10 from generate_series(1, 10000) i on conflict (tag,ts) do nothing;

explain (costs off) select sum(col), count(col) from foo group by tag;

select sum(col), count(col) from foo group by tag;

select mars.destroy_timeseries_table('foo');

drop table foo;

-- test varlen type as tagkey
CREATE TABLE test_varlen_as_tag1(
     ts timestamp with time zone,
     tag_id name,
     read float,
     write float
 )
 Distributed by (tag_id)
 Partition by range(ts);

CREATE TABLE test_varlen_as_tag2(
     ts timestamp with time zone,
     tag_id text,
     read float,
     write float
 )
 Distributed by (tag_id)
 Partition by range(ts);

CREATE TABLE test_varlen_as_tag3(
     ts timestamp with time zone,
     tag_id varchar,
     read float,
     write float
 )
 Distributed by (tag_id)
 Partition by range(ts);

CREATE TABLE test_varlen_as_tag4(
     ts timestamp with time zone,
     tag_id NUMERIC,
     read float,
     write float
 )
 Distributed by (tag_id)
 Partition by range(ts);

CREATE TABLE test_varlen_as_tag5(
     ts timestamp with time zone,
     tag_id INTERVAL,
     read float,
     write float
 )
 Distributed by (tag_id)
 Partition by range(ts);

SELECT mars.build_timeseries_table('test_varlen_as_tag1', 'tagkey="tag_id", timekey="ts", timebucket="1 day"');
SELECT mars.build_timeseries_table('test_varlen_as_tag2', 'tagkey="tag_id", timekey="ts", timebucket="1 day"');
SELECT mars.build_timeseries_table('test_varlen_as_tag3', 'tagkey="tag_id", timekey="ts", timebucket="1 day"');
SELECT mars.build_timeseries_table('test_varlen_as_tag4', 'tagkey="tag_id", timekey="ts", timebucket="1 day"');
SELECT mars.build_timeseries_table('test_varlen_as_tag5', 'tagkey="tag_id", timekey="ts", timebucket="1 day"');

DROP TABLE test_varlen_as_tag1;
DROP TABLE test_varlen_as_tag2;
DROP TABLE test_varlen_as_tag3;
DROP TABLE test_varlen_as_tag4;
DROP TABLE test_varlen_as_tag5;

-- test mars.get_bloat_info
create table foo (c1 int, c2 int) using mars with(group_col_='{c1}') distributed by (c1);
insert into foo select i/3, i%3 from generate_series(0,8) i;
select * from mars.get_bloat_info('foo');

insert into foo values (1, 10);
select * from mars.get_bloat_info('foo');

insert into foo values (2, 10);
select * from mars.get_bloat_info('foo');

insert into foo values (3, 10);
select * from mars.get_bloat_info('foo');

select * from mars.get_bloat_info(null);

drop table foo;

--
-- allow multiple tagkeys
--
\set tname test_mars_udf_multi_groupkeys_t1

create table :tname (tag1 int, tag2 bigint, ts timestamp)
 distributed by (tag1, tag2)
 partition by range(ts);

select mars.build_timeseries_table(:'tname', $$
    tagkey = '{ tag1, tag2 }',
    timekey = ts,
    timebucket = '1 hour'
$$);

select mars.add_partition(:'tname', '2020-01-01', '2 days');
select mars.add_partition(:'tname', '2020-01-03', '2020-01-04', '1 day');

select mars.compress_partition(inhrelid::regclass)
  from pg_inherits
 where inhparent = :'tname'::regclass;

--
-- allow multiple tagkeys with special chars in column names
--
\set tname test_mars_udf_multi_groupkeys_t2

create table :tname ("{a, b}" int, "<""x""; 'y'>" bigint, ts timestamp)
 distributed by ("{a, b}", "<""x""; 'y'>")
 partition by range(ts);

select mars.build_timeseries_table(:'tname', $$
    tagkey = '{ "{a, b}", "<\"x\"; ''y''>" }',
    timekey = 'ts',
    timebucket = '1 hour'
$$);

select mars.add_partition(:'tname', '2020-01-01', '2 days');
select mars.add_partition(:'tname', '2020-01-03', '2020-01-04', '1 day');

select mars.compress_partition(inhrelid::regclass)
  from pg_inherits
 where inhparent = :'tname'::regclass;

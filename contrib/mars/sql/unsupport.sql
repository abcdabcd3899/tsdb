--
-- Many features are disabled in MARS, some of them are unneeded by MARS, some
-- of them will be supported in the future.
--

-- start_ignore
create extension if not exists mars;
-- end_ignore

--
-- VACUUM is not supported by MARS, and is usually unnecessary by MARS
--

\set tname test_mars_unsupport_vacuum
drop table if exists :tname;
create table :tname (c1 int, c2 int, c3 int)
  using mars distributed by (c1);
vacuum :tname;

--
-- ALTER TABLE column related subcmds are not supported by MARS
--

-- 1) a single table
\set tname test_mars_unsupport_alter_table_t1
drop table if exists :tname;
create table :tname (c1 int, c2 int, c3 int)
  using mars distributed by (c1);
-- ADD COLUMN is not supported
alter table :tname add column c4 int;
-- DROP COLUMN is not supported
alter table :tname drop column c3;
-- ALTER COLUMN TYPE is not supported
alter table :tname alter column c3 type bigint;
-- ALTER COLUMN SET/DROP NOT NULL, etc., are supported
alter table :tname alter column c3 set not null;
alter table :tname alter column c3 drop not null;

-- 2) partition table with greenplum syntax
\set tname test_mars_unsupport_alter_table_t2
drop table if exists :tname;
create table :tname (c1 int, c2 int, c3 int)
  using mars distributed by (c1)
  partition by range(c2) (start(0) end(1) every(1));
-- ADD COLUMN is not supported
alter table :tname add column c4 int;
-- DROP COLUMN is not supported
alter table :tname drop column c3;
-- ALTER COLUMN TYPE is not supported
alter table :tname alter column c3 type bigint;
-- ALTER COLUMN SET/DROP NOT NULL, etc., are supported
alter table :tname alter column c3 set not null;
alter table :tname alter column c3 drop not null;

-- 3) partition table with postgres syntax
\set tname test_mars_unsupport_alter_table_t3
\set pname test_mars_unsupport_alter_table_t3_p1
drop table if exists :tname;
create table :tname (c1 int, c2 int, c3 int)
  distributed by (c1)
  partition by range(c2);
create table :pname
  partition of :tname
  for values from (0) to (1)
  using mars;
-- ADD COLUMN is not supported
alter table :tname add column c4 int;
-- DROP COLUMN is not supported
alter table :tname drop column c3;
-- ALTER COLUMN TYPE is not supported
alter table :tname alter column c3 type bigint;
-- ALTER COLUMN SET/DROP NOT NULL, etc., are supported
alter table :tname alter column c3 set not null;
alter table :tname alter column c3 drop not null;

--
-- MARS does not allow concurrent INSERT
--

\set tname test_mars_unsupport_concurrent_insert
\set tnamestr '''' :tname ''''
drop table if exists :tname;
create table :tname (c1 int, c2 int, c3 int) using mars distributed by (c1);

begin;
	insert into :tname values (1, 1, 1);

	-- we should have acquired the ExclusiveLock lock
	select mode, granted from pg_locks
	 where relation = :tnamestr::regclass
	   and mode = 'ExclusiveLock'
	   and gp_segment_id = -1;
abort;

--
-- MARS does not inheritance
--
-- update: we have fixed it, so re-enabled.
--

\set tname test_mars_unsupport_inherit
\set tnamechild test_mars_unsupport_inherit_child
drop table if exists :tname;
create table :tname (c1 int, c2 int, c3 int) using mars distributed by (c1);
create table :tnamechild () inherits (:tname) using mars distributed by (c1);
insert into :tname values (1, 2, 3);
insert into :tnamechild values (4, 5, 6);

explain (costs off)
select sum(c1), count(c2), min(c3) from :tname;
select sum(c1), count(c2), min(c3) from :tname;

explain (costs off)
select sum(c1), count(c2), min(c3) from :tnamechild;
select sum(c1), count(c2), min(c3) from :tnamechild;

drop table :tname cascade;

create table :tname (tag int, tz timestamp, col1 int) using mars with (tagkey="tag", timekey="tz", timebucket="1 hour");
explain select time_bucket('90 mins', tz) as tb from :tname order by tb;
explain select time_bucket('60 mins', tz) as tb from :tname order by tb;
explain select time_bucket('90 mins', tz) as tb from :tname order by time_bucket('60 mins', tz);

-- using local_order, we still can give ts order
explain select * from :tname order by tz;
explain select * from :tname order by tag, tz;
drop table :tname;

create table :tname (tag int, tz timestamp, col1 int) using mars with (group_col_="{tag}", group_param_col_="{tz in 1 hour}");
-- without local_order we can promise tb order
explain select time_bucket('90 mins', tz) as tb from :tname order by tb;
explain select time_bucket('60 mins', tz) as tb from :tname order by tb;
explain select time_bucket('90 mins', tz) as tb from :tname order by time_bucket('60 mins', tz);

-- without local_order can not give ts order
explain select * from :tname order by tz;
explain select * from :tname order by tag, tz;
drop table :tname;

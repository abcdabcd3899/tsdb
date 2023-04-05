-- start_ignore
create extension if not exists mars;
create extension if not exists matrixts;
drop schema if exists test_wide;
-- end_ignore

create schema test_wide;

\set ntags 1000
\set ncols 1600
\set naggs 200

-- generate column definition
\out /dev/null
select string_agg(', c' || i || ' float', e'\n') as cols
  from generate_series(3, :ncols) i
;
\out
\gset

create table test_wide.t1
     ( time  timestamp
     , tag   int
     :cols
     )
 using mars
  with (group_param_col_ = "{time in 15 mins}")
  distributed by (tag)
;

-- generate column values
\out /dev/null
select string_agg(', 0 * 10000 + tag + ' || i || '/10000.', e'\n') as cols
  from generate_series(3, :ncols) i
;
\out
\gset

insert into test_wide.t1
select '2000-01-01 12:00:00'::timestamp + '15min'::interval * 0 as time
     , tag as tag
	 :cols
  from generate_series(1, :ntags) tag
;

insert into test_wide.t1
select '2000-01-01 12:00:00'::timestamp + '15min'::interval * 1 as time
     , tag as tag
	 :cols
  from generate_series(1, :ntags) tag
;

insert into test_wide.t1
select '2000-01-01 12:00:00'::timestamp + '15min'::interval * 2 as time
     , tag as tag
	 :cols
  from generate_series(1, :ntags) tag
;

insert into test_wide.t1
select '2000-01-01 12:00:00'::timestamp + '15min'::interval * 3 as time
     , tag as tag
	 :cols
  from generate_series(1, :ntags) tag
;

-- generate column aggs
\out /dev/null
select string_agg(  ', avg   (      c' || i || ') as c' || i || '_cur_val'
	             || ', min   (      c' || i || ') as c' || i || '_min_val'
	             || ', max   (      c' || i || ') as c' || i || '_max_val'
	             || ', sum   (      c' || i || ') as c' || i || '_sum_val'
	             || ', first (time, c' || i || ') as c' || i || '_min_val_occur_time'
	             || ', last  (time, c' || i || ') as c' || i || '_max_val_occur_time'
				 , e'\n') as cols
  from generate_series(3, :naggs) i
;
\out
\gset

create table test_wide.result
 using mars
as
select tag
     , time_bucket('1 hour', time) as timeslot
	 :cols
  from test_wide.t1
 group by 1, 2
 distributed by (tag)
;

select count(*) from test_wide.result;

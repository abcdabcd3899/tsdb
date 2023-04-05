create table foo ( tag int ,tz timestamp(6) ,col float, unique(tag,tz)) distributed by (tag) partition by range(tz);
create table in1 (like foo);
create table in2 (like foo);

select mars.build_timeseries_table('foo','tagkey="tag", timekey="tz", timebucket="1 hour"', true);
select mars.add_partition('foo', '2020-01-08', '2020-01-09','1 day');

select mars.compress_partition(inhrelid::regclass) from pg_inherits where inhparent = 'foo'::regclass;

insert into in1 select i/10, '2020-01-08 00:00:00'::timestamp + concat(i/10., 'mins')::interval from generate_series(0, 19) i;
insert into in2 select i/10, '2020-01-08 00:00:00'::timestamp + concat(i/10., 'mins')::interval, i/10. from generate_series(0, 19) i;


-- merge ok
insert into foo select * from in1 order by tag, tz on conflict (tag,tz) do update set col = coalesce(excluded.col, foo.col);
insert into foo select * from in2 order by tag, tz on conflict (tag,tz) do update set col = coalesce(excluded.col, foo.col);
select * from foo order by tag, tz;

-- not merge
insert into foo select * from in2 order by tag, tz on conflict (tag,tz) do update set col = coalesce(excluded.col, foo.col);
select * from foo order by tag, tz;

truncate foo;

insert into foo select * from in2 order by tag, tz on conflict (tag,tz) do update set col = coalesce(excluded.col, foo.col);
insert into foo select * from in1 order by tag, tz on conflict (tag,tz) do update set col = coalesce(excluded.col, foo.col);
select * from foo order by tag, tz;

drop table foo;

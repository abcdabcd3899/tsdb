-- start_ignore
create extension if not exists matrixts;
create extension if not exists mars;
-- end_ignore

-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

CREATE TABLE gorilla_with_filter
(batch int ENCODING (compresstype=zstd), ts timestamp, f1 float ENCODING (compresstype=zstd), f2 float ENCODING (compresstype=lz4))
USING mars WITH (group_col_ = "{batch}", group_param_col_ = "{ts in 1 day}")
DISTRIBUTED by (batch);

\set f1_agg_funcs 'count(*) as f1count, min(f1) as f1min, max(f1) as f1max, avg(f1) as f1avg, sum(f1) as f1sum'
\set f2_agg_funcs 'count(*) as f2count, min(f2) as f2min, max(f2) as f2max, avg(f2) as f2avg, sum(f2) as f2sum'
\set select_target 'select :f1_agg_funcs, :f2_agg_funcs from gorilla_with_filter'

insert into gorilla_with_filter select 1, '2020-10-10 09:01:02'::timestamp + i * interval '10 milliseconds', i/103::float, i/101::float
 from generate_series(1, 1000000)i;
insert into gorilla_with_filter select 2, '2020-10-10 09:01:02'::timestamp + i * interval '10 milliseconds', i/103::float, i/101::float
 from generate_series(1, 1000000)i;

-- start_ignore
select pg_size_pretty(pg_total_relation_size('gorilla_with_filter'));
-- end_ignore
select batch, f1, f2 from gorilla_with_filter where batch = 1 limit 10;

-- without footer info;
set mars.enable_customscan to off;
:select_target where batch = 1
UNION ALL
:select_target where batch = 1 and f2 < 3
UNION ALL
:select_target where batch = 1 and f2 < 3 and f2 > 4
UNION ALL
:select_target where batch = 1 and f2 > 3 and f2 < 4
UNION ALL
:select_target where batch = 1 and f2 > 3 and f2 <> 4
UNION ALL
:select_target where batch <> 1 and f2 > 3 and f2 <> 4
UNION ALL
:select_target where batch <> 1 and f1 > 10 and f2 > 3 and f2 <> 4
UNION ALL
:select_target where batch <> 1 and f1 < 30 and f1 > 10 and f2 > 3 and f2 <> 4;

-- with footer info;
-- results of filters which include not equal conditions is wrong.
-- Marsscan does not support 'not equal' condition yet.
set mars.enable_customscan to on;
:select_target where batch = 1
UNION ALL
:select_target where batch = 1 and f2 < 3
UNION ALL
:select_target where batch = 1 and f2 < 3 and f2 > 4
UNION ALL
:select_target where batch = 1 and f2 > 3 and f2 < 4
UNION ALL
:select_target where batch = 1 and f2 > 3 and f2 <> 4
UNION ALL
:select_target where batch <> 1 and f2 > 3 and f2 <> 4
UNION ALL
:select_target where batch <> 1 and f1 > 10 and f2 > 3 and f2 <> 4
UNION ALL
:select_target where batch <> 1 and f1 < 30 and f1 > 10 and f2 > 3 and f2 <> 4;

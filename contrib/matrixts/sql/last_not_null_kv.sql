--
-- verify the last_not_null_kv agg.
--

\set tname test_last_not_null_kv_t1

-- start_ignore
drop table if exists :tname;
-- end_ignore

create table :tname (batch int, ts int, value jsonb, part int default 0)
 partition by range(part) (start(0) end(2) every(1))
 distributed by (batch);

-- single-phase agg, parallel disabled
set enable_parallel_mode to off;
set gp_enable_multiphase_agg to off;
set enable_partitionwise_aggregate to off;
\i sql/last_not_null_kv.template

-- multi-phase agg, parallel disabled
set enable_parallel_mode to off;
set gp_enable_multiphase_agg to on;
set enable_partitionwise_aggregate to on;
\i sql/last_not_null_kv.template

-- single-phase agg, parallel enabled
set enable_parallel_mode to on;
set gp_enable_multiphase_agg to off;
set enable_partitionwise_aggregate to off;
\i sql/last_not_null_kv.template

-- multi-phase agg, parallel enabled
set enable_parallel_mode to on;
set gp_enable_multiphase_agg to on;
set enable_partitionwise_aggregate to on;
\i sql/last_not_null_kv.template

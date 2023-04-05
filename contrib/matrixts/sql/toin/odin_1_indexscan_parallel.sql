set search_path to test_toin;
set enable_seqscan to off;
set enable_bitmapscan to off;
set enable_indexonlyscan to off;
set enable_indexscan to on;
-- enable parallel scan
set enable_parallel_mode to on;
set max_parallel_workers_per_gather to 1;
set min_parallel_index_scan_size to '32kB';

--
-- fill the settings for the template
--

\set tname 't_data'
\set explain 'explain (costs off)'

--
-- run the template
--

\i sql/toin/operator.template

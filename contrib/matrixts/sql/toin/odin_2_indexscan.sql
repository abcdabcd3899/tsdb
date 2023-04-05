set search_path to test_toin;
set enable_seqscan to off;
set enable_bitmapscan to off;
set enable_indexonlyscan to off;
set enable_indexscan to on;

--
-- fill the settings for the template
--

\set tname 't_data'
\set explain 'explain (costs off)'

--
-- run the template
--

\i sql/toin/operator.template

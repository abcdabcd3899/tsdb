set search_path to test_toin;
set enable_seqscan to off;
set enable_bitmapscan to off;

--
-- fill the settings for the template
--

\set tname 't_data'
\set explain 'explain (costs off)'

--
-- run the template
--

\i sql/toin/operator.template

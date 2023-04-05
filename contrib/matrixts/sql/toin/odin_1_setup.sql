set search_path to test_toin;

--
-- fill the settings for the template
--

\set tname 't_data'
\set iname 'i_data'
\set itype 'odin'
\set keys 'c1'
\set opts 'include (c1)'

--
-- run the template
--

\i sql/toin/operator_setup.template

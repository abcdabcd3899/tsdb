set search_path to test_toin;

--
-- fill the settings for the template
--

\set tname 't_data'
\set iname 'i_data'
\set itype 'toin'
\set keys 'c1, c2'
\set opts 'with (pages_per_range=1)'

--
-- run the template
--

\i sql/toin/operator_setup.template

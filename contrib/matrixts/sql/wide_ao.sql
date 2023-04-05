-- start_ignore
create extension if not exists matrixts;
drop schema if exists test_wide;
-- end_ignore

create schema test_wide;

\set ncols 1600
\set nrows 10

\set tname test_wide.t_aoco
\set am ao_column
\i sql/wide_ao.template

\set tname test_wide.t_ao
\set am ao_row
\i sql/wide_ao.template

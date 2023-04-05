-- start_ignore
CREATE EXTENSION IF NOT EXISTS matrixts;
CREATE EXTENSION IF NOT EXISTS mars;
create extension gp_debug_numsegments;
select gp_debug_set_create_table_default_numsegments(1);
-- end_ignore

\set tname test_expand_mars_group_col_
\set withopts 'with (group_col_=''{c1}'')'
\i sql/mars_expand.template

\set tname test_expand_mars_group_param_col_
\set withopts 'with (group_param_col_=''{c1 in 500}'')'
\i sql/mars_expand.template

\set tname test_expand_mars_group_col_local_order_col_
\set withopts 'with (group_col_=''{c1}'', local_order_col_=''{c2}'')'
\i sql/mars_expand.template

\set tname test_expand_mars_local_order_col_
\set withopts 'with (local_order_col_=''{c1}'')'
\i sql/mars_expand.template

\set tname test_expand_mars_group_col_group_param_col_
\set withopts 'with (group_col_=''{c1}'', group_param_col_=''{c2 in 500}'')'
\i sql/mars_expand.template

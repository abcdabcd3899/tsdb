--
-- verify the automatically block merging on disordered insertions.
--
-- to have stable output a trick is played that all the data are distributed to
-- the same segment by set the distribution key to the same value in all the
-- rows.
--
-- in the tests we want to verify the block ids of the new, maybe merged, data,
-- and we want to verify the data content, we must verify them separately,
-- because some tests don't have stable data order on param groupkeys.
--
-- NOTE: new test items can only be appended to the template file; if any item
-- is inserted in the middle, all the tests after it will fail due to block id
-- changes.
--

\set tname test_mars_disordered_insert_group_0_0
\set withopts ''
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_1_0
\set withopts 'with (group_col_=''{c1}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_4_0
\set withopts 'with (group_param_col_=''{c1 in 4}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_1_4
\set withopts 'with (group_col_=''{c1}'', group_param_col_=''{c2 in 4}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_1_0_order1
\set withopts 'with (group_col_=''{c1}'', local_order_col_=''{c1}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_1_0_order2
\set withopts 'with (group_col_=''{c1}'', local_order_col_=''{c2}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_1_0_order12
\set withopts 'with (group_col_=''{c1}'', local_order_col_=''{c1, c2}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_4_0_order1
\set withopts 'with (group_param_col_=''{c1 in 4}'', local_order_col_=''{c1}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_4_0_order2
\set withopts 'with (group_param_col_=''{c1 in 4}'', local_order_col_=''{c2}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_1_4_order1
\set withopts 'with (group_col_=''{c1}'', group_param_col_=''{c2 in 4}'', local_order_col_=''{c1}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_1_4_order2
\set withopts 'with (group_col_=''{c1}'', group_param_col_=''{c2 in 4}'', local_order_col_=''{c2}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_1_4_order12
\set withopts 'with (group_col_=''{c1}'', group_param_col_=''{c2 in 4}'', local_order_col_=''{c1, c2}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_0_0_order1
\set withopts 'with (local_order_col_=''{c1}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_0_0_order2
\set withopts 'with (local_order_col_=''{c2}'')'
\i sql/disordered_insert.template

\set tname test_mars_disordered_insert_group_0_0_order12
\set withopts 'with (local_order_col_=''{c1, c2}'')'
\i sql/disordered_insert.template

-- vi: et :

\set explain 'explain (costs off)'

\set tname test_mars_multi_orderkeys
\set global_order_cols '"{c1, c2, c3, c4}"'
\set local_order_cols  '"{c1, c2, c3, c4}"'
\set group_cols        '"{}"'
\set group_param_cols  '"{}"'
\i sql/multikeys.template

\set tname test_mars_multi_orderkeys_groupkeys
\set global_order_cols '"{c1, c2, c3, c4}"'
\set local_order_cols  '"{c1, c2, c3, c4}"'
\set group_cols        '"{c1, c2}"'
\set group_param_cols  '"{}"'
\i sql/multikeys.template

\set tname test_mars_multi_orderkeys_param_groupkeys
\set global_order_cols '"{c1, c2, c3, c4}"'
\set local_order_cols  '"{c1, c2, c3, c4}"'
\set group_cols        '"{c1, c2}"'
\set group_param_cols  '"{c3 in 1, c4 in 1}"'
\i sql/multikeys.template

\set tname test_mars_multi_groupkeys
\set global_order_cols '"{}"'
\set local_order_cols  '"{c1, c2, c3, c4}"'
\set group_cols        '"{c1, c2}"'
\set group_param_cols  '"{}"'
\i sql/multikeys.template

\set tname test_mars_multi_param_groupkeys
\set global_order_cols '"{}"'
\set local_order_cols  '"{c1, c2, c3, c4}"'
\set group_cols        '"{c1, c2}"'
\set group_param_cols  '"{c3 in 1, c4 in 1}"'
\i sql/multikeys.template

-- vi: et :

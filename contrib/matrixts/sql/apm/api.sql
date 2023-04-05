-- start_ignore
CREATE EXTENSION matrixts;
SET enable_mergejoin TO off;
SET enable_nestloop TO off;
SET enable_parallel_mode TO off;
SET gp_enable_multiphase_agg TO off;
SET optimizer TO off;
CREATE SCHEMA IF NOT EXISTS apm_test;
SET matrix.ts_guc_apm_allow_dml TO ON;
TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy;
TRUNCATE matrixts_internal.apm_rel_policy_action;
SET matrix.ts_guc_apm_allow_dml TO OFF;
-- end_ignore

------------------------------------
----          set_policy        ----
------------------------------------
-- should error invalid regclass;
SELECT public.set_policy('no_such_table'::regclass, 'abc');

-- start_ignore
DROP TABLE IF EXISTS apm_test.set_tab;
CREATE TABLE apm_test.set_tab(id int);
-- end_ignore

-- should error when rel is NULL
SELECT public.set_policy(NULL, 'abc');

-- should error when policy_name is NULL or empty
SELECT public.set_policy('apm_test.set_tab'::regclass, NULL);
SELECT public.set_policy('apm_test.set_tab'::regclass, '');

-- should error "Only partitioned table can set_policy"
SELECT public.set_policy('apm_test.set_tab'::regclass, 'abc');

-- start_ignore
DROP TABLE IF EXISTS apm_test."_Fo0_!";
CREATE TABLE apm_test."_Fo0_!" (trans_id int, date date, amount
decimal(9,2), region text)
DISTRIBUTED BY (trans_id)
PARTITION BY RANGE (date)
SUBPARTITION BY LIST (region)
SUBPARTITION TEMPLATE
( SUBPARTITION usa VALUES ('usa'),
  SUBPARTITION china VALUES ('china'),
  DEFAULT SUBPARTITION other_regions)
  (START (date '2011-01-01') INCLUSIVE
   END (date '2012-01-01') EXCLUSIVE
   EVERY (INTERVAL '6 month'),
   DEFAULT PARTITION outlying_dates);
-- end_ignore

-- should error "Cannot set_policy to a partition child table"
SELECT public.set_policy('apm_test."_Fo0_!_1_prt_2"'::regclass, 'abc');
SELECT public.set_policy('apm_test."_Fo0_!_1_prt_2_2_prt_china"'::regclass, 'abc');

-- should error "No such policy "ab'c"
SELECT public.set_policy('apm_test."_Fo0_!"'::regclass, 'ab''c');

-- should success
SELECT public.set_policy('apm_test."_Fo0_!"'::regclass, 'auto_partitioning');
-- should add 1 line to apm_rel_policy
SELECT reloid::regclass, class_id, list_func, list_args, version FROM matrixts_internal.apm_rel_policy;
-- should be empty in apm_rel_policy_action
SELECT reloid::regclass, class_id, action, disabled, check_func, check_args, act_func, act_args, term_func, term_args, version FROM matrixts_internal.apm_rel_policy_action;
-- should add 1 line in apm_operation_log
SELECT reloid::regclass, relname, operation, class_id FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should error "already has the policy"
SELECT public.set_policy('apm_test."_Fo0_!"'::regclass, 'auto_partitioning');
-- should still 1 line in apm_operation_log
SELECT reloid::regclass, relname, operation, class_id FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should success
SELECT public.drop_policy('apm_test."_Fo0_!"'::regclass);
-- should be empty in apm_rel_policy
SELECT reloid::regclass, class_id, list_func, list_args, version FROM matrixts_internal.apm_rel_policy;
-- should be empty in apm_rel_policy_action
SELECT reloid::regclass, class_id, action, disabled, check_func, check_args, act_func, act_args, term_func, term_args, version FROM matrixts_internal.apm_rel_policy_action;

-- should be 2 lines in apm_operation_log
SELECT reloid::regclass, relname, operation, class_id FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- start_ignore
TRUNCATE matrixts_internal.apm_operation_log;
-- end_ignore

---- assign two policy to a table is not allowed in this version ----
-- should success
SELECT public.set_policy('apm_test."_Fo0_!"'::regclass, 'auto_splitting');
-- should fail because 1 policy per table
SELECT public.set_policy('apm_test."_Fo0_!"'::regclass, 'auto_partitioning');
-- should success update table action setting
SELECT public.set_policy_action('apm_test."_Fo0_!"'::regclass, 'retention', '6 month');
SELECT public.set_policy_action('apm_test."_Fo0_!"'::regclass, 'auto_split', '{ "age": "20 days", "period": "15 day" }');
-- should warn rel has no such policy
SELECT public.drop_policy('apm_test."_Fo0_!"'::regclass, 'auto_partitioning');
-- should have 1 line of operation log
SELECT reloid::regclass, relname, operation, class_id, class_name FROM matrixts_internal.apm_operation_log ORDER BY ts;
-- should drop successfully
SELECT public.drop_policy('apm_test."_Fo0_!"'::regclass, 'auto_splitting');
-- should have 2 lines of operation log
SELECT reloid::regclass, relname, operation, class_id, class_name FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- the GUC should be still off
SHOW matrix.ts_guc_apm_allow_dml;

------------------------------------
----      set_policy_action     ----
------------------------------------
-- start_ignore
SET matrix.ts_guc_apm_allow_dml TO ON;
TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy;
TRUNCATE matrixts_internal.apm_rel_policy_action;
SET matrix.ts_guc_apm_allow_dml TO OFF;
-- end_ignore

-- should error when rel is NULL
SELECT set_policy(NULL, 'auto_partitioning');

-- should error when action_name is NULL or empty
SELECT set_policy_action('apm_test."_Fo0_!"'::regclass, NULL, '');
SELECT set_policy_action('apm_test."_Fo0_!"'::regclass, '', '');

-- should error when table don't have a policy
SELECT set_policy_action('apm_test."_Fo0_!"'::regclass, 'retention', '2 year');

-- should success bind policy to table
SELECT set_policy('apm_test."_Fo0_!"'::regclass, 'auto_partitioning');
-- should be empty in apm_rel_policy_action
SELECT reloid::regclass, class_id, action, disabled, check_func, check_args, act_func, act_args, term_func, term_args, version FROM matrixts_internal.apm_rel_policy_action;

-- should success set action args
SELECT set_policy_action('apm_test."_Fo0_!"'::regclass, 'retention', '2 year');
-- should have 1 row in apm_rel_policy_action
SELECT reloid::regclass, class_id, action, disabled, check_func, check_args, act_func, act_args, term_func, term_args, version FROM matrixts_internal.apm_rel_policy_action;
-- should be 2 lines in apm_operation_log
SELECT reloid::regclass, relname, operation, class_id FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- the GUC should be still off
SHOW matrix.ts_guc_apm_allow_dml;

----------------------------------------
----      disable_policy_action     ----
----      enable_policy_action      ----
----------------------------------------
SELECT enable_policy_action('apm_test."_Fo0_!"'::regclass, 'retention');
-- should be false in disabled column in apm_rel_policy_action
SELECT reloid::regclass, class_id, action, disabled, check_func, check_args, act_func, act_args, term_func, term_args, version FROM matrixts_internal.apm_rel_policy_action;
-- should be 3 lines in apm_operation_log
SELECT reloid::regclass, relname, operation, class_id FROM matrixts_internal.apm_operation_log ORDER BY ts;

SELECT disable_policy_action('apm_test."_Fo0_!"'::regclass, 'retention');
-- should be true in disabled column in apm_rel_policy_action
SELECT reloid::regclass, class_id, action, disabled, check_func, check_args, act_func, act_args, term_func, term_args, version FROM matrixts_internal.apm_rel_policy_action;
-- should be 4 lines in apm_operation_log
SELECT reloid::regclass, relname, operation, class_id FROM matrixts_internal.apm_operation_log ORDER BY ts;

SELECT disable_policy_action('apm_test."_Fo0_!"'::regclass, 'auto_create');
-- should be true in disabled column in apm_rel_policy_action
SELECT reloid::regclass, class_id, action, disabled, check_func, check_args, act_func, act_args, term_func, term_args, version FROM matrixts_internal.apm_rel_policy_action;
-- should be 5 lines in apm_operation_log
SELECT reloid::regclass, relname, operation, class_id FROM matrixts_internal.apm_operation_log ORDER BY ts;

SELECT enable_policy_action('apm_test."_Fo0_!"'::regclass, 'auto_create');
-- should be false in disabled column in apm_rel_policy_action
SELECT reloid::regclass, class_id, action, disabled, check_func, check_args, act_func, act_args, term_func, term_args, version FROM matrixts_internal.apm_rel_policy_action;
-- should be 6 lines in apm_operation_log
SELECT reloid::regclass, relname, operation, class_id FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should return effective apm settings
SELECT relname, class_name, action, disabled, check_func, check_args, act_func, act_args, version FROM apm_settings;
SELECT relname, class_name, action, disabled, check_func, check_args, act_func, act_args, version FROM list_policy('apm_test."_Fo0_!"');
-- should error for non-exist table
SELECT relname, class_name, action, disabled, check_func, check_args, act_func, act_args, version FROM list_policy('aaaXXXX');

----------------------------------------------------------
-- Tests for following policy UDFs
--   * apm_generic_expired
--   * apm_generic_drop_partition
--   * apm_generic_expired_set
-- These UDF is for auto drop old partition.
----------------------------------------------------------
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

---------------------------------------------
---- apm_generic_expired
---------------------------------------------
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_ts;
CREATE TABLE apm_test.by_ts (ts timestamp, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (now() - '10 year'::interval) END (now() - '5 year'::interval) EVERY ('1 year') ) ;

DROP TABLE IF EXISTS apm_test.by_tstz;
CREATE TABLE apm_test.by_tstz (ts timestamp with time zone, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (now() - '10 year'::interval) END (now() - '5 year'::interval) EVERY ('1 year') ) ;
-- end_ignore

-- should return '{NULL}' as partition boundary totally before now - 1 year, should drop
SELECT matrixts_internal.apm_generic_expired(
    NULL
  , 'apm_test.by_ts_1_prt_yr_3'::regclass
  , 'apm_test.by_ts'::regclass
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = 'apm_test.by_ts_1_prt_yr_3'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{ "after": "1 year" }'
);
-- should return '{NULL}' as partition boundary totally before now - 7 year, should drop
SELECT matrixts_internal.apm_generic_expired(
    NULL
  , 'apm_test.by_ts_1_prt_yr_3'::regclass
  , 'apm_test.by_ts'::regclass
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = 'apm_test.by_ts_1_prt_yr_3'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{ "after": "7 year" }'
);
-- should return NULL as partition boundary includes now - 8 year
SELECT matrixts_internal.apm_generic_expired(
    NULL
  , 'apm_test.by_ts_1_prt_yr_3'::regclass
  , 'apm_test.by_ts'::regclass
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = 'apm_test.by_ts_1_prt_yr_3'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{ "after": "8 year" }'
);
-- should return NULL as partition boundary totally after now - 9 year
SELECT matrixts_internal.apm_generic_expired(
    NULL
  , 'apm_test.by_ts_1_prt_yr_3'::regclass
  , 'apm_test.by_ts'::regclass
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = 'apm_test.by_ts_1_prt_yr_3'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{ "after": "9 year" }'
);

-- should return '{NULL}' as partition boundary totally before now - 1 year
SELECT matrixts_internal.apm_generic_expired(
    NULL
  , 'apm_test.by_tstz_1_prt_yr_3'::regclass
  , 'apm_test.by_tstz'::regclass
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = 'apm_test.by_tstz_1_prt_yr_3'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{ "after": "1 year" }'
);
-- should return '{NULL}' as partition boundary totally before now - 7 year
SELECT matrixts_internal.apm_generic_expired(
    NULL
  , 'apm_test.by_tstz_1_prt_yr_3'::regclass
  , 'apm_test.by_tstz'::regclass
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = 'apm_test.by_tstz_1_prt_yr_3'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{ "after": "7 year" }'
);
-- should return NULL as partition boundary includes now - 8 year
SELECT matrixts_internal.apm_generic_expired(
    NULL
  , 'apm_test.by_tstz_1_prt_yr_3'::regclass
  , 'apm_test.by_tstz'::regclass
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = 'apm_test.by_tstz_1_prt_yr_3'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{ "after": "8 year" }'
);
-- should return NULL as partition boundary totally after now - 9 year
SELECT matrixts_internal.apm_generic_expired(
    NULL
  , 'apm_test.by_tstz_1_prt_yr_3'::regclass
  , 'apm_test.by_tstz'::regclass
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = 'apm_test.by_tstz_1_prt_yr_3'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{ "after": "9 year" }'
);

ROLLBACK;

------------------------------------
---- apm_generic_drop_partition ----
------------------------------------
-- start_ignore
CREATE TABLE apm_test.xyz(id int);
-- end_ignore
-- should return true
SELECT matrixts_internal.apm_generic_drop_partition(NULL, 'apm_test.xyz'::regclass, NULL, NULL, NULL, NULL, NULL, '{}'::matrixts_internal._apm_boundary);
-- should report partition does not exist
SELECT matrixts_internal.apm_generic_drop_partition(NULL, 'apm_test.xyz'::regclass, NULL, NULL, NULL, NULL, NULL, '{}'::matrixts_internal._apm_boundary);

-- start_ignore
CREATE SCHEMA IF NOT EXIST "a""b""C""";
CREATE TABLE "a""b""C"""."x''""Y.""z"(id int);
-- end_ignore
-- should return true
SELECT matrixts_internal.apm_generic_drop_partition(NULL, '"a""b""C"""."x''''""Y.""z"'::regclass, NULL, NULL, NULL, NULL, NULL, '{}'::matrixts_internal._apm_boundary);
-- should report partition does not exist
SELECT matrixts_internal.apm_generic_drop_partition(NULL, '"a""b""C"""."x''''""Y.""z"'::regclass, NULL, NULL, NULL, NULL, NULL, '{}'::matrixts_internal._apm_boundary);

---------------------------------
---- apm_generic_expired_set ----
---------------------------------
SHOW matrix.ts_guc_apm_allow_dml;
-- start_ignore
DROP TABLE IF EXISTS apm_test.expired_set;
CREATE TABLE apm_test.expired_set (id int, year int, qtr int, day int, region text)
DISTRIBUTED BY (id) PARTITION BY RANGE (year)
(PARTITION yr START (2010) END (2012) EVERY (1));
TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy_action;
-- end_ignore

-- should report error on invalid input
SELECT matrixts_internal.apm_generic_expired_set(NULL, NULL, NULL, NULL);
SELECT matrixts_internal.apm_generic_expired_set('apm_test.expired_set'::regclass, 1, NULL, NULL);
SELECT matrixts_internal.apm_generic_expired_set('apm_test.expired_set'::regclass, 1, 'fake_action', 'abc');
SELECT matrixts_internal.apm_generic_expired_set('apm_test.expired_set'::regclass, NULL, 'test_expire', '3 years');

-- should be empty
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry
SELECT matrixts_internal.apm_generic_expired_set('apm_test.expired_set'::regclass, 1, 'test_expire', '3 years');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should have reset GUC to false
SHOW matrix.ts_guc_apm_allow_dml;
-- should append 1 add record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should update to new value
SELECT matrixts_internal.apm_generic_expired_set('apm_test.expired_set'::regclass, 1, 'test_expire', '4 years');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 modify record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should delete the entry
SELECT matrixts_internal.apm_generic_expired_set('apm_test.expired_set'::regclass, 1, 'test_expire', NULL);
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 drop record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

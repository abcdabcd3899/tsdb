----------------------------------------------------------
-- Tests for time-series to mars policy functions
--   * apm_list_partition_with_am

--   * apm_expired_with_am_set
----------------------------------------------------------
-- start_ignore
CREATE EXTENSION matrixts;
CREATE EXTENSION mars;
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
---- apm_list_partition_with_am ----
------------------------------------
-- start_ignore
DROP TABLE IF EXISTS apm_test.sales;
CREATE TABLE apm_test.sales (id int, year int, qtr int, day int, region text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
(PARTITION yr START (2010) END (2014) EVERY (1));
-- end_ignore

SELECT
    root_reloid IS NULL AS root_reloid
  , root_relname
  , parent_reloid IS NULL AS parent_reloid
  , parent_relname
  , reloid IS NULL AS reloid
  , relname
  , partitiontype
  , partitionlevel
  , partitionisdefault
  , partitionboundary
  , addopt
FROM matrixts_internal.apm_list_partition_with_am('apm_test.sales'::regclass, NULL)
ORDER BY relname;

-- start_ignore
DROP TABLE IF EXISTS apm_test.sales2;
CREATE TABLE apm_test.sales2 (trans_id int, date date, amount
decimal(9,2), region text)
DISTRIBUTED BY (trans_id)
PARTITION BY RANGE (date)
SUBPARTITION BY LIST (region)
SUBPARTITION TEMPLATE
( SUBPARTITION usa VALUES ('usa'),
  SUBPARTITION asia VALUES ('asia'),
  SUBPARTITION europe VALUES ('europe'),
  DEFAULT SUBPARTITION other_regions)
  (START (date '2011-01-01') INCLUSIVE
   END (date '2012-01-01') EXCLUSIVE
   EVERY (INTERVAL '1 month'),
   DEFAULT PARTITION outlying_dates);
-- end_ignore

SELECT
    root_reloid IS NULL AS root_reloid
  , root_relname
  , parent_reloid IS NULL AS parent_reloid
  , parent_relname
  , reloid IS NULL AS reloid
  , relname
  , partitiontype
  , partitionlevel
  , partitionisdefault
  , partitionboundary
  , addopt
FROM matrixts_internal.apm_list_partition_with_am('apm_test.sales2'::regclass, NULL)
ORDER BY relname;

---------------------------------
----   apm_expired_with_am   ----
---------------------------------
-- start_ignore
DROP TABLE IF EXISTS apm_test.expired_am;
CREATE TABLE apm_test.expired_am (ts timestamp with time zone, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION yr START (now() - '10 year'::interval) END (now() - '5 year'::interval) EVERY ('1 year') ) ;
-- end_ignore

-- should return '{NULL}' as partition boundary totally after now - 1 year and storage is not heap
SELECT matrixts_internal.apm_expired_with_am(
    '2020-01-01 11:22:33'::timestamptz
  , 'apm_test.expired_am_1_prt_yr_2'::regclass
  , 'apm_test.expired_am'::regclass
  , 'apm_test.expired_am'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_list_partition_with_am('apm_test.expired_am'::regclass, NULL) lp WHERE reloid = 'apm_test.expired_am_1_prt_yr_2'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_list_partition_with_am('apm_test.expired_am'::regclass, NULL))
  , '{ "after": "1 year", "storage_is_not": "mars" }'
);

-- should return NULL as partition boundary is not expired
SELECT matrixts_internal.apm_expired_with_am(
    '2020-01-01 11:22:33'::timestamptz
  , 'apm_test.expired_am_1_prt_yr_2'::regclass
  , 'apm_test.expired_am'::regclass
  , 'apm_test.expired_am'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_list_partition_with_am('apm_test.expired_am'::regclass, NULL) lp WHERE reloid = 'apm_test.expired_am_1_prt_yr_2'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_list_partition_with_am('apm_test.expired_am'::regclass, NULL))
  , '{ "after": "9 year", "storage_is_not": "mars" }'
);

-- should return NULL as storage is same
SELECT matrixts_internal.apm_expired_with_am(
    '2020-01-01 11:22:33'::timestamptz
  , 'apm_test.expired_am_1_prt_yr_2'::regclass
  , 'apm_test.expired_am'::regclass
  , 'apm_test.expired_am'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_list_partition_with_am('apm_test.expired_am'::regclass, NULL) lp WHERE reloid = 'apm_test.expired_am_1_prt_yr_2'::regclass))[1]
  , ARRAY(SELECT matrixts_internal.apm_list_partition_with_am('apm_test.expired_am'::regclass, NULL))
  , '{ "after": "1 year", "storage_is_not": "heap" }'
);

-------------------------------------
---- apm_mars_append_partition   ----
---- apm_mars_compress_partition ----
-------------------------------------
DROP TABLE IF EXISTS apm_test."FoO_";
CREATE TABLE apm_test."FoO_" ("tag@" int, "timestamp" timestamp, measurement float)
DISTRIBUTED BY ("tag@") PARTITION BY RANGE("timestamp");
SELECT mars.build_timeseries_table('apm_test."FoO_"','tagkey="tag@", timekey="timestamp", timebucket="1 hour"', true);
SELECT matrixts_internal.apm_mars_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test."FoO_"'::regclass
  , NULL
  , 'apm_test."FoO_"'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test."FoO_"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test."FoO_"'::regclass, NULL))
  , '{"period":"1 week"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test."FoO_"'::regclass
      , NULL
      , 'apm_test."FoO_"'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test."FoO_"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test."FoO_"'::regclass, NULL))
      , '{"before":"3 days"}'::json
    ))
);

SELECT relname, relnsp, addopt FROM matrixts_internal.apm_list_partition_with_am('apm_test."FoO_"', NULL);

SELECT matrixts_internal.apm_mars_compress_partition(NULL, 'apm_test."FoO__1_prt_1"', NULL, NULL, NULL, NULL, NULL, '{}'::matrixts_internal._apm_boundary);

-- do not compress if check_res is NULL
SELECT matrixts_internal.apm_mars_compress_partition(NULL, 'apm_test."FoO__1_prt_2"', NULL, NULL, NULL, NULL, NULL, NULL);

SELECT relname, relnsp, addopt FROM matrixts_internal.apm_list_partition_with_am('apm_test."FoO_"', NULL);

SELECT mars.destroy_timeseries_table('apm_test."FoO_"'::regclass);

DROP TABLE IF EXISTS apm_test."'""'|!@#$%^&为*()_+-斯=[]{}卡\;':蒂"",.<>/ 	?献上心脏！P两面包";
CREATE TABLE apm_test."'""'|!@#$%^&为*()_+-斯=[]{}卡\;':蒂"",.<>/ 	?献上心脏！P两面包" ("tag""@" int, "timestamp" timestamp, measurement float)
DISTRIBUTED BY ("tag""@") PARTITION BY RANGE ("timestamp");

SELECT mars.build_timeseries_table(
  '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"',
  'tagkey="tag""@", timekey="timestamp", timebucket="1 hour"', true);
SELECT matrixts_internal.apm_mars_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , NULL
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL))
  , '{"period":"1 week"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
      , NULL
      , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL))
      , '{"before":"3 days"}'::json
    ))
);

SELECT relname, relnsp, addopt FROM matrixts_internal.apm_list_partition_with_am('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"', NULL);

SELECT matrixts_internal.apm_mars_compress_partition(NULL, '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心_1_prt_1"', NULL, NULL, NULL, NULL, NULL, '{}'::matrixts_internal._apm_boundary);

-- do not compress if check_res is NULL
SELECT matrixts_internal.apm_mars_compress_partition(NULL, '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心_1_prt_2"', NULL, NULL, NULL, NULL, NULL, NULL);

SELECT relname, relnsp, addopt FROM matrixts_internal.apm_list_partition_with_am('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"', NULL);

SELECT mars.destroy_timeseries_table('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass);

---------------------------------
---- apm_expired_with_am_set ----
---------------------------------
SHOW matrix.ts_guc_apm_allow_dml;
-- start_ignore
DROP TABLE IF EXISTS apm_test.expired_am_set;
CREATE TABLE apm_test.expired_am_set (id int, year int, qtr int, day int, region text)
DISTRIBUTED BY (id) PARTITION BY RANGE (year)
(PARTITION yr START (2010) END (2012) EVERY (1));
TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy_action;
-- end_ignore

-- should report error on invalid input
SELECT matrixts_internal.apm_expired_with_am_set(NULL, 1, NULL, NULL);
SELECT matrixts_internal.apm_expired_with_am_set('apm_test.expired_am_set'::regclass, 1, NULL, NULL);
SELECT matrixts_internal.apm_expired_with_am_set('apm_test.expired_am_set'::regclass, 1, 'fake_action', 'abc');
SELECT matrixts_internal.apm_expired_with_am_set('apm_test.expired_am_set'::regclass, 1, 'fake_action', '{ "after": "bcd" }');
SELECT matrixts_internal.apm_expired_with_am_set('apm_test.expired_am_set'::regclass, 1, 'fake_action', '{ "storage_is_not": "def" }');
SELECT matrixts_internal.apm_expired_with_am_set('apm_test.expired_am_set'::regclass, 1, 'fake_action', '{ "after": "efg", "storage_is_not": "xyz" }');
SELECT matrixts_internal.apm_expired_with_am_set('apm_test.expired_am_set'::regclass, 1, 'fake_action', '{ "after": "1 month" }');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry
SELECT matrixts_internal.apm_expired_with_am_set('apm_test.expired_am_set'::regclass, 1, 'test_split', '{ "after": "1 month", "storage_is_not": "xyz" }');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should have reset GUC to false
SHOW matrix.ts_guc_apm_allow_dml;
-- should append 2 add records to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should update check_args
SELECT matrixts_internal.apm_expired_with_am_set('apm_test.expired_am_set'::regclass, 1, 'test_split', '{ "after": "2 month", "storage_is_not": "abc" }');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 mod record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should delete the entry
SELECT matrixts_internal.apm_expired_with_am_set('apm_test.expired_am_set'::regclass, 1, 'test_split', NULL);
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 drop record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

---------------------------------
---- apm_mars_guard ----
---------------------------------
-- start_ignore
DROP EXTENSION mars CASCADE;
-- end_ignore

DROP TABLE IF EXISTS apm_test."FoO_";
CREATE TABLE apm_test."FoO_" ("tag@" int, "timestamp" timestamp, measurement float)
DISTRIBUTED BY ("tag@") PARTITION BY RANGE("timestamp");

-- should error due to no mars extension
SELECT public.set_policy('apm_test."FoO_"'::regclass, 'mars_time_series');

-- start_ignore
CREATE EXTENSION mars VERSION '0.1';
-- end_ignore

DROP TABLE IF EXISTS apm_test."FoO_";
CREATE TABLE apm_test."FoO_" ("tag@" int, "timestamp" timestamp, measurement float)
DISTRIBUTED BY ("tag@") PARTITION BY RANGE("timestamp");

-- should error due to mars extension is too old
SELECT public.set_policy('apm_test."FoO_"'::regclass, 'mars_time_series');

SELECT mars.build_timeseries_table('apm_test."FoO_"','tagkey="tag@", timekey="timestamp", timebucket="1 hour"', true);
-- should error due to mars extension is too old
SELECT public.set_policy('apm_test."FoO_"'::regclass, 'mars_time_series');

-- start_ignore
DROP EXTENSION mars CASCADE;
CREATE EXTENSION mars;
-- end_ignore

DROP TABLE IF EXISTS apm_test."FoO_";
CREATE TABLE apm_test."FoO_" ("tag@" int, "timestamp" timestamp, measurement float)
DISTRIBUTED BY ("tag@") PARTITION BY RANGE("timestamp");

-- should error due to non-time-series table
SELECT public.set_policy('apm_test."FoO_"'::regclass, 'mars_time_series');

SELECT mars.build_timeseries_table('apm_test."FoO_"','tagkey="tag@", timekey="timestamp", timebucket="1 hour"', true);
-- should success
SELECT public.set_policy('apm_test."FoO_"'::regclass, 'mars_time_series');

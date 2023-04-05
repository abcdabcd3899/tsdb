----------------------------------------------------------
-- Tests for following policy UDFs
--   * apm_generic_incoming
--   * apm_generic_append_partition
--   * apm_generic_incoming_set
-- These UDF is for auto-splitting default partition to
-- create historical partitions.
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

------------------------------
---- apm_generic_incoming ----
------------------------------
-- input checks

-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.incoming_by_ts;
CREATE TABLE apm_test.incoming_by_ts (ts timestamp, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION yr START (timestamp '2016-01-01') END (timestamp '2020-01-01') EVERY ('1 year') ) ;
-- end_ignore

-- should raise error: Must have "before" expression.
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33+08:00'::timestamptz
  , 'apm_test.incoming_by_ts'::regclass
  , NULL
  , 'apm_test.incoming_by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL))
  , '{"fakeconfig":"2 days"}'::json
);
ROLLBACK;

-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.by_hash;
CREATE TABLE apm_test.by_hash (a int, b int) PARTITION BY HASH (b);
CREATE TABLE by_hash_0 PARTITION OF apm_test.by_hash FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE by_hash_1 PARTITION OF apm_test.by_hash FOR VALUES WITH (MODULUS 2, REMAINDER 1);
-- end_ignore

-- should return NULL for hash partition
SELECT matrixts_internal.apm_generic_incoming(
    NULL
  , 'apm_test.by_hash'::regclass
  , NULL
  , 'apm_test.by_hash'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_hash'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_hash'::regclass, NULL))
  , '{"before":"2 days"}'::json
);
ROLLBACK;

-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.by_list;
CREATE TABLE apm_test.by_list (type int, region text)
DISTRIBUTED BY (region) PARTITION BY LIST (type)
( PARTITION t1 VALUES (1), PARTITION t234 VALUES (2, 3, 4) ) ;
-- end_ignore

-- should return NULL for list partition
SELECT matrixts_internal.apm_generic_incoming(
    NULL
  , 'apm_test.by_list'::regclass
  , NULL
  , 'apm_test.by_list'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_list'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_list'::regclass, NULL))
  , '{"before":"2 days"}'::json
);
ROLLBACK;

-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.by_int;
CREATE TABLE apm_test.by_int (ts int, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION yr START (2010) END (2014) EVERY (1) ) ;
-- end_ignore

-- should return NULL for int partition
SELECT matrixts_internal.apm_generic_incoming(
    NULL
  , 'apm_test.by_int'::regclass
  , NULL
  , 'apm_test.by_int'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_int'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_int'::regclass, NULL))
  , '{"before":"2 days"}'::json
);
ROLLBACK;

-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.by_bigint;
CREATE TABLE apm_test.by_bigint (ts bigint, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION yr START (2010) END (2014) EVERY (1) ) ;
-- end_ignore

-- should return NULL for bigint partition
SELECT matrixts_internal.apm_generic_incoming(
    NULL
  , 'apm_test.by_bigint'::regclass
  , NULL
  , 'apm_test.by_bigint'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_bigint'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_bigint'::regclass, NULL))
  , '{"before":"2 days"}'::json
);
ROLLBACK;

-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.by_max;
CREATE TABLE apm_test.by_max (ts date, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION yr START (now()::date - '3 month'::interval) END (now()::date) EVERY ('1 month'::interval) ) ;
ALTER TABLE apm_test.by_max ADD PARTITION max_part START (now()::date) INCLUSIVE;
-- end_ignore

-- should return NULL for partition from now() - '3 month' to max
SELECT matrixts_internal.apm_generic_incoming(
    NULL
  , 'apm_test.by_max'::regclass
  , NULL
  , 'apm_test.by_max'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_max'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_max'::regclass, NULL))
  , '{"before":"2 days"}'::json
);
ROLLBACK;

-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.by_time;
CREATE TABLE apm_test.by_time (ts time without time zone, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION hr START ('00:00') END ('04:00') EVERY ('1 hour') ) ;
-- end_ignore

-- should return NULL for time partition
SELECT matrixts_internal.apm_generic_incoming(
    NULL
  , 'apm_test.by_time'::regclass
  , NULL
  , 'apm_test.by_time'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_time'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_time'::regclass, NULL))
  , '{"before":"2 days"}'::json
);
ROLLBACK;

-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.by_timetz;
CREATE TABLE apm_test.by_timetz (ts time with time zone, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION hr START ('00:00+08:00') END ('04:00+08:00') EVERY ('1 hour'::interval) ) ;
-- end_ignore

-- should return NULL for timetz partition
SELECT matrixts_internal.apm_generic_incoming(
    NULL
  , 'apm_test.by_timetz'::regclass
  , NULL
  , 'apm_test.by_timetz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_timetz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_timetz'::regclass, NULL))
  , '{"before":"2 days"}'::json
);
ROLLBACK;

--------------------------------------
---- apm_generic_append_partition ----
--------------------------------------

--*****************************************************************--
--                      no existing partition                      --
--*****************************************************************--

---------------------------------------------------------- date as partition key
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_date;
CREATE TABLE apm_test.by_date (ts date, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts);
-- end_ignore

-- should report root table only
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-06-01 11:22:33.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"3 days"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"period":"1 week"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test.by_date'::regclass
      , NULL
      , 'apm_test.by_date'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
      , '{"before":"3 days"}'::json
    ))
);

-- should have 2 new partitions covering ['2021-5-30 11:22:33.54321' to +3days] with each period 1week
---- 2021-05-24 ~ 2021-05-31 (Sunday ~ Saturday whole week)
---- 2021-05-31 ~ 2021-06-07 (Sunday ~ Saturday whole week)
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

\d+ apm_test.by_date

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"3 days"}'::json
);

ROLLBACK;

---------------------------------------------------------- date as partition key
----------------------------------------------- now is just a time_bucket border
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_date;
CREATE TABLE apm_test.by_date (ts date, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts);
-- end_ignore

-- should report root table only
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-06-03 00:00:00)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-05-31 00:00:00'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"3 days"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-05-31 00:00:00'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"period":"1 week"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-05-31 00:00:00'::timestamptz
      , 'apm_test.by_date'::regclass
      , NULL
      , 'apm_test.by_date'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
      , '{"before":"3 days"}'::json
    ))
);

-- should have 1 new partition covering ['2021-05-31 00:00:00' to +3days] with each period 1week
---- 2021-05-31 ~ 2021-06-07 (Sunday ~ Saturday whole week)
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-05-31 00:00:00'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"3 days"}'::json
);

ROLLBACK;

---------------------------------------------------------- date as partition key
-------------------------------------- extreme long table name with special char
---------------------------------- "period" is significantly small than "before"
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test."'""'|!@#$%^&为*()_+-斯=[]{}卡\;':蒂"",.<>/ 	?献上心脏！P两面包";
CREATE TABLE apm_test."'""'|!@#$%^&为*()_+-斯=[]{}卡\;':蒂"",.<>/ 	?献上心脏！P两面包" (ts date, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts);
SELECT octet_length(relname), length(relname), relname
FROM pg_class WHERE oid = '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass;
-- end_ignore

-- should report root table only
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-06-06 11:22:33.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , NULL
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL))
  , '{"before":"7 days"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , NULL
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL))
  , '{"period":"2 days"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
      , NULL
      , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL))
      , '{"before":"7 days"}'::json
    ))
);

-- should have 4 new partitions covering ['2021-5-30 11:22:33.54321' to +7days] with each period 2 day
---- 2021-05-30 ~ 2021-06-01
---- 2021-06-01 ~ 2021-06-03
---- 2021-06-03 ~ 2021-06-05
---- 2021-06-05 ~ 2021-06-07
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , NULL
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL))
  , '{"before":"7 days"}'::json
);

ROLLBACK;

----------------------------------------------------- timestamp as partition key
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_ts;
CREATE TABLE apm_test.by_ts (ts timestamp, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts);
-- end_ignore

-- should report root table only
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-07-15 11:22:36.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"before":"1 month 15 day 3 sec"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"period":"3 month"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test.by_ts'::regclass
      , NULL
      , 'apm_test.by_ts'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
      , '{"before":"1 month 15 day 3 sec"}'::json
    ))
);

-- should have 2 new partitions covering ['2021-5-30 11:22:33.54321' to +1month 15day 3sec] with each period 3 month
---- 2021-04-01 ~ 2021-07-01
---- 2021-07-01 ~ 2021-10-01
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"before":"1 month 15 day 3 sec"}'::json
);

ROLLBACK;

--------------------------------------------------- timestamptz as partition key
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tz;
CREATE TABLE apm_test.by_tz (ts timestamptz, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts);
-- end_ignore

-- should report root table only
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-05-30 11:42:33.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_tz'::regclass
  , NULL
  , 'apm_test.by_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
  , '{"before":"20 min"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_tz'::regclass
  , NULL
  , 'apm_test.by_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
  , '{"period":"1 hour"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test.by_tz'::regclass
      , NULL
      , 'apm_test.by_tz'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
      , '{"before":"20 min"}'::json
    ))
);

-- should have 1 new partitions covering ['2021-5-30 11:22:33.54321' to + 20min] with each period 8h
---- 2021-05-30 11:00:00 ~ 2021-05-30 12:00:00
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_tz'::regclass
  , NULL
  , 'apm_test.by_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
  , '{"before":"20 min"}'::json
);

ROLLBACK;


--*****************************************************************--
--                      has default partition                      --
--*****************************************************************--

---------------------------------------------------------- date as partition key
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_date;
CREATE TABLE apm_test.by_date (ts date, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
(DEFAULT PARTITION hot);
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-06-02 11:22:33.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"3 days"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"period":"1 week"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test.by_date'::regclass
      , NULL
      , 'apm_test.by_date'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
      , '{"before":"3 days"}'::json
    ))
);

-- should have 2 new partitions covering ['2021-5-30 11:22:33.54321' to +3days] with each period 1week
---- 2021-05-24 ~ 2021-05-31 (Sunday ~ Saturday whole week)
---- 2021-05-31 ~ 2021-06-07 (Sunday ~ Saturday whole week)
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"3 days"}'::json
);

ROLLBACK;

---------------------------------------------------------- date as partition key
-------------------------------------- now + period is just a time_bucket border
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_date;
CREATE TABLE apm_test.by_date (ts date, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
(DEFAULT PARTITION hot);
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-05-31 00:00:00)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-05-24 00:00:00'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"1 week"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-05-24 00:00:00'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"period":"1 week"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-05-24 00:00:00'::timestamptz
      , 'apm_test.by_date'::regclass
      , NULL
      , 'apm_test.by_date'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
      , '{"before":"1 week"}'::json
    ))
);

-- should have 2 new partition covering ['2021-05-24 00:00:00' to +1week] with each period 1 week
---- 2021-05-24 ~ 2021-05-31 (Sunday ~ Saturday whole week)
---- 2021-05-31 ~ 2021-06-07 (Sunday ~ Saturday whole week)
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-05-24 00:00:00'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"1 week"}'::json
);

ROLLBACK;

---------------------------------------------------------- date as partition key
-------------------------------------- extreme long table name with special char
---------------------------------- "period" is significantly small than "before"
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test."'""'|!@#$%^&为*()_+-斯=[]{}卡\;':蒂"",.<>/ 	?献上心脏！P两面包";
CREATE TABLE apm_test."'""'|!@#$%^&为*()_+-斯=[]{}卡\;':蒂"",.<>/ 	?献上心脏！P两面包" (ts date, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
(DEFAULT PARTITION hot);
SELECT octet_length(relname), length(relname), relname
FROM pg_class WHERE oid = '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass;
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-06-02 11:22:33.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , NULL
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL))
  , '{"before":"3 days"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , NULL
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL))
  , '{"period":"8h"}'::json
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

-- should have 4 new partitions covering ['2021-5-30 11:22:33.54321' to +7days] with each period 2 day
---- 2021-05-30 ~ 2021-05-31
---- 2021-05-31 ~ 2021-06-01
---- 2021-06-01 ~ 2021-06-02
---- 2021-06-02 ~ 2021-06-03
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , NULL
  , '"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('"apm_test"."''""''|!@#$%^&为*()_+-斯=[]{}卡\;'':蒂"",.<>/ 	?献上心脏！P两面包"'::regclass, NULL))
  , '{"before":"3 days"}'::json
);

ROLLBACK;

----------------------------------------------------- timestamp as partition key
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_ts;
CREATE TABLE apm_test.by_ts (ts timestamp, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
(DEFAULT PARTITION hot);
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-07-15 11:22:36.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"before":"1 month 15 day 3 sec"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"period":"3 month"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test.by_ts'::regclass
      , NULL
      , 'apm_test.by_ts'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
      , '{"before":"1 month 15 day 3 sec"}'::json
    ))
);

-- should have 2 new partitions covering ['2021-5-30 11:22:33.54321' to +1month 15day 3sec] with each period 3 month
---- 2021-04-01 ~ 2021-07-01
---- 2021-07-01 ~ 2021-10-01
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"before":"1 month 15 day 3 sec"}'::json
);

ROLLBACK;

--------------------------------------------------- timestamptz as partition key
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tz;
CREATE TABLE apm_test.by_tz (ts timestamptz, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
(DEFAULT PARTITION hot);
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(-inf, inf, 2021-05-30 11:42:33.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_tz'::regclass
  , NULL
  , 'apm_test.by_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
  , '{"before":"20 min"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_tz'::regclass
  , NULL
  , 'apm_test.by_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
  , '{"period":"1 hour"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test.by_tz'::regclass
      , NULL
      , 'apm_test.by_tz'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
      , '{"before":"20 min"}'::json
    ))
);

-- should have 1 new partitions covering ['2021-5-30 11:22:33.54321' to + 20min] with each period 8h
---- 2021-05-30 11:00:00 ~ 2021-05-30 12:00:00
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_tz'::regclass
  , NULL
  , 'apm_test.by_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
  , '{"before":"20 min"}'::json
);

ROLLBACK;

--*****************************************************************--
--                  existing range partition                       --
--*****************************************************************--


----------------------------------------------------- timestamp as partition key
---------------------------------- "period" is significantly small than "before"
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.incoming_by_ts;
CREATE TABLE apm_test.incoming_by_ts (ts timestamp, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION yr START (timestamp '2016-01-01') END (timestamp '2020-01-01') EVERY ('1 year') ) ;
-- end_ignore

-- should have 5 partitions
SELECT count(1)
FROM matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL);

-- should return {(2020-01-01 00:00:00, inf, 2021-6-1 11:22:33)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33'::timestamptz
  , 'apm_test.incoming_by_ts'::regclass
  , NULL
  , 'apm_test.incoming_by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL))
  , '{"before":"2 days"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33'::timestamptz
  , 'apm_test.incoming_by_ts'::regclass
  , NULL
  , 'apm_test.incoming_by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL))
  , '{"period":"8 h"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33'::timestamptz
      , 'apm_test.incoming_by_ts'::regclass
      , NULL
      , 'apm_test.incoming_by_ts'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL))
      , '{"before":"2 days"}'::json
    ))
);

-- should have 7 new partitions covering ['2021-5-30 11:22:33' to +2days] with each period 8h
---- 2021-05-30 08:00 ~ 2021-05-30 16:00
---- 2021-05-30 16:00 ~ 2021-05-31 00:00
---- 2021-05-31 00:00 ~ 2021-05-31 08:00
---- 2021-05-31 08:00 ~ 2021-05-31 16:00
---- 2021-05-31 16:00 ~ 2021-06-01 00:00
---- 2021-06-01 00:00 ~ 2021-06-01 08:00
---- 2021-06-01 08:00 ~ 2021-06-01 16:00
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33'::timestamptz
  , 'apm_test.incoming_by_ts'::regclass
  , NULL
  , 'apm_test.incoming_by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.incoming_by_ts'::regclass, NULL))
  , '{"before":"2 days"}'::json
);

ROLLBACK;

----------------------------------------------------- timestamp as partition key
--------------------------------------- existing partition squeeze new partition
--------------------------------- "period" is significantly larger than "before"
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_ts;
CREATE TABLE apm_test.by_ts (ts timestamp, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
(DEFAULT PARTITION hot);
ALTER TABLE apm_test.by_ts ADD PARTITION END('2021-4-17 01:02:03.658633');
ALTER TABLE apm_test.by_ts ADD PARTITION START('2021-06-28 23:59:59.999999');
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(2021-04-17 01:02:03.658633, 2021-06-28 23:59:59.999999, 2021-06-30 11:22:36.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"before":"1 month 3 sec"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"period":"3 month"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test.by_ts'::regclass
      , NULL
      , 'apm_test.by_ts'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
      , '{"before":"1 month 3 sec"}'::json
    ))
);

-- should have 2 new partitions covering ['2021-5-30 11:22:33.54321' to +1month 3sec] with each period 3 month squeezed by existing parition
---- 2021-04-17 01:02:03.658633 ~ 2021-06-28 23:59:59.999999
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"before":"1 month 3 sec"}'::json
);

ROLLBACK;

--------------------------------------------------- timestamptz as partition key
--------------------------------------- existing partition squeeze new partition
---------------------------- existing partition lays inside in the wanted period
--------------------------------- "period" is significantly larger than "before"
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tz;
CREATE TABLE apm_test.by_tz (ts timestamp, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
(DEFAULT PARTITION hot);
ALTER TABLE apm_test.by_tz ADD PARTITION END('2021-4-17 01:02:03.658633');
ALTER TABLE apm_test.by_tz ADD PARTITION START('2021-07-01 23:59:59.999999');
ALTER TABLE apm_test.by_tz ADD PARTITION START('2021-04-18 15:29:52.000001') END('2021-4-22 03:04:05.778899');
ALTER TABLE apm_test.by_tz ADD PARTITION START('2021-06-18 03:01:13.000002') END('2021-6-22 05:06:07.112233');
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return two gap zone
--  { (2021-04-22 03:04:05.778899, 2021-06-18 03:01:13.000002, 2021-06-30 11:22:36.54321)
--    (2021-06-22 05:06:07.112233, 2021-07-01 23:59:59.999999, 2021-06-30 11:22:36.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_tz'::regclass
  , NULL
  , 'apm_test.by_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
  , '{"before":"1 month 3 sec"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_tz'::regclass
  , NULL
  , 'apm_test.by_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
  , '{"period":"3 month"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test.by_tz'::regclass
      , NULL
      , 'apm_test.by_tz'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
      , '{"before":"1 month 3 sec"}'::json
    ))
);

-- should have 2 new partitions covering ['2021-5-30 11:22:33.54321' to +1month 3sec] to fill gaps until 2021-07-01 00:00:00
---- 2021-04-22 03:04:05.778899 ~ 2021-06-18 03:01:13.000002
---- 2021-06-22 05:06:07.112233 ~ 2021-07-01 00:00:00
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_tz'::regclass
  , NULL
  , 'apm_test.by_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tz'::regclass, NULL))
  , '{"before":"1 month 3 sec"}'::json
);

ROLLBACK;

---------------------------------------------------------- date as partition key
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_date;
CREATE TABLE apm_test.by_date (ts date, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION yr START ('2001-07-29') END ('2001-08-03') EVERY ('2 days'::interval) ) ;

-- should return {(2001-08-03 00:00:00, inf, 2001-08-05 00:00:00)}
SELECT * FROM matrixts_internal.apm_generic_incoming(
    '2001-08-03'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"2 days"}'::json
) AS res;

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2001-08-03'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"period":"1 day"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2001-08-03'::timestamptz
      , 'apm_test.by_date'::regclass
      , NULL
      , 'apm_test.by_date'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
      , '{"before":"2 days"}'::json
    ))
);

-- should have 3 new partitions covering ['2001-08-03' to +2days] with each period 1 day
---- 2001-08-03 ~ 2001-08-04
---- 2001-08-04 ~ 2001-08-05
---- 2001-08-05 ~ 2001-08-06
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT * FROM matrixts_internal.apm_generic_incoming(
    '2001-08-03'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"2 days"}'::json
) AS res;

ROLLBACK;

---------------------------------------------------------- date as partition key
--------------------------------------now is just a partition time_bucket border
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_date;
CREATE TABLE apm_test.by_date(ts date, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
( PARTITION yr START ('2019-01-01'::date) END ('2021-01-01'::date) EVERY ('1 year'::interval) ) ;
ALTER TABLE apm_test.by_date ADD PARTITION START ('2022-12-31'::date) END ('2023-01-01'::date);
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return {(2021-01-01, 2022-12-31, 2022-01-01)}
SELECT * FROM matrixts_internal.apm_generic_incoming(
    '2021-01-01 00:00:00'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"1 year"}'::json
) AS res;

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-01-01 00:00:00'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"period":"1 year"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-01-01 00:00:00'::timestamptz
      , 'apm_test.by_date'::regclass
      , NULL
      , 'apm_test.by_date'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
      , '{"before":"1 year"}'::json
    ))
);

-- should have 2 new partitions
---- 2021-01-01 ~ 2022-01-01
---- 2022-01-01 ~ 2022-12-31
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT * FROM matrixts_internal.apm_generic_incoming(
    '2021-01-01 00:00:00'::timestamptz
  , 'apm_test.by_date'::regclass
  , NULL
  , 'apm_test.by_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_date'::regclass, NULL))
  , '{"before":"1 year"}'::json
) AS res;

ROLLBACK;


----------------------------------------------------- timestamp as partition key
--------------------------------------- existing partition squeeze new partition
---------------------------- existing partition lays inside in the wanted period
-------------------------------- "period" is significantly smaller than "before"
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_ts;
CREATE TABLE apm_test.by_ts (ts timestamp, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts)
(DEFAULT PARTITION hot);
ALTER TABLE apm_test.by_ts ADD PARTITION END('2021-4-17 01:02:03.658633');
ALTER TABLE apm_test.by_ts ADD PARTITION START('2021-07-01 23:59:59.999999');
ALTER TABLE apm_test.by_ts ADD PARTITION START('2021-04-18 15:29:52.000001') END('2021-4-22 03:04:05.778899');
ALTER TABLE apm_test.by_ts ADD PARTITION START('2021-05-30 10:12:13.000004') END('2021-6-10 10:51:21.223344');
ALTER TABLE apm_test.by_ts ADD PARTITION START('2021-06-18 03:01:13.000002') END('2021-6-22 05:06:07.112233');
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return two gap zone
--  { (2021-06-10 10:51:21.223344, 2021-06-18 03:01:13.000002, 2021-06-30 11:22:36.54321)
--    (2021-06-22 05:06:07.112233, 2021-07-01 23:59:59.999999, 2021-06-30 11:22:36.54321)}
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"before":"1 month 3 sec"}'::json
);

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"period":"3 days"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2021-5-30 11:22:33.54321'::timestamptz
      , 'apm_test.by_ts'::regclass
      , NULL
      , 'apm_test.by_ts'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
      , '{"before":"1 month 3 sec"}'::json
    ))
);

-- should have 8 new partitions covering ['2021-5-30 11:22:33.54321' to +1month 3sec] to fill gaps
---- 2021-06-10 10:51:21.223344 ~ 2021-06-11 00:00:00
---- 2021-06-11 00:00:00 ~ 2021-06-14 00:00:00
---- 2021-06-14 00:00:00 ~ 2021-06-17 00:00:00
---- 2021-06-17 00:00:00 ~ 2021-06-18 03:01:13.000002
---- 2021-06-22 05:06:07.112233 ~ 2021-06-23 00:00:00
---- 2021-06-23 00:00:00 ~ 2021-06-26 00:00:00
---- 2021-06-26 00:00:00 ~ 2021-06-29 00:00:00
---- 2021-06-29 00:00:00 ~ 2021-07-01 23:59:59.999999
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT matrixts_internal.apm_generic_incoming(
    '2021-5-30 11:22:33.54321'::timestamptz
  , 'apm_test.by_ts'::regclass
  , NULL
  , 'apm_test.by_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_ts'::regclass, NULL))
  , '{"before":"1 month 3 sec"}'::json
);

ROLLBACK;

--------------------------------------------------- timestamptz as partition key
--------------------------------- multiple existing partition inside wanted zone
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tstz_0;
DROP TABLE IF EXISTS apm_test.by_tstz_1;
DROP TABLE IF EXISTS apm_test.by_tstz;
CREATE TABLE apm_test.by_tstz(ts timestamp with time zone, region text)
DISTRIBUTED BY (region) PARTITION BY RANGE (ts);
CREATE TABLE by_tstz_0 PARTITION OF apm_test.by_tstz FOR VALUES FROM ('2020-03-02 01:33:41') TO ('2020-03-03 01:33:41');
CREATE TABLE by_tstz_1 PARTITION OF apm_test.by_tstz FOR VALUES FROM ('2020-03-05 01:33:41') TO ('2020-03-08 01:33:41');
-- end_ignore

-- should report existing partitions
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return three gap zone
--  { (-inf, 2020-03-02 01:33:41, 2020-03-13 00:00:00)
--    (2020-03-03 01:33:41, 2020-03-05 01:33:41, 2020-03-13 00:00:00)
--    (2020-03-08 01:33:41, inf, 2020-03-13 00:00:00)}
SELECT *
FROM matrixts_internal.apm_generic_incoming(
    '2020-03-01'::timestamptz
  , 'apm_test.by_tstz'::regclass
  , NULL
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{"before":"12 days"}'::json
) AS res;

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2020-03-01'::timestamptz
  , 'apm_test.by_tstz'::regclass
  , NULL
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{"period":"1 day"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2020-03-01'::timestamptz
      , 'apm_test.by_tstz'::regclass
      , NULL
      , 'apm_test.by_tstz'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
      , '{"before":"12 days"}'::json
    ))
);

-- should have 11 new partitions covering ['2020-03-01' to +12days] to fill gaps
---- 2020-03-01 00:00:00 ~ 2020-03-02 00:00:00
---- 2020-03-02 00:00:00 ~ 2020-03-02 01:33:41
---- 2020-03-03 01:33:41 ~ 2020-03-04 00:00:00
---- 2020-03-04 00:00:00 ~ 2020-03-05 00:00:00
---- 2020-03-05 00:00:00 ~ 2020-03-05 01:33:41
---- 2020-03-08 01:33:41 ~ 2020-03-09 00:00:00
---- 2020-03-09 00:00:00 ~ 2020-03-10 00:00:00
---- 2020-03-10 00:00:00 ~ 2020-03-11 00:00:00
---- 2020-03-11 00:00:00 ~ 2020-03-12 00:00:00
---- 2020-03-12 00:00:00 ~ 2020-03-13 00:00:00
---- 2020-03-13 00:00:00 ~ 2020-03-14 00:00:00
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) ORDER BY partitionlevel, reloid;

-- should return NULL if check again
SELECT *
FROM matrixts_internal.apm_generic_incoming(
    '2020-03-01'::timestamptz
  , 'apm_test.by_tstz'::regclass
  , NULL
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{"before":"12 days"}'::json
) AS res;

ROLLBACK;

----------------------------------
---- apm_generic_incoming_set ----
----------------------------------
SHOW matrix.ts_guc_apm_allow_dml;
-- start_ignore
DROP TABLE IF EXISTS apm_test.incoming_set;
CREATE TABLE apm_test.incoming_set (id int, year int, qtr int, day int, region text)
DISTRIBUTED BY (id) PARTITION BY RANGE (year)
(PARTITION yr START (2010) END (2012) EVERY (1));
TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy_action;
-- end_ignore

-- should report error on invalid input
SELECT matrixts_internal.apm_generic_incoming_set(NULL, NULL, NULL, NULL);
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, NULL, NULL);
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'fake_action', 'abc');
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'fake_action', '{ "before": "bcd" }');
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'fake_action', '{ "period": "def" }');
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'fake_action', '{ "before": "efg", "period": "xyz" }');
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, NULL, 'test_incoming', '{"before": "3 days", "period": "8 hour"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"before": "3 days", "period": "8 hour"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should have reset GUC to false
SHOW matrix.ts_guc_apm_allow_dml;
-- should append 2 add records to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should update act_args only
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"period": "1 hour"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 modify record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should update check_args only
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"before": "10 days"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 modify record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should update both
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"before": "5 days", "period": "10 hour"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 2 modify records to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should delete the entry
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', NULL);
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 drop record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry with only check_args
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"before": "10 days"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 add record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry with only act_args
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"period": "1 hour"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 add record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry with only act_args
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"storage_type": "heap"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 add record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry with only act_args
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"period": "2h", "storage_type": "sortheap"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 add record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry with only act_args
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"period": "4h", "before": "12 days", "storage_type": "ao_row"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 2 add record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should report error due to invalid storage type
SELECT matrixts_internal.apm_generic_incoming_set('apm_test.incoming_set'::regclass, 1, 'test_incoming', '{"period": "4h", "before": "12 days", "storage_type": "blablabla"}');

--------------------------------
-- test for customize am
--------------------------------
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tstz CASCADE;
CREATE TABLE apm_test.by_tstz(ts timestamp with time zone, region text)
USING sortheap DISTRIBUTED BY (region)
PARTITION BY RANGE (ts) (DEFAULT PARTITION hot);
CREATE INDEX idx_sheap ON apm_test.by_tstz USING sortheap_btree(region, time_bucket('1 hour', ts));
-- end_ignore

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2020-03-01'::timestamptz
  , 'apm_test.by_tstz'::regclass
  , NULL
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{"period":"1 day"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2020-03-01'::timestamptz
      , 'apm_test.by_tstz'::regclass
      , NULL
      , 'apm_test.by_tstz'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
      , '{"before":"3 days"}'::json
    ))
);

-- should be sortheap for new partition
SELECT amname FROM pg_class pc INNER JOIN pg_am pa ON pc.relam = pa.oid
WHERE pc.relname LIKE 'by_tstz_%' AND relkind = 'r' AND relispartition;

ROLLBACK;

-- default partition am != expected storage type
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tstz CASCADE;
CREATE TABLE apm_test.by_tstz(ts timestamp with time zone, region text)
PARTITION BY RANGE (ts) (DEFAULT PARTITION hot);
-- end_ignore

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2020-03-01'::timestamptz
  , 'apm_test.by_tstz'::regclass
  , NULL
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{"period":"1 day", "storage_type":"sortheap"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2020-03-01'::timestamptz
      , 'apm_test.by_tstz'::regclass
      , NULL
      , 'apm_test.by_tstz'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
      , '{"before":"3 days"}'::json
    ))
);

-- should be heap with WARNING for new partition
SELECT amname FROM pg_class pc INNER JOIN pg_am pa ON pc.relam = pa.oid
WHERE pc.relname LIKE 'by_tstz_%' AND relkind = 'r' AND relispartition;

ROLLBACK;

-- default partition am != expected storage type
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tstz CASCADE;
CREATE TABLE apm_test.by_tstz(ts timestamp with time zone, region text)
PARTITION BY RANGE (ts);
-- end_ignore

-- invoke append partition
SELECT matrixts_internal.apm_generic_append_partition(
    '2020-03-01'::timestamptz
  , 'apm_test.by_tstz'::regclass
  , NULL
  , 'apm_test.by_tstz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
  , '{"period":"1 day", "storage_type":"ao_row"}'::json
  , (SELECT matrixts_internal.apm_generic_incoming(
        '2020-03-01'::timestamptz
      , 'apm_test.by_tstz'::regclass
      , NULL
      , 'apm_test.by_tstz'::regclass
      , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL) lp WHERE reloid = root_reloid))[1]
      , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.by_tstz'::regclass, NULL))
      , '{"before":"3 days"}'::json
    ))
);

-- should be ao_row for new partitions
SELECT amname FROM pg_class pc INNER JOIN pg_am pa ON pc.relam = pa.oid
WHERE pc.relname LIKE 'by_tstz_%' AND relkind = 'r' AND relispartition;

ROLLBACK;

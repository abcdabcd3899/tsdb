---------------------------------------------
-- Internal utility functions
---------------------------------------------

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
-- apm_eval_partbound
-- apm_partition_boundary
---------------------------------------------

-- should return error if oid is NULL
SELECT matrixts_internal.apm_eval_partbound(NULL, '1 year');

-- should return error if interval is NULL
SELECT matrixts_internal.apm_eval_partbound('pg_class'::regclass::oid, NULL);

-- should return error when oid does not point to a valid relation
SELECT matrixts_internal.apm_eval_partbound(9999999::oid, '1 year');

-- should read from pg_class table when syscache is out of date
BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.this_is_a_new_table;
CREATE TABLE apm_test.this_is_a_new_table (id int UNIQUE);
-- end_ignore

-- should report missing relpartbound
SELECT matrixts_internal.apm_eval_partbound((SELECT max(oid) FROM pg_class WHERE relname = 'this_is_a_new_table')::oid, '1 year');
ROLLBACK;

BEGIN;
-- start_ignore
DROP TABLE IF EXISTS apm_test.this_is_a_new_table;
CREATE TABLE apm_test.this_is_a_new_table (id int UNIQUE);
-- end_ignore

-- should report missing relpartbound
SELECT matrixts_internal.apm_partition_boundary('apm_test.this_is_a_new_table'::regclass::oid);
ROLLBACK;

-- start_ignore
CREATE TABLE apm_test.has_default (ts timestamp, region text)
   DISTRIBUTED BY (region)
   PARTITION BY RANGE (ts)
  ( DEFAULT PARTITION other ) ;
-- end_ignore

-- should return NULL for default partition
SELECT matrixts_internal.apm_partition_boundary('apm_test.has_default_1_prt_other'::regclass::oid);

-- should return 0 because default partition cannot compare to timestamp
SELECT matrixts_internal.apm_eval_partbound('apm_test.has_default_1_prt_other'::regclass::oid, '1 year');

-- start_ignore
CREATE TABLE apm_test.by_list (type int, region text)
  DISTRIBUTED BY (region)
  PARTITION BY LIST (type)
  ( PARTITION t1 VALUES (1),
    PARTITION t234 VALUES (2, 3, 4) ) ;
-- end_ignore

-- should return NULL for list partition
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_list_1_prt_t234'::regclass::oid);

-- should return 0 because list partition does not have a partition boundary to compare with timestamp
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_list_1_prt_t234'::regclass::oid, '1 year');

-- start_ignore
CREATE TABLE apm_test.by_hash (a int, b int) PARTITION BY HASH (b);
CREATE TABLE apm_test.by_hash_0 PARTITION OF apm_test.by_hash FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE apm_test.by_hash_1 PARTITION OF apm_test.by_hash FOR VALUES WITH (MODULUS 2, REMAINDER 1);
-- end_ignore

-- should return NULL for hash partition
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_hash_1'::regclass::oid);

-- should return 0 because hash partition does not have a partition boundary to compare with timestamp
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_hash_1'::regclass::oid, '1 year');

-- start_ignore
CREATE TABLE apm_test.by_int (ts int, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (2010) END (2014) EVERY (1) ) ;
-- end_ignore

-- should return NULL for int partition
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_int_1_prt_yr_3'::regclass::oid);

-- should return 0 because int partition key cannot compare to timestamp
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_int_1_prt_yr_3'::regclass::oid, '1 year');

-- start_ignore
CREATE TABLE apm_test.by_smallint (ts smallint, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (2010) END (2014) EVERY (1) ) ;
-- end_ignore

-- should return NULL for smallint partition
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_smallint_1_prt_yr_3'::regclass::oid);

-- should return 0 because smallint partition key cannot compare to timestamp
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_smallint_1_prt_yr_3'::regclass::oid, '1 year');

-- start_ignore
CREATE TABLE apm_test.by_bigint (ts bigint, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (2010) END (2014) EVERY (1) ) ;
-- end_ignore

-- should return NULL for bigint partition
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_bigint_1_prt_yr_3'::regclass::oid);

-- should return 0 because smallint partition key cannot compare to timestamp
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_bigint_1_prt_yr_3'::regclass::oid, '1 year');

-- start_ignore
CREATE TABLE apm_test.by_time (ts time without time zone, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION hr START ('00:00') END ('04:00') EVERY ('1 hour') ) ;
-- end_ignore

-- should return NULL for time partition
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_time_1_prt_hr_2'::regclass::oid);

-- should return 0 because time partition key cannot compare to timestamp
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_time_1_prt_hr_2'::regclass::oid, '1 hour');

-- start_ignore
CREATE TABLE apm_test.by_timetz (ts time with time zone, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION hr START ('00:00+08:00') END ('04:00+08:00') EVERY ('1 hour'::interval) ) ;
-- end_ignore

-- should return NULL for timetz partition
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_timetz_1_prt_hr_2'::regclass::oid);

-- should return 0 because timetz partition key cannot compare to timestamp
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_timetz_1_prt_hr_2'::regclass::oid, '1 hour');

-- start_ignore
CREATE TABLE apm_test.by_interval (ts interval, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION iv START ('1 min') END ('20 min') EVERY ('10 min') ) ;
-- end_ignore

-- should return NULL for interval partition
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_interval_1_prt_iv_1'::regclass::oid);
SELECT * FROM matrixts_internal.apm_partition_boundary('apm_test.by_interval_1_prt_iv_1'::regclass::oid);

-- should return 0 because interval partition key cannot compare to timestamp
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_interval_1_prt_iv_1'::regclass::oid, '1 hour');

-- Partition by timestamp without time zone
BEGIN;
-- timezone 00:00
SET timezone TO UTC;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_ts;
CREATE TABLE apm_test.by_ts (ts timestamp, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (now() - '10 year'::interval) END (now() - '5 year'::interval) EVERY ('1 year') ) ;

DROP TABLE IF EXISTS apm_test.by_ts2;
CREATE TABLE apm_test.by_ts2 (ts timestamp, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START ('2010-01-01') END ('2015-01-01') EVERY ('1 year') ) ;
-- end_ignore

-- should return 1 because now() - 1 year is after this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '1 year');
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '1 year', now());
-- should return 2 because now() - 8 year is inside partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '8 year');
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '8 year', now());
-- should return 3 because now() - 9 year is before this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '9 year');
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '9 year', now());

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_ts2_1_prt_yr_3'::regclass::oid);
SELECT * FROM matrixts_internal.apm_partition_boundary('apm_test.by_ts2_1_prt_yr_3'::regclass::oid);
ROLLBACK;

BEGIN;
-- timezone +08:00
SET timezone TO 'Asia/Shanghai';
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_ts;
CREATE TABLE apm_test.by_ts (ts timestamp, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (now() - '10 year'::interval) END (now() - '5 year'::interval) EVERY ('1 year') ) ;

DROP TABLE IF EXISTS apm_test.by_ts2;
CREATE TABLE apm_test.by_ts2 (ts timestamp, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START ('2010-01-01') END ('2015-01-01') EVERY ('1 year') ) ;
-- end_ignore

-- should return 1 because now() - 1 year is after this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '1 year');
-- should return 1 because now() - 7 year is inside this partition boundary due to timezone diff
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '7 year');
-- should return 2 because now() - 8 year is before this partition boundary due to timezone diff
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '8 year');
-- should return 3 because now() - 9 year is before this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '9 year');

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_ts2_1_prt_yr_3'::regclass::oid);
SELECT * FROM matrixts_internal.apm_partition_boundary('apm_test.by_ts2_1_prt_yr_3'::regclass::oid);
ROLLBACK;

BEGIN;
-- timezone -05:00
SET timezone TO 'America/Detroit';
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_ts;
CREATE TABLE apm_test.by_ts (ts timestamp, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (now() - '10 year'::interval) END (now() - '5 year'::interval) EVERY ('1 year') ) ;

DROP TABLE IF EXISTS apm_test.by_ts2;
CREATE TABLE apm_test.by_ts2 (ts timestamp, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START ('2010-01-01') END ('2015-01-01') EVERY ('1 year') ) ;
-- end_ignore

-- should return 1 because now() - 1 year is after this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '1 year');
-- should return 1 because now() - 7 year is after this partition boundary due to timezone diff
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '7 year');
-- should return 2 because now() - 8 year is inside this partition boundary due to timezone diff
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '8 year');
-- should return 3 because now() - 9 year is before this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_ts_1_prt_yr_3'::regclass::oid, '9 year');

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_ts2_1_prt_yr_3'::regclass::oid);
SELECT * FROM matrixts_internal.apm_partition_boundary('apm_test.by_ts2_1_prt_yr_3'::regclass::oid);
ROLLBACK;

-- Partition by timestamp with time zone
BEGIN;
-- timezone 00:00
SET timezone TO UTC;
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tstz;
CREATE TABLE apm_test.by_tstz (ts timestamp with time zone, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (now() - '10 year'::interval) END (now() - '5 year'::interval) EVERY ('1 year') ) ;

DROP TABLE IF EXISTS apm_test.by_tstz2;
CREATE TABLE apm_test.by_tstz2 (ts timestamp with time zone, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START ('2010-03-02 01:02:03 ') END ('2015-01-01') EVERY ('1 year') ) ;
-- end_ignore

-- should return 1 because now() - 1 year is after this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '1 year');
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '1 year', now());
-- should return 2 because now() - 8 year is inside partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '8 year');
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '8 year', now());
-- should return 3 because now() - 9 year is before this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '9 year');
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '9 year', now());

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_tstz2_1_prt_yr_3'::regclass::oid);
ROLLBACK;

BEGIN;
-- timezone +08:00
SET timezone TO 'Asia/Shanghai';
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tstz;
CREATE TABLE apm_test.by_tstz (ts timestamp with time zone, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (now() - '10 year'::interval) END (now() - '5 year'::interval) EVERY ('1 year') ) ;

DROP TABLE IF EXISTS apm_test.by_tstz2;
CREATE TABLE apm_test.by_tstz2 (ts timestamp with time zone, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START ('2010-03-02 01:02:03 ') END ('2015-01-01') EVERY ('1 year') ) ;
-- end_ignore

-- should return 1 because now() - 1 year is after this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '1 year');
-- should return 1 because now() - 7 year is after this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '7 year');
-- should return 2 because now() - 8 year is inside this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '8 year');
-- should return 3 because now() - 9 year is before this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '9 year');

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_tstz2_1_prt_yr_3'::regclass::oid);
ROLLBACK;

BEGIN;
-- timezone -05:00
SET timezone TO 'America/Detroit';
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_tstz;
CREATE TABLE apm_test.by_tstz (ts timestamp with time zone, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (now() - '10 year'::interval) END (now() - '5 year'::interval) EVERY ('1 year') ) ;

DROP TABLE IF EXISTS apm_test.by_tstz2;
CREATE TABLE apm_test.by_tstz2 (ts timestamp with time zone, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START ('2010-03-02 01:02:03 ') END ('2015-01-01') EVERY ('1 year') ) ;
-- end_ignore

-- should return 1 because now() - 1 year is after this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '1 year');
-- should return 1 because now() - 7 year is after this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '7 year');
-- should return 2 because now() - 8 year is inside this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '8 year');
-- should return 3 because now() - 9 year is before this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_tstz_1_prt_yr_3'::regclass::oid, '9 year');

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_tstz2_1_prt_yr_3'::regclass::oid);
ROLLBACK;

-- Partition by date
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_date;
CREATE TABLE apm_test.by_date (ts date, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION dt START (now()::date - '5 days'::interval) END (now()::date) EVERY ('2 days'::interval) ) ;

DROP TABLE IF EXISTS apm_test.by_date2;
CREATE TABLE apm_test.by_date2 (ts date, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION dt START ('2020-03-02') END ('2020-03-07') EVERY ('2 days'::interval) ) ;
-- end_ignore

-- should return 1 because now() - 1 min is after this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_date_1_prt_dt_2'::regclass::oid, '1 min', now());
-- should return 2 because now() - 2 days is inside this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_date_1_prt_dt_2'::regclass::oid, '2 days');
-- should return 3 because now() - 1 month is before this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_date_1_prt_dt_2'::regclass::oid, '1 month');

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_date2_1_prt_dt_2'::regclass::oid);

-- MAXVALUE
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_max;
CREATE TABLE apm_test.by_max (ts date, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START (now()::date - '3 month'::interval) END (now()::date) EVERY ('1 month'::interval) ) ;
ALTER TABLE apm_test.by_max ADD PARTITION max_part START (now()::date) INCLUSIVE;

DROP TABLE IF EXISTS apm_test.by_max2;
CREATE TABLE apm_test.by_max2 (ts date, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION yr START ('2020-03-02') END ('2020-05-07')  EVERY ('1 month'::interval) ) ;
ALTER TABLE apm_test.by_max2 ADD PARTITION max_part START ('2020-12-31') INCLUSIVE;
-- end_ignore

-- should return 2 because now() - 0 sec is inside this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_max_1_prt_max_part'::regclass::oid, '0 sec');
-- should return 2 because now() + 2000 years is inside this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_max_1_prt_max_part'::regclass::oid, '- 2000 years', now());
-- should return 3 because now() - 1 day is before this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_max_1_prt_max_part'::regclass::oid, '1 days');

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_max2_1_prt_max_part'::regclass::oid);

-- MINVALUE
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_min;
CREATE TABLE apm_test.by_min (ts timestamp, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION mo START (now()::date - '3 month'::interval) END (now()::date) EVERY ('1 month'::interval) ) ;
ALTER TABLE apm_test.by_min ADD PARTITION min_part END (now() - '1 year'::interval) INCLUSIVE;

DROP TABLE IF EXISTS apm_test.by_min2;
CREATE TABLE apm_test.by_min2 (ts timestamp, region text)
  DISTRIBUTED BY (region)
  PARTITION BY RANGE (ts)
  ( PARTITION mo START ('2020-03-02 11:55:59') END ('2020-05-07 22:47:26')  EVERY ('1 month'::interval) ) ;
ALTER TABLE apm_test.by_min2 ADD PARTITION min_part END ('2020-03-02 11:55:58') INCLUSIVE;
-- end_ignore

-- should return 1 because now() - 0 sec is inside this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_min_1_prt_min_part'::regclass::oid, '0 sec', now());
-- should return 1 because now() - 10 month is before this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_min_1_prt_min_part'::regclass::oid, '10 month');
-- should return 2 because now() + 2000 years is inside this partition boundary
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_min_1_prt_min_part'::regclass::oid, '2000 years');

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_min2_1_prt_min_part'::regclass::oid);

-- MAXVALUE + MINVALUE
-- start_ignore
DROP TABLE IF EXISTS apm_test.by_min_max;
CREATE TABLE apm_test.by_min_max (ts timestamp, region text)
DISTRIBUTED BY (region)
PARTITION BY RANGE (ts) (DEFAULT PARTITION others);
ALTER TABLE apm_test.by_min_max ADD PARTITION all_part START ('-infinity'::timestamp) INCLUSIVE;
-- end_ignore

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_min_max_1_prt_all_part'::regclass::oid);
-- should always return 2
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_min_max_1_prt_all_part'::regclass::oid, '- 6000 years');
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_min_max_1_prt_all_part'::regclass::oid, '6000 years');

DROP TABLE IF EXISTS apm_test.by_max_min;
CREATE TABLE apm_test.by_max_min (ts timestamp, region text)
DISTRIBUTED BY (region)
PARTITION BY RANGE (ts) (DEFAULT PARTITION others);
ALTER TABLE apm_test.by_max_min ADD PARTITION all_part END ('infinity'::timestamp) EXCLUSIVE;

-- should return partition boundary in tz
SELECT matrixts_internal.apm_partition_boundary('apm_test.by_max_min_1_prt_all_part'::regclass::oid);
-- should always return 2
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_max_min_1_prt_all_part'::regclass::oid, '- 6000 years');
SELECT matrixts_internal.apm_eval_partbound('apm_test.by_max_min_1_prt_all_part'::regclass::oid, '6000 years', now());

---------------------------------------------
-- apm_is_valid_partition_period
---------------------------------------------

-- should return NULL
SELECT matrixts_internal.apm_is_valid_partition_period(NULL);

-- should raise error
SELECT matrixts_internal.apm_is_valid_partition_period('apple pie');

-- should return true for supported interval
SELECT matrixts_internal.apm_is_valid_partition_period('1 day');
SELECT matrixts_internal.apm_is_valid_partition_period('1 day 1 second');
SELECT matrixts_internal.apm_is_valid_partition_period('1 week');
SELECT matrixts_internal.apm_is_valid_partition_period('1 week 1 hour');
SELECT matrixts_internal.apm_is_valid_partition_period('1 month');
SELECT matrixts_internal.apm_is_valid_partition_period('3 month');
SELECT matrixts_internal.apm_is_valid_partition_period('1 year');

-- should return false for unsupported interval
SELECT matrixts_internal.apm_is_valid_partition_period('2 month');
SELECT matrixts_internal.apm_is_valid_partition_period('2 year');
SELECT matrixts_internal.apm_is_valid_partition_period('1 month 1 min');
SELECT matrixts_internal.apm_is_valid_partition_period('3 month 1 second');
SELECT matrixts_internal.apm_is_valid_partition_period('1 year 3 day');

---------------------------------------------
-- apm_time_trunc
---------------------------------------------

-- should return NULL
SELECT matrixts_internal.apm_time_trunc(NULL, '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc(NULL, '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc(NULL, '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 day', NULL::date);
SELECT matrixts_internal.apm_time_trunc('1 day', NULL::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 day', NULL::timestamptz);

-- UTC timezone
BEGIN;
SET timezone TO UTC;
SELECT matrixts_internal.apm_time_trunc('1 day', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 week', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('3 month', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 year', '2017-03-02'::date);

SELECT matrixts_internal.apm_time_trunc('1 day', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 day 1 second', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 week', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 week 1 hour', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('3 month', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 year', '2017-03-02 01:30:42'::timestamp);

SELECT matrixts_internal.apm_time_trunc('1 day', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 day 1 second', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 week', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 week 1 hour', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('3 month', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 year', '2017-03-02 01:30:42+08:00'::timestamptz);
ROLLBACK;

-- timezone +08:00
BEGIN;
SET timezone TO 'Asia/Shanghai';
SELECT matrixts_internal.apm_time_trunc('1 day', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 week', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('3 month', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 year', '2017-03-02'::date);

SELECT matrixts_internal.apm_time_trunc('1 day', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 day 1 second', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 week', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 week 1 hour', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('3 month', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 year', '2017-03-02 01:30:42'::timestamp);

SELECT matrixts_internal.apm_time_trunc('1 day', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 day 1 second', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 week', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 week 1 hour', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('3 month', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 year', '2017-03-02 01:30:42+08:00'::timestamptz);
ROLLBACK;

-- timezone -05:00
BEGIN;
SET timezone TO 'America/Detroit';
SELECT matrixts_internal.apm_time_trunc('1 day', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 week', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('3 month', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 year', '2017-03-02'::date);

SELECT matrixts_internal.apm_time_trunc('1 day', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 day 1 second', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 week', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 week 1 hour', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('3 month', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 year', '2017-03-02 01:30:42'::timestamp);

SELECT matrixts_internal.apm_time_trunc('1 day', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 day 1 second', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 week', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 week 1 hour', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('3 month', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 year', '2017-03-02 01:30:42+08:00'::timestamptz);
ROLLBACK;

-- timezone behavior difference
BEGIN;
SET timezone TO 'Asia/Shanghai';
-- should be 2017-03-01 00:00:00
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-01 00:00:00'::timestamp);
-- should be 2017-03-01 00:00:00+08
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-01 00:00:00+08:00'::timestamptz);

SET timezone TO 'America/Detroit';
-- should be 2017-03-01 00:00:00
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-01 00:00:00'::timestamp);
-- should be 2017-02-01 00:00:00-05
SELECT matrixts_internal.apm_time_trunc('1 month', '2017-03-01 00:00:00+08:00'::timestamptz);
ROLLBACK;

-- should return error for invalid input
SELECT matrixts_internal.apm_time_trunc('1 hour', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 day 1 second', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('1 week 1 hour', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('2 month', '2017-03-02'::date);
SELECT matrixts_internal.apm_time_trunc('2 year', '2017-03-02 01:30:42+08:00'::timestamptz);
SELECT matrixts_internal.apm_time_trunc('1 month 1 min', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('3 month 1 second', '2017-03-02 01:30:42'::timestamp);
SELECT matrixts_internal.apm_time_trunc('1 year 3 day', '2017-03-02'::date);

---------------------------------------------------------
-- _unique_partition_name
---------------------------------------------------------
-- start_ignore
DROP TABLE IF EXISTS apm_test.abc;
DROP TABLE IF EXISTS apm_test.abc_1_prt_1;
DROP TABLE IF EXISTS apm_test."甄士隐梦幻识通灵 贾雨村风尘怀闺秀 贾_1_prt_1"(id int);
DROP TABLE IF EXISTS apm_test."ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABC_1_prt_1"(id int);
-- end_ignore

-- should return abc_1_prt_1
SELECT *, length(s), octet_length(s)  FROM matrixts_internal._unique_partition_name('abc') s;
CREATE TABLE apm_test.abc_1_prt_1(id int);
-- should return abc_1_prt_2
SELECT *, length(s), octet_length(s)  FROM matrixts_internal._unique_partition_name('abc') s;

-- should return 甄士隐梦幻识通灵 贾雨村风尘怀闺秀 贾_1_prt_1
SELECT *, length(s), octet_length(s) FROM matrixts_internal._unique_partition_name('甄士隐梦幻识通灵 贾雨村风尘怀闺秀 贾夫人仙逝扬州城 冷子兴演说荣国府 托内兄如海荐西宾 接外孙贾母惜孤女') s;
CREATE TABLE apm_test."甄士隐梦幻识通灵 贾雨村风尘怀闺秀 贾_1_prt_1"(id int);
-- should return 甄士隐梦幻识通灵 贾雨村风尘怀闺秀 贾_1_prt_2
SELECT *, length(s), octet_length(s) FROM matrixts_internal._unique_partition_name('甄士隐梦幻识通灵 贾雨村风尘怀闺秀 贾夫人仙逝扬州城 冷子兴演说荣国府 托内兄如海荐西宾 接外孙贾母惜孤女') s;

-- should return ABC_1_prt_1
SELECT *, length(s), octet_length(s)  FROM matrixts_internal._unique_partition_name('ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ') s;
CREATE TABLE apm_test."ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABC_1_prt_1"(id int);
-- should return ABC_1_prt_2
SELECT *, length(s), octet_length(s)  FROM matrixts_internal._unique_partition_name('ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ') s;

-- should cut into <= 63 bytes
SELECT *, length(s), octet_length(s) FROM matrixts_internal._unique_partition_name('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') s;
SELECT *, length(s), octet_length(s) FROM matrixts_internal._unique_partition_name('a甄士隐梦幻识通灵 贾雨村风尘怀闺秀 贾夫人仙逝扬州城 冷子兴演说荣国府 托内兄如海荐西宾 接外孙贾母惜孤女') s;
SELECT *, length(s), octet_length(s) FROM matrixts_internal._unique_partition_name('abc甄士隐梦幻识通灵 贾雨村风尘怀闺秀 贾夫人仙逝扬州城 冷子兴演说荣国府 托内兄如海荐西宾 接外孙贾母惜孤女') s;
SELECT *, length(s), octet_length(s) FROM matrixts_internal._unique_partition_name('''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''') s;
SELECT *, length(s), octet_length(s) FROM matrixts_internal._unique_partition_name('"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""') s;

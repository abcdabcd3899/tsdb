----------------------------------------------------------
-- Tests for following policy UDFs
--   * apm_generic_older_than
--   * apm_generic_split_default_partition
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

--------------------------------
---- apm_generic_older_than ----
--------------------------------

-- start_ignore
BEGIN;
CREATE TABLE apm_test.to_split (ts timestamp, a int)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (ts) (DEFAULT PARTITION hot);
-- end_ignore

-- should return NULL when default partition has no tuple
SELECT matrixts_internal.apm_generic_older_than(
    '2020-11-23 11:22:33'::timestamptz,
    'apm_test.to_split_1_prt_hot'::regclass,
    'apm_test.to_split'::regclass,
    'apm_test.to_split'::regclass,
    (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split'::regclass, NULL) lp WHERE partitionisdefault))[1],
    ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split'::regclass, NULL)),
    '{ "age": "1 month" }'
);

INSERT INTO apm_test.to_split VALUES('2020-11-23 11:22:33'::timestamptz, 1);

-- should return NULL when tuples in default partition are not old enough
SELECT matrixts_internal.apm_generic_older_than(
    '2020-11-23 11:22:33'::timestamptz + interval '1 month',
    'apm_test.to_split_1_prt_hot'::regclass,
    'apm_test.to_split'::regclass,
    'apm_test.to_split'::regclass,
    (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split'::regclass, NULL) lp WHERE partitionisdefault))[1],
    ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split'::regclass, NULL)),
    '{ "age": "1 month" }'
);

-- should return { -inf, inf, 2020-11-23 11:22:33 } when tuples in default partition are old enough and no existing range partition
SELECT matrixts_internal.apm_generic_older_than(
    '2020-11-23 11:22:33'::timestamptz + interval '1 month 1 sec',
    'apm_test.to_split_1_prt_hot'::regclass,
    'apm_test.to_split'::regclass,
    'apm_test.to_split'::regclass,
    (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split'::regclass, NULL) lp WHERE partitionisdefault))[1],
    ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split'::regclass, NULL)),
    '{ "age": "1 month" }'
);
ROLLBACK;

---------------------------------------------------
---- apm_generic_split_default_partition
---------------------------------------------------

------------ partition by date -------------
-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.to_split_date;
CREATE TABLE apm_test.to_split_date (
    ts date
  , a  int
) DISTRIBUTED RANDOMLY
PARTITION BY RANGE (ts) (DEFAULT PARTITION hot);

INSERT INTO apm_test.to_split_date
VALUES ('2020-05-15', 1), ('2020-05-20', 2), ('2020-05-21', 3)
, (now()::date, 4)
, ((now() - interval '36 h')::date, 5)
, ((now() + interval '36 h')::date, 6);
-- end_ignore

-- should return 1 element with { -inf, inf, 2020-05-15 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_date_1_prt_hot'::regclass
  , 'apm_test.to_split_date'::regclass
  , 'apm_test.to_split_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
  , '{ "age": "1 day" }'
);

-- should split partition for 2020-05-15 ~ 2020-05-16
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_date_1_prt_hot'::regclass
  , 'apm_test.to_split_date'::regclass
  , 'apm_test.to_split_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
  , '{ "period": "1 day" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      NULL
    , 'apm_test.to_split_date_1_prt_hot'::regclass
    , 'apm_test.to_split_date'::regclass
    , 'apm_test.to_split_date'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
    , '{ "age": "1 day" }'
  ))
);

-- should left 5 tuples in default partition
SELECT count(*) FROM apm_test.to_split_date_1_prt_hot;
-- should have 1 range partition (5.15~5.16) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

-- should return 1 element with { 2020-05-16, inf, 2020-05-20 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_date_1_prt_hot'::regclass
  , 'apm_test.to_split_date'::regclass
  , 'apm_test.to_split_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
  , '{ "age": "1 day" }'
);

-- should split partition for 2020-05-20 ~ 2020-05-21
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_date_1_prt_hot'::regclass
  , 'apm_test.to_split_date'::regclass
  , 'apm_test.to_split_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
  , '{ "period": "1 day" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_date_1_prt_hot'::regclass
    , 'apm_test.to_split_date'::regclass
    , 'apm_test.to_split_date'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
    , '{ "age": "1 day" }'
  ))
);

-- should left 4 tuples in default partition
SELECT count(*) FROM apm_test.to_split_date_1_prt_hot;
-- should have 2 range partition (5.15~5.16 and 5.20~5.21) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

-- should return 1 element with { 2020-05-21, inf, 2020-05-21 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_date_1_prt_hot'::regclass
  , 'apm_test.to_split_date'::regclass
  , 'apm_test.to_split_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
  , '{ "age": "1 day" }'
);

-- should split partition for 2020-05-21 ~ 2020-05-22
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_date_1_prt_hot'::regclass
  , 'apm_test.to_split_date'::regclass
  , 'apm_test.to_split_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
  , '{ "period": "1 day" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_date_1_prt_hot'::regclass
    , 'apm_test.to_split_date'::regclass
    , 'apm_test.to_split_date'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
    , '{ "age": "1 day" }'
  ))
);

-- should left 3 tuples in default partition
SELECT count(*) FROM apm_test.to_split_date_1_prt_hot;
-- should have 3 range partition (5.15~5.16 and 5.20~5.21 and 5.21~5.22) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

INSERT INTO apm_test.to_split_date
VALUES ('2020-05-24', 7);

-- create another placeholder partition
ALTER TABLE apm_test.to_split_date
SPLIT DEFAULT PARTITION START('2020-05-26') END('2020-05-28');

-- should return 1 element with { 5-22, 5-26, 5-24 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_date_1_prt_hot'::regclass
  , 'apm_test.to_split_date'::regclass
  , 'apm_test.to_split_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
  , '{ "age": "1 year" }'
);

-- should split partition for 5-22 ~ 5-26 (ending not overlap existing)
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_date_1_prt_hot'::regclass
  , 'apm_test.to_split_date'::regclass
  , 'apm_test.to_split_date'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
  , '{ "period": "1 month" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_date_1_prt_hot'::regclass
    , 'apm_test.to_split_date'::regclass
    , 'apm_test.to_split_date'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL))
    , '{ "age": "1 year" }'
  ))
);

-- should left 3 tuples in default partition
SELECT count(*) FROM apm_test.to_split_date_1_prt_hot;
-- should have 5 range partition (5.15~5.16, 5.20~5.21, 5.21~5.22, 5.22~5.26, 5.26~5.28) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_date'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

ROLLBACK;

------------ partition by timestamp -------------
-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.to_split_ts;
CREATE TABLE apm_test.to_split_ts (
    ts timestamp without time zone
  , a  int
) DISTRIBUTED RANDOMLY
PARTITION BY RANGE (ts) (DEFAULT PARTITION hot);

INSERT INTO apm_test.to_split_ts
VALUES ('2020-05-15 00:00:00', 1), ('2020-05-20 00:00:00', 2)
, ('2020-05-20 06:00:00', 3)
, ('2020-05-21 03:00:00', 4)
, (now()::timestamp, 5)
, ((now() - interval '36 h')::timestamp, 6)
, ((now() + interval '36 h')::timestamp, 7);
-- end_ignore

-- should return 1 element with { -inf, inf, 5-15_00:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-15_00:00 ~ 5-15_06:00
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_ts_1_prt_hot'::regclass
    , 'apm_test.to_split_ts'::regclass
    , 'apm_test.to_split_ts'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 6 tuples in default partition
SELECT count(*) FROM apm_test.to_split_ts_1_prt_hot;
-- should have 1 range partition (5-15_00:00~5-15_06:00) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

-- should return 1 element with { 5-15_06:00, inf, 5-20_00:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-20_00:00 ~ 5-20_06:00
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_ts_1_prt_hot'::regclass
    , 'apm_test.to_split_ts'::regclass
    , 'apm_test.to_split_ts'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 5 tuples in default partition
SELECT count(*) FROM apm_test.to_split_ts_1_prt_hot;
-- should have 2 range partition (5-15_00:00~5-15_06:00 and 5-20_00:00~5-20_06:00) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

-- should return 1 element with { 5-20_06:00, inf, 5-20_06:00 } (handle edge)
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-20_06:00 ~ 5-20_12:00 (handle edge)
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_ts_1_prt_hot'::regclass
    , 'apm_test.to_split_ts'::regclass
    , 'apm_test.to_split_ts'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 4 tuples in default partition
SELECT count(*) FROM apm_test.to_split_ts_1_prt_hot;
-- should have 3 range partition (5-15_00:00~5-15_06:00 and 5-20_00:00~5-20_06:00 and 5-20_06:00~5-20_12:00) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

-- create a placeholder partition
ALTER TABLE apm_test.to_split_ts
SPLIT DEFAULT PARTITION START('2020-05-20 21:00:00') END('2020-05-21 02:00:00');

-- should return 1 element with { 5-21_02:00, inf, 5-21_03:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-21_02:00 ~ 5-21_06:00 (beginning not overlap existing)
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_ts_1_prt_hot'::regclass
    , 'apm_test.to_split_ts'::regclass
    , 'apm_test.to_split_ts'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 3 tuples in default partition
SELECT count(*) FROM apm_test.to_split_ts_1_prt_hot;
-- should have 5 range partition and 1 default partition
-- 5-15_00:00~5-15_06:00, 5-20_00:00~5-20_06:00, 5-20_06:00~5-20_12:00, 5-20_21:00~5-21_02:00, 5-21_02:00~5-21_06:00
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

INSERT INTO apm_test.to_split_ts
VALUES ('2020-05-21 07:00:00', 8);

-- create another placeholder partition
ALTER TABLE apm_test.to_split_ts
SPLIT DEFAULT PARTITION START('2020-05-21 10:30:00') END('2020-05-21 14:50:00');

-- should return 1 element with { 5-21_06:00, 5-21_10:30, 5-21_07:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-21_06:00 ~ 5-21_10:30 (ending not overlap existing)
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_ts_1_prt_hot'::regclass
    , 'apm_test.to_split_ts'::regclass
    , 'apm_test.to_split_ts'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 3 tuples in default partition
SELECT count(*) FROM apm_test.to_split_ts_1_prt_hot;
-- should have 7 range partition and 1 default partition
-- 5-15_00:00~5-15_06:00, 5-20_00:00~5-20_06:00, 5-20_06:00~5-20_12:00, 5-20_21:00~5-21_02:00, 5-21_02:00~5-21_06:00, 5-21_06:00~5-21_10:30, 5-21_11:30~5-21_14:50
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

INSERT INTO apm_test.to_split_ts
VALUES ('2020-05-21 15:00:00', 9), ('2020-05-21 17:50:00', 10);

-- create one more placeholder partition
ALTER TABLE apm_test.to_split_ts
SPLIT DEFAULT PARTITION START('2020-05-21 17:55:00') END('2020-05-21 19:00:00');

-- should return 1 element with { 5-21_14:50, 5-21_17:55, 5-21_15:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-21_14:50, 5-21_17:55 (begin/end not overlap existing)
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_ts_1_prt_hot'::regclass
  , 'apm_test.to_split_ts'::regclass
  , 'apm_test.to_split_ts'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_ts_1_prt_hot'::regclass
    , 'apm_test.to_split_ts'::regclass
    , 'apm_test.to_split_ts'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 3 tuples in default partition
SELECT count(*) FROM apm_test.to_split_ts_1_prt_hot;
-- should have 7 range partition and 1 default partition
-- 5-15_00:00~5-15_06:00, 5-20_00:00~5-20_06:00, 5-20_06:00~5-20_12:00, 5-20_21:00~5-21_02:00
-- 5-21_02:00~5-21_06:00, 5-21_06:00~5-21_10:30, 5-21_11:30~5-21_14:50, 5-21_14:50~5-21_17:55, 5-21_17:55~5_21_19:00
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_ts'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

ROLLBACK;

------------ partition by timestamptz -------------
-- start_ignore
BEGIN;
DROP TABLE IF EXISTS apm_test.to_split_tz;
CREATE TABLE apm_test.to_split_tz (
    ts timestamp with time zone
  , a  int
) DISTRIBUTED RANDOMLY
PARTITION BY RANGE (ts) (DEFAULT PARTITION hot);

INSERT INTO apm_test.to_split_tz
VALUES ('2020-05-15 00:00:00', 1), ('2020-05-20 00:00:00', 2)
, ('2020-05-20 06:00:00', 3)
, ('2020-05-21 03:00:00', 4)
, (now()::timestamp, 5)
, ((now() - interval '36 h')::timestamp, 6)
, ((now() + interval '36 h')::timestamp, 7);
-- end_ignore

-- should return 1 element with { -inf, inf, 5-15_00:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-15_00:00 ~ 5-15_06:00
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_tz_1_prt_hot'::regclass
    , 'apm_test.to_split_tz'::regclass
    , 'apm_test.to_split_tz'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 6 tuples in default partition
SELECT count(*) FROM apm_test.to_split_tz_1_prt_hot;
-- should have 1 range partition (5-15_00:00~5-15_06:00) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

-- should return 1 element with { 5-15_06:00, inf, 5-20_00:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-20_00:00 ~ 5-20_06:00
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_tz_1_prt_hot'::regclass
    , 'apm_test.to_split_tz'::regclass
    , 'apm_test.to_split_tz'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 5 tuples in default partition
SELECT count(*) FROM apm_test.to_split_tz_1_prt_hot;
-- should have 2 range partition (5-15_00:00~5-15_06:00 and 5-20_00:00~5-20_06:00) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

-- should return 1 element with { 5-20_06:00, inf, 5-20_06:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-20_06:00 ~ 5-20_12:00
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_tz_1_prt_hot'::regclass
    , 'apm_test.to_split_tz'::regclass
    , 'apm_test.to_split_tz'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 4 tuples in default partition
SELECT count(*) FROM apm_test.to_split_tz_1_prt_hot;
-- should have 3 range partition (5-15_00:00~5-15_06:00 and 5-20_00:00~5-20_06:00 and 5-20_06:00~5-20_12:00) and 1 default partition
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

-- create a placeholder partition
ALTER TABLE apm_test.to_split_tz
SPLIT DEFAULT PARTITION START('2020-05-20 21:00:00') END('2020-05-21 02:00:00');

-- should return 1 element with { 5-21_02:00, inf, 5-21_03:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-21_02:00 ~ 5-21_06:00
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_tz_1_prt_hot'::regclass
    , 'apm_test.to_split_tz'::regclass
    , 'apm_test.to_split_tz'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 3 tuples in default partition
SELECT count(*) FROM apm_test.to_split_tz_1_prt_hot;
-- should have 5 range partition and 1 default partition
-- 5-15_00:00~5-15_06:00, 5-20_00:00~5-20_06:00, 5-20_06:00~5-20_12:00, 5-20_21:00~5-21_02:00, 5-21_02:00~5-21_06:00
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

INSERT INTO apm_test.to_split_tz
VALUES ('2020-05-21 07:00:00', 8);

-- create another placeholder partition
ALTER TABLE apm_test.to_split_tz
SPLIT DEFAULT PARTITION START('2020-05-21 10:30:00') END('2020-05-21 14:50:00');

-- should return 1 element with { 5-21_06:00, 5-21_10:30, 5-21_07:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-21_06:00 ~ 5-21_10:30 (ending not overlap existing)
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_tz_1_prt_hot'::regclass
    , 'apm_test.to_split_tz'::regclass
    , 'apm_test.to_split_tz'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 3 tuples in default partition
SELECT count(*) FROM apm_test.to_split_tz_1_prt_hot;
-- should have 7 range partition and 1 default partition
-- 5-15_00:00~5-15_06:00, 5-20_00:00~5-20_06:00, 5-20_06:00~5-20_12:00, 5-20_21:00~5-21_02:00, 5-21_02:00~5-21_06:00, 5-21_06:00~5-21_10:30, 5-21_11:30~5-21_14:50
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

INSERT INTO apm_test.to_split_tz
VALUES ('2020-05-21 15:00:00', 9), ('2020-05-21 17:50:00', 10);

-- create one more placeholder partition
ALTER TABLE apm_test.to_split_tz
SPLIT DEFAULT PARTITION START('2020-05-21 17:55:00') END('2020-05-21 19:00:00');

-- should return 1 element with { 5-21_14:50, 5-21_17:55, 5-21_15:00 }
SELECT matrixts_internal.apm_generic_older_than(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "age": "8 hour" }'
);

-- should split partition for 5-21_14:50, 5-21_17:55 (begin/end not overlap existing)
SELECT matrixts_internal.apm_generic_split_default_partition(
    now()
  , 'apm_test.to_split_tz_1_prt_hot'::regclass
  , 'apm_test.to_split_tz'::regclass
  , 'apm_test.to_split_tz'::regclass
  , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
  , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
  , '{ "period": "6 hour" }'
  , (SELECT matrixts_internal.apm_generic_older_than(
      now()
    , 'apm_test.to_split_tz_1_prt_hot'::regclass
    , 'apm_test.to_split_tz'::regclass
    , 'apm_test.to_split_tz'::regclass
    , (ARRAY(SELECT lp FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) lp WHERE partitionisdefault))[1]
    , ARRAY(SELECT matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL))
    , '{ "age": "8 hour" }'
  ))
);

-- should left 3 tuples in default partition
SELECT count(*) FROM apm_test.to_split_tz_1_prt_hot;
-- should have 7 range partition and 1 default partition
-- 5-15_00:00~5-15_06:00, 5-20_00:00~5-20_06:00, 5-20_06:00~5-20_12:00, 5-20_21:00~5-21_02:00
-- 5-21_02:00~5-21_06:00, 5-21_06:00~5-21_10:30, 5-21_11:30~5-21_14:50, 5-21_14:50~5-21_17:55, 5-21_17:55~5_21_19:00
SELECT root_relname, parent_relname, relname, partitiontype, partitionlevel, partitionisdefault, partitionboundary
FROM matrixts_internal.apm_generic_list_partitions('apm_test.to_split_tz'::regclass, NULL) ORDER BY partitionlevel, partitionboundary;

ROLLBACK;

-------------------------------
---- apm_generic_split_set ----
-------------------------------
SHOW matrix.ts_guc_apm_allow_dml;
-- start_ignore
CREATE TABLE apm_test.split_set (id int, year int, qtr int, day int, region text)
DISTRIBUTED BY (id) PARTITION BY RANGE (year)
(PARTITION yr START (2010) END (2012) EVERY (1));
TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy_action;
-- end_ignore

-- should report error on invalid input
SELECT matrixts_internal.apm_generic_split_set(NULL, NULL, NULL, NULL);
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, NULL, NULL);
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'fake_action', 'abc');
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'fake_action', '{ "age": "bcd" }');
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'fake_action', '{ "period": "def" }');
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'fake_action', '{ "age": "efg", "period": "xyz" }');
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, NULL, 'test_split', '{"age": "1 month", "period": "2 weeks"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_operation_log;
TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'test_split', '{"age": "1 month", "period": "2 weeks"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should have reset GUC to false
SHOW matrix.ts_guc_apm_allow_dml;
-- should append 2 add records to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should update act_args only
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'test_split', '{"period": "12 hours"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 modify record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should update check_args only
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'test_split', '{"age": "15 days"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 modify record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should update both
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'test_split', '{"age": "5 days", "period": "2 days"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 2 modify records to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

-- should delete the entry
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'test_split', NULL);
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 drop record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry with only check_args
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'test_split', '{"age": "10 days"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 add record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

TRUNCATE matrixts_internal.apm_rel_policy_action;
-- should insert a new entry with only act_args
SELECT matrixts_internal.apm_generic_split_set('apm_test.split_set'::regclass, 1, 'test_split', '{"period": "1 hour"}');
SELECT reloid::regclass, action, disabled, check_func, check_args, act_func, act_args, version
FROM matrixts_internal.apm_rel_policy_action;
-- should append 1 add record to operation log
SELECT username = CURRENT_USER, relname, class_id, class_name, action, operation, mod_field, old_val, new_val
FROM matrixts_internal.apm_operation_log ORDER BY ts;

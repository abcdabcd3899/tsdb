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
-- List function
---- apm_generic_list_partitions
---------------------------------------------

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
  , relnsp
  , partitiontype
  , partitionlevel
  , partitionisdefault
  , partitionboundary
  , addopt
FROM matrixts_internal.apm_generic_list_partitions('apm_test.sales'::regclass, NULL)
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
  , relnsp
  , partitiontype
  , partitionlevel
  , partitionisdefault
  , partitionboundary
  , addopt
FROM matrixts_internal.apm_generic_list_partitions('apm_test.sales2'::regclass, NULL)
ORDER BY relname;

---------------------------------------------
-- Schema protection
---------------------------------------------

-- apm_schema_protect event trigger should not crash normal DDL
CREATE TABLE apm_test.to_alter(id int, id2 int);
ALTER TABLE apm_test.to_alter SET DISTRIBUTED BY (id2);
SET search_path TO apm_test;
ALTER TABLE to_alter SET DISTRIBUTED BY (id);
RESET search_path;

-- should ban DDL changes
ALTER TYPE matrixts_internal.apm_partition_item ADD ATTRIBUTE test int;
ALTER TYPE matrixts_internal.apm_partition_item DROP ATTRIBUTE test;

ALTER TABLE matrixts_internal.apm_policy_class ADD COLUMN test int;
ALTER TABLE matrixts_internal.apm_policy_class DROP COLUMN test;

ALTER TABLE matrixts_internal.apm_policy_action ADD COLUMN test int;
ALTER TABLE matrixts_internal.apm_policy_action DROP COLUMN test;

ALTER TABLE matrixts_internal.apm_rel_policy ADD COLUMN test int;
ALTER TABLE matrixts_internal.apm_rel_policy DROP COLUMN test;

ALTER TABLE matrixts_internal.apm_rel_policy_action ADD COLUMN test int;
ALTER TABLE matrixts_internal.apm_rel_policy_action DROP COLUMN test;

SET search_path TO matrixts_internal;
ALTER TABLE apm_rel_policy_action ADD COLUMN extra text;
ALTER TABLE apm_rel_policy_action SET DISTRIBUTED BY (action);
RESET search_path;

-- should allow DDL changes with internal GUC set
SET matrix.ts_guc_apm_allow_ddl TO ON;

ALTER TYPE matrixts_internal.apm_partition_item ADD ATTRIBUTE test int;
ALTER TYPE matrixts_internal.apm_partition_item DROP ATTRIBUTE test;

ALTER TABLE matrixts_internal.apm_policy_class ADD COLUMN test int;
ALTER TABLE matrixts_internal.apm_policy_class DROP COLUMN test;

ALTER TABLE matrixts_internal.apm_policy_action ADD COLUMN test int;
ALTER TABLE matrixts_internal.apm_policy_action DROP COLUMN test;

ALTER TABLE matrixts_internal.apm_rel_policy ADD COLUMN test int;
ALTER TABLE matrixts_internal.apm_rel_policy DROP COLUMN test;

ALTER TABLE matrixts_internal.apm_rel_policy_action ADD COLUMN test int;
ALTER TABLE matrixts_internal.apm_rel_policy_action DROP COLUMN test;

RESET matrix.ts_guc_apm_allow_ddl;

-- should say "table has already been expanded"
-- not ERROR:  MatrixTS: cannot alter metatable apm_action_log
ALTER TABLE matrixts_internal.apm_action_log EXPAND TABLE;

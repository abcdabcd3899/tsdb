-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION matrixts" to load this file. \quit

---------------------------------------------
-- Type for function interface
---------------------------------------------
CREATE TYPE matrixts_internal.apm_partition_item AS (
    root_reloid        oid
  , root_relname       name
  , parent_reloid      oid
  , parent_relname     name
  , reloid             oid
  , relname            name
  , relnsp             name
  , partitiontype      text
  , partitionlevel     int
  , partitionisdefault bool
  , partitionisleaf    bool
  , partitionboundary  text
  , addopt             jsonb
);

CREATE TYPE matrixts_internal.apm_boundary AS (
    fromts  timestamptz -- lowerborder of a period
  , tots    timestamptz -- upperborder of a period
  , limitts timestamptz -- stop timestamp when creating partition
);

---------------------------------------------
-- Internal utility functions
---------------------------------------------
CREATE OR REPLACE FUNCTION matrixts_internal.apm_eval_partbound(rel oid, p interval, now timestamptz DEFAULT NULL)
RETURNS INT4 AS 'MODULE_PATHNAME', 'apm_eval_partbound'
LANGUAGE C VOLATILE EXECUTE ON MASTER;

CREATE FUNCTION matrixts_internal.apm_partition_boundary(
    IN rel     oid
  , OUT fromts timestamptz
  , OUT tots   timestamptz
) AS 'MODULE_PATHNAME', 'apm_partition_boundary'
LANGUAGE C VOLATILE EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION matrixts_internal.apm_is_valid_partition_period(period interval)
RETURNS BOOLEAN AS 'MODULE_PATHNAME', 'apm_is_valid_partition_period'
LANGUAGE C IMMUTABLE STRICT EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION matrixts_internal.apm_time_trunc(bucket_width INTERVAL, ts TIMESTAMP)
RETURNS TIMESTAMP AS 'MODULE_PATHNAME', 'apm_time_trunc_ts' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION matrixts_internal.apm_time_trunc(bucket_width INTERVAL, ts TIMESTAMPTZ)
RETURNS TIMESTAMPTZ AS 'MODULE_PATHNAME', 'apm_time_trunc_tz' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION matrixts_internal.apm_time_trunc(bucket_width INTERVAL, ts DATE)
RETURNS DATE AS 'MODULE_PATHNAME', 'apm_time_trunc_date' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE OR REPLACE FUNCTION matrixts_internal._apm_metadata_protect()
RETURNS TRIGGER AS 'MODULE_PATHNAME', 'apm_metadata_protect'
LANGUAGE C STRICT;

-- input argument ARRAY[matrixts_internal.apm_boundary],
-- matrixts_internal.apm_boundary
-- exclude minuend from src
CREATE FUNCTION matrixts_internal._apm_minus(
    src     matrixts_internal._apm_boundary
  , minuend matrixts_internal.apm_boundary
) RETURNS matrixts_internal._apm_boundary AS $$
DECLARE
  boundary          matrixts_internal.apm_boundary;
  src_tstzrange     tstzrange;
  minuend_tstzrange tstzrange;
  res               matrixts_internal._apm_boundary := '{}'::matrixts_internal._apm_boundary;
  fromts            timestamptz;
  tots              timestamptz;
  minuend_fromts    timestamptz;
  minuend_tots      timestamptz;
BEGIN
  minuend_fromts := minuend.fromts;
  minuend_tots := minuend.tots;

  -- if minuend.fromts is NULL, reset to -infinity
  IF minuend_fromts IS NULL THEN
    minuend_fromts := '-infinity'::timestamptz;
  END IF;

  -- if minuend.tots is NULL, reset to infinity
  IF minuend_tots IS NULL THEN
    minuend_tots := 'infinity'::timestamptz;
  END IF;

  minuend_tstzrange := ('['||minuend_fromts||','||minuend_tots||']')::tstzrange;

  -- loop source boundary array, and take out minuend boundary, return remained
  -- boundary array
  FOREACH boundary IN ARRAY src LOOP
    fromts := boundary.fromts;
    tots := boundary.tots;

    -- same to minuend.fromts
    IF fromts IS NULL THEN
      fromts := '-infinity'::timestamptz;
    END IF;

    -- same to minuend.tots
    IF tots IS NULL THEN
      tots := 'infinity'::timestamptz;
    END IF;

    src_tstzrange := ('['||fromts||','||tots||']')::tstzrange;

    -- if src_tstzrange include by minuend_tstzrange, include src_tstzrange ==
    -- minuend_tstzrange, do nothing
    IF src_tstzrange <@ minuend_tstzrange THEN
      -- do nothing, ignore
    -- if minuend_tstzrange include by src_tstzrange, split src_tstzrange by
    -- minuend_tstzrange
    ELSIF src_tstzrange @> minuend_tstzrange THEN
      IF boundary.fromts != minuend.fromts OR boundary.fromts IS NULL THEN
        res := array_append(res, (boundary.fromts, minuend.fromts, NULL)::matrixts_internal.apm_boundary);
      END IF;
      IF boundary.tots != minuend.tots OR boundary.tots IS NULL THEN
        res := array_append(res, (minuend.tots, boundary.tots, NULL)::matrixts_internal.apm_boundary);
      END IF;
    -- if src_tstzrange and minuend_tstzrange has intersection, exclude
    -- intersection from src_tstzrange
    ELSIF src_tstzrange && minuend_tstzrange THEN
      IF src_tstzrange < minuend_tstzrange THEN
        res := array_append(res,(boundary.fromts, minuend.fromts, NULL)::matrixts_internal.apm_boundary);
      ELSE
        res := array_append(res, (minuend.tots, boundary.tots, NULL)::matrixts_internal.apm_boundary);
      END IF;
    -- no need exclude anything, append whole boundary
    ELSE
      res := array_append(res, boundary);
    END IF;
  END LOOP;
  RETURN res;
END
$$ LANGUAGE PLPGSQL IMMUTABLE;

-- unique_partition_name input unquoted name because we need to measure the
-- length of name no larger than 63
CREATE FUNCTION matrixts_internal._unique_partition_name(tablename text)
RETURNS text AS $$
DECLARE
  cut_length    int;
  part_ident    text;
BEGIN
  IF tablename IS NULL OR tablename = '' THEN
    RAISE EXCEPTION 'Table name should not be NULL or empty';
  END IF;
  -- generate name of the new partition
  -- the name of new partition is based on parent table's name, and not
  -- conflict with existing relations in pg_class
  -- consider multi-byte table name, also need to ensure octet_length of
  -- the generated table name <= 63
  cut_length := 55;
  LOOP
    IF octet_length(substring(tablename FROM 1 FOR cut_length)) > 55 THEN
      -- cut more bytes when multi-byte character in the name
      cut_length := cut_length - 1;
      CONTINUE;
    END IF;
    EXECUTE format($sub$
        WITH RECURSIVE newname(name, idx) AS (
            SELECT substring(%L FROM 1 FOR %s) || '_1_prt_' || 1, 1
          UNION ALL
            SELECT substring(%L FROM 1 FOR %s) || '_1_prt_' || newname.idx+1, newname.idx+1
            FROM pg_catalog.pg_class pc INNER JOIN newname ON pc.relname = newname.name
        )
        SELECT first_value(name) OVER (ORDER BY idx DESC)
        FROM newname LIMIT 1
    ;$sub$, tablename, cut_length, tablename, cut_length)
    INTO part_ident;
    IF octet_length(part_ident) > 63 THEN
      cut_length := cut_length - 1;
    ELSE
      EXIT;
    END IF;
    IF cut_length <= 0 THEN
      -- impossible
      RAISE EXCEPTION 'cannot generate partition table name for %', tablename;
    END IF;
  END LOOP;
  RETURN part_ident;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---------------------------------------------
-- Metadata tables
---------------------------------------------
CREATE TABLE matrixts_internal.apm_policy_class (
    class_id      SERIAL  NOT NULL
  , class_name    name    NOT NULL UNIQUE
  , family_id     int     DEFAULT NULL
  , list_func     regproc NOT NULL
  , list_args     jsonb   NOT NULL DEFAULT '{}'
  , validate_func regproc DEFAULT NULL
  , validate_args jsonb   NOT NULL DEFAULT '{}'
  , version       name    NOT NULL
  , PRIMARY KEY (class_id)
) DISTRIBUTED MASTERONLY;
COMMENT ON COLUMN matrixts_internal.apm_policy_class.class_id IS 'The unique identity of a policy, generated from SERIAL.';
COMMENT ON COLUMN matrixts_internal.apm_policy_class.class_name IS 'Specify unique name of a policy, used as parameter of set_policy().';
COMMENT ON COLUMN matrixts_internal.apm_policy_class.family_id IS 'Not implemented, reserved for future use.';
COMMENT ON COLUMN matrixts_internal.apm_policy_class.list_func IS 'A regproc for retrieving partition list from given parent table.';
COMMENT ON COLUMN matrixts_internal.apm_policy_class.list_args IS 'Static arguments to be passed into list_func.';
COMMENT ON COLUMN matrixts_internal.apm_policy_class.validate_func IS 'A regproc invoked just before set_policy() for checking relation satisfies this policy, default NULL means no checking.';
COMMENT ON COLUMN matrixts_internal.apm_policy_class.validate_args IS 'Static arguments to be passed into validate_func.';
COMMENT ON COLUMN matrixts_internal.apm_policy_class.version IS 'Interface version of list_func.';

CREATE TRIGGER apm_policy_class_protect BEFORE INSERT OR UPDATE OR DELETE
ON matrixts_internal.apm_policy_class
FOR EACH ROW EXECUTE FUNCTION matrixts_internal._apm_metadata_protect();

-- FIXME: statement level trigger does not work: https://github.com/greenplum-db/gpdb/issues/12045
-- CREATE TRIGGER apm_policy_class_protect2 BEFORE TRUNCATE
-- ON matrixts_internal.apm_policy_class
-- EXECUTE FUNCTION matrixts_internal._apm_metadata_protect();

SELECT setting FROM pg_settings WHERE "name" = 'client_min_messages';

DO $$
DECLARE
    saved text;
BEGIN

-- tune down to avoid FOREIGN KEY warnings
SELECT setting INTO saved FROM pg_settings WHERE "name" = 'client_min_messages';
SET client_min_messages TO error;

CREATE TABLE matrixts_internal.apm_policy_action (
    class_id           int     NOT NULL
  , action             name    NOT NULL
  , seq                int     NOT NULL
  , default_disabled   bool    NOT NULL DEFAULT FALSE
  , default_check_func regproc NOT NULL
  , default_check_args jsonb   NOT NULL DEFAULT '{}'
  , default_act_func   regproc NOT NULL
  , default_act_args   jsonb   NOT NULL DEFAULT '{}'
  , default_term_func  regproc DEFAULT NULL
  , default_term_args  jsonb   DEFAULT NULL
  , set_func           regproc NOT NULL
  , version            name    NOT NULL
  , PRIMARY KEY (class_id, action)
  , UNIQUE (class_id, seq)
  , FOREIGN KEY (class_id) REFERENCES matrixts_internal.apm_policy_class (class_id)
) DISTRIBUTED MASTERONLY;
COMMENT ON COLUMN matrixts_internal.apm_policy_action.class_id IS 'The foreign class_id of the policy which this action belongs to.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.action IS 'The name of action be referred in set_policy_action(), must be unique under same class_id.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.seq IS 'The check order of sibling actions which belongs to same policy class.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.default_disabled IS 'Defines if the action is disabled by default, be overridden by apm_rel_policy_action.disabled.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.default_check_func IS 'UDF for checking given relation satisfies to execute act_func.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.default_check_args IS 'Static arguments to be passed into check_func, be overridden by apm_rel_policy_action.check_args.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.default_act_func IS 'UDF which is the actual payload for partitioning/compressing/retention a relation.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.default_act_args IS 'Static arguments to be passed into act_func, be overridden by apm_rel_policy_action.act_args.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.default_term_func IS 'UDF for terminating running act_func, default NULL means use pg_cancel_backend().';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.default_term_args IS 'Static arguments to be passed into term_func, be overridden by apm_rel_policy_action.term_args.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.set_func IS 'UDF for overriding static arguments into apm_rel_policy_action.';
COMMENT ON COLUMN matrixts_internal.apm_policy_action.version IS 'Interface version of UDFs in this action.';

CREATE TRIGGER apm_policy_action_protect BEFORE INSERT OR UPDATE OR DELETE
ON matrixts_internal.apm_policy_action
FOR EACH ROW EXECUTE FUNCTION matrixts_internal._apm_metadata_protect();

-- FIXME: statement level trigger does not work: https://github.com/greenplum-db/gpdb/issues/12045
-- CREATE TRIGGER apm_policy_action_protect2 BEFORE TRUNCATE
-- ON matrixts_internal.apm_policy_action
-- EXECUTE FUNCTION matrixts_internal._apm_metadata_protect();

CREATE TABLE matrixts_internal.apm_rel_policy (
    reloid    oid     NOT NULL
  , class_id  int     NOT NULL
  , list_func regproc DEFAULT NULL
  , list_args jsonb   DEFAULT NULL
  , version   name    DEFAULT NULL
  , PRIMARY KEY (reloid, class_id)
  , FOREIGN KEY (class_id) REFERENCES matrixts_internal.apm_policy_class (class_id)
) DISTRIBUTED MASTERONLY;
COMMENT ON COLUMN matrixts_internal.apm_rel_policy.reloid IS 'Relation oid of partition root table.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy.class_id IS 'The foreign class_id of the policy who this relation applies to.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy.list_func IS 'Per table level customization override to apm_policy_class.list_func.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy.list_args IS 'Per table level customization override to apm_policy_class.list_args.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy.version IS 'Per table level customization override to apm_policy_class.version.';

CREATE TRIGGER apm_rel_policy_protect BEFORE INSERT OR UPDATE OR DELETE
ON matrixts_internal.apm_rel_policy
FOR EACH ROW EXECUTE FUNCTION matrixts_internal._apm_metadata_protect();

-- FIXME: statement level trigger does not work: https://github.com/greenplum-db/gpdb/issues/12045
-- CREATE TRIGGER apm_rel_policy_protect2 BEFORE TRUNCATE
-- ON matrixts_internal.apm_rel_policy
-- EXECUTE FUNCTION matrixts_internal._apm_metadata_protect();

CREATE TABLE matrixts_internal.apm_rel_policy_action (
    reloid     oid     NOT NULL
  , class_id   int     NOT NULL
  , action     name    NOT NULL
  , disabled   bool    DEFAULT NULL
  , check_func regproc DEFAULT NULL -- If not NULL, override apm_policy_action
  , check_args jsonb   DEFAULT NULL
  , act_func   regproc DEFAULT NULL
  , act_args   jsonb   DEFAULT NULL
  , term_func  regproc DEFAULT NULL
  , term_args  jsonb   DEFAULT NULL
  , version    name    DEFAULT NULL
  , PRIMARY KEY (reloid, class_id, action)
  , FOREIGN KEY (reloid, class_id) REFERENCES matrixts_internal.apm_rel_policy (reloid, class_id)
  , FOREIGN KEY (class_id, action) REFERENCES matrixts_internal.apm_policy_action (class_id, action)
) DISTRIBUTED MASTERONLY;
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.reloid IS 'The foreign relation oid of the partition root table refers to apm_rel_policy.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.class_id IS 'The foreign class_id refers to apm_rel_policy.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.action IS 'The foreign action name refers to apm_policy_action.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.disabled IS 'Per table level customization override to apm_policy_action.default_disabled.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.check_func IS 'Per table level customization override to apm_policy_action.default_check_func.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.check_args IS 'Per table level customization override to apm_policy_action.default_check_args.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.act_func IS 'Per table level customization override to apm_policy_action.default_act_func.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.act_args IS 'Per table level customization override to apm_policy_action.default_act_args.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.term_func IS 'Per table level customization override to apm_policy_action.default_term_func.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.term_args IS 'Per table level customization override to apm_policy_action.default_term_args.';
COMMENT ON COLUMN matrixts_internal.apm_rel_policy_action.version IS 'Per table level customization override to apm_policy_action.version.';

EXECUTE format('SET client_min_messages TO %s', saved);

END $$;

CREATE TRIGGER apm_rel_policy_action_protect BEFORE INSERT OR UPDATE OR DELETE
ON matrixts_internal.apm_rel_policy_action
FOR EACH ROW EXECUTE FUNCTION matrixts_internal._apm_metadata_protect();

-- FIXME: statement level trigger does not work: https://github.com/greenplum-db/gpdb/issues/12045
-- CREATE TRIGGER apm_rel_policy_action_protect2 BEFORE TRUNCATE
-- ON matrixts_internal.apm_rel_policy_action
-- EXECUTE FUNCTION matrixts_internal._apm_metadata_protect();

CREATE TABLE matrixts_internal.apm_action_log (
    ts         timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
  , pid        int       NOT NULL
  , reloid     oid       NOT NULL
  , relname    name      NOT NULL
  , nspname    name      NOT NULL
  , class_id   int       NOT NULL
  , class_name name      NOT NULL
  , action     name      NOT NULL
  , check_func regproc   NOT NULL
  , check_args jsonb     DEFAULT NULL
  , act_func   regproc   NOT NULL
  , act_args   jsonb     DEFAULT NULL
  , state      text -- LOG/NOTICE/WARNING/ERROR/etc...
  , message    text
  , details    text
  , addopt     jsonb
) DISTRIBUTED RANDOMLY
PARTITION BY RANGE (ts) (DEFAULT PARTITION hot);

---------------------------------------------
-- Operation log table and triggers
---------------------------------------------
CREATE TABLE matrixts_internal.apm_operation_log (
    ts         timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
  , username   name      NOT NULL DEFAULT CURRENT_USER
  , reloid     oid
  , relname    text
  , class_id   int
  , class_name name
  , action     name
  , operation  name -- add/mod/drop | register/unregister | disabled/enabled
  , mod_field  name
  , old_val    name
  , new_val    name
) DISTRIBUTED RANDOMLY
PARTITION BY RANGE (ts) (DEFAULT PARTITION hot);

CREATE FUNCTION matrixts_internal._apm_op_log_add() RETURNS TRIGGER AS $$
DECLARE
  cid   int;
  cname name;
BEGIN
  IF (TG_OP <> 'INSERT') THEN
    RAISE EXCEPTION '_apm_op_log_add() only triggers for INSERT';
  END IF;
  IF (TG_WHEN <> 'AFTER') THEN
    RAISE EXCEPTION '_apm_op_log_add() only triggers on AFTER';
  END IF;
  IF NEW.reloid IS NULL THEN
    RAISE EXCEPTION 'reloid must not be NULL';
  END IF;

  SELECT pc.class_id, pc.class_name
  INTO cid, cname
  FROM matrixts_internal.apm_policy_class pc
  INNER JOIN matrixts_internal.apm_rel_policy rp
  ON pc.class_id = rp.class_id
  WHERE rp.reloid = NEW.reloid;

  IF NOT FOUND THEN
    RAISE WARNING 'relation "%" does not have a policy class to add', NEW.reloid::regclass::name;
  END IF;

  IF NEW.check_func IS NOT NULL THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'add', 'check_func', NEW.check_func);
  END IF;
  IF NEW.check_args IS NOT NULL THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'add', 'check_args', NEW.check_args);
  END IF;
  IF NEW.act_func IS NOT NULL THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'add', 'act_func', NEW.act_func);
  END IF;
  IF NEW.act_args IS NOT NULL THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'add', 'act_args', NEW.act_args);
  END IF;
  IF NEW.term_func IS NOT NULL THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'add', 'term_func', NEW.term_func);
  END IF;
  IF NEW.term_args IS NOT NULL THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'add', 'term_args', NEW.term_args);
  END IF;
  IF NEW.disabled IS NOT NULL THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, CASE WHEN NEW.disabled THEN 'disabled' ELSE 'enabled' END);
  END IF;

  RETURN NEW;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION matrixts_internal._apm_op_log_mod() RETURNS TRIGGER AS $$
DECLARE
  cid   int;
  cname name;
BEGIN
  IF (TG_OP <> 'UPDATE') THEN
    RAISE EXCEPTION '_apm_op_log_add() only triggers for INSERT';
  END IF;
  IF (TG_WHEN <> 'AFTER') THEN
    RAISE EXCEPTION '_apm_op_log_add() only triggers on AFTER';
  END IF;
  IF NEW.reloid IS NULL THEN
    RAISE EXCEPTION 'reloid must not be NULL';
  END IF;

  SELECT pc.class_id, pc.class_name
  INTO cid, cname
  FROM matrixts_internal.apm_policy_class pc
  INNER JOIN matrixts_internal.apm_rel_policy rp
  ON pc.class_id = rp.class_id
  WHERE rp.reloid = NEW.reloid;

  IF NOT FOUND THEN
    RAISE WARNING 'relation "%" does not have a policy class to update', NEW.reloid::regclass::name;
  END IF;

  IF (NEW.check_func <> OLD.check_func) OR
     (NEW.check_func IS NULL AND OLD.check_func IS NOT NULL) OR
     (NEW.check_func IS NOT NULL AND OLD.check_func IS NULL)
  THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, old_val, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'mod', 'check_func', OLD.check_func, NEW.check_func);
  END IF;
  IF (NEW.check_args <> OLD.check_args) OR
     (NEW.check_args IS NULL AND OLD.check_args IS NOT NULL) OR
     (NEW.check_args IS NOT NULL AND OLD.check_args IS NULL)
  THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, old_val, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'mod', 'check_args', OLD.check_args, NEW.check_args);
  END IF;
  IF (NEW.act_func <> OLD.act_func) OR
     (NEW.act_func IS NULL AND OLD.act_func IS NOT NULL) OR
     (NEW.act_func IS NOT NULL AND OLD.act_func IS NULL)
  THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, old_val, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'mod', 'act_func', OLD.act_func, NEW.act_func);
  END IF;
  IF (NEW.act_args <> OLD.act_args) OR
     (NEW.act_args IS NULL AND OLD.act_args IS NOT NULL) OR
     (NEW.act_args IS NOT NULL AND OLD.act_args IS NULL)
  THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, old_val, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'mod', 'act_args', OLD.act_args, NEW.act_args);
  END IF;
    IF (NEW.term_func <> OLD.term_func) OR
     (NEW.term_func IS NULL AND OLD.term_func IS NOT NULL) OR
     (NEW.term_func IS NOT NULL AND OLD.term_func IS NULL)
  THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, old_val, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'mod', 'term_func', OLD.term_func, NEW.term_func);
  END IF;
  IF (NEW.term_args <> OLD.term_args) OR
     (NEW.term_args IS NULL AND OLD.term_args IS NOT NULL) OR
     (NEW.term_args IS NOT NULL AND OLD.term_args IS NULL)
  THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation, mod_field, old_val, new_val)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, 'mod', 'term_args', OLD.term_args, NEW.term_args);
  END IF;
  IF (NEW.disabled <> OLD.disabled) OR
     (NEW.disabled IS NULL AND OLD.disabled IS NOT NULL) OR
     (NEW.disabled IS NOT NULL AND OLD.disabled IS NULL)
  THEN
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation)
    VALUES (NEW.reloid, NEW.reloid::regclass::name, cid, cname, NEW.action, CASE WHEN NEW.disabled THEN 'disabled' ELSE 'enabled' END);
  END IF;

  RETURN NEW;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION matrixts_internal._apm_op_log_drop() RETURNS TRIGGER AS $$
DECLARE
  cid   int;
  cname name;
BEGIN
  IF (TG_OP <> 'DELETE') THEN
    RAISE EXCEPTION '_apm_op_log_add() only triggers for INSERT';
  END IF;
  IF (TG_WHEN <> 'BEFORE') THEN
    RAISE EXCEPTION '_apm_op_log_add() only triggers on BEFORE';
  END IF;
  IF OLD.reloid IS NULL THEN
    RAISE EXCEPTION 'reloid must not be NULL';
  END IF;

  SELECT pc.class_id, pc.class_name
  INTO cid, cname
  FROM matrixts_internal.apm_policy_class pc
  INNER JOIN matrixts_internal.apm_rel_policy rp
  ON pc.class_id = rp.class_id
  WHERE rp.reloid = OLD.reloid;

  IF NOT FOUND THEN
    RAISE WARNING 'relation "%" does not have a policy class to delete', OLD.reloid::regclass::name;
  END IF;

  INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, action, operation)
    VALUES (OLD.reloid, OLD.reloid::regclass::name, cid, cname, OLD.action, 'drop');

  RETURN OLD;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE TRIGGER operation_log_add AFTER INSERT
ON matrixts_internal.apm_rel_policy_action
FOR EACH ROW EXECUTE FUNCTION matrixts_internal._apm_op_log_add();

CREATE TRIGGER operation_log_mod AFTER UPDATE
ON matrixts_internal.apm_rel_policy_action
FOR EACH ROW EXECUTE FUNCTION matrixts_internal._apm_op_log_mod();

CREATE TRIGGER operation_log_drop BEFORE DELETE
ON matrixts_internal.apm_rel_policy_action
FOR EACH ROW EXECUTE FUNCTION matrixts_internal._apm_op_log_drop();

---------------------------------------------
-- List function
---------------------------------------------
CREATE FUNCTION matrixts_internal.apm_generic_list_partitions(rel regclass, args json)
RETURNS SETOF matrixts_internal.apm_partition_item AS $$

  WITH RECURSIVE pr(rootrelid, rootname, parentrelid, parentname, relid, relname, isleaf, level) AS (
    SELECT ppt.relid, pc.relname, NULL::oid, NULL::name, ppt.relid, pc.relname, isleaf, level
    FROM pg_catalog.pg_partition_tree(rel) ppt
    INNER JOIN pg_catalog.pg_class pc ON ppt.relid = pc.oid
    WHERE ppt.parentrelid IS NULL
  UNION ALL
    SELECT pr.rootrelid, pr.rootname, ppt.parentrelid, pcp.relname, ppt.relid, pcc.relname, ppt.isleaf, ppt.level
    FROM pg_catalog.pg_partition_tree(rel) ppt
    INNER JOIN pr ON pr.relid = ppt.parentrelid
    INNER JOIN pg_catalog.pg_class pcp ON ppt.parentrelid = pcp.oid
    INNER JOIN pg_catalog.pg_class pcc ON ppt.relid = pcc.oid
  )
  SELECT
      pr.rootrelid
    , pr.rootname
    , pr.parentrelid
    , pr.parentname
    , pr.relid
    , pr.relname
    , pn.nspname AS relnsp
    , CASE
        WHEN pt.partstrat = 'h'::"char" THEN 'hash'::text
        WHEN pt.partstrat = 'r'::"char" THEN 'range'::text
        WHEN pt.partstrat = 'l'::"char" THEN 'list'::text
        ELSE NULL::text
      END
    , pr.level
    , CASE
        WHEN pg_catalog.pg_get_expr(pc.relpartbound, 0) = 'DEFAULT'
        THEN TRUE
        ELSE FALSE
      END
    , pr.isleaf
    , pg_catalog.pg_get_expr(pc.relpartbound, 0)
    , NULL::jsonb
  FROM pr
  INNER JOIN pg_catalog.pg_class pc ON pr.relid = pc.oid
  INNER JOIN pg_catalog.pg_namespace pn ON pc.relnamespace = pn.oid
  LEFT JOIN pg_catalog.pg_partitioned_table pt ON pr.parentrelid = pt.partrelid
  ORDER BY pr.level, pr.relid;

$$ LANGUAGE SQL VOLATILE;

--------------------------------------------------------------------
---- apm_list_partition_with_am
--      expands on apm_generic_list_partitions by
--      adding storage type to addopt.
--      This is also a good example for developer who wants to expand APM
--      framework to check different action condition.
--------------------------------------------------------------------
CREATE FUNCTION matrixts_internal.apm_list_partition_with_am(rel regclass, args json)
RETURNS SETOF matrixts_internal.apm_partition_item AS $$

  SELECT
      lp.root_reloid
    , lp.root_relname
    , lp.parent_reloid
    , lp.parent_relname
    , lp.reloid
    , lp.relname
    , lp.relnsp
    , lp.partitiontype
    , lp.partitionlevel
    , lp.partitionisdefault
    , lp.partitionisleaf
    , lp.partitionboundary
    , ('{ "storage": "' || pa.amname || '" }')::jsonb AS addopt
  FROM matrixts_internal.apm_generic_list_partitions(rel, args) AS lp
  INNER JOIN pg_class AS pc ON lp.reloid = pc.oid
  LEFT JOIN pg_am AS pa ON pc.relam = pa.oid;

$$ LANGUAGE SQL VOLATILE;

--------------------------------------------------------------------
-- Check functions
-- Return value:
--   Error: check failed, should not trigger action but log.
--   NULL/Empty Array:  should not trigger action.
--   Non-Empty Array: should trigger action. The array contains a list of
--          periods that no overlapping partition exists, this
--          this array is a hint for actions who may create new
--          partitions, or just be ignored for dropping actions.
--------------------------------------------------------------------

--------------------------------------------------
-- apm_generic_expired
--   input argument '{ "after": "1 year" }'
--   checks only leaf partition, return non-NULL
--   if curprt is expired and should be archived
--------------------------------------------------
CREATE FUNCTION matrixts_internal.apm_generic_expired(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
) RETURNS matrixts_internal._apm_boundary AS $$
DECLARE
  after_str      text;
  after_interval interval;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'rel must not be NULL';
  END IF;
  IF curprt IS NULL THEN
    RAISE EXCEPTION 'curprt must not be NULL';
  END IF;

  IF (NOT curprt.partitionisleaf OR curprt.partitionisleaf IS NULL) THEN
    -- this check only care leaf partitions
    RETURN NULL;
  END IF;

  after_str := args->>'after';
  IF after_str IS NULL OR after_str = '' THEN
    RAISE EXCEPTION 'missing "after" expression';
  END IF;

  BEGIN
    after_interval := after_str::interval;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid "after" expression: %.', after_str;
  END;
  IF after_interval IS NULL THEN
    RAISE EXCEPTION 'Unknown after interval';
  END IF;

  -- 1 means (now() - after_interval) is later than rel->relpartbound
  IF 1 = (SELECT matrixts_internal.apm_eval_partbound(rel::Oid, after_interval, now)) THEN
    RETURN ARRAY[(NULL)]::matrixts_internal._apm_boundary;
  END IF;

  RETURN NULL;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE OR REPLACE FUNCTION matrixts_internal.apm_expired_with_am(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
) RETURNS matrixts_internal._apm_boundary AS $$
DECLARE
  storage_is_not text := args->>'storage_is_not';
  res            matrixts_internal._apm_boundary;
BEGIN
  IF curprt IS NULL THEN
    RAISE EXCEPTION 'Unknown partition';
  END IF;
  IF storage_is_not IS NULL THEN
    RAISE EXCEPTION 'Unknown storage engine';
  END IF;

  IF storage_is_not = curprt.addopt->>'storage' THEN
    -- return if storage not meets
    RETURN NULL;
  END IF;

  RETURN (SELECT matrixts_internal.apm_generic_expired(now, rel, parentrel, rootrel, curprt, allprt, args));
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION matrixts_internal.apm_expired_with_am_set(
    rel         regclass
  , cid         int
  , action_name text
  , args        text
) RETURNS VOID AS $$
DECLARE
  jargs            json;
  after_str        text;
  after_interval   interval;
  storage_str      text;
  input_check_args jsonb;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'argument "rel" must not be NULL.';
  END IF;
  IF action_name IS NULL OR action_name = '' THEN
    RAISE EXCEPTION 'argument "action_name" must not be NULL.';
  END IF;

  IF args IS NULL THEN
    -- branch for deleting apm_rel_policy_action entry
    SET matrix.ts_guc_apm_allow_dml TO ON;
    DELETE FROM matrixts_internal.apm_rel_policy_action
      WHERE reloid = rel::oid AND action = action_name;
    RESET matrix.ts_guc_apm_allow_dml;
    RETURN;
  END IF;

  BEGIN -- check argument must be able to cast to JSON
    jargs := args::json;
    after_str := jargs->>'after';
    storage_str := jargs->>'storage_is_not';
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid argument expression: "%".', args
      USING HINT = 'Example ''{ "after": "7 days", "storage_is_not": "heap" }''';
  END;

  IF after_str IS NULL THEN -- must have after interval
    RAISE EXCEPTION 'Missing "after" expression in "%".', args
      USING HINT = 'Example ''{ "after": "7 days", "storage_is_not": "heap" }''';
  END IF;

  IF storage_str IS NULL THEN -- must have storage_is_not
    RAISE EXCEPTION 'Missing "storage_is_not" expression in "%".', args
      USING HINT = 'Example ''{ "after": "7 days", "storage_is_not": "heap" }''';
  END IF;

  BEGIN -- check "after" must be able to cast to interval
    after_interval := after_str::interval;
    EXCEPTION WHEN OTHERS THEN
      RAISE EXCEPTION 'Invalid "after" expression: %.', after_str
        USING HINT = 'Example ''{ "after": "7 days", "storage_is_not": "heap" }''';
  END;

  input_check_args := ('{ "after": "' || after_str || '", "storage_is_not": "' || storage_str || '" }')::jsonb;

  BEGIN -- Update/Insert to apm_rel_policy_action
    SET matrix.ts_guc_apm_allow_dml TO ON;
    LOCK matrixts_internal.apm_rel_policy_action IN EXCLUSIVE MODE;
    UPDATE matrixts_internal.apm_rel_policy_action
    SET check_args = input_check_args
    WHERE reloid = rel::oid AND action = action_name;
    IF NOT FOUND THEN
      INSERT INTO matrixts_internal.apm_rel_policy_action(reloid, class_id, action, check_args)
        VALUES(rel::oid, cid, action_name, input_check_args);
    END IF;
    RESET matrix.ts_guc_apm_allow_dml;
  END;

  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---- invoke mars.compress_partition(rel)
CREATE FUNCTION matrixts_internal.apm_mars_compress_partition(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
  , check_res matrixts_internal._apm_boundary
) RETURNS BOOLEAN AS $$
DECLARE
  r pg_extension%rowtype;
BEGIN
  SELECT * FROM pg_extension INTO r WHERE extname = 'mars';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'require "mars" extension to perform this action';
  END IF;
  IF string_to_array(r.extversion, '.')::int[] < array[1::int] THEN
    RAISE EXCEPTION 'extension mars % is too old to perform this action', r.extversion
      USING DETAIL = 'ATTENTION: for mars tables created in early version of mars, may need to drop, re-populate those tables after upgrading.',
            HINT = 'upgrade mars with: DROP EXTENSION mars CASCADE; CREATE EXTENSION mars;';
  END IF;

  IF rel IS NULL THEN
    RAISE EXCEPTION 'rel must not be NULL';
  END IF;
  IF check_res IS NULL THEN
    RAISE EXCEPTION 'check_res must not be NULL';
  END IF;

  PERFORM mars.compress_partition(rel);

  RETURN TRUE;
END
$$ LANGUAGE PLPGSQL VOLATILE;

----------------------------------------------------------
-- apm_generic_incoming
--   input argument '{ "before": "3 days" }'
--   checks a non-leaf partition, if it is partitioned by
--   time-based column, then find missing periods of its
--   direct children, which overlapping with
--   now() ~ now() + 'before' interval.
----------------------------------------------------------
CREATE FUNCTION matrixts_internal.apm_generic_incoming(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
) RETURNS matrixts_internal._apm_boundary AS $$
DECLARE
  before_str          text;
  before_interval     interval;
  prt                 matrixts_internal.apm_partition_item;
  fromts              timestamptz;
  tots                timestamptz;
  prt_boundary        matrixts_internal.apm_boundary;
  void_boundary_array matrixts_internal._apm_boundary := ARRAY[('-infinity'::timestamptz,'infinity'::timestamptz, NULL)::matrixts_internal.apm_boundary];
  void_boundary       matrixts_internal.apm_boundary;
  void_fromts         timestamptz;
  void_tots           timestamptz;
  void_tstzrange      tstzrange;
  target_tstzrange    tstzrange;
  res                 matrixts_internal._apm_boundary := '{}'::matrixts_internal._apm_boundary;
BEGIN
  IF now IS NULL THEN
    now := now();
  END IF;
  IF curprt IS NULL THEN
    RAISE EXCEPTION 'curprt must not be NULL';
  END IF;

  IF curprt.partitionisleaf THEN
    -- this check only cares about non-leaf partitions
    RETURN NULL;
  END IF;

  BEGIN
    before_str := args->>'before';
    before_interval := before_str::interval;
  EXCEPTION WHEN OTHERS THEN
    -- FIXME, must before interval ?
    RAISE EXCEPTION 'Invalid "before" expression: %.', before_str;
  END;

  IF before_interval IS NULL THEN
    RAISE EXCEPTION 'Must have "before" expression.';
  END IF;

  fromts := now;
  tots := fromts + before_interval;
  target_tstzrange := ('['||fromts||','||tots||']')::tstzrange;

  BEGIN
    -- TODO, exclude unneeded partition loop
    -- ____|__p0__|__p1__|__now____before n days__|__p3__|__p4__|__
    -- can exclude partition before p1 and after p3
    FOREACH prt IN ARRAY allprt LOOP
      IF prt.partitionisdefault THEN
        CONTINUE;
      END IF;
      IF (curprt.reloid <> prt.parent_reloid OR prt.parent_reloid IS NULL) THEN
        CONTINUE;
      END IF;
      prt_boundary := matrixts_internal.apm_partition_boundary(prt.reloid);
      -- if udf response is NULL, means unsupported partition type, return
      -- immediately.
      IF prt_boundary IS NULL THEN
        RETURN NULL;
      END IF;
      -- exclude part_boundary from void_boundary_array
      void_boundary_array := matrixts_internal._apm_minus(void_boundary_array, prt_boundary);
    END LOOP;
    FOREACH void_boundary IN ARRAY void_boundary_array LOOP
      void_fromts := void_boundary.fromts;
      void_tots := void_boundary.tots;
      IF void_fromts IS NULL THEN
        void_fromts := '-infinity'::timestamptz;
      END IF;
      IF void_tots IS NULL THEN
        void_tots := 'infinity'::timestamptz;
      END IF;
      IF void_tots = now THEN
        -- handle edge
        CONTINUE;
      END IF;
      void_tstzrange := ('['||void_fromts||','||void_tots||']')::tstzrange;
      void_boundary.limitts := tots;
      IF void_tstzrange && target_tstzrange THEN
        res := array_append(res, void_boundary);
      END IF;
    END LOOP;
  END;
  IF (array_length(res, 1) < 1 OR array_length(res, 1) IS NULL) THEN
    -- if no missing period found, should not trigger action
    RETURN NULL;
  END IF;
  RETURN res;
END
$$ LANGUAGE PLPGSQL VOLATILE;

-------------------------------------------------------------
-- apm_generic_older_than
--   Input argument '{ "age": "1 month" }'
--   checks a default partition, find the oldest tuple.
--   If it is older than "age" interval, then return a
--   non-NULL list. The list must have 1 element, starting
--   with the end of the most recent partition which before
--   this default partition, ending with the oldest tuple.
-------------------------------------------------------------
CREATE FUNCTION matrixts_internal.apm_generic_older_than(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
) RETURNS matrixts_internal._apm_boundary AS $$
DECLARE
  void_boundary_array matrixts_internal._apm_boundary := ARRAY[('-infinity'::timestamptz,'infinity'::timestamptz, NULL)::matrixts_internal.apm_boundary];
  res                 matrixts_internal._apm_boundary := '{}'::matrixts_internal._apm_boundary;
  prt_boundary        matrixts_internal.apm_boundary;
  prt                 matrixts_internal.apm_partition_item;
  parttbl             pg_catalog.pg_partitioned_table;
  partkey             name;
  older               boolean;
  age_str             text;
  age_interval        interval;
  table_ident         text := '';
  tup_time            timestamptz;
  x                   timestamptz := '-infinity'::timestamptz;
  y                   timestamptz := 'infinity'::timestamptz;
BEGIN
  IF (rel IS NULL OR parentrel IS NULL OR curprt IS NULL) THEN
    RETURN NULL;
  END IF;
  IF now IS NULL THEN
    now := now();
  END IF;

  -- only split default partition
  IF (curprt.partitionisdefault IS NULL OR NOT curprt.partitionisdefault) THEN
    RETURN NULL;
  END IF;

  BEGIN
    age_str := args->>'age';
    age_interval := age_str::interval;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid "age" expression: %.', age_str;
  END;

  -- query catalog to get the partition key
  SELECT * FROM pg_catalog.pg_partitioned_table
  WHERE partrelid = parentrel::oid INTO parttbl;

  IF parttbl IS NULL THEN
    RAISE EXCEPTION 'no pg_partitioned_table entry found for "%"', rel::name;
  END IF;
  IF parttbl.partnatts > 1 THEN
    RAISE EXCEPTION 'more than 1 partition key is not supported "%"', rel::name;
  END IF;

  -- get name of partition key
  SELECT attname FROM pg_catalog.pg_attribute
  WHERE attrelid = parentrel::oid AND attnum = parttbl.partattrs[0]
  INTO partkey;

  IF (partkey IS NULL OR partkey = '') THEN
    RAISE EXCEPTION 'no partition key found';
  END IF;

  SELECT quote_ident(pn.nspname)::text || '.' || quote_ident(pc.relname)::text
  FROM pg_catalog.pg_namespace pn
  INNER JOIN pg_catalog.pg_class pc
  ON pc.relnamespace = pn.oid WHERE pc.oid = rel::oid
  INTO table_ident;

  IF table_ident IS NULL THEN
    RAISE EXCEPTION 'no such table has OID %.', rel;
  END IF;

  IF curprt.partitionisdefault THEN
    EXECUTE format('SELECT min(%I)::timestamptz, min(%I) < $1 - $2 FROM %s', partkey, partkey, table_ident)
    INTO tup_time, older USING now, age_interval;
  END IF;

  IF tup_time IS NULL THEN
    RETURN NULL;
  END IF;
  IF (NOT older OR older IS NULL) THEN
    -- the default partition is not old enough to trigger split
    RETURN NULL;
  END IF;

  FOREACH prt IN ARRAY allprt LOOP
    IF prt.partitionisdefault THEN
      CONTINUE;
    END IF;
    IF (curprt.parent_reloid <> prt.parent_reloid OR prt.parent_reloid IS NULL) THEN
      -- only sibling of current default partition takes count
      CONTINUE;
    END IF;
    prt_boundary := matrixts_internal.apm_partition_boundary(prt.reloid);
    -- if udf response is NULL, means unsupported partition type, return
    -- immediately.
    IF prt_boundary IS NULL THEN
      RETURN NULL;
    END IF;
    -- find greatest end boundary of existing partitions, which less than tup_time
    IF prt_boundary.tots > x AND prt_boundary.tots <= tup_time THEN
      x := prt_boundary.tots;
    END IF;
    IF prt_boundary.fromts < y AND prt_boundary.fromts > tup_time THEN
      y := prt_boundary.fromts;
    END IF;
  END LOOP;

  res := array_append(res, (x, y, tup_time)::matrixts_internal.apm_boundary);
  RETURN res;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---------------------------------------------
-- Action functions
---------------------------------------------
CREATE FUNCTION matrixts_internal.apm_generic_drop_partition(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
  , check_res matrixts_internal._apm_boundary
) RETURNS BOOLEAN AS $$
DECLARE
  table_ident text;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'rel must not be NULL';
  END IF;
  IF check_res IS NULL THEN
    RAISE EXCEPTION 'check_res must not be NULL';
  END IF;

  SELECT quote_ident(pn.nspname)::text || '.' || quote_ident(pc.relname)::text
  FROM pg_catalog.pg_namespace pn
  INNER JOIN pg_catalog.pg_class pc
  ON pc.relnamespace = pn.oid WHERE pc.oid = rel
  INTO table_ident;

  IF table_ident IS NULL THEN
    RAISE EXCEPTION 'No table to drop for OID %.', rel;
  END IF;

  EXECUTE format('DROP TABLE %s', table_ident);
  RETURN TRUE;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION matrixts_internal._apm_append_partition_internal(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
  , check_res matrixts_internal._apm_boundary
  , dofunc    regproc
) RETURNS BOOLEAN AS $$
DECLARE
  period_str      text;
  period_interval interval;
  prt             matrixts_internal.apm_partition_item;
  boundary        matrixts_internal.apm_boundary;
  fromtstz        timestamptz;
  totstz          timestamptz;
  limittstz       timestamptz;
  lowerborder     timestamptz;
  upperborder     timestamptz;
  has_prt         boolean;
  has_default     boolean;
  schema_ident    text;
  table_ident     text;
  tablename       text;
  partname        text;
  part_key_type   name;
  period_is_valid boolean;
BEGIN
  IF now IS NULL THEN
    now := now();
  END IF;
  IF rel IS NULL THEN
    RAISE EXCEPTION 'rel must not be NULL';
  END IF;
  IF check_res IS NULL THEN
    RAISE EXCEPTION 'check_res must not be NULL';
  END IF;
  IF args IS NULL THEN
    RAISE EXCEPTION 'args must not be NULL';
  END IF;
  IF (array_length(check_res, 1) < 1 OR array_length(check_res, 1) IS NULL) THEN
    RAISE EXCEPTION 'check_res must not be empty';
  END IF;

  EXECUTE 'SELECT matrixts_internal.apm_is_valid_partition_period($1)'
  INTO period_is_valid USING period_interval;
  IF NOT period_is_valid THEN
    RAISE EXCEPTION 'Unsupported "period" expression: %.', period_str;
  END IF;

  BEGIN
    period_str := args->>'period';
    period_interval := period_str::interval;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid "period" expression: %.', period_str;
  END;

  SELECT quote_ident(pn.nspname)::text, quote_ident(pc.relname)::text, pc.relname::text
  FROM pg_catalog.pg_namespace pn
  INNER JOIN pg_catalog.pg_class pc
  ON pc.relnamespace = pn.oid WHERE pc.oid = rel
  INTO schema_ident, table_ident, tablename;

  IF schema_ident IS NULL THEN
    RAISE EXCEPTION 'unknown schema name for OID %.', rel;
  END IF;
  IF table_ident IS NULL THEN
    RAISE EXCEPTION 'unknown table name for OID %.', rel;
  END IF;
  IF tablename IS NULL THEN
    RAISE EXCEPTION 'unknown table name literal for OID %.', rel;
  END IF;

  -- if partition key is 'date', period_interval should no less than '1day'
  SELECT typname FROM pg_partitioned_table ppt
  INNER JOIN pg_attribute pa ON ppt.partrelid = pa.attrelid
  AND pa.attnum = ANY(ppt.partattrs)
  INNER JOIN pg_type pt ON pa.atttypid = pt.oid
  WHERE partrelid = rel
  INTO part_key_type;
  IF part_key_type = 'date' AND period_interval < interval '1 day' THEN
    RAISE NOTICE 'partition key is date and partition period is %s, adjust to 1 day', period_interval;
    period_interval := interval '1 day';
  END IF;

  -- init has_prt/has_default value
  FOREACH prt IN ARRAY allprt LOOP
    IF (curprt.reloid <> prt.parent_reloid OR prt.parent_reloid IS NULL) THEN
      CONTINUE;
    END IF;
    has_prt = true;
    IF prt.partitionisdefault THEN
      has_default = true;
      EXIT;
    END IF;
  END LOOP;

  -- traverse each missing period
  FOREACH boundary IN ARRAY check_res LOOP
    fromtstz := boundary.fromts;
    totstz := boundary.tots;
    limittstz := boundary.limitts;

    IF limittstz IS NULL THEN
      RAISE EXCEPTION 'invalid limitts "%"', boundary;
    END IF;

    now := GREATEST(now, fromtstz);

    EXECUTE ('SELECT matrixts_internal.apm_time_trunc($1, $2::timestamp)::timestamptz')
    INTO lowerborder USING period_interval, now;
    IF lowerborder IS NULL THEN
      RAISE EXCEPTION 'invalid low border "%" "%"', period_interval, now;
    END IF;

    upperborder := LEAST(lowerborder + period_interval, totstz);
    lowerborder := GREATEST(lowerborder, fromtstz);

    -- fill this missing period by creating partitions
    -- may create more than 1 partition if period is smaller than the gap
    LOOP
      IF lowerborder > upperborder THEN
        RAISE EXCEPTION 'invalid borders "%" "%"', lowerborder, upperborder;
      ELSIF lowerborder = upperborder THEN
        EXIT;  -- exit loop special case
      END IF;
      -- create the new partition
      SET datestyle TO 'ISO, MDY';
      IF dofunc IS NULL THEN
        IF has_default THEN
          EXECUTE format('ALTER TABLE %s.%s SPLIT DEFAULT PARTITION START(%L) END(%L)', schema_ident, table_ident, lowerborder, upperborder);
        ELSIF has_prt THEN
          EXECUTE format('ALTER TABLE %s.%s ADD PARTITION START(%L) END(%L)', schema_ident, table_ident, lowerborder, upperborder);
        ELSE
          -- matrixts_internal._unique_partition_name() input unquoted name and output unquoted name
          partname := matrixts_internal._unique_partition_name(tablename);
          EXECUTE format('CREATE TABLE %s.%s PARTITION OF %s.%s FOR VALUES FROM(%L) TO(%L)', schema_ident, quote_ident(partname), schema_ident, table_ident, lowerborder, upperborder);
        END IF;
      ELSE
        EXECUTE format('SELECT %s($1, $2, $3, $4)', dofunc)
          USING rel, lowerborder::timestamp, upperborder::timestamp, period_interval;
      END IF;
      RESET datestyle;
      has_prt = true;

      IF upperborder > totstz THEN
        EXIT; -- exit loop
      END IF;
      IF upperborder > limittstz THEN
        EXIT; -- exit loop
      END IF;

      lowerborder := upperborder;
      upperborder := LEAST(lowerborder + period_interval, totstz);

      IF lowerborder >= upperborder THEN
        EXIT;
      END IF;
    END LOOP;
  END LOOP;
  RETURN TRUE;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---- args '{ "period": "8 hours" }'
CREATE FUNCTION matrixts_internal.apm_generic_append_partition(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
  , check_res matrixts_internal._apm_boundary
) RETURNS boolean AS $$

  SELECT matrixts_internal._apm_append_partition_internal(
    now, rel, parentrel, rootrel, curprt, allprt, args, check_res, NULL::regproc
  );

$$ LANGUAGE SQL VOLATILE;

---- args '{ "period": "8 hours" }'
CREATE FUNCTION matrixts_internal.apm_mars_append_partition(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
  , check_res matrixts_internal._apm_boundary
) RETURNS boolean AS $$
DECLARE
  r      pg_extension%rowtype;
  procid oid;
BEGIN
  SELECT * FROM pg_extension WHERE extname = 'mars' INTO r;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'require "mars" extension to perform this action';
  END IF;

  SELECT pp.oid INTO procid FROM pg_proc pp
  INNER JOIN pg_namespace pn ON pp.pronamespace = pn.oid
  WHERE pp.proname = 'add_partition'
  AND pn.nspname = 'mars'
  AND pp.pronargs = 4;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'function mars.add_partition is not found';
  END IF;

  RETURN (SELECT matrixts_internal._apm_append_partition_internal(
    now, rel, parentrel, rootrel, curprt, allprt, args, check_res, procid::regproc
  ));
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION matrixts_internal.apm_generic_split_default_partition(
    now       timestamptz
  , rel       regclass
  , parentrel regclass
  , rootrel   regclass
  , curprt    matrixts_internal.apm_partition_item
  , allprt    matrixts_internal._apm_partition_item
  , args      json
  , check_res matrixts_internal._apm_boundary
) RETURNS BOOLEAN AS $$
DECLARE
  period_str      text;
  period_interval interval;
  fromtstz        timestamptz;
  totstz          timestamptz;
  limittstz       timestamptz;
  lowerborder     timestamptz;
  upperborder     timestamptz;
  table_ident     text;
  period_is_valid boolean;
BEGIN
  -- input checks
  IF rel IS NULL THEN
    RAISE EXCEPTION 'rel must not be NULL';
  END IF;
  IF parentrel IS NULL THEN
    RAISE EXCEPTION 'parentrel must not be NULL';
  END IF;
  IF check_res IS NULL THEN
    RAISE EXCEPTION 'check_res must not be NULL';
  END IF;
  IF args IS NULL THEN
    RAISE EXCEPTION 'args must not be NULL';
  END IF;
  IF (array_length(check_res, 1) <> 1 OR array_length(check_res, 1) IS NULL) THEN
    RAISE EXCEPTION 'check_res must have only 1 element';
  END IF;

  BEGIN
    period_str := args->>'period';
    period_interval := period_str::interval;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid "period" expression: %.', period_str;
  END;

  EXECUTE 'SELECT matrixts_internal.apm_is_valid_partition_period($1)'
  INTO period_is_valid USING period_interval;
  IF NOT period_is_valid THEN
    RAISE EXCEPTION 'Unsupported "period" expression: %.', period_str;
  END IF;

  fromtstz := check_res[1].fromts;
  totstz := check_res[1].tots;
  limittstz := check_res[1].limitts;
  IF fromtstz IS NULL THEN
    RAISE EXCEPTION 'invalid "FROM" timestamptz %', check_res;
  END IF;
  IF totstz IS NULL THEN
    RAISE EXCEPTION 'invalid "TO" timestamptz %', check_res;
  END IF;
  IF (limittstz IS NULL OR limittstz = '-infinity'::timestamptz OR limittstz = 'infinity'::timestamptz) THEN
    RAISE EXCEPTION 'invalid "LIMIT" timestamptz %', check_res;
  END IF;

  EXECUTE ('SELECT matrixts_internal.apm_time_trunc($1, $2::timestamp)::timestamptz')
  INTO lowerborder USING period_interval, limittstz;

  IF lowerborder IS NULL THEN
    RAISE EXCEPTION 'invalid low border "%" "%"', period_interval, limittstz;
  END IF;

  -- handle edge
  IF lowerborder + period_interval = limittstz THEN
    lowerborder := limittstz;
  END IF;

  upperborder := LEAST(lowerborder + period_interval, totstz);
  lowerborder := GREATEST(lowerborder, fromtstz);

  IF lowerborder >= upperborder THEN
    RAISE EXCEPTION 'invalid borders "%" "%"', lowerborder, upperborder;
  END IF;

  SELECT quote_ident(pn.nspname)::text || '.' || quote_ident(pc.relname)::text
  FROM pg_catalog.pg_namespace pn
  INNER JOIN pg_catalog.pg_class pc
  ON pc.relnamespace = pn.oid WHERE pc.oid = parentrel
  INTO table_ident;

  IF table_ident IS NULL THEN
    RAISE EXCEPTION 'no table to alter for OID %.', parentrel;
  END IF;

  SET datestyle TO 'ISO, MDY'; -- workaround CST conflict
  EXECUTE format('ALTER TABLE %s SPLIT DEFAULT PARTITION START(%L) END(%L)', table_ident, lowerborder, upperborder);
  RESET datestyle;

  RETURN TRUE;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---------------------------------------------
-- Set functions
---------------------------------------------

---- SELECT set_policy_action('foo', 'retention', '1 year');
CREATE FUNCTION matrixts_internal.apm_generic_expired_set(
    rel         regclass
  , cid         int
  , action_name text
  , args        text
) RETURNS VOID AS $$
DECLARE
  after_interval interval;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'argument "rel" must not be NULL.';
  END IF;
  IF cid IS NULL THEN
    RAISE EXCEPTION 'argument "cid" must not be NULL.';
  END IF;
  IF action_name IS NULL OR action_name = '' THEN
    RAISE EXCEPTION 'argument "action_name" must not be NULL.';
  END IF;

  IF args IS NULL THEN
    -- branch for deleting apm_rel_policy_action entry
    SET matrix.ts_guc_apm_allow_dml TO ON;
    DELETE FROM matrixts_internal.apm_rel_policy_action
      WHERE reloid = rel::oid AND action = action_name AND class_id = cid;
    RESET matrix.ts_guc_apm_allow_dml;
    RETURN;
  END IF;

  BEGIN -- check input param must be able to cast to interval
    after_interval := args::interval;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid interval expression: "%".', args
      USING HINT = 'Example ''1 year''';
  END;

  BEGIN -- Update/Insert to apm_rel_policy_action
    SET matrix.ts_guc_apm_allow_dml TO ON;
    LOCK matrixts_internal.apm_rel_policy_action IN EXCLUSIVE MODE;
    UPDATE matrixts_internal.apm_rel_policy_action
      SET check_args = ('{ "after": "' || args || '" }')::jsonb
      WHERE reloid = rel::oid AND action = action_name AND class_id = cid;
    IF NOT FOUND THEN
      INSERT INTO matrixts_internal.apm_rel_policy_action(reloid, class_id, action, check_args)
        VALUES(rel::oid, cid, action_name, ('{ "after": "' || args || '" }')::jsonb);
    END IF;
    RESET matrix.ts_guc_apm_allow_dml;
  END;

  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---- SELECT set_policy_action('foo', 'auto_create', '{ "before": "3 days", "period": "8 hour" }');
CREATE FUNCTION matrixts_internal.apm_generic_incoming_set(
    rel         regclass
  , cid         int
  , action_name text
  , args        text
) RETURNS VOID AS $$
DECLARE
  jargs            json;
  before_str       text;
  before_interval  interval;
  input_check_args jsonb;
  period_str       text;
  period_interval  interval;
  period_is_valid  boolean;
  input_act_args   jsonb;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'argument "rel" must not be NULL.';
  END IF;
  IF cid IS NULL THEN
    RAISE EXCEPTION 'argument "cid" must not be NULL.';
  END IF;
  IF action_name IS NULL OR action_name = '' THEN
    RAISE EXCEPTION 'argument "action_name" must not be NULL.';
  END IF;

  IF args IS NULL THEN
    -- branch for deleting apm_rel_policy_action entry
    SET matrix.ts_guc_apm_allow_dml TO ON;
    DELETE FROM matrixts_internal.apm_rel_policy_action
      WHERE reloid = rel::oid AND action = action_name AND class_id = cid;
    RESET matrix.ts_guc_apm_allow_dml;
    RETURN;
  END IF;

  BEGIN -- check argument must be able to cast to JSON
    jargs := args::json;
    before_str := jargs->>'before';
    period_str := jargs->>'period';
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid argument expression: "%".', args
      USING HINT = 'Example ''{ "before": "3 days", "period": "8 hour" }''';
  END;

  BEGIN -- check "before" must be able to cast to interval
    before_interval := before_str::interval;
    input_check_args := ('{ "before": "' || before_str || '" }')::jsonb;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid "before" expression: %.', before_str
      USING HINT = 'Example ''{ "before": "3 days", ... }''';
  END;

  BEGIN -- check "period" must be able to cast to interval
    period_interval := period_str::interval;
    input_act_args := ('{ "period": "' || period_str || '" }')::jsonb;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid "period" expression: %.', period_str
      USING HINT = 'Example ''{ "period": "8 hours", ... }''';
  END;

  EXECUTE 'SELECT matrixts_internal.apm_is_valid_partition_period($1)'
  INTO period_is_valid USING period_interval;
  IF NOT period_is_valid THEN
    RAISE EXCEPTION 'Unsupported "period" expression: %.', period_str;
  END IF;

  BEGIN -- Update/Insert to apm_rel_policy_action
    SET matrix.ts_guc_apm_allow_dml TO ON;
    LOCK matrixts_internal.apm_rel_policy_action IN EXCLUSIVE MODE;
    UPDATE matrixts_internal.apm_rel_policy_action
      SET check_args = COALESCE(input_check_args, check_args)
        , act_args = COALESCE(input_act_args, act_args)
      WHERE reloid = rel::oid AND action = action_name AND class_id = cid;
    IF NOT FOUND THEN
      INSERT INTO matrixts_internal.apm_rel_policy_action(reloid, class_id, action, check_args, act_args)
        VALUES(rel::oid, cid, action_name, input_check_args, input_act_args);
    END IF;
    RESET matrix.ts_guc_apm_allow_dml;
  END;

  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---- SELECT set_policy_action('foo', 'auto_split', '{ "age": "1 month", "period": "1 month" }');
CREATE FUNCTION matrixts_internal.apm_generic_split_set(
    rel         regclass
  , cid         int
  , action_name text
  , args        text
) RETURNS VOID AS $$
DECLARE
  jargs            json;
  age_str          text;
  age_interval     interval;
  input_check_args jsonb;
  period_str       text;
  period_interval  interval;
  period_is_valid  boolean;
  input_act_args   jsonb;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'argument "rel" must not be NULL.';
  END IF;
  IF cid IS NULL THEN
    RAISE EXCEPTION 'argument "cid" must not be NULL.';
  END IF;
  IF action_name IS NULL OR action_name = '' THEN
    RAISE EXCEPTION 'argument "action_name" must not be NULL.';
  END IF;

  IF args IS NULL THEN
    -- branch for deleting apm_rel_policy_action entry
    SET matrix.ts_guc_apm_allow_dml TO ON;
    DELETE FROM matrixts_internal.apm_rel_policy_action
      WHERE reloid = rel::oid AND action = action_name AND class_id = cid;
    RESET matrix.ts_guc_apm_allow_dml;
    RETURN;
  END IF;

  BEGIN -- check argument must be able to cast to JSON
    jargs := args::json;
    age_str := jargs->>'age';
    period_str := jargs->>'period';
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid argument expression: "%".', args
      USING HINT = 'Example ''{ "age": "1 month", "period": "1 month" }''';
  END;

  BEGIN -- check "age" must be able to cast to interval
    age_interval := age_str::interval;
    input_check_args := ('{ "age": "' || age_str || '" }')::jsonb;
  EXCEPTION WHEN OTHERS THEN
    IF age_str IS NOT NULL THEN
      RAISE EXCEPTION 'Invalid "age" expression: %.', age_str
        USING HINT = 'Example ''{ "age": "1 month", ... }''';
    END IF;
  END;

  BEGIN -- check "period" must be able to cast to interval
    period_interval := period_str::interval;
    input_act_args := ('{ "period": "' || period_str || '" }')::jsonb;
  EXCEPTION WHEN OTHERS THEN
    IF period_str IS NOT NULL THEN
      RAISE EXCEPTION 'Invalid "period" expression: %.', period_str
        USING HINT = 'Example ''{ "period": "1 month", ... }''';
    END IF;
  END;

  EXECUTE 'SELECT matrixts_internal.apm_is_valid_partition_period($1)'
  INTO period_is_valid USING period_interval;
  IF NOT period_is_valid THEN
    RAISE EXCEPTION 'Unsupported "period" expression: %.', period_str;
  END IF;

  BEGIN -- Update/Insert to apm_rel_policy_action
    SET matrix.ts_guc_apm_allow_dml TO ON;
    LOCK matrixts_internal.apm_rel_policy_action IN EXCLUSIVE MODE;
    UPDATE matrixts_internal.apm_rel_policy_action
      SET check_args = COALESCE(input_check_args, check_args)
        , act_args = COALESCE(input_act_args, act_args)
      WHERE reloid = rel::oid AND action = action_name AND class_id = cid;
    IF NOT FOUND THEN
      INSERT INTO matrixts_internal.apm_rel_policy_action(reloid, class_id, action, check_args, act_args)
        VALUES(rel::oid, cid, action_name, input_check_args, input_act_args);
    END IF;
    RESET matrix.ts_guc_apm_allow_dml;
  END;

  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION matrixts_internal.apm_mars_guard(
    rel         regclass
  , policy_name text
  , args        jsonb
) RETURNS VOID AS $$
DECLARE
  r       pg_extension%rowtype;
  relname name;
BEGIN
  SELECT * FROM pg_extension INTO r WHERE extname = 'mars';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'require "mars" extension to use % policy', policy_name;
  END IF;
  IF string_to_array(r.extversion, '.')::int[] < array[1::int] THEN
    RAISE EXCEPTION 'extension mars % is too old to use % policy', r.extversion, policy_name
      USING DETAIL = 'ATTENTION: for mars tables created in early version of mars, may need to drop, re-populate those tables after upgrading.',
            HINT = 'upgrade mars with: DROP EXTENSION mars CASCADE; CREATE EXTENSION mars;';
  END IF;

  -- fetch relation namespace and relname
  SELECT pg_class.relname FROM pg_class
  WHERE pg_class.oid = rel INTO relname;

  BEGIN
    PERFORM mars.fetch_template_table_name(rel::oid)::regclass;
    PERFORM FORMAT('mars.%I', relname)::regclass;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION '% is not a time-series partition table.', rel
      USING DETAIL = 'please refer to documentation on how to use following function',
            HINT = format('execute mars.build_timeseries_table(%L, ...); first', rel);
  END;
  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---------------------------------------------
-- Populate builtin policy for logs
---------------------------------------------
SET matrix.ts_guc_apm_allow_dml TO ON;
--------------------------------------------------------------------------------
-- Builtin policy class #1: auto_splitting
--  * This policy is a good example for using APM framework, also it applies
--    to apm_operation_log and apm_action_log.
--  * These log tables are partitioned by timestamp, has default partition 'hot'.
--  * Initially, all data insert into the 'hot' partition.
--  * Check func seek for first(ts) in the 'hot' partition,
--    when first(ts) < now() - 'age', then call action_func.
--  * Action func splits 'hot' partition into a range partition with specified period.
--  * A "retention" rule checks boundary of existing partitions, when out of
--    1 year, it drops that partition. This action is disabled by default.
--  * This policy is designed for log tables, which focusing on store any incoming
--    logs, infrequently access on historical data.
INSERT INTO matrixts_internal.apm_policy_class(class_name, list_func, version)
VALUES('auto_splitting', 'matrixts_internal.apm_generic_list_partitions'::regproc, '1.0');
INSERT INTO matrixts_internal.apm_policy_action(class_id, action, seq, default_disabled,
default_check_func, default_check_args, default_act_func, default_act_args, set_func, version)
VALUES
(
  1, 'retention', 1, TRUE,
  'matrixts_internal.apm_generic_expired'::regproc, '{ "after": "1 year" }',
  'matrixts_internal.apm_generic_drop_partition'::regproc, '{}',
  'matrixts_internal.apm_generic_expired_set'::regproc, '1.0'
),
(
  1, 'auto_split', 2, FALSE,
  'matrixts_internal.apm_generic_older_than'::regproc, '{ "age": "1 month 2days" }',
  'matrixts_internal.apm_generic_split_default_partition'::regproc, '{ "period": "1 month" }',
  'matrixts_internal.apm_generic_split_set', '1.0'
);

--------------------------------------------------------------------------------
-- Builtin policy class #2: auto_partitioning
--  * This policy is for generic partitioned table without default partition.
--  * A "retention" rule checks boundary of existing partitions, when out of
--    1 year, it drops that partition. This action is disabled by default.
--  * An "auto_create" action makes sure any time with range [now()-1day ~ now()+3days)
--    can be inserted into a partition. It checks existing partitions with above range
--    and find "holes", then create partition to full-fill holes.
--  * This policy works on traditional heap/ao/aoco storage.
--  * This policy only automates management of partitioned table, but not support
--    polymorphic storage.
INSERT INTO matrixts_internal.apm_policy_class(class_name, list_func, version)
VALUES('auto_partitioning', 'matrixts_internal.apm_generic_list_partitions'::regproc, '1.0');
INSERT INTO matrixts_internal.apm_policy_action(class_id, action, seq, default_disabled,
default_check_func, default_check_args, default_act_func, default_act_args, set_func, version)
VALUES
(
  2, 'retention', 1, TRUE,
  'matrixts_internal.apm_generic_expired'::regproc, '{ "after": "1 year" }',
  'matrixts_internal.apm_generic_drop_partition'::regproc, '{}',
  'matrixts_internal.apm_generic_expired_set'::regproc, '1.0'
),
(
  2, 'auto_create', 2, FALSE,
  'matrixts_internal.apm_generic_incoming'::regproc, '{ "before": "3 days" }',
  'matrixts_internal.apm_generic_append_partition'::regproc, '{ "period": "8 hours" }',
  'matrixts_internal.apm_generic_incoming_set', '1.0'
);
--------------------------------------------------------------------------------
-- Builtin policy class #3: mars_time_series
--  * This policy is recommended for time-series data, it uses heap as hot partition
--    and mars for cold data. The initial root table must be heap with range
--    partition on a time column.
--  * An auto create action makes sure any time with range [now()-1day ~ now()+3days)
--    can be inserted into a partition. It checks existing partitions with above range
--    and find "holes", then create partition to full-fill holes.
--  * An auto compress action migrates cold data to mars partition. It checks non-mars
--    partitions boundary, when out of 7 days, it creates a mars partition from
--    template and copy-sort-compress to mars table, finally exchange the cold
--    partition with mars sub-table.
--  * An auto retention rule checks boundary of existing partitions, when out of
--    1 year, it drops that partition. This action is disabled by default.
--  * This policy requires mars extension.
INSERT INTO matrixts_internal.apm_policy_class(class_name, list_func, validate_func, version)
VALUES('mars_time_series', 'matrixts_internal.apm_list_partition_with_am'::regproc, 'matrixts_internal.apm_mars_guard'::regproc, '1.0');
INSERT INTO matrixts_internal.apm_policy_action(class_id, action, seq, default_disabled,
                                                default_check_func, default_check_args, default_act_func, default_act_args, set_func, version)
VALUES
(
  3, 'retention', 1, TRUE,
  'matrixts_internal.apm_generic_expired'::regproc, '{ "after": "1 year" }',
  'matrixts_internal.apm_generic_drop_partition'::regproc, '{}',
  'matrixts_internal.apm_generic_expired_set'::regproc, '1.0'
),
(
  3, 'auto_create', 2, FALSE,
  'matrixts_internal.apm_generic_incoming'::regproc, '{ "before": "3 days" }',
  'matrixts_internal.apm_mars_append_partition'::regproc, '{ "period": "8 hours" }',
  'matrixts_internal.apm_generic_incoming_set', '1.0'
),
(
  3, 'compress', 3, FALSE,
  'matrixts_internal.apm_expired_with_am'::regproc, '{ "after": "7 days", "storage_is_not": "mars" }',
  'matrixts_internal.apm_mars_compress_partition'::regproc, '{}',
  'matrixts_internal.apm_expired_with_am_set'::regproc, '1.0'
);
RESET matrix.ts_guc_apm_allow_dml;

---------------------------------------------------
-- Event trigger to protect catalog structure
---------------------------------------------------
CREATE OR REPLACE FUNCTION matrixts_internal._apm_schema_protect()
RETURNS event_trigger
AS 'MODULE_PATHNAME', 'apm_schema_protect'
LANGUAGE C STRICT;

CREATE EVENT TRIGGER apm_protect_event_trigger ON ddl_command_start
EXECUTE PROCEDURE matrixts_internal._apm_schema_protect();

---------------------------------------------------
-- User API
--  * set_policy()
--  * drop_policy()
---------------------------------------------------
CREATE FUNCTION public.set_policy(
    rel         regclass
  , policy_name text
) RETURNS VOID AS $$
DECLARE
  cid     int;
  cname   name;
  rkind   "char";
  rispart boolean;
  valfunc regproc;
  valargs jsonb;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'argument "rel" must not be NULL.';
  END IF;
  IF policy_name IS NULL OR policy_name = '' THEN
    RAISE EXCEPTION 'argument "policy_name" must not be NULL or empty.';
  END IF;

  -- Check if rel is valid for partitioning
  SELECT relkind, relispartition INTO rkind, rispart
  FROM pg_catalog.pg_class WHERE oid = rel::oid;
  IF rispart THEN
    -- FIXME: multi-level partition is not supported yet.
    RAISE EXCEPTION 'Cannot set_policy to a partition child table.';
  END IF;
  IF (rkind <> 'p' OR rkind IS NULL) THEN
    RAISE EXCEPTION 'Only partitioned table can set_policy.';
  END IF;

  -- Get class_id from policy_name
  EXECUTE format('SELECT class_id, validate_func, validate_args FROM matrixts_internal.apm_policy_class WHERE class_name = $1')
  INTO cid, valfunc, valargs USING policy_name;
  IF cid IS NULL THEN
    RAISE EXCEPTION 'No such policy "%"', policy_name;
  END IF;

  -- Check if rel has a policy already
  LOCK matrixts_internal.apm_rel_policy IN EXCLUSIVE MODE;
  SELECT pc.class_name INTO cname FROM matrixts_internal.apm_rel_policy rp
  INNER JOIN matrixts_internal.apm_policy_class pc
  ON pc.class_id = rp.class_id WHERE rp.reloid = rel::oid;
  IF FOUND THEN
    IF cname = policy_name THEN
      RAISE WARNING 'Table "%" already has the policy "%", skipping...', rel::name, policy_name;
      RETURN;
    ELSE
      RAISE EXCEPTION 'Table "%" already has a policy class "%"', rel::name, cname
        USING HINT = format('Only 1 policy per table allowed on this version. '
        || 'Run "SELECT drop_policy(%L::regclass)" to remove current policy.', rel);
    END IF;
  END IF;

  -- Customized checking provided by policy's validate_func
  IF valfunc IS NOT NULL THEN
    -- Raise exception inside validate_func to terminate set_policy()
    EXECUTE format('SELECT %s($1, $2, $3)', valfunc)
      USING rel, policy_name, valargs;
  END IF;

  BEGIN
    SET matrix.ts_guc_apm_allow_dml TO ON;
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, operation)
    VALUES (rel::oid, rel::regclass::name, cid, policy_name, 'register');
    INSERT INTO matrixts_internal.apm_rel_policy(reloid, class_id) VALUES (rel, cid);
    RESET matrix.ts_guc_apm_allow_dml;
  END;
  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION public.drop_policy(
  rel regclass
) RETURNS VOID AS $$
DECLARE
  m RECORD;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'argument "rel" must not be NULL.';
  END IF;

  LOCK matrixts_internal.apm_rel_policy IN EXCLUSIVE MODE;
  FOR m IN SELECT pc.class_id, pc.class_name FROM matrixts_internal.apm_rel_policy rp INNER JOIN matrixts_internal.apm_policy_class pc ON pc.class_id = rp.class_id WHERE rp.reloid = rel::oid LOOP
    BEGIN
      SET matrix.ts_guc_apm_allow_dml TO ON;
      DELETE FROM matrixts_internal.apm_rel_policy_action WHERE reloid = rel::oid AND class_id = m.class_id;
      DELETE FROM matrixts_internal.apm_rel_policy WHERE reloid = rel::oid AND class_id = m.class_id;
      INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, operation)
      VALUES (rel::oid, rel::regclass::name, m.class_id, m.class_name, 'unregister');
      RESET matrix.ts_guc_apm_allow_dml;
    END;
  END LOOP;

  IF NOT FOUND THEN
    RAISE WARNING 'Table "%" has no policy, skipping...', rel::name;
  END IF;
  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION public.drop_policy(
    rel         regclass
  , policy_name text
) RETURNS VOID AS $$
DECLARE
  cid   int;
  cname text;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'argument "rel" must not be NULL.';
  END IF;
  IF policy_name IS NULL THEN
    RAISE NOTICE 'ignoring NULL argument "policy_name"';
    SELECT public.drop_policy(rel);
    RETURN;
  END IF;

  LOCK matrixts_internal.apm_rel_policy IN EXCLUSIVE MODE;
  SELECT pc.class_id, pc.class_name INTO cid, cname
  FROM matrixts_internal.apm_rel_policy rp
  INNER JOIN matrixts_internal.apm_policy_class pc
  ON pc.class_id = rp.class_id
  WHERE rp.reloid = rel::oid AND pc.class_name = policy_name LIMIT 1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Table "%" does not have policy "%", skipping...', rel::name, policy_name;
  END IF;

  BEGIN
    SET matrix.ts_guc_apm_allow_dml TO ON;
    DELETE FROM matrixts_internal.apm_rel_policy_action WHERE reloid = rel::oid AND class_id = cid;
    DELETE FROM matrixts_internal.apm_rel_policy WHERE reloid = rel::oid AND class_id = cid;
    INSERT INTO matrixts_internal.apm_operation_log(reloid, relname, class_id, class_name, operation)
    VALUES (rel::oid, rel::regclass::name, cid, cname, 'unregister');
    RESET matrix.ts_guc_apm_allow_dml;
  END;
  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---------------------------------------------------
-- User API
--  * set_policy_action()
---------------------------------------------------
CREATE FUNCTION public.set_policy_action(
    rel         regclass
  , action_name text
  , args        text
) RETURNS VOID AS $$
DECLARE
  cid     int;
  cname   name;
  setfunc regproc;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'argument "rel" must not be NULL.';
  END IF;
  IF action_name IS NULL OR action_name = '' THEN
    RAISE EXCEPTION 'argument "action_name" must not be NULL or empty.';
  END IF;

  SELECT pc.class_id, pc.class_name INTO cid, cname
  FROM matrixts_internal.apm_rel_policy rp
  INNER JOIN matrixts_internal.apm_policy_class pc
  ON pc.class_id = rp.class_id WHERE rp.reloid = rel::oid;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Table "%" does not have a policy', rel::name;
    RETURN;
  END IF;

  SELECT pa.set_func INTO setfunc
  FROM matrixts_internal.apm_policy_action pa
  WHERE pa.class_id = cid AND pa.action = action_name;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Policy "%" does not have action "%"', cname, action_name;
    RETURN;
  END IF;

  EXECUTE format('SELECT %s($1, $2, $3, $4)', setfunc)
    USING rel, cid, action_name, args;
  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

---------------------------------------------------
-- User API
--  * disable_policy_action()
--  * enable_policy_action()
---------------------------------------------------
CREATE FUNCTION public.disable_policy_action(
    rel         regclass
  , action_name text
) RETURNS VOID AS $$
BEGIN
  EXECUTE public.set_policy_action(rel, action_name, true);
  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION public.enable_policy_action(
    rel         regclass
  , action_name text
) RETURNS VOID AS $$
BEGIN
  EXECUTE public.set_policy_action(rel, action_name, false);
  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

CREATE FUNCTION public.set_policy_action(
    rel         regclass
  , action_name text
  , in_disabled boolean
) RETURNS VOID AS $$
DECLARE
  cid     int;
  cname   name;
  setfunc regproc;
BEGIN
  IF rel IS NULL THEN
    RAISE EXCEPTION 'argument "rel" must not be NULL.';
  END IF;
  IF action_name IS NULL OR action_name = '' THEN
    RAISE EXCEPTION 'argument "action_name" must not be NULL or empty.';
  END IF;
  IF in_disabled IS NULL THEN
    RAISE EXCEPTION 'argument "in_disabled" must not be NULL.';
  END IF;

  LOCK matrixts_internal.apm_rel_policy_action IN EXCLUSIVE MODE;
  SELECT pc.class_id, pc.class_name INTO cid, cname
  FROM matrixts_internal.apm_rel_policy rp
  INNER JOIN matrixts_internal.apm_policy_class pc
  ON pc.class_id = rp.class_id WHERE rp.reloid = rel::oid;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Table "%" does not have a policy', rel::name;
    RETURN;
  END IF;

  SELECT pa.set_func INTO setfunc
  FROM matrixts_internal.apm_policy_action pa
  WHERE pa.class_id = cid AND pa.action = action_name;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Policy "%" does not have action "%"', cname, action_name;
    RETURN;
  END IF;

  BEGIN -- Update/Insert to apm_rel_policy_action
    SET matrix.ts_guc_apm_allow_dml TO ON;
    UPDATE matrixts_internal.apm_rel_policy_action
      SET disabled = in_disabled
      WHERE reloid = rel::oid AND action = action_name AND class_id = cid;
    IF NOT FOUND THEN
      INSERT INTO matrixts_internal.apm_rel_policy_action(reloid, class_id, action, disabled)
        VALUES(rel::oid, cid, action_name, in_disabled);
    END IF;
    RESET matrix.ts_guc_apm_allow_dml;
  END;

  RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

-----------------------------------------------
-- View public.apm_settings list settings
-----------------------------------------------
CREATE VIEW public.apm_settings AS(
  SELECT
    rp.reloid
    , rp.reloid::regclass::name AS relname
    , pc.class_id
    , pc.class_name
    , pa.action
    , pa.seq
    , COALESCE(disabled, default_disabled) AS disabled
    , COALESCE(rpa.check_func, pa.default_check_func) AS check_func
    , COALESCE(rpa.check_args, pa.default_check_args) AS check_args
    , COALESCE(rpa.act_func, pa.default_act_func) AS act_func
    , COALESCE(rpa.act_args, pa.default_act_args) AS act_args
    , COALESCE(rpa.version,pa.version) AS version
  FROM matrixts_internal.apm_policy_class pc
    INNER JOIN matrixts_internal.apm_policy_action pa
    ON pc.class_id = pa.class_id
    INNER JOIN matrixts_internal.apm_rel_policy rp
    ON pc.class_id = rp.class_id
    INNER JOIN pg_catalog.pg_class pg
    ON pg.oid = rp.reloid
    LEFT JOIN matrixts_internal.apm_rel_policy_action rpa
    ON rpa.reloid = rp.reloid
    AND rpa.class_id = rpa.class_id
    AND rpa.action = pa.action
);

---------------------------------------------------------
-- public.list_policy list settings of single relation
---------------------------------------------------------
CREATE FUNCTION public.list_policy(rel regclass)
RETURNS TABLE(
    reloid     oid
  , relname    name
  , class_id   int
  , class_name name
  , action     name
  , seq        int
  , disabled   boolean
  , check_func regproc
  , check_args jsonb
  , act_func   regproc
  , act_args   jsonb
  , version    name
) AS $$

  SELECT * FROM public.apm_settings
  WHERE reloid = rel::oid;

$$ LANGUAGE SQL VOLATILE;

RESET client_min_messages;

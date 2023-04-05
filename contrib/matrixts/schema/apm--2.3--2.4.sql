-- APM updates for matrixts--2.3--2.4

-- Add a column to apm_action_log to indicate who triggered this action.
SET matrix.ts_guc_apm_allow_ddl TO ON;
ALTER TABLE matrixts_internal.apm_action_log ADD COLUMN client TEXT DEFAULT 'schedule' NOT NULL;
RESET matrix.ts_guc_apm_allow_ddl;

-- unique_partition_name input unquoted name because we need to measure the
-- length of name no larger than max_identifier_length
CREATE OR REPLACE FUNCTION matrixts_internal._unique_partition_name(tablename text)
RETURNS text AS $$
DECLARE
  max_identifier_length int;
  max_cut_length        int;
  cut_length            int;
  part_ident            text;
BEGIN
  IF tablename IS NULL OR tablename = '' THEN
    RAISE EXCEPTION 'Table name should not be NULL or empty';
  END IF;
  -- generate name of the new partition
  -- the name of new partition is based on parent table's name, and not
  -- conflict with existing relations in pg_class
  -- consider multi-byte table name, also need to ensure octet_length of
  -- the generated table name <= max_identifier_length
  SELECT setting::int INTO max_identifier_length FROM pg_settings WHERE "name" = 'max_identifier_length';
  max_cut_length := max_identifier_length - 8;
  cut_length := max_cut_length;
  LOOP
    IF cut_length <= 0 THEN
      -- impossible
      RAISE EXCEPTION 'cannot generate partition table name for %', tablename;
    END IF;
    IF octet_length(substring(tablename FROM 1 FOR cut_length)) > max_cut_length THEN
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
    IF octet_length(part_ident) > max_identifier_length THEN
      cut_length := cut_length - 1;
    ELSE
      EXIT;
    END IF;
  END LOOP;
  RETURN part_ident;
END
$$ LANGUAGE PLPGSQL VOLATILE;

-- expand auto_partitioning policy to create partition with given storage type
---- SELECT set_policy_action('foo', 'auto_create', '{ "before": "3 days", "period": "8 hour", "storage_type": "sortheap" }');
---- "storage_type" is a new option, and the default value is "heap".
CREATE OR REPLACE FUNCTION matrixts_internal.apm_generic_incoming_set(
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
  storage_type     text;
  storage_type_am  text;
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
    storage_type := jargs->>'storage_type';
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid argument expression: "%".', args
      USING HINT = 'Example ''{ "before": "3 days", "period": "8 hour", "storage_type": "sortheap" }''';
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

  IF storage_type IS NOT NULL AND storage_type != '' THEN
    EXECUTE 'SELECT amname FROM pg_am WHERE amname = $1' INTO storage_type_am USING storage_type;
    IF storage_type_am IS NULL THEN
      RAISE EXCEPTION 'Invalid storage_type: %.', storage_type
        USING HINT = format('Don''t forget to CREATE EXTENSION for %s', storage_type);
    END IF;
    input_act_args := COALESCE(input_act_args, '{}'::jsonb) || ('{ "storage_type": "' || storage_type_am || '" }')::jsonb;
  END IF;

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
        , act_args = COALESCE(act_args, '{}'::jsonb) || COALESCE(input_act_args, '{}'::jsonb)
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

-- support storage_type
CREATE OR REPLACE FUNCTION matrixts_internal._apm_append_partition_internal(
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
  storage_type    text;
  storage_type_am text;
  defprt_am       text;
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

  BEGIN
    storage_type := args->>'storage_type';
    IF storage_type IS NOT NULL THEN -- backward compatibility
      EXECUTE 'SELECT amname FROM pg_am WHERE amname = $1' INTO storage_type_am USING storage_type;
      IF storage_type_am IS NULL THEN
        RAISE EXCEPTION 'Invalid storage_type: %.', storage_type
          USING HINT = format('Don''t forget to CREATE EXTENSION for %s', storage_type);
      END IF;
    END IF;
  EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Invalid storage_type: %.', storage_type;
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
      SELECT amname INTO defprt_am
      FROM pg_am pa INNER JOIN pg_class pc ON pc.relam = pa.oid
      WHERE pc.oid = prt.reloid;
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
          -- if the table has a default partition, we have to use ALTER TABLE ... SPLIT DEFAULT PARTITION
          -- to spawn new partitions, otherwise may lead to constraint error.
          IF storage_type IS NULL THEN
            -- if no storage_type is specified, it is safe to split the default
            -- partition without bothering about storage of new partition, it is
            -- always same with the default partition.
            EXECUTE format('ALTER TABLE %s.%s SPLIT DEFAULT PARTITION START(%L) END(%L)', schema_ident, table_ident, lowerborder, upperborder);
          ELSE
            IF defprt_am = storage_type THEN
              -- if the expected storage type is equal to default partition, we
              -- are cool to proceed.
              EXECUTE format('ALTER TABLE %s.%s SPLIT DEFAULT PARTITION START(%L) END(%L)', schema_ident, table_ident, lowerborder, upperborder);
            ELSE
              -- FIXME: for now we still split the default partition, maybe improve later
              EXECUTE format('ALTER TABLE %s.%s SPLIT DEFAULT PARTITION START(%L) END(%L)', schema_ident, table_ident, lowerborder, upperborder);
              RAISE WARNING 'APM cannot create partition using ''%'', the default partition is ''%''', storage_type, defprt_am;
            END IF;
          END IF;
        ELSIF has_prt AND storage_type IS NULL THEN
          EXECUTE format('ALTER TABLE %s.%s ADD PARTITION START(%L) END(%L)', schema_ident, table_ident, lowerborder, upperborder);
        ELSE
          -- matrixts_internal._unique_partition_name() input unquoted name and output unquoted name
          partname := matrixts_internal._unique_partition_name(tablename);
          IF storage_type IS NULL THEN
            EXECUTE format('CREATE TABLE %s.%s PARTITION OF %s.%s FOR VALUES FROM(%L) TO(%L)', schema_ident, quote_ident(partname), schema_ident, table_ident, lowerborder, upperborder);
          ELSE
            -- Only CREATE TABLE ... PARTITION OF supports USING clause.
            EXECUTE format('CREATE TABLE %s.%s PARTITION OF %s.%s FOR VALUES FROM(%L) TO(%L) USING %s', schema_ident, quote_ident(partname), schema_ident, table_ident, lowerborder, upperborder, storage_type_am);
          END IF;
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

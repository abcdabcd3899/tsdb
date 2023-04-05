-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION matrixts" to load this file. \quit

-- time_bucket returns the left edge of the bucket where ts falls into.
-- Buckets span an interval of time equal to the bucket_width and are aligned with the epoch.
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP) RETURNS TIMESTAMP
    AS '$libdir/matrixts', 'ts_timestamp_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- bucketing of timestamptz happens at UTC time
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ) RETURNS TIMESTAMPTZ
    AS '$libdir/matrixts', 'ts_timestamptz_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

--bucketing on date should not do any timezone conversion
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE) RETURNS DATE
    AS '$libdir/matrixts', 'ts_date_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

--bucketing with origin
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP, origin TIMESTAMP) RETURNS TIMESTAMP
    AS '$libdir/matrixts', 'ts_timestamp_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, origin TIMESTAMPTZ) RETURNS TIMESTAMPTZ
    AS '$libdir/matrixts', 'ts_timestamptz_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE, origin DATE) RETURNS DATE
    AS '$libdir/matrixts', 'ts_date_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- bucketing of int
CREATE OR REPLACE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT) RETURNS SMALLINT
    AS '$libdir/matrixts', 'ts_int16_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INT, ts INT) RETURNS INT
    AS '$libdir/matrixts', 'ts_int32_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT) RETURNS BIGINT
    AS '$libdir/matrixts', 'ts_int64_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- bucketing of int with offset
CREATE OR REPLACE FUNCTION time_bucket(bucket_width SMALLINT, ts SMALLINT, "offset" SMALLINT) RETURNS SMALLINT
    AS '$libdir/matrixts', 'ts_int16_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INT, ts INT, "offset" INT) RETURNS INT
    AS '$libdir/matrixts', 'ts_int32_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;
CREATE OR REPLACE FUNCTION time_bucket(bucket_width BIGINT, ts BIGINT, "offset" BIGINT) RETURNS BIGINT
    AS '$libdir/matrixts', 'ts_int64_bucket' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

-- If an interval is given as the third argument, the bucket alignment is offset by the interval.
CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMP, "offset" INTERVAL)
    RETURNS TIMESTAMP LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT AS
$BODY$
    SELECT time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts TIMESTAMPTZ, "offset" INTERVAL)
    RETURNS TIMESTAMPTZ LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT AS
$BODY$
    SELECT time_bucket(bucket_width, ts-"offset")+"offset";
$BODY$;

CREATE OR REPLACE FUNCTION time_bucket(bucket_width INTERVAL, ts DATE, "offset" INTERVAL)
    RETURNS DATE LANGUAGE SQL IMMUTABLE PARALLEL SAFE STRICT AS
$BODY$
    SELECT (time_bucket(bucket_width, ts-"offset")+"offset")::date;
$BODY$;


CREATE SCHEMA matrixts_internal;

CREATE OR REPLACE FUNCTION matrixts_internal.first_sfunc(internal, anyelement, "any")
RETURNS internal
AS '$libdir/matrixts', 'ts_first_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION matrixts_internal.first_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/matrixts', 'ts_first_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION matrixts_internal.last_sfunc(internal, anyelement, "any")
RETURNS internal
AS '$libdir/matrixts', 'ts_last_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION matrixts_internal.last_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/matrixts', 'ts_last_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION matrixts_internal.bookend_finalfunc(internal, anyelement, "any")
RETURNS anyelement
AS '$libdir/matrixts', 'ts_bookend_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION matrixts_internal.bookend_serializefunc(internal)
RETURNS bytea
AS '$libdir/matrixts', 'ts_bookend_serializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION matrixts_internal.bookend_deserializefunc(bytea, internal)
RETURNS internal
AS '$libdir/matrixts', 'ts_bookend_deserializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

--This aggregate returns the "first" element of the first argument when ordered by the second argument.
--Ex. first(temp, time) returns the temp value for the row with the lowest time
CREATE AGGREGATE first(anyelement, "any") (
    SFUNC = matrixts_internal.first_sfunc,
    STYPE = internal,
    COMBINEFUNC = matrixts_internal.first_combinefunc,
    SERIALFUNC = matrixts_internal.bookend_serializefunc,
    DESERIALFUNC = matrixts_internal.bookend_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = matrixts_internal.bookend_finalfunc,
    FINALFUNC_EXTRA
);

--This aggregate returns the "last" element of the first argument when ordered by the second argument.
--Ex. last(temp, time) returns the temp value for the row with highest time
CREATE AGGREGATE last(anyelement, "any") (
    SFUNC = matrixts_internal.last_sfunc,
    STYPE = internal,
    COMBINEFUNC = matrixts_internal.last_combinefunc,
    SERIALFUNC = matrixts_internal.bookend_serializefunc,
    DESERIALFUNC = matrixts_internal.bookend_deserializefunc,
    PARALLEL = SAFE,
    FINALFUNC = matrixts_internal.bookend_finalfunc,
    FINALFUNC_EXTRA
);

-- This aggregate partitions the dataset into a specified number of buckets (nbuckets) ranging
-- from the inputted min to max values.
--CREATE AGGREGATE histogram (DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, INTEGER) (
--    SFUNC = matrixts_internal.hist_sfunc,
--    STYPE = INTERNAL,
--    COMBINEFUNC = matrixts_internal.hist_combinefunc,
--    SERIALFUNC = matrixts_internal.hist_serializefunc,
--    DESERIALFUNC = matrixts_internal.hist_deserializefunc,
--    PARALLEL = SAFE,
--    FINALFUNC = matrixts_internal.hist_finalfunc,
--    FINALFUNC_EXTRA
--);
--
--
--CREATE AGGREGATE matrixts_internal.finalize_agg(agg_name TEXT,  inner_agg_collation_schema NAME,  inner_agg_collation_name NAME, inner_agg_input_types NAME[][], inner_agg_serialized_state BYTEA, return_type_dummy_val anyelement) (
--    SFUNC = matrixts_internal.finalize_agg_sfunc,
--    STYPE = internal,
--    FINALFUNC = matrixts_internal.finalize_agg_ffunc,
--    FINALFUNC_EXTRA
--);


--
-- Return the row count of a relation, in a fast way.
--
-- When the relation is an AO or AOCS table, we use the tupcount from the
-- corresponding visimap aux tables.  This value might be different from the
-- real count (TODO: when and how?).
--
-- For other relation typles, silently fallback to the count(*).
--
CREATE OR REPLACE FUNCTION fast_count(rel REGCLASS)
RETURNS BIGINT AS $$
  DECLARE
    am          TEXT;
    count       BIGINT;
  BEGIN
    SELECT amname
      INTO am
      FROM pg_am AS a
      JOIN pg_class AS c
        ON a.oid = c.relam
     WHERE c.oid = rel;

    IF am = 'ao_row' OR am = 'ao_column' THEN
      -- For AO/AOCS tables, we can use the information from the visimap.
      SELECT sum(total_tupcount - hidden_tupcount)
        INTO count
        FROM gp_toolkit.__gp_aovisimap_hidden_info(rel);
    ELSE
      -- For other tables, simply fallback to the count(1).  We have to use
      -- EXECUTE in case the rel name contains special chars.
      EXECUTE 'SELECT count(1) FROM ' || rel
         INTO count;
    END IF;

    RETURN count;
  END;
$$ LANGUAGE PLPGSQL EXECUTE ON MASTER VOLATILE;


--
-- Reorder functions
--

CREATE OR REPLACE FUNCTION __reorder_swap_relation_files(r1 REGCLASS, r2 REGCLASS) RETURNS VOID
    AS '$libdir/matrixts', '__reorder_swap_relation_files'
    LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION __reorder_build_dist_by_clause(rel REGCLASS)
RETURNS text AS $$
  DECLARE
    clause      TEXT;
    _policytype CHAR;
    _distkey    INT2VECTOR;
    _distclass  OIDVECTOR;
    _opcname    NAME;
    _opclass    OID;
    _attname    NAME;
    _attnum     INT2;
    nth         INT;
  BEGIN
    SELECT policytype, distkey, distclass
      INTO _policytype, _distkey, _distclass
      FROM gp_distribution_policy
     WHERE localoid = rel;

    IF _policytype = 'r' THEN
      clause = 'replicated';
    ELSIF _policytype = 'p' AND (_distkey IS NULL OR array_length(_distkey, 1) = 0) THEN
      clause = 'randomly';
    ELSIF _policytype = 'p' AND _distkey IS NOT NULL THEN
      clause = 'by (';

      nth = 0;
      FOREACH _attnum IN ARRAY _distkey LOOP
        _opclass = _distclass[nth];
        SELECT opcname
          INTO _opcname
          FROM pg_opclass
         WHERE oid = _opclass;

        SELECT attname
          INTO _attname
          FROM pg_attribute
         WHERE attrelid = rel
           AND attnum = _attnum;

        IF nth > 0 THEN
          clause = clause || ', ';
        END IF;
        clause = clause || '"' || _attname || '"' || ' ' || _opcname;

        nth = nth + 1;
      END LOOP;

      clause = clause || ')';
    ELSE
      RAISE EXCEPTION 'invalid policy type: %', _policytype;
    END IF;

    RETURN clause;
  END;
$$ LANGUAGE PLPGSQL EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION __reorder_build_sort_by_clause(ind REGCLASS)
RETURNS text AS $$
  DECLARE
    i           SMALLINT;
    nkeyatts    SMALLINT;
    keys        INT2VECTOR;
    opts        INT2VECTOR;
    classes     OIDVECTOR;
    exprs       PG_NODE_TREE;
    clause      TEXT;
    unsupport   BOOL;
  BEGIN
    -- list the index keys
    SELECT indnkeyatts, indkey, indoption, indclass, indexprs
      INTO nkeyatts, keys, opts, classes, exprs
      FROM pg_index
     WHERE indexrelid = ind;

    -- TODO: support expression index keys
    IF exprs IS NOT NULL THEN
      RAISE EXCEPTION 'expression index keys are not supported';
    END IF;

    -- TODO: support non-default opclass
    SELECT TRUE
      INTO unsupport
      FROM pg_opclass
     WHERE oid = ANY (classes)
       AND NOT opcdefault;

    IF unsupport IS NOT NULL THEN
      RAISE EXCEPTION 'non-default operator classes on index keys are not supported';
    END IF;

    -- construct the order by clause
    clause = '';
    FOR i IN 0 .. nkeyatts-1 LOOP
      IF i > 0 THEN
        clause = clause || ', ';
      END IF;
      clause = clause || keys[i];

      -- indoption is or'ed of below bits:
      -- - INDOPTION_DESC        0x0001 /* values are in reverse order */
      -- - INDOPTION_NULLS_FIRST 0x0002 /* NULLs are first instead of last */
      IF opts[i] = 1 THEN
        clause = clause || ' DESC NULLS LAST';
	  ELSIF opts[i] = 2 THEN
        clause = clause || ' NULLS FIRST';
	  ELSIF opts[i] = 3 THEN
        clause = clause || ' DESC';
      END IF;
    END LOOP;

    RETURN clause;
  END;
$$ LANGUAGE PLPGSQL EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION __reorder_build_with_options(rel REGCLASS)
RETURNS TEXT AS $$
  DECLARE
    clause      TEXT;
    _blocksize  INT;
    _checksum   BOOL;
    _compresslevel INT2;
    _compresstype TEXT;
    _columnstore BOOL;
  BEGIN
    SELECT blocksize, checksum, compresslevel, compresstype, columnstore
      INTO _blocksize, _checksum, _compresslevel, _compresstype, _columnstore
      FROM pg_appendonly
     WHERE relid = rel;

    IF _columnstore IS NULL THEN
      -- not appendonly
      -- TODO: fillfactor
      RETURN '';
    END IF;

    clause = ' with (appendoptimized=true';

    IF _columnstore THEN
      clause = clause || ',orientation=column';
    ELSE
      clause = clause || ',orientation=row';
    END IF;

    IF _compresstype IS NOT NULL AND _compresstype != '' THEN
      clause = clause || ',compresstype=' || _compresstype;

      if _compresslevel <> 1 THEN
        clause = clause || ',compresslevel=' || _compresslevel;
      END IF;
    END IF;

    IF _blocksize <> 32768 THEN
      clause = clause || ',blocksize=' || _blocksize;
    END IF;

    IF not _checksum THEN
      clause = clause || ',checksum=' || _checksum;
    END IF;

    clause = clause || ')';

    RETURN clause;
  END;
$$ LANGUAGE PLPGSQL EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION reorder_part_by_index(ind REGCLASS)
RETURNS TABLE(relation REGCLASS, index REGCLASS) AS $$
  DECLARE
    rel         REGCLASS;
    distby      TEXT;
    sortby      TEXT;
    opts        TEXT;
    tmpname     TEXT;
    partitioned BOOL;
    appendonly  BOOL;
    firstattr   TEXT;
  BEGIN
    -- lookup the relation of the index
    SELECT indrelid
      INTO rel
      FROM pg_index
     WHERE indexrelid = ind;

    -- is it a partitioned table?
    SELECT TRUE
      INTO partitioned
      FROM pg_partitioned_table
     WHERE partrelid = rel;

    -- only reorder leaves
    IF partitioned IS NULL THEN
      SELECT TRUE INTO appendonly FROM pg_appendonly WHERE relid = rel;

      SELECT __reorder_build_with_options(rel) INTO opts;
      SELECT __reorder_build_dist_by_clause(rel) INTO distby;
      SELECT __reorder_build_sort_by_clause(ind) INTO sortby;

      -- TODO: tablespace, etc.
      -- TODO: append a random suffix
      -- TODO: lock rel ahead
      tmpname = '__reorder_by_index_' || ind::oid || '__';
      -- TODO: fail if tmpname exists
      -- TODO: if the original table contains dropped columns,
      -- we must also create them in the tmp table and drop them.
      EXECUTE 'CREATE TABLE ' || tmpname || ' (LIKE ' || rel || ')' || opts || ' DISTRIBUTED ' || distby;

      -- for an AO/AOCO table we must also create an index, so the blockdir is
      -- created for it, it does not really matter about the index type and
      -- keys, in fact we could even drop the immediately after the creation.
      -- When rewritting with C, we could set buildAoBlkdir=true to avoid the
      -- trick.
      IF appendonly IS NOT NULL THEN
        EXECUTE 'CREATE INDEX idx_' || tmpname || ' ON ' || tmpname || ' ((true))';
        EXECUTE 'DROP INDEX idx_' || tmpname;
      END IF;

      EXECUTE 'INSERT INTO ' || tmpname || ' SELECT * FROM ' || rel || ' ORDER BY ' || sortby;
      EXECUTE 'SELECT __reorder_swap_relation_files(' || rel::OID || ', ' || tmpname::REGCLASS::OID || ')';
      EXECUTE 'DROP TABLE ' || tmpname;

/*
      execute 'analyze ' || rel;
*/
      -- reindex is a necessary step for non-btree indexes to produce correct
      -- results.
      EXECUTE 'REINDEX INDEX ' || ind;

      RETURN QUERY SELECT rel, ind;
    END IF;
  END;
$$ LANGUAGE PLPGSQL EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION reorder_by_index(ind REGCLASS)
RETURNS TABLE(relation REGCLASS, index REGCLASS) AS $$
  DECLARE
    inh         RECORD;
    seqscan     BOOL;
    indexscan   BOOL;
    indexonlyscan BOOL;
    bitmapscan  BOOL;
    allow       BOOL;
  BEGIN
    -- it is important to only use seqscan during the reordering, so we have to
    -- adjust the GUCs explicitly.  And do not forget to save the original
    -- settings so we can restore them at the end; in the case of exceptions,
    -- the GUCs are restored rollbacked.
    SHOW enable_seqscan       INTO seqscan;
    SHOW enable_indexscan     INTO indexscan;
    SHOW enable_indexonlyscan INTO indexonlyscan;
    SHOW enable_bitmapscan    INTO bitmapscan;
    SHOW allow_system_table_mods INTO allow;
	SET enable_seqscan       TO ON;
	SET enable_indexscan     TO OFF;
	SET enable_indexonlyscan TO OFF;
    SET enable_bitmapscan    TO OFF;
    SET allow_system_table_mods TO ON;

    -- TODO: lock index

    RETURN QUERY EXECUTE 'SELECT relation, index FROM reorder_part_by_index($1)' USING ind;

    FOR inh IN SELECT inhrelid FROM pg_inherits WHERE inhparent = ind
    LOOP
      IF inh IS NOT NULL THEN
        RETURN QUERY EXECUTE 'SELECT relation, index FROM reorder_part_by_index($1)' USING inh.inhrelid;
      END IF;
    END LOOP;

    EXECUTE 'SET enable_seqscan       TO ' || seqscan;
    EXECUTE 'SET enable_indexscan     TO ' || indexscan;
    EXECUTE 'SET enable_indexonlyscan TO ' || indexonlyscan;
    EXECUTE 'SET enable_bitmapscan    TO ' || bitmapscan;
    EXECUTE 'SET allow_system_table_mods TO ' || allow;
  END;
$$ LANGUAGE PLPGSQL EXECUTE ON MASTER;

--
-- TOIN indexes
--

CREATE OR REPLACE FUNCTION toinhandler(INTERNAL) RETURNS INDEX_AM_HANDLER
    AS '$libdir/matrixts', 'toinhandler' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE ACCESS METHOD toin TYPE INDEX HANDLER toinhandler;

--
-- operator classes to support brin and btree operations
--

CREATE OPERATOR CLASS bytea_minmax_ops DEFAULT FOR TYPE bytea USING toin
    AS OPERATOR 1 <(bytea,bytea)
     , OPERATOR 2 <=(bytea,bytea)
     , OPERATOR 3 =(bytea,bytea)
     , OPERATOR 4 >=(bytea,bytea)
     , OPERATOR 5 >(bytea,bytea)
     , FUNCTION 1 (bytea,bytea) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (bytea,bytea) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (bytea,bytea) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (bytea,bytea) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (bytea,bytea) byteacmp(bytea,bytea)
;

CREATE OPERATOR CLASS char_minmax_ops DEFAULT FOR TYPE "char" USING toin
    AS OPERATOR 1 <("char","char")
     , OPERATOR 2 <=("char","char")
     , OPERATOR 3 =("char","char")
     , OPERATOR 4 >=("char","char")
     , OPERATOR 5 >("char","char")
     , FUNCTION 1 ("char","char") brin_minmax_opcinfo(internal)
     , FUNCTION 2 ("char","char") brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 ("char","char") brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 ("char","char") brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 ("char","char") btcharcmp("char","char")
;

CREATE OPERATOR CLASS name_minmax_ops DEFAULT FOR TYPE name USING toin
    AS OPERATOR 1 <(name,name)
     , OPERATOR 2 <=(name,name)
     , OPERATOR 3 =(name,name)
     , OPERATOR 4 >=(name,name)
     , OPERATOR 5 >(name,name)
     , FUNCTION 1 (name,name) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (name,name) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (name,name) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (name,name) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (name,name) btnamecmp(name,name)
     , FUNCTION 15 (name,text) btnametextcmp(name,text)
     , FUNCTION 15 (text,name) bttextnamecmp(text,name)
;

CREATE OPERATOR CLASS int8_minmax_ops DEFAULT FOR TYPE bigint USING toin
    AS OPERATOR 1 <(bigint,bigint)
     , OPERATOR 1 <(bigint,smallint)
     , OPERATOR 1 <(bigint,integer)
     , OPERATOR 1 <(smallint,bigint)
     , OPERATOR 1 <(integer,bigint)
     , OPERATOR 2 <=(bigint,bigint)
     , OPERATOR 2 <=(bigint,smallint)
     , OPERATOR 2 <=(bigint,integer)
     , OPERATOR 2 <=(smallint,bigint)
     , OPERATOR 2 <=(integer,bigint)
     , OPERATOR 3 =(bigint,bigint)
     , OPERATOR 3 =(bigint,smallint)
     , OPERATOR 3 =(bigint,integer)
     , OPERATOR 3 =(smallint,bigint)
     , OPERATOR 3 =(integer,bigint)
     , OPERATOR 4 >=(bigint,bigint)
     , OPERATOR 4 >=(bigint,smallint)
     , OPERATOR 4 >=(bigint,integer)
     , OPERATOR 4 >=(smallint,bigint)
     , OPERATOR 4 >=(integer,bigint)
     , OPERATOR 5 >(bigint,bigint)
     , OPERATOR 5 >(bigint,smallint)
     , OPERATOR 5 >(bigint,integer)
     , OPERATOR 5 >(smallint,bigint)
     , OPERATOR 5 >(integer,bigint)
     , FUNCTION 1 (bigint,bigint) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (bigint,smallint) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (bigint,integer) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (smallint,bigint) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (integer,bigint) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (bigint,bigint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (bigint,smallint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (bigint,integer) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (smallint,bigint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (integer,bigint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (bigint,bigint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (bigint,smallint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (bigint,integer) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (smallint,bigint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (integer,bigint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (bigint,bigint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (bigint,smallint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (bigint,integer) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (smallint,bigint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (integer,bigint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (bigint,bigint) btint8cmp(bigint,bigint)
     , FUNCTION 15 (bigint,smallint) btint82cmp(bigint,smallint)
     , FUNCTION 15 (bigint,integer) btint84cmp(bigint,integer)
     , FUNCTION 15 (smallint,bigint) btint28cmp(smallint,bigint)
     , FUNCTION 15 (integer,bigint) btint48cmp(integer,bigint)
;

CREATE OPERATOR CLASS int2_minmax_ops DEFAULT FOR TYPE smallint USING toin
    AS OPERATOR 1 <(bigint,smallint)
     , OPERATOR 1 <(smallint,bigint)
     , OPERATOR 1 <(smallint,smallint)
     , OPERATOR 1 <(smallint,integer)
     , OPERATOR 1 <(integer,smallint)
     , OPERATOR 2 <=(bigint,smallint)
     , OPERATOR 2 <=(smallint,bigint)
     , OPERATOR 2 <=(smallint,smallint)
     , OPERATOR 2 <=(smallint,integer)
     , OPERATOR 2 <=(integer,smallint)
     , OPERATOR 3 =(bigint,smallint)
     , OPERATOR 3 =(smallint,bigint)
     , OPERATOR 3 =(smallint,smallint)
     , OPERATOR 3 =(smallint,integer)
     , OPERATOR 3 =(integer,smallint)
     , OPERATOR 4 >=(bigint,smallint)
     , OPERATOR 4 >=(smallint,bigint)
     , OPERATOR 4 >=(smallint,smallint)
     , OPERATOR 4 >=(smallint,integer)
     , OPERATOR 4 >=(integer,smallint)
     , OPERATOR 5 >(bigint,smallint)
     , OPERATOR 5 >(smallint,bigint)
     , OPERATOR 5 >(smallint,smallint)
     , OPERATOR 5 >(smallint,integer)
     , OPERATOR 5 >(integer,smallint)
     , FUNCTION 1 (bigint,smallint) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (smallint,bigint) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (smallint,smallint) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (smallint,integer) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (integer,smallint) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (bigint,smallint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (smallint,bigint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (smallint,smallint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (smallint,integer) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (integer,smallint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (bigint,smallint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (smallint,bigint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (smallint,smallint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (smallint,integer) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (integer,smallint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (bigint,smallint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (smallint,bigint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (smallint,smallint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (smallint,integer) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (integer,smallint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (bigint,smallint) btint82cmp(bigint,smallint)
     , FUNCTION 15 (smallint,bigint) btint28cmp(smallint,bigint)
     , FUNCTION 15 (smallint,smallint) btint2cmp(smallint,smallint)
     , FUNCTION 15 (smallint,integer) btint24cmp(smallint,integer)
     , FUNCTION 15 (integer,smallint) btint42cmp(integer,smallint)
;

CREATE OPERATOR CLASS int4_minmax_ops DEFAULT FOR TYPE integer USING toin
    AS OPERATOR 1 <(bigint,integer)
     , OPERATOR 1 <(smallint,integer)
     , OPERATOR 1 <(integer,bigint)
     , OPERATOR 1 <(integer,smallint)
     , OPERATOR 1 <(integer,integer)
     , OPERATOR 2 <=(bigint,integer)
     , OPERATOR 2 <=(smallint,integer)
     , OPERATOR 2 <=(integer,bigint)
     , OPERATOR 2 <=(integer,smallint)
     , OPERATOR 2 <=(integer,integer)
     , OPERATOR 3 =(bigint,integer)
     , OPERATOR 3 =(smallint,integer)
     , OPERATOR 3 =(integer,bigint)
     , OPERATOR 3 =(integer,smallint)
     , OPERATOR 3 =(integer,integer)
     , OPERATOR 4 >=(bigint,integer)
     , OPERATOR 4 >=(smallint,integer)
     , OPERATOR 4 >=(integer,bigint)
     , OPERATOR 4 >=(integer,smallint)
     , OPERATOR 4 >=(integer,integer)
     , OPERATOR 5 >(bigint,integer)
     , OPERATOR 5 >(smallint,integer)
     , OPERATOR 5 >(integer,bigint)
     , OPERATOR 5 >(integer,smallint)
     , OPERATOR 5 >(integer,integer)
     , FUNCTION 1 (bigint,integer) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (smallint,integer) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (integer,bigint) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (integer,smallint) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (integer,integer) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (bigint,integer) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (smallint,integer) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (integer,bigint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (integer,smallint) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (integer,integer) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (bigint,integer) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (smallint,integer) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (integer,bigint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (integer,smallint) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (integer,integer) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (bigint,integer) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (smallint,integer) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (integer,bigint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (integer,smallint) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (integer,integer) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (bigint,integer) btint84cmp(bigint,integer)
     , FUNCTION 15 (smallint,integer) btint24cmp(smallint,integer)
     , FUNCTION 15 (integer,bigint) btint48cmp(integer,bigint)
     , FUNCTION 15 (integer,smallint) btint42cmp(integer,smallint)
     , FUNCTION 15 (integer,integer) btint4cmp(integer,integer)
;

CREATE OPERATOR CLASS text_minmax_ops DEFAULT FOR TYPE text USING toin
    AS OPERATOR 1 <(text,text)
     , OPERATOR 2 <=(text,text)
     , OPERATOR 3 =(text,text)
     , OPERATOR 4 >=(text,text)
     , OPERATOR 5 >(text,text)
     , FUNCTION 1 (text,text) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (text,text) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (text,text) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (text,text) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (name,text) btnametextcmp(name,text)
     , FUNCTION 15 (text,name) bttextnamecmp(text,name)
     , FUNCTION 15 (text,text) bttextcmp(text,text)
;

CREATE OPERATOR CLASS oid_minmax_ops DEFAULT FOR TYPE oid USING toin
    AS OPERATOR 1 <(oid,oid)
     , OPERATOR 2 <=(oid,oid)
     , OPERATOR 3 =(oid,oid)
     , OPERATOR 4 >=(oid,oid)
     , OPERATOR 5 >(oid,oid)
     , FUNCTION 1 (oid,oid) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (oid,oid) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (oid,oid) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (oid,oid) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (oid,oid) btoidcmp(oid,oid)
;

CREATE OPERATOR CLASS tid_minmax_ops DEFAULT FOR TYPE tid USING toin
    AS OPERATOR 1 <(tid,tid)
     , OPERATOR 2 <=(tid,tid)
     , OPERATOR 3 =(tid,tid)
     , OPERATOR 4 >=(tid,tid)
     , OPERATOR 5 >(tid,tid)
     , FUNCTION 1 (tid,tid) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (tid,tid) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (tid,tid) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (tid,tid) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (tid,tid) bttidcmp(tid,tid)
;

CREATE OPERATOR CLASS float4_minmax_ops DEFAULT FOR TYPE real USING toin
    AS OPERATOR 1 <(real,real)
     , OPERATOR 1 <(real,double precision)
     , OPERATOR 1 <(double precision,real)
     , OPERATOR 2 <=(real,real)
     , OPERATOR 2 <=(real,double precision)
     , OPERATOR 2 <=(double precision,real)
     , OPERATOR 3 =(real,real)
     , OPERATOR 3 =(real,double precision)
     , OPERATOR 3 =(double precision,real)
     , OPERATOR 4 >=(real,real)
     , OPERATOR 4 >=(real,double precision)
     , OPERATOR 4 >=(double precision,real)
     , OPERATOR 5 >(real,real)
     , OPERATOR 5 >(real,double precision)
     , OPERATOR 5 >(double precision,real)
     , FUNCTION 1 (real,real) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (real,double precision) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (double precision,real) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (real,real) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (real,double precision) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (double precision,real) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (real,real) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (real,double precision) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (double precision,real) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (real,real) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (real,double precision) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (double precision,real) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (real,real) btfloat4cmp(real,real)
     , FUNCTION 15 (real,double precision) btfloat48cmp(real,double precision)
     , FUNCTION 15 (double precision,real) btfloat84cmp(double precision,real)
;

CREATE OPERATOR CLASS float8_minmax_ops DEFAULT FOR TYPE double precision USING toin
    AS OPERATOR 1 <(real,double precision)
     , OPERATOR 1 <(double precision,real)
     , OPERATOR 1 <(double precision,double precision)
     , OPERATOR 2 <=(real,double precision)
     , OPERATOR 2 <=(double precision,real)
     , OPERATOR 2 <=(double precision,double precision)
     , OPERATOR 3 =(real,double precision)
     , OPERATOR 3 =(double precision,real)
     , OPERATOR 3 =(double precision,double precision)
     , OPERATOR 4 >=(real,double precision)
     , OPERATOR 4 >=(double precision,real)
     , OPERATOR 4 >=(double precision,double precision)
     , OPERATOR 5 >(real,double precision)
     , OPERATOR 5 >(double precision,real)
     , OPERATOR 5 >(double precision,double precision)
     , FUNCTION 1 (real,double precision) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (double precision,real) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (double precision,double precision) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (real,double precision) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (double precision,real) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (double precision,double precision) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (real,double precision) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (double precision,real) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (double precision,double precision) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (real,double precision) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (double precision,real) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (double precision,double precision) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (real,double precision) btfloat48cmp(real,double precision)
     , FUNCTION 15 (double precision,real) btfloat84cmp(double precision,real)
     , FUNCTION 15 (double precision,double precision) btfloat8cmp(double precision,double precision)
;

CREATE OPERATOR CLASS macaddr_minmax_ops DEFAULT FOR TYPE macaddr USING toin
    AS OPERATOR 1 <(macaddr,macaddr)
     , OPERATOR 2 <=(macaddr,macaddr)
     , OPERATOR 3 =(macaddr,macaddr)
     , OPERATOR 4 >=(macaddr,macaddr)
     , OPERATOR 5 >(macaddr,macaddr)
     , FUNCTION 1 (macaddr,macaddr) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (macaddr,macaddr) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (macaddr,macaddr) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (macaddr,macaddr) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (macaddr,macaddr) macaddr_cmp(macaddr,macaddr)
;

CREATE OPERATOR CLASS macaddr8_minmax_ops DEFAULT FOR TYPE macaddr8 USING toin
    AS OPERATOR 1 <(macaddr8,macaddr8)
     , OPERATOR 2 <=(macaddr8,macaddr8)
     , OPERATOR 3 =(macaddr8,macaddr8)
     , OPERATOR 4 >=(macaddr8,macaddr8)
     , OPERATOR 5 >(macaddr8,macaddr8)
     , FUNCTION 1 (macaddr8,macaddr8) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (macaddr8,macaddr8) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (macaddr8,macaddr8) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (macaddr8,macaddr8) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (macaddr8,macaddr8) macaddr8_cmp(macaddr8,macaddr8)
;

CREATE OPERATOR CLASS inet_inclusion_ops DEFAULT FOR TYPE inet USING toin
    AS OPERATOR 3 &&(inet,inet)
     , OPERATOR 7 >>=(inet,inet)
     , OPERATOR 8 <<=(inet,inet)
     , OPERATOR 18 =(inet,inet)
     , OPERATOR 24 >>(inet,inet)
     , OPERATOR 26 <<(inet,inet)
     , FUNCTION 1 (inet,inet) brin_inclusion_opcinfo(internal)
     , FUNCTION 2 (inet,inet) brin_inclusion_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (inet,inet) brin_inclusion_consistent(internal,internal,internal)
     , FUNCTION 4 (inet,inet) brin_inclusion_union(internal,internal,internal)
     , FUNCTION 11 (inet,inet) inet_merge(inet,inet)
     , FUNCTION 12 (inet,inet) inet_same_family(inet,inet)
     , FUNCTION 13 (inet,inet) network_supeq(inet,inet)
     , FUNCTION 15 (inet,inet) network_cmp(inet,inet)
;

CREATE OPERATOR CLASS bpchar_minmax_ops DEFAULT FOR TYPE character USING toin
    AS OPERATOR 1 <(character,character)
     , OPERATOR 2 <=(character,character)
     , OPERATOR 3 =(character,character)
     , OPERATOR 4 >=(character,character)
     , OPERATOR 5 >(character,character)
     , FUNCTION 1 (character,character) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (character,character) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (character,character) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (character,character) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (character,character) bpcharcmp(character,character)
;

CREATE OPERATOR CLASS time_minmax_ops DEFAULT FOR TYPE time without time zone USING toin
    AS OPERATOR 1 <(time without time zone,time without time zone)
     , OPERATOR 2 <=(time without time zone,time without time zone)
     , OPERATOR 3 =(time without time zone,time without time zone)
     , OPERATOR 4 >=(time without time zone,time without time zone)
     , OPERATOR 5 >(time without time zone,time without time zone)
     , FUNCTION 1 (time without time zone,time without time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (time without time zone,time without time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (time without time zone,time without time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (time without time zone,time without time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (time without time zone,time without time zone) time_cmp(time without time zone,time without time zone)
;

CREATE OPERATOR CLASS date_minmax_ops DEFAULT FOR TYPE date USING toin
    AS OPERATOR 1 <(date,date)
     , OPERATOR 1 <(date,timestamp without time zone)
     , OPERATOR 1 <(date,timestamp with time zone)
     , OPERATOR 1 <(timestamp without time zone,date)
     , OPERATOR 1 <(timestamp with time zone,date)
     , OPERATOR 2 <=(date,date)
     , OPERATOR 2 <=(date,timestamp without time zone)
     , OPERATOR 2 <=(date,timestamp with time zone)
     , OPERATOR 2 <=(timestamp without time zone,date)
     , OPERATOR 2 <=(timestamp with time zone,date)
     , OPERATOR 3 =(date,date)
     , OPERATOR 3 =(date,timestamp without time zone)
     , OPERATOR 3 =(date,timestamp with time zone)
     , OPERATOR 3 =(timestamp without time zone,date)
     , OPERATOR 3 =(timestamp with time zone,date)
     , OPERATOR 4 >=(date,date)
     , OPERATOR 4 >=(date,timestamp without time zone)
     , OPERATOR 4 >=(date,timestamp with time zone)
     , OPERATOR 4 >=(timestamp without time zone,date)
     , OPERATOR 4 >=(timestamp with time zone,date)
     , OPERATOR 5 >(date,date)
     , OPERATOR 5 >(date,timestamp without time zone)
     , OPERATOR 5 >(date,timestamp with time zone)
     , OPERATOR 5 >(timestamp without time zone,date)
     , OPERATOR 5 >(timestamp with time zone,date)
     , FUNCTION 1 (date,date) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (date,timestamp without time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (date,timestamp with time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp without time zone,date) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp with time zone,date) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (date,date) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (date,timestamp without time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (date,timestamp with time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp without time zone,date) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp with time zone,date) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (date,date) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (date,timestamp without time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (date,timestamp with time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp without time zone,date) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp with time zone,date) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (date,date) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (date,timestamp without time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (date,timestamp with time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp without time zone,date) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp with time zone,date) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (date,date) date_cmp(date,date)
     , FUNCTION 15 (date,timestamp without time zone) date_cmp_timestamp(date,timestamp without time zone)
     , FUNCTION 15 (date,timestamp with time zone) date_cmp_timestamptz(date,timestamp with time zone)
     , FUNCTION 15 (timestamp without time zone,date) timestamp_cmp_date(timestamp without time zone,date)
     , FUNCTION 15 (timestamp with time zone,date) timestamptz_cmp_date(timestamp with time zone,date)
;

CREATE OPERATOR CLASS timestamp_minmax_ops DEFAULT FOR TYPE timestamp without time zone USING toin
    AS OPERATOR 1 <(date,timestamp without time zone)
     , OPERATOR 1 <(timestamp without time zone,date)
     , OPERATOR 1 <(timestamp without time zone,timestamp without time zone)
     , OPERATOR 1 <(timestamp without time zone,timestamp with time zone)
     , OPERATOR 1 <(timestamp with time zone,timestamp without time zone)
     , OPERATOR 2 <=(date,timestamp without time zone)
     , OPERATOR 2 <=(timestamp without time zone,date)
     , OPERATOR 2 <=(timestamp without time zone,timestamp without time zone)
     , OPERATOR 2 <=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 2 <=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 3 =(date,timestamp without time zone)
     , OPERATOR 3 =(timestamp without time zone,date)
     , OPERATOR 3 =(timestamp without time zone,timestamp without time zone)
     , OPERATOR 3 =(timestamp without time zone,timestamp with time zone)
     , OPERATOR 3 =(timestamp with time zone,timestamp without time zone)
     , OPERATOR 4 >=(date,timestamp without time zone)
     , OPERATOR 4 >=(timestamp without time zone,date)
     , OPERATOR 4 >=(timestamp without time zone,timestamp without time zone)
     , OPERATOR 4 >=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 4 >=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 5 >(date,timestamp without time zone)
     , OPERATOR 5 >(timestamp without time zone,date)
     , OPERATOR 5 >(timestamp without time zone,timestamp without time zone)
     , OPERATOR 5 >(timestamp without time zone,timestamp with time zone)
     , OPERATOR 5 >(timestamp with time zone,timestamp without time zone)
     , FUNCTION 1 (date,timestamp without time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp without time zone,date) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp without time zone,timestamp without time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp without time zone,timestamp with time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp with time zone,timestamp without time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (date,timestamp without time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp without time zone,date) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp without time zone,timestamp without time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp without time zone,timestamp with time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp with time zone,timestamp without time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (date,timestamp without time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp without time zone,date) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp without time zone,timestamp without time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp without time zone,timestamp with time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp with time zone,timestamp without time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (date,timestamp without time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp without time zone,date) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp without time zone,timestamp without time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp without time zone,timestamp with time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp with time zone,timestamp without time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (date,timestamp without time zone) date_cmp_timestamp(date,timestamp without time zone)
     , FUNCTION 15 (timestamp without time zone,date) timestamp_cmp_date(timestamp without time zone,date)
     , FUNCTION 15 (timestamp without time zone,timestamp without time zone) timestamp_cmp(timestamp without time zone,timestamp without time zone)
     , FUNCTION 15 (timestamp without time zone,timestamp with time zone) timestamp_cmp_timestamptz(timestamp without time zone,timestamp with time zone)
     , FUNCTION 15 (timestamp with time zone,timestamp without time zone) timestamptz_cmp_timestamp(timestamp with time zone,timestamp without time zone)
;

CREATE OPERATOR CLASS timestamptz_minmax_ops DEFAULT FOR TYPE timestamp with time zone USING toin
    AS OPERATOR 1 <(date,timestamp with time zone)
     , OPERATOR 1 <(timestamp without time zone,timestamp with time zone)
     , OPERATOR 1 <(timestamp with time zone,date)
     , OPERATOR 1 <(timestamp with time zone,timestamp without time zone)
     , OPERATOR 1 <(timestamp with time zone,timestamp with time zone)
     , OPERATOR 2 <=(date,timestamp with time zone)
     , OPERATOR 2 <=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 2 <=(timestamp with time zone,date)
     , OPERATOR 2 <=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 2 <=(timestamp with time zone,timestamp with time zone)
     , OPERATOR 3 =(date,timestamp with time zone)
     , OPERATOR 3 =(timestamp without time zone,timestamp with time zone)
     , OPERATOR 3 =(timestamp with time zone,date)
     , OPERATOR 3 =(timestamp with time zone,timestamp without time zone)
     , OPERATOR 3 =(timestamp with time zone,timestamp with time zone)
     , OPERATOR 4 >=(date,timestamp with time zone)
     , OPERATOR 4 >=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 4 >=(timestamp with time zone,date)
     , OPERATOR 4 >=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 4 >=(timestamp with time zone,timestamp with time zone)
     , OPERATOR 5 >(date,timestamp with time zone)
     , OPERATOR 5 >(timestamp without time zone,timestamp with time zone)
     , OPERATOR 5 >(timestamp with time zone,date)
     , OPERATOR 5 >(timestamp with time zone,timestamp without time zone)
     , OPERATOR 5 >(timestamp with time zone,timestamp with time zone)
     , FUNCTION 1 (date,timestamp with time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp without time zone,timestamp with time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp with time zone,date) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp with time zone,timestamp without time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 1 (timestamp with time zone,timestamp with time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (date,timestamp with time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp without time zone,timestamp with time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp with time zone,date) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp with time zone,timestamp without time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 2 (timestamp with time zone,timestamp with time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (date,timestamp with time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp without time zone,timestamp with time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp with time zone,date) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp with time zone,timestamp without time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 3 (timestamp with time zone,timestamp with time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (date,timestamp with time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp without time zone,timestamp with time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp with time zone,date) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp with time zone,timestamp without time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 4 (timestamp with time zone,timestamp with time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (date,timestamp with time zone) date_cmp_timestamptz(date,timestamp with time zone)
     , FUNCTION 15 (timestamp without time zone,timestamp with time zone) timestamp_cmp_timestamptz(timestamp without time zone,timestamp with time zone)
     , FUNCTION 15 (timestamp with time zone,date) timestamptz_cmp_date(timestamp with time zone,date)
     , FUNCTION 15 (timestamp with time zone,timestamp without time zone) timestamptz_cmp_timestamp(timestamp with time zone,timestamp without time zone)
     , FUNCTION 15 (timestamp with time zone,timestamp with time zone) timestamptz_cmp(timestamp with time zone,timestamp with time zone)
;

CREATE OPERATOR CLASS interval_minmax_ops DEFAULT FOR TYPE interval USING toin
    AS OPERATOR 1 <(interval,interval)
     , OPERATOR 2 <=(interval,interval)
     , OPERATOR 3 =(interval,interval)
     , OPERATOR 4 >=(interval,interval)
     , OPERATOR 5 >(interval,interval)
     , FUNCTION 1 (interval,interval) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (interval,interval) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (interval,interval) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (interval,interval) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (interval,interval) interval_cmp(interval,interval)
;

CREATE OPERATOR CLASS timetz_minmax_ops DEFAULT FOR TYPE time with time zone USING toin
    AS OPERATOR 1 <(time with time zone,time with time zone)
     , OPERATOR 2 <=(time with time zone,time with time zone)
     , OPERATOR 3 =(time with time zone,time with time zone)
     , OPERATOR 4 >=(time with time zone,time with time zone)
     , OPERATOR 5 >(time with time zone,time with time zone)
     , FUNCTION 1 (time with time zone,time with time zone) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (time with time zone,time with time zone) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (time with time zone,time with time zone) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (time with time zone,time with time zone) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (time with time zone,time with time zone) timetz_cmp(time with time zone,time with time zone)
;

CREATE OPERATOR CLASS bit_minmax_ops DEFAULT FOR TYPE bit USING toin
    AS OPERATOR 1 <(bit,bit)
     , OPERATOR 2 <=(bit,bit)
     , OPERATOR 3 =(bit,bit)
     , OPERATOR 4 >=(bit,bit)
     , OPERATOR 5 >(bit,bit)
     , FUNCTION 1 (bit,bit) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (bit,bit) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (bit,bit) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (bit,bit) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (bit,bit) bitcmp(bit,bit)
;

CREATE OPERATOR CLASS varbit_minmax_ops DEFAULT FOR TYPE bit varying USING toin
    AS OPERATOR 1 <(bit varying,bit varying)
     , OPERATOR 2 <=(bit varying,bit varying)
     , OPERATOR 3 =(bit varying,bit varying)
     , OPERATOR 4 >=(bit varying,bit varying)
     , OPERATOR 5 >(bit varying,bit varying)
     , FUNCTION 1 (bit varying,bit varying) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (bit varying,bit varying) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (bit varying,bit varying) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (bit varying,bit varying) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (bit varying,bit varying) varbitcmp(bit varying,bit varying)
;

CREATE OPERATOR CLASS numeric_minmax_ops DEFAULT FOR TYPE numeric USING toin
    AS OPERATOR 1 <(numeric,numeric)
     , OPERATOR 2 <=(numeric,numeric)
     , OPERATOR 3 =(numeric,numeric)
     , OPERATOR 4 >=(numeric,numeric)
     , OPERATOR 5 >(numeric,numeric)
     , FUNCTION 1 (numeric,numeric) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (numeric,numeric) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (numeric,numeric) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (numeric,numeric) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (numeric,numeric) numeric_cmp(numeric,numeric)
;

CREATE OPERATOR CLASS uuid_minmax_ops DEFAULT FOR TYPE uuid USING toin
    AS OPERATOR 1 <(uuid,uuid)
     , OPERATOR 2 <=(uuid,uuid)
     , OPERATOR 3 =(uuid,uuid)
     , OPERATOR 4 >=(uuid,uuid)
     , OPERATOR 5 >(uuid,uuid)
     , FUNCTION 1 (uuid,uuid) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (uuid,uuid) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (uuid,uuid) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (uuid,uuid) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (uuid,uuid) uuid_cmp(uuid,uuid)
;

CREATE OPERATOR CLASS range_inclusion_ops DEFAULT FOR TYPE anyrange USING toin
    AS OPERATOR 1 <<(anyrange,anyrange)
     , OPERATOR 2 &<(anyrange,anyrange)
     , OPERATOR 3 &&(anyrange,anyrange)
     , OPERATOR 4 &>(anyrange,anyrange)
     , OPERATOR 5 >>(anyrange,anyrange)
     , OPERATOR 7 @>(anyrange,anyrange)
     , OPERATOR 8 <@(anyrange,anyrange)
     , OPERATOR 16 @>(anyrange,anyelement)
     , OPERATOR 17 -|-(anyrange,anyrange)
     , OPERATOR 18 =(anyrange,anyrange)
     , OPERATOR 20 <(anyrange,anyrange)
     , OPERATOR 21 <=(anyrange,anyrange)
     , OPERATOR 22 >(anyrange,anyrange)
     , OPERATOR 23 >=(anyrange,anyrange)
     , FUNCTION 1 (anyrange,anyrange) brin_inclusion_opcinfo(internal)
     , FUNCTION 2 (anyrange,anyrange) brin_inclusion_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (anyrange,anyrange) brin_inclusion_consistent(internal,internal,internal)
     , FUNCTION 4 (anyrange,anyrange) brin_inclusion_union(internal,internal,internal)
     , FUNCTION 11 (anyrange,anyrange) range_merge(anyrange,anyrange)
     , FUNCTION 13 (anyrange,anyrange) range_contains(anyrange,anyrange)
     , FUNCTION 14 (anyrange,anyrange) isempty(anyrange)
     , FUNCTION 15 (anyrange,anyrange) range_cmp(anyrange,anyrange)
;

CREATE OPERATOR CLASS pg_lsn_minmax_ops DEFAULT FOR TYPE pg_lsn USING toin
    AS OPERATOR 1 <(pg_lsn,pg_lsn)
     , OPERATOR 2 <=(pg_lsn,pg_lsn)
     , OPERATOR 3 =(pg_lsn,pg_lsn)
     , OPERATOR 4 >=(pg_lsn,pg_lsn)
     , OPERATOR 5 >(pg_lsn,pg_lsn)
     , FUNCTION 1 (pg_lsn,pg_lsn) brin_minmax_opcinfo(internal)
     , FUNCTION 2 (pg_lsn,pg_lsn) brin_minmax_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (pg_lsn,pg_lsn) brin_minmax_consistent(internal,internal,internal)
     , FUNCTION 4 (pg_lsn,pg_lsn) brin_minmax_union(internal,internal,internal)
     , FUNCTION 15 (pg_lsn,pg_lsn) pg_lsn_cmp(pg_lsn,pg_lsn)
;

CREATE OPERATOR CLASS box_inclusion_ops DEFAULT FOR TYPE box USING toin
    AS OPERATOR 1 <<(box,box)
     , OPERATOR 2 &<(box,box)
     , OPERATOR 3 &&(box,box)
     , OPERATOR 4 &>(box,box)
     , OPERATOR 5 >>(box,box)
     , OPERATOR 6 ~=(box,box)
     , OPERATOR 7 @>(box,point)
     , OPERATOR 7 @>(box,box)
     , OPERATOR 8 <@(box,box)
     , OPERATOR 9 &<|(box,box)
     , OPERATOR 10 <<|(box,box)
     , OPERATOR 11 |>>(box,box)
     , OPERATOR 12 |&>(box,box)
     , FUNCTION 1 (box,box) brin_inclusion_opcinfo(internal)
     , FUNCTION 2 (box,box) brin_inclusion_add_value(internal,internal,internal,internal)
     , FUNCTION 3 (box,box) brin_inclusion_consistent(internal,internal,internal)
     , FUNCTION 4 (box,box) brin_inclusion_union(internal,internal,internal)
     , FUNCTION 11 (box,box) bound_box(box,box)
     , FUNCTION 13 (box,box) box_contain(box,box)
;

--
-- ODIN indexes
--

CREATE OR REPLACE FUNCTION odinhandler(INTERNAL) RETURNS INDEX_AM_HANDLER
    AS '$libdir/matrixts', 'odinhandler' LANGUAGE C IMMUTABLE PARALLEL SAFE STRICT;

CREATE ACCESS METHOD odin TYPE INDEX HANDLER odinhandler;

--
-- operator classes to support btree operations
--

CREATE OPERATOR CLASS array_ops DEFAULT FOR TYPE anyarray USING odin
    AS OPERATOR 1 <(anyarray,anyarray)
     , OPERATOR 2 <=(anyarray,anyarray)
     , OPERATOR 3 =(anyarray,anyarray)
     , OPERATOR 4 >=(anyarray,anyarray)
     , OPERATOR 5 >(anyarray,anyarray)
     , FUNCTION 1 (anyarray,anyarray) btarraycmp(anyarray,anyarray)
;

CREATE OPERATOR CLASS bit_ops DEFAULT FOR TYPE bit USING odin
    AS OPERATOR 1 <(bit,bit)
     , OPERATOR 2 <=(bit,bit)
     , OPERATOR 3 =(bit,bit)
     , OPERATOR 4 >=(bit,bit)
     , OPERATOR 5 >(bit,bit)
     , FUNCTION 1 (bit,bit) bitcmp(bit,bit)
;

CREATE OPERATOR CLASS bool_ops DEFAULT FOR TYPE boolean USING odin
    AS OPERATOR 1 <(boolean,boolean)
     , OPERATOR 2 <=(boolean,boolean)
     , OPERATOR 3 =(boolean,boolean)
     , OPERATOR 4 >=(boolean,boolean)
     , OPERATOR 5 >(boolean,boolean)
     , FUNCTION 1 (boolean,boolean) btboolcmp(boolean,boolean)
;

CREATE OPERATOR CLASS bpchar_ops DEFAULT FOR TYPE character USING odin
    AS OPERATOR 1 <(character,character)
     , OPERATOR 2 <=(character,character)
     , OPERATOR 3 =(character,character)
     , OPERATOR 4 >=(character,character)
     , OPERATOR 5 >(character,character)
     , FUNCTION 1 (character,character) bpcharcmp(character,character)
;

CREATE OPERATOR CLASS bytea_ops DEFAULT FOR TYPE bytea USING odin
    AS OPERATOR 1 <(bytea,bytea)
     , OPERATOR 2 <=(bytea,bytea)
     , OPERATOR 3 =(bytea,bytea)
     , OPERATOR 4 >=(bytea,bytea)
     , OPERATOR 5 >(bytea,bytea)
     , FUNCTION 1 (bytea,bytea) byteacmp(bytea,bytea)
;

CREATE OPERATOR CLASS char_ops DEFAULT FOR TYPE "char" USING odin
    AS OPERATOR 1 <("char","char")
     , OPERATOR 2 <=("char","char")
     , OPERATOR 3 =("char","char")
     , OPERATOR 4 >=("char","char")
     , OPERATOR 5 >("char","char")
     , FUNCTION 1 ("char","char") btcharcmp("char","char")
;

CREATE OPERATOR CLASS date_ops DEFAULT FOR TYPE date USING odin
    AS OPERATOR 1 <(date,date)
     , OPERATOR 1 <(date,timestamp without time zone)
     , OPERATOR 1 <(date,timestamp with time zone)
     , OPERATOR 1 <(timestamp without time zone,date)
     , OPERATOR 1 <(timestamp with time zone,date)
     , OPERATOR 2 <=(date,date)
     , OPERATOR 2 <=(date,timestamp without time zone)
     , OPERATOR 2 <=(date,timestamp with time zone)
     , OPERATOR 2 <=(timestamp without time zone,date)
     , OPERATOR 2 <=(timestamp with time zone,date)
     , OPERATOR 3 =(date,date)
     , OPERATOR 3 =(date,timestamp without time zone)
     , OPERATOR 3 =(date,timestamp with time zone)
     , OPERATOR 3 =(timestamp without time zone,date)
     , OPERATOR 3 =(timestamp with time zone,date)
     , OPERATOR 4 >=(date,date)
     , OPERATOR 4 >=(date,timestamp without time zone)
     , OPERATOR 4 >=(date,timestamp with time zone)
     , OPERATOR 4 >=(timestamp without time zone,date)
     , OPERATOR 4 >=(timestamp with time zone,date)
     , OPERATOR 5 >(date,date)
     , OPERATOR 5 >(date,timestamp without time zone)
     , OPERATOR 5 >(date,timestamp with time zone)
     , OPERATOR 5 >(timestamp without time zone,date)
     , OPERATOR 5 >(timestamp with time zone,date)
     , FUNCTION 1 (date,date) date_cmp(date,date)
     , FUNCTION 1 (date,timestamp without time zone) date_cmp_timestamp(date,timestamp without time zone)
     , FUNCTION 1 (date,timestamp with time zone) date_cmp_timestamptz(date,timestamp with time zone)
     , FUNCTION 1 (timestamp without time zone,date) timestamp_cmp_date(timestamp without time zone,date)
     , FUNCTION 1 (timestamp with time zone,date) timestamptz_cmp_date(timestamp with time zone,date)
;

CREATE OPERATOR CLASS float4_ops DEFAULT FOR TYPE real USING odin
    AS OPERATOR 1 <(real,real)
     , OPERATOR 1 <(real,double precision)
     , OPERATOR 1 <(double precision,real)
     , OPERATOR 2 <=(real,real)
     , OPERATOR 2 <=(real,double precision)
     , OPERATOR 2 <=(double precision,real)
     , OPERATOR 3 =(real,real)
     , OPERATOR 3 =(real,double precision)
     , OPERATOR 3 =(double precision,real)
     , OPERATOR 4 >=(real,real)
     , OPERATOR 4 >=(real,double precision)
     , OPERATOR 4 >=(double precision,real)
     , OPERATOR 5 >(real,real)
     , OPERATOR 5 >(real,double precision)
     , OPERATOR 5 >(double precision,real)
     , FUNCTION 1 (real,real) btfloat4cmp(real,real)
     , FUNCTION 1 (real,double precision) btfloat48cmp(real,double precision)
     , FUNCTION 1 (double precision,real) btfloat84cmp(double precision,real)
;

CREATE OPERATOR CLASS float8_ops DEFAULT FOR TYPE double precision USING odin
    AS OPERATOR 1 <(real,double precision)
     , OPERATOR 1 <(double precision,real)
     , OPERATOR 1 <(double precision,double precision)
     , OPERATOR 2 <=(real,double precision)
     , OPERATOR 2 <=(double precision,real)
     , OPERATOR 2 <=(double precision,double precision)
     , OPERATOR 3 =(real,double precision)
     , OPERATOR 3 =(double precision,real)
     , OPERATOR 3 =(double precision,double precision)
     , OPERATOR 4 >=(real,double precision)
     , OPERATOR 4 >=(double precision,real)
     , OPERATOR 4 >=(double precision,double precision)
     , OPERATOR 5 >(real,double precision)
     , OPERATOR 5 >(double precision,real)
     , OPERATOR 5 >(double precision,double precision)
     , FUNCTION 1 (real,double precision) btfloat48cmp(real,double precision)
     , FUNCTION 1 (double precision,real) btfloat84cmp(double precision,real)
     , FUNCTION 1 (double precision,double precision) btfloat8cmp(double precision,double precision)
;

CREATE OPERATOR CLASS inet_ops DEFAULT FOR TYPE inet USING odin
    AS OPERATOR 1 <(inet,inet)
     , OPERATOR 2 <=(inet,inet)
     , OPERATOR 3 =(inet,inet)
     , OPERATOR 4 >=(inet,inet)
     , OPERATOR 5 >(inet,inet)
     , FUNCTION 1 (inet,inet) network_cmp(inet,inet)
;

CREATE OPERATOR CLASS int2_ops DEFAULT FOR TYPE smallint USING odin
    AS OPERATOR 1 <(bigint,smallint)
     , OPERATOR 1 <(smallint,bigint)
     , OPERATOR 1 <(smallint,smallint)
     , OPERATOR 1 <(smallint,integer)
     , OPERATOR 1 <(integer,smallint)
     , OPERATOR 2 <=(bigint,smallint)
     , OPERATOR 2 <=(smallint,bigint)
     , OPERATOR 2 <=(smallint,smallint)
     , OPERATOR 2 <=(smallint,integer)
     , OPERATOR 2 <=(integer,smallint)
     , OPERATOR 3 =(bigint,smallint)
     , OPERATOR 3 =(smallint,bigint)
     , OPERATOR 3 =(smallint,smallint)
     , OPERATOR 3 =(smallint,integer)
     , OPERATOR 3 =(integer,smallint)
     , OPERATOR 4 >=(bigint,smallint)
     , OPERATOR 4 >=(smallint,bigint)
     , OPERATOR 4 >=(smallint,smallint)
     , OPERATOR 4 >=(smallint,integer)
     , OPERATOR 4 >=(integer,smallint)
     , OPERATOR 5 >(bigint,smallint)
     , OPERATOR 5 >(smallint,bigint)
     , OPERATOR 5 >(smallint,smallint)
     , OPERATOR 5 >(smallint,integer)
     , OPERATOR 5 >(integer,smallint)
     , FUNCTION 1 (bigint,smallint) btint82cmp(bigint,smallint)
     , FUNCTION 1 (smallint,bigint) btint28cmp(smallint,bigint)
     , FUNCTION 1 (smallint,smallint) btint2cmp(smallint,smallint)
     , FUNCTION 1 (smallint,integer) btint24cmp(smallint,integer)
     , FUNCTION 1 (integer,smallint) btint42cmp(integer,smallint)
;

CREATE OPERATOR CLASS int4_ops DEFAULT FOR TYPE integer USING odin
    AS OPERATOR 1 <(bigint,integer)
     , OPERATOR 1 <(smallint,integer)
     , OPERATOR 1 <(integer,bigint)
     , OPERATOR 1 <(integer,smallint)
     , OPERATOR 1 <(integer,integer)
     , OPERATOR 2 <=(bigint,integer)
     , OPERATOR 2 <=(smallint,integer)
     , OPERATOR 2 <=(integer,bigint)
     , OPERATOR 2 <=(integer,smallint)
     , OPERATOR 2 <=(integer,integer)
     , OPERATOR 3 =(bigint,integer)
     , OPERATOR 3 =(smallint,integer)
     , OPERATOR 3 =(integer,bigint)
     , OPERATOR 3 =(integer,smallint)
     , OPERATOR 3 =(integer,integer)
     , OPERATOR 4 >=(bigint,integer)
     , OPERATOR 4 >=(smallint,integer)
     , OPERATOR 4 >=(integer,bigint)
     , OPERATOR 4 >=(integer,smallint)
     , OPERATOR 4 >=(integer,integer)
     , OPERATOR 5 >(bigint,integer)
     , OPERATOR 5 >(smallint,integer)
     , OPERATOR 5 >(integer,bigint)
     , OPERATOR 5 >(integer,smallint)
     , OPERATOR 5 >(integer,integer)
     , FUNCTION 1 (bigint,integer) btint84cmp(bigint,integer)
     , FUNCTION 1 (smallint,integer) btint24cmp(smallint,integer)
     , FUNCTION 1 (integer,bigint) btint48cmp(integer,bigint)
     , FUNCTION 1 (integer,smallint) btint42cmp(integer,smallint)
     , FUNCTION 1 (integer,integer) btint4cmp(integer,integer)
;

CREATE OPERATOR CLASS int8_ops DEFAULT FOR TYPE bigint USING odin
    AS OPERATOR 1 <(bigint,bigint)
     , OPERATOR 1 <(bigint,smallint)
     , OPERATOR 1 <(bigint,integer)
     , OPERATOR 1 <(smallint,bigint)
     , OPERATOR 1 <(integer,bigint)
     , OPERATOR 2 <=(bigint,bigint)
     , OPERATOR 2 <=(bigint,smallint)
     , OPERATOR 2 <=(bigint,integer)
     , OPERATOR 2 <=(smallint,bigint)
     , OPERATOR 2 <=(integer,bigint)
     , OPERATOR 3 =(bigint,bigint)
     , OPERATOR 3 =(bigint,smallint)
     , OPERATOR 3 =(bigint,integer)
     , OPERATOR 3 =(smallint,bigint)
     , OPERATOR 3 =(integer,bigint)
     , OPERATOR 4 >=(bigint,bigint)
     , OPERATOR 4 >=(bigint,smallint)
     , OPERATOR 4 >=(bigint,integer)
     , OPERATOR 4 >=(smallint,bigint)
     , OPERATOR 4 >=(integer,bigint)
     , OPERATOR 5 >(bigint,bigint)
     , OPERATOR 5 >(bigint,smallint)
     , OPERATOR 5 >(bigint,integer)
     , OPERATOR 5 >(smallint,bigint)
     , OPERATOR 5 >(integer,bigint)
     , FUNCTION 1 (bigint,bigint) btint8cmp(bigint,bigint)
     , FUNCTION 1 (bigint,smallint) btint82cmp(bigint,smallint)
     , FUNCTION 1 (bigint,integer) btint84cmp(bigint,integer)
     , FUNCTION 1 (smallint,bigint) btint28cmp(smallint,bigint)
     , FUNCTION 1 (integer,bigint) btint48cmp(integer,bigint)
;

CREATE OPERATOR CLASS interval_ops DEFAULT FOR TYPE interval USING odin
    AS OPERATOR 1 <(interval,interval)
     , OPERATOR 2 <=(interval,interval)
     , OPERATOR 3 =(interval,interval)
     , OPERATOR 4 >=(interval,interval)
     , OPERATOR 5 >(interval,interval)
     , FUNCTION 1 (interval,interval) interval_cmp(interval,interval)
;

CREATE OPERATOR CLASS macaddr_ops DEFAULT FOR TYPE macaddr USING odin
    AS OPERATOR 1 <(macaddr,macaddr)
     , OPERATOR 2 <=(macaddr,macaddr)
     , OPERATOR 3 =(macaddr,macaddr)
     , OPERATOR 4 >=(macaddr,macaddr)
     , OPERATOR 5 >(macaddr,macaddr)
     , FUNCTION 1 (macaddr,macaddr) macaddr_cmp(macaddr,macaddr)
;

CREATE OPERATOR CLASS macaddr8_ops DEFAULT FOR TYPE macaddr8 USING odin
    AS OPERATOR 1 <(macaddr8,macaddr8)
     , OPERATOR 2 <=(macaddr8,macaddr8)
     , OPERATOR 3 =(macaddr8,macaddr8)
     , OPERATOR 4 >=(macaddr8,macaddr8)
     , OPERATOR 5 >(macaddr8,macaddr8)
     , FUNCTION 1 (macaddr8,macaddr8) macaddr8_cmp(macaddr8,macaddr8)
;

CREATE OPERATOR CLASS name_ops DEFAULT FOR TYPE name USING odin
    AS OPERATOR 1 <(name,name)
     , OPERATOR 1 <(name,text)
     , OPERATOR 1 <(text,name)
     , OPERATOR 2 <=(name,name)
     , OPERATOR 2 <=(name,text)
     , OPERATOR 2 <=(text,name)
     , OPERATOR 3 =(name,name)
     , OPERATOR 3 =(name,text)
     , OPERATOR 3 =(text,name)
     , OPERATOR 4 >=(name,name)
     , OPERATOR 4 >=(name,text)
     , OPERATOR 4 >=(text,name)
     , OPERATOR 5 >(name,name)
     , OPERATOR 5 >(name,text)
     , OPERATOR 5 >(text,name)
     , FUNCTION 1 (name,name) btnamecmp(name,name)
     , FUNCTION 1 (name,text) btnametextcmp(name,text)
     , FUNCTION 1 (text,name) bttextnamecmp(text,name)
;

CREATE OPERATOR CLASS numeric_ops DEFAULT FOR TYPE numeric USING odin
    AS OPERATOR 1 <(numeric,numeric)
     , OPERATOR 2 <=(numeric,numeric)
     , OPERATOR 3 =(numeric,numeric)
     , OPERATOR 4 >=(numeric,numeric)
     , OPERATOR 5 >(numeric,numeric)
     , FUNCTION 1 (numeric,numeric) numeric_cmp(numeric,numeric)
;

CREATE OPERATOR CLASS complex_ops DEFAULT FOR TYPE complex USING odin
    AS OPERATOR 1 <<(complex,complex)
     , OPERATOR 2 <<=(complex,complex)
     , OPERATOR 3 =(complex,complex)
     , OPERATOR 4 >>=(complex,complex)
     , OPERATOR 5 >>(complex,complex)
     , FUNCTION 1 (complex,complex) complex_cmp(complex,complex)
;

CREATE OPERATOR CLASS oid_ops DEFAULT FOR TYPE oid USING odin
    AS OPERATOR 1 <(oid,oid)
     , OPERATOR 2 <=(oid,oid)
     , OPERATOR 3 =(oid,oid)
     , OPERATOR 4 >=(oid,oid)
     , OPERATOR 5 >(oid,oid)
     , FUNCTION 1 (oid,oid) btoidcmp(oid,oid)
;

CREATE OPERATOR CLASS oidvector_ops DEFAULT FOR TYPE oidvector USING odin
    AS OPERATOR 1 <(oidvector,oidvector)
     , OPERATOR 2 <=(oidvector,oidvector)
     , OPERATOR 3 =(oidvector,oidvector)
     , OPERATOR 4 >=(oidvector,oidvector)
     , OPERATOR 5 >(oidvector,oidvector)
     , FUNCTION 1 (oidvector,oidvector) btoidvectorcmp(oidvector,oidvector)
;

CREATE OPERATOR CLASS record_ops DEFAULT FOR TYPE record USING odin
    AS OPERATOR 1 <(record,record)
     , OPERATOR 2 <=(record,record)
     , OPERATOR 3 =(record,record)
     , OPERATOR 4 >=(record,record)
     , OPERATOR 5 >(record,record)
     , FUNCTION 1 (record,record) btrecordcmp(record,record)
;

CREATE OPERATOR CLASS text_ops DEFAULT FOR TYPE text USING odin
    AS OPERATOR 1 <(name,text)
     , OPERATOR 1 <(text,name)
     , OPERATOR 1 <(text,text)
     , OPERATOR 2 <=(name,text)
     , OPERATOR 2 <=(text,name)
     , OPERATOR 2 <=(text,text)
     , OPERATOR 3 =(name,text)
     , OPERATOR 3 =(text,name)
     , OPERATOR 3 =(text,text)
     , OPERATOR 4 >=(name,text)
     , OPERATOR 4 >=(text,name)
     , OPERATOR 4 >=(text,text)
     , OPERATOR 5 >(name,text)
     , OPERATOR 5 >(text,name)
     , OPERATOR 5 >(text,text)
     , FUNCTION 1 (name,text) btnametextcmp(name,text)
     , FUNCTION 1 (text,name) bttextnamecmp(text,name)
     , FUNCTION 1 (text,text) bttextcmp(text,text)
;

CREATE OPERATOR CLASS time_ops DEFAULT FOR TYPE time without time zone USING odin
    AS OPERATOR 1 <(time without time zone,time without time zone)
     , OPERATOR 2 <=(time without time zone,time without time zone)
     , OPERATOR 3 =(time without time zone,time without time zone)
     , OPERATOR 4 >=(time without time zone,time without time zone)
     , OPERATOR 5 >(time without time zone,time without time zone)
     , FUNCTION 1 (time without time zone,time without time zone) time_cmp(time without time zone,time without time zone)
;

CREATE OPERATOR CLASS timestamptz_ops DEFAULT FOR TYPE timestamp with time zone USING odin
    AS OPERATOR 1 <(date,timestamp with time zone)
     , OPERATOR 1 <(timestamp without time zone,timestamp with time zone)
     , OPERATOR 1 <(timestamp with time zone,date)
     , OPERATOR 1 <(timestamp with time zone,timestamp without time zone)
     , OPERATOR 1 <(timestamp with time zone,timestamp with time zone)
     , OPERATOR 2 <=(date,timestamp with time zone)
     , OPERATOR 2 <=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 2 <=(timestamp with time zone,date)
     , OPERATOR 2 <=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 2 <=(timestamp with time zone,timestamp with time zone)
     , OPERATOR 3 =(date,timestamp with time zone)
     , OPERATOR 3 =(timestamp without time zone,timestamp with time zone)
     , OPERATOR 3 =(timestamp with time zone,date)
     , OPERATOR 3 =(timestamp with time zone,timestamp without time zone)
     , OPERATOR 3 =(timestamp with time zone,timestamp with time zone)
     , OPERATOR 4 >=(date,timestamp with time zone)
     , OPERATOR 4 >=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 4 >=(timestamp with time zone,date)
     , OPERATOR 4 >=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 4 >=(timestamp with time zone,timestamp with time zone)
     , OPERATOR 5 >(date,timestamp with time zone)
     , OPERATOR 5 >(timestamp without time zone,timestamp with time zone)
     , OPERATOR 5 >(timestamp with time zone,date)
     , OPERATOR 5 >(timestamp with time zone,timestamp without time zone)
     , OPERATOR 5 >(timestamp with time zone,timestamp with time zone)
     , FUNCTION 1 (date,timestamp with time zone) date_cmp_timestamptz(date,timestamp with time zone)
     , FUNCTION 1 (timestamp without time zone,timestamp with time zone) timestamp_cmp_timestamptz(timestamp without time zone,timestamp with time zone)
     , FUNCTION 1 (timestamp with time zone,date) timestamptz_cmp_date(timestamp with time zone,date)
     , FUNCTION 1 (timestamp with time zone,timestamp without time zone) timestamptz_cmp_timestamp(timestamp with time zone,timestamp without time zone)
     , FUNCTION 1 (timestamp with time zone,timestamp with time zone) timestamptz_cmp(timestamp with time zone,timestamp with time zone)
;

CREATE OPERATOR CLASS timetz_ops DEFAULT FOR TYPE time with time zone USING odin
    AS OPERATOR 1 <(time with time zone,time with time zone)
     , OPERATOR 2 <=(time with time zone,time with time zone)
     , OPERATOR 3 =(time with time zone,time with time zone)
     , OPERATOR 4 >=(time with time zone,time with time zone)
     , OPERATOR 5 >(time with time zone,time with time zone)
     , FUNCTION 1 (time with time zone,time with time zone) timetz_cmp(time with time zone,time with time zone)
;

CREATE OPERATOR CLASS varbit_ops DEFAULT FOR TYPE bit varying USING odin
    AS OPERATOR 1 <(bit varying,bit varying)
     , OPERATOR 2 <=(bit varying,bit varying)
     , OPERATOR 3 =(bit varying,bit varying)
     , OPERATOR 4 >=(bit varying,bit varying)
     , OPERATOR 5 >(bit varying,bit varying)
     , FUNCTION 1 (bit varying,bit varying) varbitcmp(bit varying,bit varying)
;

CREATE OPERATOR CLASS timestamp_ops DEFAULT FOR TYPE timestamp without time zone USING odin
    AS OPERATOR 1 <(date,timestamp without time zone)
     , OPERATOR 1 <(timestamp without time zone,date)
     , OPERATOR 1 <(timestamp without time zone,timestamp without time zone)
     , OPERATOR 1 <(timestamp without time zone,timestamp with time zone)
     , OPERATOR 1 <(timestamp with time zone,timestamp without time zone)
     , OPERATOR 2 <=(date,timestamp without time zone)
     , OPERATOR 2 <=(timestamp without time zone,date)
     , OPERATOR 2 <=(timestamp without time zone,timestamp without time zone)
     , OPERATOR 2 <=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 2 <=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 3 =(date,timestamp without time zone)
     , OPERATOR 3 =(timestamp without time zone,date)
     , OPERATOR 3 =(timestamp without time zone,timestamp without time zone)
     , OPERATOR 3 =(timestamp without time zone,timestamp with time zone)
     , OPERATOR 3 =(timestamp with time zone,timestamp without time zone)
     , OPERATOR 4 >=(date,timestamp without time zone)
     , OPERATOR 4 >=(timestamp without time zone,date)
     , OPERATOR 4 >=(timestamp without time zone,timestamp without time zone)
     , OPERATOR 4 >=(timestamp without time zone,timestamp with time zone)
     , OPERATOR 4 >=(timestamp with time zone,timestamp without time zone)
     , OPERATOR 5 >(date,timestamp without time zone)
     , OPERATOR 5 >(timestamp without time zone,date)
     , OPERATOR 5 >(timestamp without time zone,timestamp without time zone)
     , OPERATOR 5 >(timestamp without time zone,timestamp with time zone)
     , OPERATOR 5 >(timestamp with time zone,timestamp without time zone)
     , FUNCTION 1 (date,timestamp without time zone) date_cmp_timestamp(date,timestamp without time zone)
     , FUNCTION 1 (timestamp without time zone,date) timestamp_cmp_date(timestamp without time zone,date)
     , FUNCTION 1 (timestamp without time zone,timestamp without time zone) timestamp_cmp(timestamp without time zone,timestamp without time zone)
     , FUNCTION 1 (timestamp without time zone,timestamp with time zone) timestamp_cmp_timestamptz(timestamp without time zone,timestamp with time zone)
     , FUNCTION 1 (timestamp with time zone,timestamp without time zone) timestamptz_cmp_timestamp(timestamp with time zone,timestamp without time zone)
;

CREATE OPERATOR CLASS money_ops DEFAULT FOR TYPE money USING odin
    AS OPERATOR 1 <(money,money)
     , OPERATOR 2 <=(money,money)
     , OPERATOR 3 =(money,money)
     , OPERATOR 4 >=(money,money)
     , OPERATOR 5 >(money,money)
     , FUNCTION 1 (money,money) cash_cmp(money,money)
;

CREATE OPERATOR CLASS tid_ops DEFAULT FOR TYPE tid USING odin
    AS OPERATOR 1 <(tid,tid)
     , OPERATOR 2 <=(tid,tid)
     , OPERATOR 3 =(tid,tid)
     , OPERATOR 4 >=(tid,tid)
     , OPERATOR 5 >(tid,tid)
     , FUNCTION 1 (tid,tid) bttidcmp(tid,tid)
;

CREATE OPERATOR CLASS uuid_ops DEFAULT FOR TYPE uuid USING odin
    AS OPERATOR 1 <(uuid,uuid)
     , OPERATOR 2 <=(uuid,uuid)
     , OPERATOR 3 =(uuid,uuid)
     , OPERATOR 4 >=(uuid,uuid)
     , OPERATOR 5 >(uuid,uuid)
     , FUNCTION 1 (uuid,uuid) uuid_cmp(uuid,uuid)
;

CREATE OPERATOR CLASS pg_lsn_ops DEFAULT FOR TYPE pg_lsn USING odin
    AS OPERATOR 1 <(pg_lsn,pg_lsn)
     , OPERATOR 2 <=(pg_lsn,pg_lsn)
     , OPERATOR 3 =(pg_lsn,pg_lsn)
     , OPERATOR 4 >=(pg_lsn,pg_lsn)
     , OPERATOR 5 >(pg_lsn,pg_lsn)
     , FUNCTION 1 (pg_lsn,pg_lsn) pg_lsn_cmp(pg_lsn,pg_lsn)
;

CREATE OPERATOR CLASS enum_ops DEFAULT FOR TYPE anyenum USING odin
    AS OPERATOR 1 <(anyenum,anyenum)
     , OPERATOR 2 <=(anyenum,anyenum)
     , OPERATOR 3 =(anyenum,anyenum)
     , OPERATOR 4 >=(anyenum,anyenum)
     , OPERATOR 5 >(anyenum,anyenum)
     , FUNCTION 1 (anyenum,anyenum) enum_cmp(anyenum,anyenum)
;

CREATE OPERATOR CLASS tsvector_ops DEFAULT FOR TYPE tsvector USING odin
    AS OPERATOR 1 <(tsvector,tsvector)
     , OPERATOR 2 <=(tsvector,tsvector)
     , OPERATOR 3 =(tsvector,tsvector)
     , OPERATOR 4 >=(tsvector,tsvector)
     , OPERATOR 5 >(tsvector,tsvector)
     , FUNCTION 1 (tsvector,tsvector) tsvector_cmp(tsvector,tsvector)
;

CREATE OPERATOR CLASS tsquery_ops DEFAULT FOR TYPE tsquery USING odin
    AS OPERATOR 1 <(tsquery,tsquery)
     , OPERATOR 2 <=(tsquery,tsquery)
     , OPERATOR 3 =(tsquery,tsquery)
     , OPERATOR 4 >=(tsquery,tsquery)
     , OPERATOR 5 >(tsquery,tsquery)
     , FUNCTION 1 (tsquery,tsquery) tsquery_cmp(tsquery,tsquery)
;

CREATE OPERATOR CLASS range_ops DEFAULT FOR TYPE anyrange USING odin
    AS OPERATOR 1 <(anyrange,anyrange)
     , OPERATOR 2 <=(anyrange,anyrange)
     , OPERATOR 3 =(anyrange,anyrange)
     , OPERATOR 4 >=(anyrange,anyrange)
     , OPERATOR 5 >(anyrange,anyrange)
     , FUNCTION 1 (anyrange,anyrange) range_cmp(anyrange,anyrange)
;

CREATE OPERATOR CLASS jsonb_ops DEFAULT FOR TYPE jsonb USING odin
    AS OPERATOR 1 <(jsonb,jsonb)
     , OPERATOR 2 <=(jsonb,jsonb)
     , OPERATOR 3 =(jsonb,jsonb)
     , OPERATOR 4 >=(jsonb,jsonb)
     , OPERATOR 5 >(jsonb,jsonb)
     , FUNCTION 1 (jsonb,jsonb) jsonb_cmp(jsonb,jsonb)
;

-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width SMALLINT, ts SMALLINT, start SMALLINT=NULL, finish SMALLINT=NULL) RETURNS SMALLINT
	AS '$libdir/matrixts', 'gapfill_bucket_int16' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width INT, ts INT, start INT=NULL, finish INT=NULL) RETURNS INT
	AS '$libdir/matrixts', 'gapfill_bucket_int32' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width BIGINT, ts BIGINT, start BIGINT=NULL, finish BIGINT=NULL) RETURNS BIGINT
	AS '$libdir/matrixts', 'gapfill_bucket_int64' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts DATE, start DATE=NULL, finish DATE=NULL) RETURNS DATE
	AS '$libdir/matrixts', 'gapfill_bucket_date' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMP, start TIMESTAMP=NULL, finish TIMESTAMP=NULL) RETURNS TIMESTAMP
	AS '$libdir/matrixts', 'gapfill_bucket_timestamp' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
	AS '$libdir/matrixts', 'gapfill_bucket_timestamptz' LANGUAGE C VOLATILE PARALLEL SAFE;

-- locf function
CREATE OR REPLACE FUNCTION locf(value ANYELEMENT) RETURNS ANYELEMENT
	AS '$libdir/matrixts', 'gapfill_interpolate' LANGUAGE C VOLATILE PARALLEL SAFE;

-- interpolate functions
CREATE OR REPLACE FUNCTION interpolate(value SMALLINT) RETURNS SMALLINT
	AS '$libdir/matrixts', 'gapfill_interpolate' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION interpolate(value INT) RETURNS INT
	AS '$libdir/matrixts', 'gapfill_interpolate' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION interpolate(value BIGINT) RETURNS BIGINT
	AS '$libdir/matrixts', 'gapfill_interpolate' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION interpolate(value REAL) RETURNS REAL
	AS '$libdir/matrixts', 'gapfill_interpolate' LANGUAGE C VOLATILE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION interpolate(value FLOAT) RETURNS FLOAT
	AS '$libdir/matrixts', 'gapfill_interpolate' LANGUAGE C VOLATILE PARALLEL SAFE;


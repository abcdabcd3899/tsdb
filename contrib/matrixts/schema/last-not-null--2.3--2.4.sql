-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION matrixts" to load this file. \quit

CREATE OR REPLACE FUNCTION
	matrixts_internal.last_not_null_kv_sfunc(internal, anyelement, "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'mx_last_not_null_kv_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
	matrixts_internal.last_not_null_kv_finalfunc(internal)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'mx_last_not_null_kv_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
	matrixts_internal.last_not_null_kv_value_finalfunc(internal)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'mx_last_not_null_kv_value_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
	matrixts_internal.last_not_null_kv_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/matrixts', 'mx_last_not_null_kv_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
	matrixts_internal.last_not_null_kv_serializefunc(internal)
RETURNS bytea
AS '$libdir/matrixts', 'mx_last_not_null_kv_serializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
	matrixts_internal.last_not_null_kv_deserializefunc(bytea, internal)
RETURNS internal
AS '$libdir/matrixts', 'mx_last_not_null_kv_deserializefunc'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- This is the kv version of the last_not_null() agg, the last not null values
-- are collected on the keys.
CREATE AGGREGATE last_not_null_kv(anyelement, "any") (
    SFUNC = matrixts_internal.last_not_null_kv_sfunc,
    COMBINEFUNC = matrixts_internal.last_not_null_kv_combinefunc,
    SERIALFUNC = matrixts_internal.last_not_null_kv_serializefunc,
    DESERIALFUNC = matrixts_internal.last_not_null_kv_deserializefunc,
    FINALFUNC = matrixts_internal.last_not_null_kv_finalfunc,
    STYPE = internal,
    PARALLEL = SAFE
);

-- This is the kv version of the last_not_null_value() agg, the last not null
-- values are collected on the keys.
CREATE AGGREGATE last_not_null_kv_value(anyelement, "any") (
    SFUNC = matrixts_internal.last_not_null_kv_sfunc,
    COMBINEFUNC = matrixts_internal.last_not_null_kv_combinefunc,
    SERIALFUNC = matrixts_internal.last_not_null_kv_serializefunc,
    DESERIALFUNC = matrixts_internal.last_not_null_kv_deserializefunc,
    FINALFUNC = matrixts_internal.last_not_null_kv_value_finalfunc,
    STYPE = internal,
    PARALLEL = SAFE
);

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION matrixts" to load this file. \quit

CREATE OR REPLACE FUNCTION
	matrixts_internal.last_not_null_sfunc(internal, anyelement, "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'mx_last_not_null_sfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
	matrixts_internal.last_not_null_finalfunc(internal)
RETURNS text
AS 'MODULE_PATHNAME', 'mx_last_not_null_finalfunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION
	matrixts_internal.last_not_null_combinefunc(internal, internal)
RETURNS internal
AS '$libdir/matrixts', 'mx_last_not_null_combinefunc'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- This aggregate returns the last not null value of the first argument (aka
-- value) when ordered by the second (aka time), the result is a text in json
-- syntax, the format is '["value", "time"]', which represents the last not
-- null value, and the time of that value.
CREATE AGGREGATE last_not_null(anyelement, "any") (
    SFUNC = matrixts_internal.last_not_null_sfunc,
    FINALFUNC = matrixts_internal.last_not_null_finalfunc,
    COMBINEFUNC = matrixts_internal.last_not_null_combinefunc,
    STYPE = internal,
    SERIALFUNC = matrixts_internal.bookend_serializefunc,
    DESERIALFUNC = matrixts_internal.bookend_deserializefunc,
    PARALLEL = SAFE
);

-- This aggregate returns the last not null value of the first argument (aka
-- value) when ordered by the second (aka time).  Unlike last_not_null(), the
-- result is the last not null value directly.
CREATE AGGREGATE last_not_null_value(anyelement, "any") (
    SFUNC = matrixts_internal.last_not_null_sfunc,
    COMBINEFUNC = matrixts_internal.last_not_null_combinefunc,
    STYPE = internal,
    SERIALFUNC = matrixts_internal.bookend_serializefunc,
    DESERIALFUNC = matrixts_internal.bookend_deserializefunc,
    FINALFUNC = matrixts_internal.bookend_finalfunc,
    FINALFUNC_EXTRA,
    PARALLEL = SAFE
);

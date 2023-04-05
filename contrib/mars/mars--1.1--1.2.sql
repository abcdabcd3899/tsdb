\echo Use "CREATE EXTENSION mars" to load this file. \quit

CREATE TABLE matrixts_internal.monotonic_func
(
    proc            regprocedure not null ,
    equivalence_arg        int2,          -- If the input is order by this arg, the `proc` result is also ordered.
    equivalence_proc       regprocedure[] -- If the input is order by `proc` result, then data is always order by these procedures' result
) DISTRIBUTED MASTERONLY;

INSERT INTO matrixts_internal.monotonic_func
VALUES ('time_bucket(INTERVAL, TIMESTAMP)'::regprocedure, 2, NULL)
     , ('time_bucket(INTERVAL, TIMESTAMPTZ)'::regprocedure, 2, NULL)
     , ('time_bucket_gapfill(INTERVAL, TIMESTAMP, TIMESTAMP, TIMESTAMP)'::regprocedure, 2,'{"time_bucket(INTERVAL, TIMESTAMP)"}'::regprocedure[])
     , ('time_bucket_gapfill(INTERVAL, TIMESTAMPTZ, TIMESTAMPTZ, TIMESTAMPTZ)'::regprocedure, 2, '{"time_bucket(INTERVAL, TIMESTAMPTZ)"}'::regprocedure[]);

CREATE OR REPLACE FUNCTION mars.catalog_manual_init_()
RETURNS BOOLEAN
AS '$libdir/mars', 'mars_catalog_manual_init' LANGUAGE C STRICT;
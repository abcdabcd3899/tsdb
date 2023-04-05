--
-- Routines for continuous aggreagtion
--
-- Create the cvsortheap access method
CREATE FUNCTION matrixts_internal.cvsortheap_tableam_handler(INTERNAL) RETURNS table_am_handler
AS '$libdir/matrixts' LANGUAGE C STRICT;

CREATE ACCESS METHOD cvsortheap
TYPE TABLE
HANDLER matrixts_internal.cvsortheap_tableam_handler;

-- Create trigger function for continuous view
CREATE FUNCTION matrixts_internal.continuous_view_insert_trigger() RETURNS trigger
AS '$libdir/matrixts' LANGUAGE C STRICT;

-- Create auxiliary table continuous_views
CREATE TABLE matrixts_internal.continuous_views(srcoid int, srcname text,
                                                viewoid int, viewname text,
                                                auxoid int, auxname text,
                                                querytree text,
                                                querytext text,
												toplevel bool) distributed replicated;

-- Create help function to manually trigger a MERGE
CREATE OR REPLACE FUNCTION matrixts_internal.vacuum_continuous_view(r1 REGCLASS, toplevel BOOL) RETURNS BOOL
    AS '$libdir/matrixts', 'vacuum_continuous_view'
    LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT EXECUTE ON MASTER;

-- Create help function to get the info of the continuous view storage
CREATE OR REPLACE FUNCTION matrixts_internal.info_continuous_view(r1 REGCLASS) RETURNS CSTRING
    AS '$libdir/matrixts', 'info_continuous_view'
    LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT EXECUTE ON ALL SEGMENTS;

-- Create help function to manually trigger an ANALYZE on continuous view
CREATE OR REPLACE FUNCTION matrixts_internal.analyze_continuous_view(r1 REGCLASS) RETURNS BOOL
    AS '$libdir/matrixts', 'analyze_continuous_view'
    LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT EXECUTE ON MASTER;

-- Create help function to manually trigger an TRUNCATE on continuous view
CREATE OR REPLACE FUNCTION matrixts_internal.truncate_continuous_view(r1 REGCLASS) RETURNS BOOL
    AS '$libdir/matrixts', 'truncate_continuous_view'
    LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT EXECUTE ON MASTER;

-- Create help function to manually rebuil the continuous view
CREATE OR REPLACE FUNCTION matrixts_internal.rebuild_continuous_view(r1 REGCLASS) RETURNS BOOL
    AS '$libdir/matrixts', 'rebuild_continuous_view'
    LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT EXECUTE ON MASTER;

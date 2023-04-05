-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mars" to load this file. \quit

CREATE OR REPLACE FUNCTION mars.get_bloat_info(rel regclass)
RETURNS TABLE (total_blocks BIGINT, bloat_blocks BIGINT) AS $$
DECLARE
    auxrel       oid;
    old_invisible   boolean;
BEGIN
    SHOW gp_select_invisible INTO old_invisible;

    SELECT segrelid::oid
      INTO STRICT auxrel
        FROM mx_mars
      WHERE relid = rel;

    SET gp_select_invisible = true;

    RETURN QUERY
      EXECUTE format($q$select count(1)
                             , count(case when xmax <> 0 then 1 else null end)
                          from gp_dist_random('%s')$q$, auxrel::regclass);

    EXECUTE format('set gp_select_invisible = %s;', old_invisible);
END;
$$ LANGUAGE plpgsql VOLATILE STRICT;

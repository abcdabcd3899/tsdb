-- APM updates for matrixts--2.1--2.2

CREATE OR REPLACE FUNCTION matrixts_internal.apm_mars_guard(
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
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '% is not a time-series partition table.', rel
            USING DETAIL = 'please refer to documentation on how to use following function',
                HINT = format('execute mars.build_timeseries_table(%L, ...); first', rel);
    END;
    RETURN;
END
$$ LANGUAGE PLPGSQL VOLATILE;

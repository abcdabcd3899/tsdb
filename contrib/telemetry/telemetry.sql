
CREATE OR REPLACE FUNCTION telemery_version()
RETURNS TEXT
LANGUAGE C VOLATILE
EXECUTE ON MASTER
AS '$libdir/telemetry.so', 'telemery_version';

CREATE OR REPLACE FUNCTION load_uuid_local()
RETURNS TEXT
LANGUAGE C VOLATILE
EXECUTE ON MASTER
AS '$libdir/telemetry.so', 'load_uuid_local';

CREATE OR REPLACE FUNCTION execute_monitor_qd()
RETURNS TABLE (key TEXT, value TEXT)
LANGUAGE C VOLATILE
EXECUTE ON MASTER
AS '$libdir/telemetry.so', 'telemetry_monitor';

CREATE OR REPLACE  FUNCTION execute_monitor_qe()
RETURNS TABLE (key TEXT, value TEXT)
LANGUAGE C VOLATILE
EXECUTE ON ALL SEGMENTS
AS '$libdir/telemetry.so', 'telemetry_monitor';

CREATE OR REPLACE  FUNCTION execute_monitor()
RETURNS TABLE (key TEXT, value TEXT)
AS $$
    SELECT * FROM execute_monitor_qd()
        UNION ALL SELECT * FROM execute_monitor_qe();
$$ LANGUAGE SQL;


CREATE OR REPLACE FUNCTION collect_gp_segment_configuration()
RETURNS json
AS $$
DECLARE
    seg_config json;
BEGIN
    SELECT json_build_object (
        'tmversion',
        (SELECT telemery_version()),
        'cluster_id',
        (SELECT load_uuid_local()),
        'basic_info',
        (SELECT  json_build_object(
            'version',
            (SELECT version()),
            'now',
            (SELECT now())
        )),
        'database_detail',
        (SELECT  json_build_object(--database_count::json_obj
            'database_count',
            (SELECT count(*)  FROM pg_database),
            'seg_config',--database_count::json_array
            (SELECT json_agg(result) FROM
                (
                SELECT dbid, content, role,
                    preferred_role, mode, status ,
                    MD5(address) as address,
                    MD5(hostname) as hostname
                    FROM gp_segment_configuration
                ) AS result
            )
        ))
    ) INTO seg_config;
    RETURN seg_config;
END;
$$ LANGUAGE plpgsql;

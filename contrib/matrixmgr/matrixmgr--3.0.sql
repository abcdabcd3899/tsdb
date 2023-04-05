-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION matrixmgr" to load this file. \quit

DO $$ BEGIN
IF current_database() != 'matrixmgr' THEN
    RAISE EXCEPTION 'This extension can only be loaded in the "matrixmgr" database';
END IF;
END $$ ;

-- create matrixmgr_internal schema and functions
CREATE SCHEMA IF NOT EXISTS matrixmgr_internal;

CREATE OR REPLACE FUNCTION matrixmgr_internal.mxmgr_cmd_run(path_name text, args text, alias text)
RETURNS bool
AS 'MODULE_PATHNAME', 'mxmgr_cmd_run'
LANGUAGE C IMMUTABLE STRICT
EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION matrixmgr_internal.mxmgr_create_meta_table(schema_name name)
RETURNS bool
AS 'MODULE_PATHNAME', 'mxmgr_create_meta_table'
LANGUAGE C STRICT
EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION matrixmgr_internal.mxmgr_create_meta_table(schema_name name, relid Oid, typeid Oid, typeidArr Oid)
RETURNS bool
AS 'MODULE_PATHNAME', 'mxmgr_create_meta_table'
LANGUAGE C STRICT
EXECUTE ON ALL SEGMENTS;

DROP EXTERNAL WEB TABLE IF EXISTS matrixmgr_internal.matrixdb_install_dir CASCADE;
CREATE EXTERNAL WEB TABLE matrixmgr_internal.matrixdb_install_dir (dir text)
EXECUTE 'echo $GPHOME' ON MASTER
FORMAT 'TEXT' (DELIMITER '|');

DROP VIEW IF EXISTS matrixmgr_internal.local_logs CASCADE;
DROP EXTERNAL WEB TABLE IF EXISTS matrixmgr_internal.__gp_log_master_ext_3d CASCADE;
DROP EXTERNAL WEB TABLE IF EXISTS matrixmgr_internal.__gp_log_segment_ext_3d CASCADE;

CREATE EXTERNAL WEB TABLE matrixmgr_internal.__gp_log_master_ext_3d
(
    logtime timestamp,
    loguser text,
    logdatabase text,
    logpid text,
    logthread text,
    loghost text,
    logport text,
    logsessiontime timestamp with time zone,
    logtransaction int,
    logsession text,
    logcmdcount text,
    logsegment text,
    logslice text,
    logdistxact text,
    loglocalxact text,
    logsubxact text,
    logseverity text,
    logstate text,
    logmessage text,
    logdetail text,
    loghint text,
    logquery text,
    logquerypos int,
    logcontext text,
    logdebug text,
    logcursorpos int,
    logfunction text,
    logfile text,
    logline int,
    logstack text
)
EXECUTE E'for LOGF in $(find $GP_SEG_DATADIR/log/ -maxdepth 1 -name "*.csv" -mtime -3 -type f | sort -r); do cat $LOGF; n=$(wc -l < $LOGF); lines=$(expr $lines + $n); if [ "$lines" -gt "20000" ]; then break; fi; done' ON MASTER
FORMAT 'CSV' (DELIMITER AS ',' NULL AS '' QUOTE AS '"');

CREATE EXTERNAL WEB TABLE matrixmgr_internal.__gp_log_segment_ext_3d
(
    logtime timestamp,
    loguser text,
    logdatabase text,
    logpid text,
    logthread text,
    loghost text,
    logport text,
    logsessiontime timestamp with time zone,
    logtransaction int,
    logsession text,
    logcmdcount text,
    logsegment text,
    logslice text,
    logdistxact text,
    loglocalxact text,
    logsubxact text,
    logseverity text,
    logstate text,
    logmessage text,
    logdetail text,
    loghint text,
    logquery text,
    logquerypos int,
    logcontext text,
    logdebug text,
    logcursorpos int,
    logfunction text,
    logfile text,
    logline int,
    logstack text
)
EXECUTE E'for LOGF in $(find $GP_SEG_DATADIR/log/ -maxdepth 1 -name "*.csv" -mtime -3 -type f | sort -r); do cat $LOGF; n=$(wc -l < $LOGF); lines=$(expr $lines + $n); if [ "$lines" -gt "20000" ]; then break; fi; done'
FORMAT 'CSV' (DELIMITER AS ',' NULL AS '' QUOTE AS '"');

CREATE VIEW matrixmgr_internal.local_logs AS
  SELECT * FROM matrixmgr_internal.__gp_log_segment_ext_3d
UNION ALL
  SELECT * FROM matrixmgr_internal.__gp_log_master_ext_3d
ORDER BY logtime;

CREATE OR REPLACE FUNCTION matrixmgr_internal.mxmgr_gate_deploy(schema_name text, is_validate bool)
RETURNS bool
AS $$
DECLARE
    tmp RECORD;
    args text;
    db_path text;
BEGIN
    EXECUTE format('SET search_path TO %I', schema_name);
    args := 'gate deploy ';
    FOR tmp IN SELECT key, CASE WHEN value = '' THEN '""' ELSE value END
        FROM matrix_manager_config WHERE category IN ('gate', 'metrics') LOOP
            args := CONCAT(args, ' --', tmp.key, ' ', tmp.value);
        END LOOP;
    IF is_validate=true THEN
        args := CONCAT(args, ' --validate');
    END IF;
    PERFORM matrixmgr_internal.mxmgr_cmd_run('mxctl', args, 'gate');

    RESET search_path;
    RETURN true;
END;
$$ LANGUAGE plpgsql
EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION matrixmgr_internal.mxmgr_telegraf_deploy(schema_name text, is_validate bool)
RETURNS bool
AS $$
DECLARE
    tmp RECORD;
    args text;
    val text;
    db_path text;
BEGIN
    EXECUTE format('SET search_path TO %I', schema_name);
    args := 'telegraf deploy ';
    FOR tmp IN SELECT key, CASE WHEN value = '' THEN '""' ELSE value END
        FROM matrix_manager_config WHERE category = 'db' LOOP
            args := CONCAT(args, ' --', tmp.key, ' ', tmp.value);
        END LOOP;
    SELECT value INTO val FROM matrix_manager_config WHERE category = 'gate' AND key = 'host';
    args := CONCAT(args, ' --gatehost ', val);
    SELECT value INTO val FROM matrix_manager_config WHERE category = 'gate' AND key = 'postport';
    args := CONCAT(args, ' --gateport ', val);
    args := CONCAT(args, ' --schemaname ', schema_name);
    IF is_validate=true THEN
        args := CONCAT(args, ' --validate');
    END IF;
    PERFORM matrixmgr_internal.mxmgr_cmd_run('mxctl', args, 'telegraf');

    RESET search_path;
    RETURN true;
END;
$$ LANGUAGE plpgsql
EXECUTE ON MASTER;

-- remove gate
-- metrics:[username, password, hostname, port], supervisor:[honstname, port]
CREATE OR REPLACE FUNCTION matrixmgr_internal.mxmgr_remove_gate(schema_name text)
RETURNS bool
AS $$
DECLARE
    tmp RECORD;
    args text;
BEGIN
    EXECUTE format('SET search_path TO %I', schema_name);
    FOR tmp IN SELECT key, CASE WHEN value = '' THEN '""' ELSE value END FROM matrix_manager_config WHERE category IN ('db', 'metrics') LOOP
        args := CONCAT(args, ' --', tmp.key, ' ', tmp.value);
    END LOOP;
    EXECUTE format('COPY (SELECT 1) TO PROGRAM ''mxctl gate remove %s'';', args);
    RESET search_path;
    RETURN true;
END;
$$ LANGUAGE plpgsql
EXECUTE ON MASTER;

-- remove telegraf
-- metrics:[username, password, hostname, port], supervisor:[honstname, port]
CREATE OR REPLACE FUNCTION matrixmgr_internal.mxmgr_remove_telegraf(schema_name text)
RETURNS bool
AS $$
DECLARE
    tmp RECORD;
    args text;
BEGIN
    EXECUTE format('SET search_path TO %I', schema_name);
    FOR tmp IN SELECT key, CASE WHEN value = '' THEN '""' ELSE value END FROM matrix_manager_config WHERE category IN ('db', 'metrics') LOOP
        args := CONCAT(args, ' --', tmp.key, ' ', tmp.value);
    END LOOP;
    EXECUTE format('COPY (SELECT 1) TO PROGRAM ''mxctl telegraf remove %s'';', args);
    RESET search_path;
    RETURN true;
END;
$$ LANGUAGE plpgsql
EXECUTE ON MASTER;

-- create UDFs for Matrix Manager
CREATE OR REPLACE FUNCTION public.mxmgr_create_metastore(schema_name text)
RETURNS bool
AS $$
DECLARE
    master_host text;
    master_port integer;
    install_dir text;
BEGIN
    SELECT hostname INTO master_host
    FROM gp_segment_configuration
    WHERE content = -1 AND preferred_role = 'p';

    SELECT port INTO master_port
    FROM gp_segment_configuration
    WHERE content = -1 AND preferred_role = 'p';

    SELECT dir INTO install_dir
    FROM matrixmgr_internal.matrixdb_install_dir LIMIT 1;

    -- on standard installation, normalize the path make upgrade easier
    IF install_dir LIKE '/usr/local/matrixdb%'
    THEN
        install_dir := '/usr/local/matrixdb';
    END IF;

    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', schema_name);
    EXECUTE format('SET search_path TO %I', schema_name);

    CREATE TABLE IF NOT EXISTS system (
        ts       timestamp NOT NULL,
        category name NOT NULL,
        host     name NOT NULL,
        device   name,
        tagattr  jsonb,
        metrics  jsonb NOT NULL
    )
    USING ao_column
    WITH (compresstype=lz4, compresslevel=1)
    DISTRIBUTED BY (category, host, device);

    CREATE INDEX ON system(host, category, ts DESC);

    CREATE OR REPLACE VIEW os_uptime AS (
        WITH master_sys(metrics, ts) AS (
          SELECT s.metrics, s.ts
          FROM gp_segment_configuration gs
          INNER JOIN system s ON gs.hostname = s.host
          WHERE s.category = 'system'
            AND gs.content = -1 AND gs.role = 'p'
            AND s.ts > now() AT TIME ZONE 'UTC' - interval '5 min'
        )
        SELECT (public.last(metrics, ts)->>'uptime')::bigint AS uptime
        FROM master_sys
    );

    CREATE OR REPLACE VIEW segstate AS (
        WITH rsync_state(segid, state) AS (
            SELECT gp_segment_id AS segid, (pg_stat_get_wal_senders()).state AS state
            FROM gp_dist_random('gp_id')
            UNION ALL
            SELECT -1, state FROM pg_stat_replication
        ), counters (u, d, b, r, p, m, s) AS (
            SELECT
              SUM(CASE WHEN g.status = 'u' THEN 1 ELSE 0 END) AS up,
              SUM(CASE WHEN g.status <> 'u' THEN 1 ELSE 0 END) AS down,
              SUM(CASE WHEN g.role <> g.preferred_role THEN 1 ELSE 0 END) AS unbalanced,
              SUM(CASE WHEN r.state = 'catchup' THEN 1 ELSE 0 END) AS rsyncing,
              SUM(CASE WHEN g.content >= 0 AND g.preferred_role = 'p' THEN 1 ELSE 0 END) AS primarys,
              SUM(CASE WHEN g.content >= 0 AND g.preferred_role = 'p' AND r.segid IS NOT NULL THEN 1 ELSE 0 END) AS mirrored_primarys,
              SUM(CASE WHEN g.content < 0 AND g.preferred_role = 'm' THEN 1 ELSE 0 END) AS standby
            FROM gp_segment_configuration g
            LEFT JOIN rsync_state r
            ON g.content = r.segid AND g.preferred_role = 'p'
        )
        SELECT
          CASE WHEN d > 0 THEN 20           -- 'Segment Down'
               WHEN r > 0 THEN 11           -- 'Resyncing'
               WHEN b > 0 THEN 10           -- 'Unbalanced'
               WHEN s <= 0 THEN 1           -- 'NoStandby'
               WHEN p > m OR m = 0 THEN 2   -- 'NoMirror'
               WHEN p <= 0 THEN 12          -- 'Master Only'
               ELSE 0                       -- 'Normal'
           END AS state
        FROM counters
    );

    CREATE OR REPLACE VIEW segdetails AS (
        WITH rsync_state(segid, state) AS (
            SELECT gp_segment_id AS segid, (pg_stat_get_wal_senders()).state AS state
            FROM gp_dist_random('gp_id')
            UNION ALL
            SELECT -1, state FROM pg_stat_replication
        )
        SELECT
          now() AS ts,
          CASE WHEN content < 0 THEN
            CASE WHEN preferred_role = 'p' THEN 'Master' ELSE 'Standby' END
          ELSE
            CASE WHEN preferred_role = 'p' THEN 'Primary' || content ELSE 'Mirror' || content END
          END AS id,
          CASE
            WHEN g.status <> 'u' THEN 20            -- 'Down'
            WHEN r.state = 'catchup' THEN 11        -- 'Resync'
            WHEN g.role <> g.preferred_role THEN 10 -- 'Switched'
            ELSE 0                                  -- 'Up'
          END AS state
        FROM gp_segment_configuration g
        LEFT JOIN rsync_state r
        ON g.content = r.segid AND g.preferred_role = 'p'
        ORDER BY content, CASE WHEN preferred_role = 'p' THEN 0 ELSE 1 END
    );

    CREATE OR REPLACE VIEW seg_disk_usage AS (
        WITH seg(host, kind) AS (
            -- 0:master, 1:seg, 2:standby
            SELECT DISTINCT hostname,
              CASE WHEN content < 0 THEN
                CASE WHEN preferred_role = 'p' THEN 0 ELSE 2 END
              ELSE 1 END
            FROM gp_segment_configuration
        ),
        sys(metrics, ts, host, device) AS (
          SELECT s.metrics, s.ts, s.host, device
          FROM seg INNER JOIN system s
            ON seg.host = s.host
          WHERE s.category = 'disk'
            AND seg.kind = 1
            AND s.ts > now() AT TIME ZONE 'UTC' - interval '5 min'
        ),
        last(metrics, ts, host, device) AS (
          SELECT public.last(metrics, ts), max(ts), host, device
          FROM sys
          GROUP BY host, device
        )
        SELECT max(ts) AS "time",
        sum((metrics->>'free')::bigint) AS free_bytes,
        sum((metrics->>'total')::bigint) AS total_bytes
        FROM last
    );

    CREATE OR REPLACE VIEW master_disk_usage AS (
        WITH seg(host, kind) AS (
            -- 0:master, 1:seg, 2:standby
            SELECT DISTINCT hostname,
              CASE WHEN content < 0 THEN
                CASE WHEN preferred_role = 'p' THEN 0 ELSE 2 END
              ELSE 1 END
            FROM gp_segment_configuration
        ),
        sys(metrics, ts, host, device) AS (
          SELECT s.metrics, s.ts, s.host, device
          FROM seg INNER JOIN system s
            ON seg.host = s.host
          WHERE s.category = 'disk'
            AND seg.kind = 0
            AND s.ts > now() AT TIME ZONE 'UTC' - interval '5 min'
        ),
        last(metrics, ts, host, device) AS (
          SELECT public.last(metrics, ts), max(ts), host, device
          FROM sys
          GROUP BY host, device
        )
        SELECT max(ts) AS "time",
        sum((metrics->>'free')::bigint) AS free_bytes,
        sum((metrics->>'total')::bigint) AS total_bytes
        FROM last
    );

    EXECUTE format('SELECT matrixmgr_internal.mxmgr_create_meta_table(''%s'');', schema_name);

    EXECUTE format('INSERT INTO matrix_manager_config(category, key, value, "desc") VALUES (''gate'', ''host'', ''%s'', ''the hostname where mxgate executes'');', master_host);
    EXECUTE format('INSERT INTO matrix_manager_config(category, key, value, "desc") VALUES (''gate'', ''db2gatehost'', ''%s'', ''hostname for segments connect to mxgate'');', master_host);
    EXECUTE format('INSERT INTO matrix_manager_config(category, key, value, "desc") VALUES (''metrics'', ''dbport'', ''%s'', ''the port connect to metrics database'');', master_port);
    EXECUTE format('INSERT INTO matrix_manager_config(category, key, value, "desc") VALUES (''metrics'', ''username'', ''%s'', ''the username connect to metrics database'');', current_user);
    EXECUTE format('INSERT INTO matrix_manager_config(category, key, value, "desc") VALUES (''db'', ''dbport'', ''%s'', ''the port of the database being monitored'');', master_port);
    EXECUTE format('INSERT INTO matrix_manager_config(category, key, value, "desc") VALUES (''db'', ''username'', ''%s'', ''the username of the database being monitored'');', current_user);
    EXECUTE format('INSERT INTO matrix_manager_config(category, key, value, "desc") VALUES (''gate'', ''path'', ''%s'', ''path of matrixdb installation dir on mxgate host'');', install_dir);
    EXECUTE format('INSERT INTO matrix_manager_config(category, key, value, "desc") VALUES (''db'', ''path'', ''%s'', ''path of matrixdb installation dir on hosts being monitored'');', install_dir);
    INSERT INTO matrix_manager_config(category, key, value, "desc") VALUES
      ('gate', 'port', '4617', 'the supervisor port of the host on which mxgate executes')
    , ('gate', 'db2gateport', '4374', 'port for segments connect to mxgate')
    , ('gate', 'postport', '4329', 'port posting http requests to mxgate')
    , ('metrics', 'dbhost', '127.0.0.1', 'the hostname connect to metrics database')
    , ('metrics', 'password', '', 'the password connect to metrics database')
    , ('db', 'dbhost', '127.0.0.1', 'the hostname of the database being monitored')
    , ('db', 'password', '', 'the password of the database being monitored')
    , ('db', 'port', '4617', 'the supervisor port of hosts in this db cluster')
    ;

    RESET search_path;
    RETURN true;
END;
$$ LANGUAGE plpgsql
EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION public.mxmgr_deploy(schema_name text)
RETURNS bool
AS $$
BEGIN
    PERFORM matrixmgr_internal.mxmgr_gate_deploy(schema_name, false);
    PERFORM matrixmgr_internal.mxmgr_telegraf_deploy(schema_name, false);
    RETURN true;
END;
$$ LANGUAGE plpgsql
EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION public.mxmgr_validate(schema_name text)
RETURNS bool
AS $$
BEGIN
    PERFORM matrixmgr_internal.mxmgr_gate_deploy(schema_name, true);
    PERFORM matrixmgr_internal.mxmgr_telegraf_deploy(schema_name, true);
    RETURN true;
END;
$$ LANGUAGE plpgsql
EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION public.mxmgr_remove_all(schema_name text)
RETURNS bool
AS $$
BEGIN
    PERFORM matrixmgr_internal.mxmgr_remove_gate(schema_name);
    PERFORM matrixmgr_internal.mxmgr_remove_telegraf(schema_name);
    RETURN true;
END;
$$ LANGUAGE plpgsql
EXECUTE ON MASTER;

CREATE OR REPLACE FUNCTION public.mxmgr_init_local()
RETURNS bool
AS $$
BEGIN
    PERFORM public.mxmgr_create_metastore('local');
    PERFORM public.mxmgr_validate('local');
    PERFORM public.mxmgr_deploy('local');
    RETURN true;
END;
$$ LANGUAGE plpgsql
EXECUTE ON MASTER;

-- event trigger for handling drop this extension
CREATE OR REPLACE FUNCTION public.mxmgr_uninstall()
RETURNS event_trigger
AS 'MODULE_PATHNAME', 'mxmgr_uninstall'
LANGUAGE C STRICT
EXECUTE ON MASTER;

CREATE EVENT TRIGGER mxmgr_drop_extension_trigger ON ddl_command_start
EXECUTE PROCEDURE mxmgr_uninstall();

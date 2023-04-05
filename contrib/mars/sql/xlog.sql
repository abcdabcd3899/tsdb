-- start_ignore
CREATE OR REPLACE language plpython3u;
CREATE EXTENSION IF NOT EXISTS mars;
create extension if not exists gp_debug_numsegments;
select gp_debug_set_create_table_default_numsegments(1);

--
-- pg_ctl:
--   datadir: data directory of process to target with `pg_ctl`
--   command: commands valid for `pg_ctl`
--   command_mode: modes valid for `pg_ctl -m`  
--
CREATE OR REPLACE function pg_ctl(datadir text, command text, command_mode text default 'immediate')
returns text as $$
    class PgCtlError(Exception):
        def __init__(self, errmsg):
            self.errmsg = errmsg
        def __str__(self):
            return repr(self.errmsg)

    import subprocess
    if command == 'promote':
        cmd = 'pg_ctl promote -D %s' % datadir
    elif command in ('stop', 'restart'):
        cmd = 'pg_ctl -l postmaster.log -D %s ' % datadir
        cmd = cmd + '-w -t 600 -m %s %s' % (command_mode, command)
    else:
        return 'Invalid command input'

    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True)
    stdout, stderr = proc.communicate()

    if proc.returncode == 0:
        return 'OK'
    else:
        raise PgCtlError(stdout.decode()+'|'+stderr.decode())
$$ language plpython3u;

DROP TABLE IF EXISTS foo;
-- end_ignore

CREATE TABLE foo(a int, b bigint, c float) using mars DISTRIBUTED BY(a);

INSERT INTO foo SELECT i, i * 10 , i * 0.1 FROM generate_series(1, 10) i;

SELECT pg_ctl((SELECT datadir FROM gp_segment_configuration c WHERE c.role='p' AND c.content=0), 'stop', 'immediate');

-- trigger failover
SELECT gp_request_fts_probe_scan();

SELECT * FROM foo;

INSERT INTO foo SELECT i, i * 10 , i * 0.1 FROM generate_series(1, 10) i;

SELECT * FROM foo;

-- start_ignore
\! gprecoverseg -aF --no-progress
\! gprecoverseg -arv --no-progress
-- end_ignore

SELECT * FROM foo;

SELECT pg_ctl((SELECT datadir FROM gp_segment_configuration c WHERE c.role='p' AND c.content=0), 'stop', 'immediate');

-- trigger failover
SELECT gp_request_fts_probe_scan();

SELECT * FROM foo;

TRUNCATE foo;

SELECT * FROM foo;

-- start_ignore
\! gprecoverseg -aF --no-progress
\! gprecoverseg -arv --no-progress
-- end_ignore

SELECT * FROM foo;

DROP TABLE foo;

\c matrixmgr

SELECT matrixmgr_internal.mxmgr_cmd_run('/bin/ls', '', 'test');

SELECT matrixmgr_internal.mxmgr_cmd_run('ls', '', 'test');

SELECT matrixmgr_internal.mxmgr_cmd_run('../../bin/ls', '', 'test');

SELECT matrixmgr_internal.mxmgr_cmd_run('../bin/pg_test_timing', '--help', 'test');

SELECT matrixmgr_internal.mxmgr_cmd_run('pg_test_timing', '--help', 'test');

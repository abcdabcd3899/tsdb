\c matrixmgr

SELECT proargnames, prosrc FROM pg_proc WHERE  proname = 'mxmgr_gate_deploy';

SELECT proargnames, prosrc FROM pg_proc WHERE  proname = 'mxmgr_remove_gate';

\c matrixmgr

SELECT proargnames, prosrc FROM pg_proc WHERE  proname = 'mxmgr_validate';

SELECT proargnames, prosrc FROM pg_proc WHERE  proname = 'mxmgr_deploy';

SELECT proargnames, prosrc FROM pg_proc WHERE  proname = 'mxmgr_remove_all';

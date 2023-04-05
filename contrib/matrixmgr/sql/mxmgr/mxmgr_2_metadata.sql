\c matrixmgr

SELECT mxmgr_create_metastore('test_mgr');

SELECT tablename FROM pg_catalog.pg_tables WHERE  schemaname = 'test_mgr';

SELECT table_name,column_name FROM information_schema.columns WHERE table_schema = 'test_mgr';

SELECT viewname, definition FROM pg_catalog.pg_views WHERE  schemaname = 'test_mgr';

-- start_matchsubs
--
-- m/^ERROR:  Error on receive from seg0.*: server closed the connection unexpectedly/
-- s/^ERROR:  Error on receive from seg0.*: server closed the connection unexpectedly/ERROR: server closed the connection unexpectedly/
--
-- end_matchsubs

-- start_ignore
create extension if not exists matrixts;
-- end_ignore

--
-- MVCC tests
--
CREATE TABLE cv_foo(c1 text, c2 text);
CREATE VIEW cv_cv
with (continuous)
AS SELECT c2, c1 FROM cv_foo GROUP BY c2, c1;

1: BEGIN;
1: insert into cv_foo values (1, 'session1');
1: insert into cv_foo values (2, 'session1');
2: BEGIN;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
1: COMMIT;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
2: COMMIT;
1: TRUNCATE cv_foo;
2: select matrixts_internal.truncate_continuous_view('cv_cv');

1: BEGIN;
1: insert into cv_foo values (1, 'session1');
1: insert into cv_foo values (2, 'session1');
2: BEGIN;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
1: ABORT;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
2: COMMIT;
1: TRUNCATE cv_foo;

-- Test isolation level read commited 
1: BEGIN;
1: insert into cv_foo values (1, 'session1');
1: insert into cv_foo values (2, 'session1');
2: BEGIN;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
3: BEGIN;
3: insert into cv_foo values (1, 'session3');
3: insert into cv_foo values (2, 'session3');
3: COMMIT;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
2: COMMIT;
1: COMMIT;
1: TRUNCATE cv_foo;
2: select matrixts_internal.truncate_continuous_view('cv_cv');

-- Test isolation level repeatable
1: BEGIN;
1: insert into cv_foo values (1, 'session1');
1: insert into cv_foo values (2, 'session1');
2: BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
3: BEGIN; 
3: insert into cv_foo values (1, 'session3');
3: insert into cv_foo values (2, 'session3');
3: COMMIT;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
2: COMMIT;
1: COMMIT;
1: TRUNCATE cv_foo;
2: select matrixts_internal.truncate_continuous_view('cv_cv');

-- Test subtransaction
1: BEGIN;
1: insert into cv_foo values (1, 'session1');
1: insert into cv_foo values (2, 'session1');
2: BEGIN;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
1: savepoint s1;
1: insert into cv_foo values (3, 'session1');
1: insert into cv_foo values (4, 'session1');
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
1: savepoint s1_sub;
1: insert into cv_foo values (5, 'session1');
1: insert into cv_foo values (6, 'session1');
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
1: release savepoint s1_sub;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
1: release savepoint s1;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
1: savepoint s1;
1: insert into cv_foo values (7, 'session1');
1: insert into cv_foo values (8, 'session1');
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
1: rollback to s1;
1: COMMIT;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
2: SELECT * from cv_foo;
2: SELECT * from cv_cv;
2: COMMIT;
1: TRUNCATE cv_foo;
2: select matrixts_internal.truncate_continuous_view('cv_cv');

-- Test crash recovery
-- init_tape: error/crash before xlog 
1: select gp_inject_fault('inittape_before_xlog', 'error', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('inittape_before_xlog', 'reset', 2);
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
1: select gp_inject_fault('inittape_before_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('inittape_before_xlog', 'reset', 2);
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- inittape: error/crash after xlog
1: select gp_inject_fault('inittape_after_xlog', 'error', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('inittape_after_xlog', 'reset', 2);
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
1: select gp_inject_fault('inittape_after_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('inittape_after_xlog', 'reset', 2);
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');

-- dump_tuples: crash/error
1: select gp_inject_fault('dump_tuples_before_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('dump_tuples_before_xlog', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
1: select gp_inject_fault('dump_tuples_after_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('dump_tuples_after_xlog', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- new tapesort: crash
1: select gp_inject_fault('new_tapesort_before_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('new_tapesort_before_xlog', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
1: select gp_inject_fault('new_tapesort_after_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('new_tapesort_after_xlog', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- new header: crash
1: select gp_inject_fault('new_tapeheader_before_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('new_tapeheader_before_xlog', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
1: select gp_inject_fault('new_tapeheader_after_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('new_tapeheader_after_xlog', 'reset', 2);
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- crash before data flush 
1: select gp_inject_fault('tape_insert_flush', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('tape_insert_flush', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- merge crash
1: select gp_inject_fault('merge_done', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: insert into cv_foo select i, 'session1' from generate_series(11, 15) i;
-- except fail and wait at most 20 seconds 
1: select pg_sleep(10) from cv_foo limit 2; 
1: select * from cv_foo;
1: select * from cv_cv;
1: select gp_inject_fault('merge_done', 'reset', 2);
1: select gp_inject_fault('merge_done', 'suspend', 2);
1: insert into cv_foo select i, 'session1' from generate_series(16, 20) i;
1: insert into cv_foo select i, 'session1' from generate_series(21, 25) i;
1: select * from cv_foo;
1: select * from cv_cv;
1: select gp_wait_until_triggered_fault('merge_done', 1, 2);
1: select gp_inject_fault('merge_done', 'resume', 2);
1: select gp_inject_fault('merge_done', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- vacuum crash
1: select gp_inject_fault('vacuum_done', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: insert into cv_foo select i, 'session1' from generate_series(11, 15) i;
-- except fail and wait at most 20 seconds 
1: select pg_sleep(10) from cv_foo limit 2; 
1: select * from cv_foo;
1: select * from cv_cv;
1: select gp_inject_fault('vacuum_done', 'reset', 2);
1: select gp_inject_fault('vacuum_done', 'suspend', 2);
1: insert into cv_foo select i, 'session1' from generate_series(16, 20) i;
1: insert into cv_foo select i, 'session1' from generate_series(21, 25) i;
1: select * from cv_foo;
1: select * from cv_cv;
1: select gp_wait_until_triggered_fault('vacuum_done', 1, 2);
1: select gp_inject_fault('vacuum_done', 'resume', 2);
1: select gp_inject_fault('vacuum_done', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- freespace: allocate crash
1: select gp_inject_fault('freepage_alloc_before_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('freepage_alloc_before_xlog', 'reset', 2);
1: select matrixts_internal.info_continuous_view('cv_cv');
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
1: select gp_inject_fault('freepage_alloc_after_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('freepage_alloc_after_xlog', 'reset', 2);
1: select matrixts_internal.info_continuous_view('cv_cv');
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- freespace: newleaf crash
1: select gp_inject_fault('newleaf_before_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('newleaf_before_xlog', 'reset', 2);
1: select matrixts_internal.info_continuous_view('cv_cv');
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
1: select gp_inject_fault('newleaf_after_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('newleaf_after_xlog', 'reset', 2);
1: select matrixts_internal.info_continuous_view('cv_cv');
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- freespace: newroot crash
1: select gp_inject_fault('newroot_before_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('newroot_before_xlog', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: select matrixts_internal.info_continuous_view('cv_cv');
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
1: select gp_inject_fault('newroot_after_xlog', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_inject_fault('newroot_alloc_after_xlog', 'reset', 2);
1: select * from cv_foo;
1: select * from cv_cv;
1: select matrixts_internal.info_continuous_view('cv_cv');
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');
-- freespace: recycle crash
1: select gp_inject_fault('freepage_recycle', 'panic', 2);
1: insert into cv_foo select i, 'session1' from generate_series(1, 5) i;
1: insert into cv_foo select i, 'session1' from generate_series(6, 10) i;
1: insert into cv_foo select i, 'session1' from generate_series(11, 15) i;
-- except fail and wait at most 20 seconds 
1: select pg_sleep(10) from cv_foo limit 2; 
1: select * from cv_foo;
1: select * from cv_cv;
1: select gp_inject_fault('freepage_recycle', 'reset', 2);
1: insert into cv_foo select i, 'session1' from generate_series(16, 20) i;
1: select * from cv_foo;
1: select * from cv_cv;
1: TRUNCATE cv_foo;
1: select matrixts_internal.truncate_continuous_view('cv_cv');

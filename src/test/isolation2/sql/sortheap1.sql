-- start_matchsubs
--
-- m/^ERROR:  Error on receive from seg0.*: server closed the connection unexpectedly/
-- s/^ERROR:  Error on receive from seg0.*: server closed the connection unexpectedly/ERROR: server closed the connection unexpectedly/
--
-- end_matchsubs

-- start_ignore
create extension if not exists matrixts;
-- end_ignore

CREATE TABLE sheap_foo(c1 text, c2 text) using sortheap;
CREATE INDEX sheap_idx on sheap_foo using sortheap_btree(c1, c2);

--
-- MVCC tests
--
1: BEGIN;
1: insert into sheap_foo values (1, 'session1');
1: insert into sheap_foo values (2, 'session1');
2: BEGIN;
2: SELECT * from sheap_foo;
1: COMMIT;
2: SELECT * from sheap_foo;
2: COMMIT;
1: TRUNCATE sheap_foo;

1: BEGIN;
1: insert into sheap_foo values (1, 'session1');
1: insert into sheap_foo values (2, 'session1');
2: BEGIN;
2: SELECT * from sheap_foo;
1: ABORT;
2: SELECT * from sheap_foo;
2: COMMIT;
1: TRUNCATE sheap_foo;

-- Test isolation level read commited 
1: BEGIN;
1: insert into sheap_foo values (1, 'session1');
1: insert into sheap_foo values (2, 'session1');
2: BEGIN;
2: SELECT * from sheap_foo;
3: BEGIN;
3: insert into sheap_foo values (1, 'session3');
3: insert into sheap_foo values (2, 'session3');
3: COMMIT;
2: SELECT * from sheap_foo;
2: COMMIT;
1: COMMIT;
1: TRUNCATE sheap_foo;

-- Test isolation level repeatable
1: BEGIN;
1: insert into sheap_foo values (1, 'session1');
1: insert into sheap_foo values (2, 'session1');
2: BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
2: SELECT * from sheap_foo;
3: BEGIN; 
3: insert into sheap_foo values (1, 'session3');
3: insert into sheap_foo values (2, 'session3');
3: COMMIT;
2: SELECT * from sheap_foo;
2: COMMIT;
1: COMMIT;
1: TRUNCATE sheap_foo;

-- Test subtransaction
1: BEGIN;
1: insert into sheap_foo values (1, 'session1');
1: insert into sheap_foo values (2, 'session1');
2: BEGIN;
2: SELECT * from sheap_foo;
1: savepoint s1;
1: insert into sheap_foo values (3, 'session1');
1: insert into sheap_foo values (4, 'session1');
2: SELECT * from sheap_foo;
1: savepoint s1_sub;
1: insert into sheap_foo values (5, 'session1');
1: insert into sheap_foo values (6, 'session1');
2: SELECT * from sheap_foo;
1: release savepoint s1_sub;
2: SELECT * from sheap_foo;
1: release savepoint s1;
2: SELECT * from sheap_foo;
1: savepoint s1;
1: insert into sheap_foo values (7, 'session1');
1: insert into sheap_foo values (8, 'session1');
2: SELECT * from sheap_foo;
1: rollback to s1;
1: COMMIT;
2: SELECT * from sheap_foo;
2: SELECT * from sheap_foo;
2: COMMIT;
1: TRUNCATE sheap_foo;

-- multiple 
1: insert into sheap_foo select i, 'session1' from generate_series(1, 5) i;
1: select gp_inject_fault('vacuum_done', 'suspend', 2);
1: insert into sheap_foo select i, 'session1' from generate_series(6, 10) i;
1: select gp_wait_until_triggered_fault('vacuum_done', 1, 2);
2: select * from sheap_foo;
2: set enable_indexscan to off;
2: set enable_bitmapscan to off;
2: select * from sheap_foo where c1 = '8';
2: select * from sheap_foo where c1 = '9';
2: select * from sheap_foo where c2 = 'session1';
2: select * from sheap_foo order by c1;
2: set enable_seqscan to off;
2: set enable_indexscan to on;
2: select * from sheap_foo where c1 = '8';
2: select * from sheap_foo where c1 = '9';
2: select * from sheap_foo where c2 = 'session1';
1: insert into sheap_foo select i, 'session1' from generate_series(11, 15) i;
1: select gp_inject_fault('vacuum_done', 'resume', 2);
1: select gp_inject_fault('vacuum_done', 'reset', 2);
2: select * from sheap_foo;
2: set enable_indexscan to off;
2: set enable_bitmapscan to off;
2: select * from sheap_foo where c1 = '8';
2: select * from sheap_foo where c1 = '9';
2: select * from sheap_foo where c1 = '11';
2: select * from sheap_foo where c2 = 'session1';
2: select * from sheap_foo order by c1;
2: set enable_seqscan to off;
2: set enable_indexscan to on;
2: select * from sheap_foo where c1 = '8';
2: select * from sheap_foo where c1 = '9';
2: select * from sheap_foo where c1 = '11';
2: select * from sheap_foo where c2 = 'session1';


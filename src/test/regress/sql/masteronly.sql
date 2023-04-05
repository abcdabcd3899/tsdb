--
-- Basic tests for masteronly table
--
create schema masteronly;
set search_path to masteronly;

---------
-- INSERT
---------
create table foo (x int, y int) distributed masteronly;
create table foo1(like foo) distributed masteronly;
create table bar (like foo) distributed randomly;
create table bar1 (like foo) distributed by (x);

-- values --> masteronly table
-- random partitioned table --> masteronly table
-- hash partitioned table --> masteronly table
-- singleQE --> masteronly table
-- masteronly --> masteronly table
insert into bar values (1, 1), (3, 1);
insert into bar1 values (1, 1), (3, 1);
insert into foo1 values (1, 1), (3, 1);
insert into foo select * from bar;
insert into foo select * from bar1;
insert into foo select * from bar order by x limit 1;
insert into foo select * from foo;

select gp_segment_id, * from foo order by x;
select bar.x, bar.y from bar, (select * from foo) as t1 order by 1,2;
select bar.x, bar.y from bar, (select * from foo order by x limit 1) as t1 order by 1,2;

truncate foo;
truncate foo1;
truncate bar;
truncate bar1;

-- masteronly table --> random partitioned table
-- masteronly table --> hash partitioned table
insert into foo values (1, 1), (3, 1);
insert into bar select * from foo order by x limit 1;
insert into bar1 select * from foo order by x limit 1;

select gp_segment_id, * from foo order by x;
-- select * from bar order by x;
-- select * from bar1 order by x;

drop table if exists foo;
drop table if exists foo1;
drop table if exists bar;
drop table if exists bar1;

--
-- CREATE UNIQUE INDEX
--
-- create unique index on non-distributed key.
create table foo (x int, y int) distributed masteronly;
create table bar (x int, y int) distributed randomly;

-- success
create unique index foo_idx on foo (y);
-- should fail
create unique index bar_idx on bar (y);

drop table if exists foo;
drop table if exists bar;

--
-- CREATE TABLE with both PRIMARY KEY and UNIQUE constraints
--
create table foo (id int primary key, name text unique) distributed masteronly;

-- success
insert into foo values (1,'aaa');
insert into foo values (2,'bbb');

-- fail
insert into foo values (1,'ccc');
insert into foo values (3,'aaa');

drop table if exists foo;

--
-- CREATE TABLE
--
--
-- Like
CREATE TABLE parent (
        name            text,
        age                     int4,
        location        point
) distributed masteronly;

CREATE TABLE child (like parent) distributed masteronly;
CREATE TABLE child1 (like parent) DISTRIBUTED BY (name);
CREATE TABLE child2 (like parent);

-- should be masteronly table
\d child
-- should distributed by name
\d child1
-- should be masteronly table
\d child2

drop table if exists parent;
drop table if exists child;
drop table if exists child1;
drop table if exists child2;

-- Inherits
CREATE TABLE parent_om (
        name            text,
        age                     int4,
        location        point
) distributed masteronly;

CREATE TABLE parent_part (
        name            text,
        age                     int4,
        location        point
) distributed by (name);

-- inherits from a masteronly table, should fail
CREATE TABLE child (
        salary          int4,
        manager         name
) INHERITS (parent_om);

-- masteronly table can not have parents, should fail
CREATE TABLE child (
        salary          int4,
        manager         name
) INHERITS (parent_part) DISTRIBUTED MASTERONLY;

drop table if exists parent_om;
drop table if exists parent_part;
drop table if exists child;

--
-- CTAS
--
-- CTAS from generate_series
create table foo as select i as c1, i as c2
from generate_series(1,3) i distributed masteronly;

-- CTAS from masteronly table
create table bar as select * from foo distributed masteronly;
select count(*) from bar;

drop table if exists foo;
drop table if exists bar;

-- CTAS from partition table table
create table foo as select i as c1, i as c2
from generate_series(1,3) i;

create table bar as select * from foo distributed masteronly;
select count(*) from bar;

drop table if exists foo;
drop table if exists bar;

-- CTAS from singleQE
create table foo as select i as c1, i as c2
from generate_series(1,3) i;
select * from foo;

create table bar as select * from foo order by c1 limit 1 distributed masteronly;
select count(*) from bar;

drop table if exists foo;
drop table if exists bar;

-- Create view can work
create table foo(x int, y int) distributed masteronly;
insert into foo values(1,1);

create view v_foo as select * from foo;
select * from v_foo;

drop view v_foo;
drop table if exists foo;

---------
-- Alter
--------
-- Drop distributed key column
create table foo(x int, y int) distributed masteronly;
create table bar(like foo) distributed by (x);

insert into foo values(1,1);
insert into bar values(1,1);

-- success
alter table foo drop column x;
-- fail
alter table bar drop column x;

drop table if exists foo;
drop table if exists foo1;
drop table if exists bar;
drop table if exists bar1;

-- Alter gp_distribution_policy
create table foo(x int, y int) distributed masteronly;
create table foo1(x int, y int) distributed masteronly;
create table bar(x int, y int) distributed by (x);
create table bar1(x int, y int) distributed randomly;

insert into foo select i,i from generate_series(1,10) i;
insert into foo1 select i,i from generate_series(1,10) i;
insert into bar select i,i from generate_series(1,10) i;
insert into bar1 select i,i from generate_series(1,10) i;

-- alter distribution policy of masteronly table
alter table foo set distributed by (x);
alter table foo1 set distributed randomly;
-- alter a partitioned table to masteronly table
alter table bar set distributed masteronly;
alter table bar1 set distributed masteronly;

-- verify the new policies
\d foo
\d foo1
\d bar
\d bar1

-- verify the reorganized data
select * from foo;
select * from foo1;
select count(*) from bar;
select count(*) from bar1;

-- alter back
alter table foo set distributed masteronly;
alter table foo1 set distributed masteronly;
alter table bar set distributed by (x);
alter table bar1 set distributed randomly;

-- verify the policies again
\d foo
\d foo1
\d bar
\d bar1

-- verify the reorganized data again
select * from foo;
select * from foo1;
select count(*) from bar;
select count(*) from bar1;

drop table if exists foo;
drop table if exists foo1;
drop table if exists bar;
drop table if exists bar1;

---------
-- UPDATE / DELETE
---------
create table foo(x int, y int) distributed masteronly;
create table bar(x int, y int);
insert into foo values (1, 1), (2, 1);
insert into bar values (1, 2), (2, 2);
update foo set y = 2 where y = 1;
select * from foo;
update foo set y = 1 from bar where bar.y = foo.y;
select * from foo;
delete from foo where y = 1;
select * from foo;

-- Test masteronly table within init plan
insert into foo values (1, 1), (2, 1);
select * from bar where exists (select * from foo);

------
-- Test Current Of is disabled for masteronly table
------
begin;
declare c1 cursor for select * from foo;
fetch 1 from c1;
delete from foo where current of c1;
abort;

begin;
declare c1 cursor for select * from foo;
fetch 1 from c1;
update foo set y = 1 where current of c1;
abort;

-----
-- Test updatable view works for masteronly table
----
truncate foo;
truncate bar;
insert into foo values (1, 1);
insert into foo values (2, 2);
insert into bar values (1, 1);
create view v_foo as select * from foo where y = 1;
begin;
update v_foo set y = 2;
select * from gp_dist_random('foo');
abort;

update v_foo set y = 3 from bar where bar.y = v_foo.y;
select * from gp_dist_random('foo');
-- Test gp_segment_id for masteronly table
-- gp_segment_id is ambiguous for masteronly table, it's been disabled now.
create table baz (c1 int, c2 int) distributed masteronly;
create table qux (c1 int, c2 int);

select gp_segment_id from baz;
select xmin from baz;
select xmax from baz;
select ctid from baz;
select * from baz where c2 = gp_segment_id;
select * from baz, qux where baz.c1 = gp_segment_id;
update baz set c2 = gp_segment_id;
update baz set c2 = 1 where gp_segment_id = 1;
update baz set c2 = 1 from qux where gp_segment_id = baz.c1;
insert into baz select i, i from generate_series(1, 1000) i;
vacuum baz;
vacuum full baz;
analyze baz;

-- Test dependencies check when alter table to masteronly table
create view v_qux as select ctid from qux;
alter table qux set distributed masteronly;
drop view v_qux;
alter table qux set distributed masteronly;

-- Test cursor for update also works for masteronly table
create table cursor_update (c1 int, c2 int) distributed masteronly;
insert into cursor_update select i, i from generate_series(1, 10) i;
begin;
declare c1 cursor for select * from cursor_update order by c2 for update;
fetch next from c1;
end;

-- Test MinMax path on masteronly table
create table minmaxtest (x int, y int) distributed masteronly;
create index on minmaxtest (x);
insert into minmaxtest select generate_series(1, 10);
set enable_seqscan=off;
select min(x) from minmaxtest;

-- Test masteronly on partition table
-- will crash by Assert
-- CREATE TABLE foopart (a int4, b int4) DISTRIBUTED MASTERONLY PARTITION BY RANGE (a) (START (1) END (10));
-- CREATE TABLE foopart (a int4, b int4) PARTITION BY RANGE (a) (START (1) END (10)) ;
-- should fail
-- ALTER TABLE foopart SET DISTRIBUTED MASTERONLY;
-- ALTER TABLE foopart_1_prt_1 SET DISTRIBUTED MASTERONLY;
-- DROP TABLE foopart;

-- Test that masteronly table can't inherit a parent table, and it also
-- can't be inherited by a child table.
-- 1. Replicated table can't inherit a parent table.
CREATE TABLE parent (t text) DISTRIBUTED BY (t);
-- This is not allowed: should fail
CREATE TABLE child () INHERITS (parent) DISTRIBUTED MASTERONLY;

CREATE TABLE child (t text) DISTRIBUTED MASTERONLY;
-- should fail
ALTER TABLE child INHERIT parent;
DROP TABLE child, parent;

-- 2. Replicated table can't be inherited
CREATE TABLE parent (t text) DISTRIBUTED MASTERONLY;
-- should fail
CREATE TABLE child () INHERITS (parent) DISTRIBUTED MASTERONLY;
CREATE TABLE child () INHERITS (parent) DISTRIBUTED BY (t);

CREATE TABLE child (t text) DISTRIBUTED MASTERONLY;
ALTER TABLE child INHERIT parent;

CREATE TABLE child2(t text) DISTRIBUTED BY (t);
ALTER TABLE child2 INHERIT parent;

DROP TABLE child, child2, parent;

-- volatile masteronly
-- General and segmentGeneral locus imply that if the corresponding
-- slice is executed in many different segments should provide the
-- same result data set. Thus, in some cases, General and segmentGeneral
-- can be treated like broadcast. But if the segmentGeneral and general
-- locus path contain volatile functions, they lose the property and
-- can only be treated as singleQE. The following cases are to check that
-- we correctly handle all these cases.

-- FIXME: ORCA does not consider this, we need to fix the cases when ORCA
-- consider this.
create table t_hashdist(a int, b int, c int) distributed by (a);
create table t_masteronly_volatile(a int, b int, c int) distributed masteronly;

---- pushed down filter
explain (costs off) select * from t_masteronly_volatile, t_hashdist where t_masteronly_volatile.a > random();

-- join qual
explain (costs off) select * from t_hashdist, t_masteronly_volatile x, t_masteronly_volatile y where x.a + y.a > random();

-- sublink & subquery
explain (costs off) select * from t_hashdist where a > All (select random() from t_masteronly_volatile);
explain (costs off) select * from t_hashdist where a in (select random()::int from t_masteronly_volatile);

-- subplan
explain (costs off, verbose) select * from t_hashdist left join t_masteronly_volatile on t_hashdist.a > any (select random() from t_masteronly_volatile);

-- targetlist
explain (costs off) select * from t_hashdist cross join (select random () from t_masteronly_volatile)x;
explain (costs off) select * from t_hashdist cross join (select a, sum(random()) from t_masteronly_volatile group by a) x;
explain (costs off) select * from t_hashdist cross join (select random() as k, sum(a) from t_masteronly_volatile group by k) x;
explain (costs off) select * from t_hashdist cross join (select a, sum(b) as s from t_masteronly_volatile group by a having sum(b) > random() order by a) x ;

-- insert
explain (costs off) insert into t_masteronly_volatile select random() from t_masteronly_volatile;
explain (costs off) insert into t_masteronly_volatile select random(), a, a from generate_series(1, 10) a;
create sequence seq_for_insert_masteronly_table;
explain (costs off) insert into t_masteronly_volatile select nextval('seq_for_insert_masteronly_table');
-- update & delete
explain (costs off) update t_masteronly_volatile set a = 1 where b > random();
explain (costs off) update t_masteronly_volatile set a = 1 from t_masteronly_volatile x where x.a + random() = t_masteronly_volatile.b;
explain (costs off) update t_masteronly_volatile set a = 1 from t_hashdist x where x.a + random() = t_masteronly_volatile.b;
explain (costs off) delete from t_masteronly_volatile where a < random();
explain (costs off) delete from t_masteronly_volatile using t_masteronly_volatile x where t_masteronly_volatile.a + x.b < random();
explain (costs off) update t_masteronly_volatile set a = random();

-- limit
explain (costs off) insert into t_masteronly_volatile select * from t_masteronly_volatile limit random();
explain (costs off) select * from t_hashdist cross join (select * from t_masteronly_volatile limit random()) x;

-- copy
CREATE TEMP TABLE x (
	a serial,
	b int,
	c text not null default 'stuff',
	d text,
	e text
) DISTRIBUTED MASTERONLY;


COPY x (a, b, c, d, e) from stdin;
9999	\N	\\N	\NN	\N
10000	21	31	41	51
\.

COPY x (b, d) from stdin;
1	test_1
\.

COPY x (b, d) from stdin;
2	test_2
3	test_3
4	test_4
5	test_5
\.

COPY x (a, b, c, d, e) from stdin;
10001	22	32	42	52
10002	23	33	43	53
10003	24	34	44	54
10004	25	35	45	55
10005	26	36	46	56
\.

-- non-existent column in column list: should fail
COPY x (xyz) from stdin;

-- too many columns in column list: should fail
COPY x (a, b, c, d, e, d, c) from stdin;

-- missing data: should fail
COPY x from stdin;

\.
COPY x from stdin;
2000	230	23	23
\.
COPY x from stdin;
2001	231	\N	\N
\.

-- extra data: should fail
COPY x from stdin;
2002	232	40	50	60	70	80
\.


-- check results of copy in
SELECT * FROM x;

-- check copy out
COPY x TO stdout;
COPY x (c, e) TO stdout;
COPY x (b, e) TO stdout WITH NULL 'I''m null';

CREATE TEMP TABLE y (
	col1 text,
	col2 text
) DISTRIBUTED MASTERONLY;

INSERT INTO y VALUES ('Jackson, Sam', E'\\h');
INSERT INTO y VALUES ('It is "perfect".',E'\t');
INSERT INTO y VALUES ('', NULL);

COPY y TO stdout WITH CSV;
COPY y TO stdout WITH CSV QUOTE '''' DELIMITER '|';
COPY y TO stdout WITH CSV FORCE QUOTE col2 ESCAPE E'\\' ENCODING 'sql_ascii';
COPY y TO stdout WITH CSV FORCE QUOTE *;

-- Repeat above tests with new 9.0 option syntax

COPY y TO stdout (FORMAT CSV);
COPY y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE *);

\copy y TO stdout (FORMAT CSV)
\copy y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE *)


--test that we read consecutive LFs properly

CREATE TEMP TABLE testnl (a int, b text, c int) DISTRIBUTED MASTERONLY;

COPY testnl FROM stdin CSV;
1,"a field with two LFs

inside",2
\.

-- test end of copy marker
CREATE TEMP TABLE testeoc (a text) DISTRIBUTED MASTERONLY;

COPY testeoc FROM stdin CSV;
a\.
\.b
c\.d
"\."
\.

COPY testeoc TO stdout CSV;


-- test handling of nonstandard null marker that violates escaping rules

CREATE TEMP TABLE testnull(a int, b text) DISTRIBUTED MASTERONLY;
INSERT INTO testnull VALUES (1, E'\\0'), (NULL, NULL);

COPY testnull TO stdout WITH NULL AS E'\\0';

COPY testnull FROM stdin WITH NULL AS E'\\0';
42	\\0
\0	\0
\.

SELECT * FROM testnull;

BEGIN;
CREATE TABLE vistest (LIKE testeoc) DISTRIBUTED MASTERONLY;
COPY vistest FROM stdin CSV;
a0
b
\.
COMMIT;
SELECT * FROM vistest;
BEGIN;
TRUNCATE vistest;
COPY vistest FROM stdin CSV;
a1
b
\.
SELECT * FROM vistest;
SAVEPOINT s1;
TRUNCATE vistest;
COPY vistest FROM stdin CSV;
d1
e
\.
SELECT * FROM vistest;
COMMIT;
SELECT * FROM vistest;

BEGIN;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
a2
b
\.
SELECT * FROM vistest;
SAVEPOINT s1;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
d2
e
\.
SELECT * FROM vistest;
COMMIT;
SELECT * FROM vistest;

BEGIN;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
x
y
\.
SELECT * FROM vistest;
COMMIT;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
p
g
\.
BEGIN;
TRUNCATE vistest;
SAVEPOINT s1;
COPY vistest FROM stdin CSV FREEZE;
m
k
\.
COMMIT;
BEGIN;
INSERT INTO vistest VALUES ('z');
SAVEPOINT s1;
TRUNCATE vistest;
ROLLBACK TO SAVEPOINT s1;
COPY vistest FROM stdin CSV FREEZE;
d3
e
\.
COMMIT;
CREATE FUNCTION truncate_in_subxact() RETURNS VOID AS
$$
BEGIN
	TRUNCATE vistest;
EXCEPTION
  WHEN OTHERS THEN
	INSERT INTO vistest VALUES ('subxact failure');
END;
$$ language plpgsql;
BEGIN;
INSERT INTO vistest VALUES ('z');
SELECT truncate_in_subxact();
COPY vistest FROM stdin CSV FREEZE;
d4
e
\.
SELECT * FROM vistest;
COMMIT;
SELECT * FROM vistest;
-- Test FORCE_NOT_NULL and FORCE_NULL options
CREATE TEMP TABLE forcetest (
    a INT NOT NULL,
    b TEXT NOT NULL,
    c TEXT,
    d TEXT,
    e TEXT
) DISTRIBUTED MASTERONLY;
\pset null NULL
-- should succeed with no effect ("b" remains an empty string, "c" remains NULL)
BEGIN;
COPY forcetest (a, b, c) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL(b), FORCE_NULL(c));
1,,""
\.
COMMIT;
SELECT b, c FROM forcetest WHERE a = 1;
-- should succeed, FORCE_NULL and FORCE_NOT_NULL can be both specified
BEGIN;
COPY forcetest (a, b, c, d) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL(c,d), FORCE_NULL(c,d));
2,'a',,""
\.
COMMIT;
SELECT c, d FROM forcetest WHERE a = 2;
-- should fail with not-null constraint violation
BEGIN;
COPY forcetest (a, b, c) FROM STDIN WITH (FORMAT csv, FORCE_NULL(b), FORCE_NOT_NULL(c));
3,,""
\.
ROLLBACK;
-- should fail with "not referenced by COPY" error
BEGIN;
COPY forcetest (d, e) FROM STDIN WITH (FORMAT csv, FORCE_NOT_NULL(b));
ROLLBACK;
-- should fail with "not referenced by COPY" error
BEGIN;
COPY forcetest (d, e) FROM STDIN WITH (FORMAT csv, FORCE_NULL(b));
ROLLBACK;
\pset null ''

-- should fail with "masteronly_range_partition is DISTRIBUTED MASTERONLY, cannot set PARTITION."
CREATE TABLE masteronly_range_partition (id int, date date)
DISTRIBUTED MASTERONLY
PARTITION BY RANGE(date)
(
START (date '2020-01-01')
END (date '2021-01-01')
EVERY (INTERVAL '1 day')
);

-- should fail with "masteronly_list_partition is DISTRIBUTED MASTERONLY, cannot set PARTITION."
CREATE TABLE masteronly_list_partition (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED MASTERONLY
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );

CREATE TABLE expand_masteronly (id int)
DISTRIBUTED MASTERONLY;

-- should fail with "cannot expand table "foo" "MASTERONLY table does not support expansion."
ALTER TABLE expand_masteronly EXPAND TABLE;

-- start_ignore
drop schema masteronly cascade;
-- end_ignore

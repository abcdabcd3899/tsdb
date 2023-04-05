set search_path to test_toin;
set enable_seqscan to off;
set enable_bitmapscan to off;
set enable_indexonlyscan to off;

\set tname 't_type'
\set iname 'i_type'
\set itype 'toin'
\set keys 'c1'
\set opts 'pages_per_range=1'
\set explain 'explain (costs off)'

-- start_ignore
drop table if exists :tname;
-- end_ignore

create table :tname (c1 bytea) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '0';
drop table :tname;

create table :tname (c1 char(8)) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '0';
drop table :tname;

create table :tname (c1 name) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '0';
drop table :tname;

create table :tname (c1 bigint) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = 0;
drop table :tname;

create table :tname (c1 smallint) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = 0;
drop table :tname;

create table :tname (c1 integer) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = 0;
drop table :tname;

create table :tname (c1 text) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '0';
drop table :tname;

create table :tname (c1 oid) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = 0;
drop table :tname;

create table :tname (c1 tid) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '(0,0)';
drop table :tname;

create table :tname (c1 real) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = 0;
drop table :tname;

create table :tname (c1 double precision) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = 0;
drop table :tname;

create table :tname (c1 macaddr) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '00:00:00:00:00:00';
drop table :tname;

create table :tname (c1 macaddr8) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '00:00:00:00:00:00';
drop table :tname;

create table :tname (c1 inet) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '0.0.0.0';
drop table :tname;

create table :tname (c1 character(8)) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '0';
drop table :tname;

create table :tname (c1 time without time zone) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '00:00:00';
drop table :tname;

create table :tname (c1 date) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '2001-01-01';
drop table :tname;

create table :tname (c1 timestamp without time zone) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '2001-01-01';
drop table :tname;

create table :tname (c1 interval) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '1';
drop table :tname;

create table :tname (c1 time with time zone) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '00:00:00-08';
drop table :tname;

create table :tname (c1 bit(8)) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '0';
drop table :tname;

create table :tname (c1 varbit) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '0'::bit(8);
drop table :tname;

create table :tname (c1 numeric(6,2)) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = 0;
drop table :tname;

create table :tname (c1 uuid) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '00000000-0000-0000-0000-000000000000';
drop table :tname;

create table :tname (c1 pg_lsn) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = '0/0';
drop table :tname;

--
-- ANYRANGE types
--

create table :tname (c1 int4range) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = int4range(0, 5);
drop table :tname;

create table :tname (c1 int8range) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = int8range(0, 5);
drop table :tname;

create table :tname (c1 numrange) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = numrange(0, 5);
drop table :tname;

create table :tname (c1 tsrange) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = tsrange('2001-01-01', '2001-01-05');
drop table :tname;

create table :tname (c1 tstzrange) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = tstzrange('2001-01-01', '2001-01-05');
drop table :tname;

create table :tname (c1 daterange) distributed by (c1);
create index :iname on :tname using :itype (:keys) with (:opts);
:explain select * from :tname where c1 = daterange('2001-01-01', '2001-01-05');
drop table :tname;

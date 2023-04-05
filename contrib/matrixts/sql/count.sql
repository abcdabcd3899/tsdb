--
-- cleanup
--

-- start_ignore
drop schema if exists test_count;
-- end_ignore

--
-- setup
--

create schema test_count;
set search_path to test_count,public;

--
-- AO
--

create table t_ao_row (c1 int, c2 int) using ao_row distributed by (c1);
insert into t_ao_row select i, i from generate_series(1, 10) i;
delete from t_ao_row where c1 = 5;
update t_ao_row set c2 = c1 where c1 = 6;

select count(*) from t_ao_row;
select * from fast_count('t_ao_row');

begin;
  delete from t_ao_row where c1 = 1;
  update t_ao_row set c2 = c1 where c1 = 2;

  select count(*) from t_ao_row;
  select * from fast_count('t_ao_row');
abort;

select count(*) from t_ao_row;
select * from fast_count('t_ao_row');

--
-- AOCS
--

create table t_ao_column (c1 int, c2 int) using ao_column distributed by (c1);
insert into t_ao_column select i, i from generate_series(1, 10) i;
delete from t_ao_column where c1 = 5;
update t_ao_column set c2 = c1 where c1 = 6;

select count(*) from t_ao_column;
select * from fast_count('t_ao_column');

begin;
  delete from t_ao_column where c1 = 1;
  update t_ao_column set c2 = c1 where c1 = 2;

  select count(*) from t_ao_column;
  select * from fast_count('t_ao_column');
abort;

select count(*) from t_ao_column;
select * from fast_count('t_ao_column');

--
-- heap
--

create table t_heap (c1 int, c2 int) using heap distributed by (c1);
insert into t_heap select i, i from generate_series(1, 10) i;
delete from t_heap where c1 = 5;
update t_heap set c2 = c1 where c1 = 6;

select count(*) from t_heap;
select * from fast_count('t_heap');

begin;
  delete from t_heap where c1 = 1;
  update t_heap set c2 = c1 where c1 = 2;

  select count(*) from t_heap;
  select * from fast_count('t_heap');
abort;

select count(*) from t_heap;
select * from fast_count('t_heap');

--
-- AO with special characters in the name
--

create table "ao row name with "" and '" (c1 int, c2 int) using ao_row distributed by (c1);
insert into "ao row name with "" and '" select i, i from generate_series(1, 10) i;
delete from "ao row name with "" and '" where c1 = 5;
update "ao row name with "" and '" set c2 = c1 where c1 = 6;

select count(*) from "ao row name with "" and '";
select * from fast_count('"ao row name with "" and ''"');

--
-- heap with special characters in the name
--

create table "heap name with "" and '" (c1 int, c2 int) using heap distributed by (c1);
insert into "heap name with "" and '" select i, i from generate_series(1, 10) i;
delete from "heap name with "" and '" where c1 = 5;
update "heap name with "" and '" set c2 = c1 where c1 = 6;

select count(*) from "heap name with "" and '";
select * from fast_count('"heap name with "" and ''"');

--
-- mars extension depends on matrixts, verify the dependence
--

drop extension if exists mars cascade;
drop extension if exists matrixts cascade;

-- this should fail due to extension dependence
create extension mars;

-- this should succeed
create extension mars cascade;

drop extension mars cascade;
drop extension matrixts cascade;

-- this should also succeed
create extension matrixts;
create extension mars;

--
-- verify extension versions
--

-- by default the latest version is installed
drop extension mars cascade;
create extension mars;
\dx mars
\dx+ mars

-- we can also specify the version
drop extension mars cascade;
create extension mars with version '1.0';
\dx mars
\dx+ mars

-- we can also update the extension manually
alter extension mars update;
\dx mars
\dx+ mars

--
-- verify basic behaviors
--

SELECT amtype = 't' FROM pg_am WHERE amname = 'mars' ;

create table foo(a int, b int) using mars;

insert into foo select i, i * 2 from generate_series(1, 5) i;

select * from foo;

drop table foo;

create table foo (a bool) using mars;
insert into foo select (i % 2)::bool from generate_series(1, 10) as i;
select * from foo;
drop table foo;

-- failed case
DROP EXTENSION mars;

SELECT count(*) FROM pg_am WHERE amname = 'mars';

-- bfv: the parent of a partioned table will has relam=0, and when mars is not
-- loaded yet, the rel will be considered as mars incorrectly, and lead to
-- below runtime error:
--
--     ERROR:  missing mx_mars entry for relation "bfv_mars_parted_t1"
--
create table bfv_mars_parted_t1 (partkey timestamp) partition by range (partkey);
create table bfv_mars_parted_t11 partition of bfv_mars_parted_t1 for values from (minvalue) to (current_timestamp);
create table bfv_mars_parted_t12 partition of bfv_mars_parted_t1 for values from (current_timestamp) to (maxvalue);
insert into bfv_mars_parted_t1 values (current_timestamp);
select count(*) from bfv_mars_parted_t1;

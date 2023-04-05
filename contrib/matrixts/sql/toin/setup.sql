--
-- cleanup
--
-- must always reload the extension
--

-- start_ignore
drop schema if exists test_toin;
drop extension matrixts cascade;
-- end_ignore

--
-- setup
--

create extension matrixts;
create schema test_toin;

--
-- load data
--

set search_path to test_toin;

create table t_data_template (c1 int, c2 int) distributed by (c1);

-- c1: [ 0 x10], [20 x10], [40 x10], ..., [199980, x10]
-- c2: [ 0,  9], [20, 29], [40, 49], ..., [199980, 199989]

insert into t_data_template
select i / 10 * 20
     , i + i / 10 * 10
  from generate_series(0, 99999) i;

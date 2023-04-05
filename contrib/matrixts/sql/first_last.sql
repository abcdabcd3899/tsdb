--
-- Test the optimizations for first(), last(), min(), max(), they should be
-- converted to subqueries like "select * from t1 order by c1 limit 1".
--

-- start_ignore
drop schema if exists test_first_last;
create extension if not exists gp_debug_numsegments;
-- end_ignore

-- we care about the plans
select gp_debug_set_create_table_default_numsegments(1);

create schema test_first_last;
set search_path to test_first_last,public;

create table t1 (c1 int, c2 int) distributed by (c1);
insert into t1 select i, i*2 from generate_series(1, 1000) i;

set enable_indexscan to on;
set enable_indexonlyscan to off;
set matrix.disable_optimizations to off;

--
-- cannot optimize w/o an index
--

\i sql/first_last.template

--
-- cannot optimize w/o a btree index
--

create index t1_idx on t1 using brin (c1);
\i sql/first_last.template
drop index t1_idx;

--
-- cannot optimize w/o a proper btree index
--

create index t1_idx on t1 using btree (c2);
\i sql/first_last.template
drop index t1_idx;

--
-- can optimize w/ a proper btree index
--

create index t1_idx on t1 using btree (c1);
\i sql/first_last.template
drop index t1_idx;

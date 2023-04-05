-- start_ignore
create extension if not exists matrixts;
-- end_ignore

-- test conf
\set con_view_count 'cv_count'
\set con_view_min 'cv_min'
\set con_view_max 'cv_max'
\set con_view_sum 'cv_sum'
\set con_view_avg 'cv_avg'

\set check_meta_data 'select srcname, viewname, toplevel from matrixts_internal.continuous_views;'

\set check_count_con_view 'select ''count'', (select count(c1) from at_partitioned) = (select * from cv_count)'
\set check_min_con_view 'select ''min'', (select min(c1) from at_partitioned) = (select * from cv_min)'
\set check_max_con_view 'select ''max'', (select max(c1) from at_partitioned) = (select * from cv_max)'
\set check_sum_con_view 'select ''sum'', (select sum(c1) from at_partitioned) = (select * from cv_sum)'
\set check_avg_con_view 'select ''avg'', (select avg(c1) from at_partitioned) = (select * from cv_avg)'

\set check_con_views ':check_count_con_view union all :check_min_con_view union all :check_max_con_view union all :check_sum_con_view union all :check_avg_con_view;'

\set check_count_view_result 'select * from :con_view_count'
\set check_min_view_result 'select * from :con_view_min'
\set check_max_view_result 'select * from :con_view_max'
\set check_sum_view_result 'select * from :con_view_sum'
\set check_avg_view_result 'select * from :con_view_avg'

\set check_view_result ':check_count_view_result union all :check_min_view_result union all :check_max_view_result union all :check_sum_view_result union all :check_avg_view_result;'
--
-- 1. Partition inherit continuous views with populate
--
CREATE TABLE at_partitioned (c1 int, c2 text)
PARTITION BY RANGE(c1);

-- create partition
CREATE TABLE at_part_1 (c1 int, c2 text);
ALTER TABLE at_partitioned ATTACH PARTITION at_part_1
FOR VALUES FROM (0) to (10);

-- create continuous view
create view :con_view_count with (continuous) as select count(c1) from at_partitioned ;
create view :con_view_min with (continuous) as select min(c1) from at_partitioned ;
create view :con_view_max with (continuous) as select max(c1) from at_partitioned ;
create view :con_view_sum with (continuous) as select sum(c1) from at_partitioned ;
create view :con_view_avg with (continuous) as select avg(c1) from at_partitioned ;

-- check meta data before insert data
-- at_part_1 created before continuous views
\d+ at_part_1
:check_meta_data
:check_con_views
:check_view_result

-- insert to partition 1
insert into at_partitioned select  0 + i % 5 , 'at_part_1_main' from generate_series(1, 50)i;
:check_con_views
:check_view_result

insert into at_partitioned select 0 + i % 10, 'at_part_1_main' from generate_series(1, 50)i;

-- Create partition
--
-- 1. Alter {Main} Attach PARTITION {Sub}
--
create table at_part_2 (c1 int, c2 text);
-- insert data before attach partition
insert into at_part_2 select 10 + i % 10, 'at_part_2_sub' from generate_series(1, 50)i;
:check_con_views
:check_view_result

-- attach partition
alter table at_partitioned attach partition at_part_2 for values from (10) to (20);
-- auto inherit
:check_meta_data
-- auto populate
:check_con_views
:check_view_result

-- insert data into main table
insert into at_partitioned select 10 + i % 5, 'at_part_2_main' from generate_series(1, 50)i;
:check_con_views
:check_view_result
-- insert data into sub table
insert into at_part_2 select 10 + i % 8, 'at_part_2_sub' from generate_series(1, 50)i;
:check_con_views
:check_view_result

--
-- 2. ALTER {Main} ADD {Sub} FOR VALUES START {} END {};
--
create table at_part_3 (c1 int, c2 text);
-- insert data before add partition
insert into at_part_3 select 20 + i % 10, 'at_part_3_sub' from generate_series(1, 50)i;
:check_con_views
:check_view_result

-- add partition
alter table at_partitioned add partition at_part_3 start (20) end (30);
-- add partition will create a new partition table that not inherit data from raw table.
-- auto inherit
:check_meta_data
-- auto populate
:check_con_views
:check_view_result

-- insert data into main table
insert into at_partitioned select 20 + i % 5, 'at_part_3_main' from generate_series(1, 50)i;
:check_con_views
:check_view_result
insert into at_partitioned_1_prt_at_part_3 select 20 + i % 8, 'at_part_3_sub' from generate_series(1, 50)i;
:check_con_views
:check_view_result

--
-- 3. CREATE {Sub} PARTITION OF {Main}
--
create table at_part_4 partition of at_partitioned for values from (30) to (40);
:check_meta_data

-- insert data into main table
insert into at_partitioned select 30 + i % 5, 'at_part_4_main' from generate_series(1, 50)i;
:check_con_views
:check_view_result
-- insert data into sub table
insert into at_part_4 select 30 + i % 10, 'at_part_4_sub' from generate_series(1, 50)i;
:check_con_views
:check_view_result

-- should error,
drop table at_partitioned;

-- succeed to drop
drop table at_partitioned CASCADE;

-- should fail, drop partition main table, sub tables should not exist.
-- no table left means they are a partition group.
\d+ at_part_1
\d+ at_part_2
-- at_part_3 is not sub partition
\d+ at_part_3
\d+ at_partitioned_1_prt_at_part_3
\d+ at_part_4

drop table at_part_3;

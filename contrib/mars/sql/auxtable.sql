--
-- verify index columns in aux table
--

-- start_matchsubs
-- #   ->  Index Scan using "idx_18104_footer" on "18104_footer"
-- m/->  Index Scan using "idx\d*__\d+_footer" on "\d+_footer"/
-- s/->  Index Scan using "idx\d*__\d+_footer" on "\d+_footer"/->  Index Scan using "idx__###_footer" on "###_footer"/
-- # Index Cond: ((attr2__min__c1 <= 5) AND (attr2__max__c1 >= 5))
-- m/Index Cond: \(\(attr2__m[ai][xn]__c1 [<>]= 5\) AND \(attr2__m[ai][xn]__c1 [<>]= 5\)\)/
-- s/Index Cond: \(\(attr2__m[ai][xn]__c1 [<>]= 5\) AND \(attr2__m[ai][xn]__c1 [<>]= 5\)\)/Index Cond: ###/
-- end_matchsubs

set datestyle = 'ISO';

\set tname test_mars_auxtable_t1
\set tnamestr '''' :tname ''''

drop table if exists :tname;

-- this test is sensitive to cluster size, so force all the data to be stored
-- on seg0 only.
select gp_debug_set_create_table_default_numsegments(1);

create table :tname (c0 int, c1 int, c2 int, c3 int, c4 real) using mars
 with (group_col_ = '{c1}', group_param_col_ = '{c0 in 10}', global_order_col_ = "{c1, c0}")
 distributed by (c1);

\out /dev/null
select segrelid as aux_table_oid
     , segrelid::regclass::name as aux_table_name
  from mx_mars
 where relid = :tnamestr::regclass;
\out

\gset
\set aux_table_name_str '''' :aux_table_name ''''

-- verify that the orderkeys' min & max columns are stored separately in the aux table
select attnum, attname, atttypid::regtype::name from pg_attribute
  where attrelid = :aux_table_oid and attnum >= 12;

insert into :tname
  select i, i/10, i, i, i/2.0 from generate_series(0, 30000) i;

-- verify that min & max are stored correctly with correct types
select attr2__min__c1, attr1__min__c0, attr2__max__c1, attr1__max__c0
  from gp_dist_random(:aux_table_name_str) limit 10;

-- verify if index exists
select count(*)
  from pg_index where indrelid = :aux_table_oid;

\out /dev/null
-- the first aux index is always for ctid seeking, skip it
select indexrelid as aux_index_oid,
       indexrelid::regclass::name as aux_index_name
  from pg_index where indrelid = :aux_table_oid
  order by indexrelid limit 1 offset 1;
\out

\gset
\set aux_index_name_str '''' :aux_index_name ''''

-- verify the index keys
select attnum, attname, atttypid::regtype::name from pg_attribute
  where attrelid = :aux_index_oid;

set enable_seqscan to off;
set enable_bitmapscan to off;
set enable_indexonlyscan to off;

-- where c1 = 5 on data table:

-- Hard to verify plan, since index name is dynamic generated, 
-- if we explain the query, it will become a flacky test.
-- verify the result
select batch
     , rowcount >> 32 as logical_rowcount
     , rowcount % 4294967296 as physical_rowcount
  from gp_dist_random(:aux_table_name_str)
  where attr2__min__c1<=5 and attr2__max__c1>=5;

-- verify that aux index is truncated together with the table
truncate :tname;
insert into :tname
  select i, i/10, i, i, i/2.0 from generate_series(0, 30000) i;

-- verify the index was truncated by querying again:

-- Hard to verify plan, since index name is dynamic generated, 
-- if we explain the query, it will become a flacky test.
-- verify the result
select batch
     , rowcount >> 32 as logical_rowcount
     , rowcount % 4294967296 as physical_rowcount
  from gp_dist_random(:aux_table_name_str)
  where attr2__min__c1<=5 and attr2__max__c1>=5;

select c1, c4 from :tname where c1 = 10;
select c1, c4 from :tname where c1 = 10 and c4 > 51.2;
select c1, c4 from :tname where c1 = 10 and c4 < 43;

select c1, c4 from :tname where 1 >= c1;
select c1, c4 from :tname where 1 >= c1 and c4 > 8;
select c1, c4 from :tname where 100 < c1 and c4 < 515;

set enable_seqscan to on;
set enable_bitmapscan to on;
set enable_indexonlyscan to on;

-- verify index on group keys

-- a) group key only
drop table if exists :tname;
create table :tname (col1 int, tag int, col2 float, ts timestamp, col3 int) using mars
with(group_col_ = "{tag}", group_param_col_ = "{ts in 1 min}") distributed by(tag);

\i sql/auxtable_groupkey.template
\i sql/auxtable_group_index.template
\i sql/auxtable_group_param_index.template

select attr2__group_key__tag, attr4__group_param_key__ts
  from gp_dist_random(:aux_table_name_str);

\i sql/auxtable_groupkey_query.template

-- b) # order key = 1
drop table if exists :tname;
create table :tname (col1 int, tag int, col2 float, ts timestamp, col3 int) using mars
with(group_col_ = "{tag}", group_param_col_ = "{ts in 1 min}", global_order_col_ = "{tag}") distributed by(tag);

\i sql/auxtable_groupkey.template
\i sql/auxtable_group_param_index.template

select attr4__group_param_key__ts
  from gp_dist_random(:aux_table_name_str);

\i sql/auxtable_groupkey_query.template

-- c) # order key = 2
drop table if exists :tname;
create table :tname (col1 int, tag int, col2 float, ts timestamp, col3 int) using mars
with(group_col_ = "{tag}", group_param_col_ = "{ts in 1 min}", global_order_col_ = "{tag, ts}") distributed by(tag);

\i sql/auxtable_groupkey.template
\i sql/auxtable_group_param_index.template

select attr4__group_param_key__ts
  from gp_dist_random(:aux_table_name_str);

\i sql/auxtable_groupkey_query.template

-- verify it works as expected when column name is long enough
drop table if exists :tname;
create table :tname (col1 int, 我是一个很长很长的名字差不多会也许会可能 int,
  col2 float, 我是一个很长很长的名字差不多会也许 timestamp, col3 int) using mars
with(group_col_ = "{我是一个很长很长的名字差不多会也许会可能}",
  group_param_col_ = "{我是一个很长很长的名字差不多会也许 in 1 min}")
  distributed by(col1);

\i sql/auxtable_groupkey.template
\i sql/auxtable_group_index.template
\i sql/auxtable_group_param_index.template

select * from :tname
 where 我是一个很长很长的名字差不多会也许会可能 > 0;

select * from :tname
 where 我是一个很长很长的名字差不多会也许 > '2021-07-15 05:14:20';

select * from :tname
 where 我是一个很长很长的名字差不多会也许 <= '2021-07-15 05:14:20'
  and 我是一个很长很长的名字差不多会也许会可能 = 0;

select attr2__group_key__我是一个很长很长的名字差不多会, attr4__group_param_key__我是一个很长很长的名字差不
  from gp_dist_random(:aux_table_name_str);

drop table if exists :tname;
create table :tname (col1 int, a我是一个很长很长的名字差不多会也许会可能 int,
  col2 float, a我是一个很长很长的名字差不多会也许 timestamp, col3 int) using mars
with(group_col_ = "{a我是一个很长很长的名字差不多会也许会可能}",
  group_param_col_ = "{a我是一个很长很长的名字差不多会也许 in 1 min}")
  distributed by(col1);

\i sql/auxtable_groupkey.template
\i sql/auxtable_group_index.template
\i sql/auxtable_group_param_index.template

select * from :tname
 where a我是一个很长很长的名字差不多会也许会可能 > 0;

select * from :tname
 where a我是一个很长很长的名字差不多会也许 > '2021-07-15 05:14:20';

select * from :tname
 where a我是一个很长很长的名字差不多会也许 <= '2021-07-15 05:14:20'
  and a我是一个很长很长的名字差不多会也许会可能 = 0;

select attr2__group_key__a我是一个很长很长的名字差不多, attr4__group_param_key__a我是一个很长很长的名字差
  from gp_dist_random(:aux_table_name_str);

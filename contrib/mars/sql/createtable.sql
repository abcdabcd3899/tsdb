-- start_ignore
drop table heap;
drop table mars;
create extension mars;
-- end_ignore
create table heap (tag int, ts timestamp, col1 int, col2 float);

create table mars (tag int, ts timestamp, col1 int, col2 float) using mars;
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
drop table mars;

-- Accurate grammar
create table mars (tag int, ts timestamp, col1 int, col2 float) using mars
with(group_col_ = "{tag}", group_param_col_ = "{ts in 1 min}", global_order_col_ = "{tag, ts}", local_order_col_ = "{ts}");
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
\d+ mars;
drop table mars;

create table mars (like heap) using mars
with(group_col_ = "{tag}", group_param_col_ = "{ts in 1 min}", global_order_col_ = "{tag, ts}", local_order_col_ = "{ts}");
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
drop table mars;

-- Simple grammar
create table mars (tag int, ts timestamp, col1 int, col2 float) using mars
with(tagkey = "tag", timekey= "ts", timebucket= "1 min");
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
\d+ mars;
drop table mars;

create table mars (like heap) using mars
with(tagkey = "tag", timekey= "ts", timebucket= "1 min");
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
drop table mars;

create table mars (like heap) using mars
with(timekey= "ts", timebucket= "1 min");
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
drop table mars;

create table mars (like heap) using mars
with(tagkey = "tag");
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
drop table mars;

create table mars (like heap) using mars
with(tagkey = tag);
drop table mars;

create table mars (like heap) using mars
with(tagkey = "tag", timekey= "ts");
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
\d+ mars;
drop table mars;

create table mars (like heap) using mars
with(tagkey = "{tag}", timekey = "ts");
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
drop table mars;

-- space char before and after curly braces can be omitted 
create table mars (like heap) using mars
with(tagkey = " {tag} ", timekey = "ts");
drop table mars;

create table mars (tag int, tag1 int, tag2 int, ts timestamp, col1 float) using mars
with(tagkey = "{tag, tag1 , tag2}", timekey = "ts");
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
drop table mars;

create table mars (tag int, "tag{" int, ts timestamp, col1 float) using mars
with(tagkey = "tag{", timekey = "ts");
drop table mars;

-- '{' is a valid char for column, but should be included by "" in tagkey definition 
create table mars (tag int, "tag{" int, ts timestamp, col1 float) using mars
with(tagkey = '{tag, "tag{"}', timekey = "ts");
drop table mars;

create table mars (c1 int, c2 float) using mars
with (global_order_col_ = "{c2}", local_order_col_ = "{c2}")
distributed by (c1);
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
drop table mars;

create table mars (c1 int, c2 real) using mars
with (global_order_col_ = "{c2}", local_order_col_ = "{c2}")
distributed by (c1);
select "localOrder", "globalOrder", "grpOpt", "grpWithParamOpt", "grpParam" from mx_mars;
drop table mars;

-- Failed cases
create table mars (like heap) using mars
with(group_col_ = "{tag}", timekey="ts", timebucket="1 min");

create table mars (like heap) using mars
with(timebucket="1 min");

create table mars (like heap) using mars
with(group_col_ = "{tags}");

create table mars (like heap) using mars
with(group_param_col_ = "{tags}");

create table mars (like heap) using mars
with(group_param_col_ = "{tags in 1}");

create table mars (like heap) using mars
with(group_param_col_ = "{tags in 1min }");

create table mars (like heap) using mars
with(group_order_col_= "{tags}");

create table mars (like heap) using mars
with(global_order_col_= "{tags}");

create table mars (like heap) using mars
with(local_order_col_= "{tags}");

create table mars (like heap) using mars
with(tagkey = "{tags}");

create table mars (like heap) using mars
with(timekey = "{tags}");

create table mars (like heap) using mars
with(timekey = " tag");

create table mars (tag real,ts real) using mars
with(timekey = "{tag}");

create table mars (tag real,ts real) using mars
with(timekey = "{ts}");

create table mars (tag float,ts float) using mars
with(timekey = "{tag}");

create table mars (tag float,ts float) using mars
with(timekey = "{ts}");

create table mars (tag int, ts timestamp) using mars
with(tagkey = "tag", timekey = "tag");

create table mars (tag int, ts timestamp) using mars
with(tagkey = "ts", timekey = "ts");

create table mars (tag int, ts timestamp) using mars
with(group_col_ = "{tag, tag}");

create table mars (tag int, ts timestamp) using mars
with(group_col_ = "{tag}", group_param_col_ = "{tag in 1}");

drop table heap;

-- bfv: we used to trigger an assertion failure when createing a temp mars
-- table:
--
--     create temp table t1 (c1 int) using mars;
--     psql: FATAL:  Unexpected internal error (assert.c:44)
--     DETAIL:  FailedAssertion("!(isTempOrTempToastNamespace(relnamespace))", File: "relcache.c", Line: 3405)
--
-- this is because we always use pg_aoseg as the namespace for the mars aux
-- table, fixed by using the temp namespace when the main table is temp.
create temp table mars (c1 int) using mars;
drop table mars;

-- bfv: we used to trigger an assertion failure when inserting to a mars table
-- for the first time:
--
--     create temp table copytest4 (id int, id1 int) using mars;
--     insert into copytest4 values (1,2);
--     FATAL:  Unexpected internal error (assert.c:44)
--     DETAIL:  FailedAssertion("!(Gp_role == GP_ROLE_UTILITY)", File: "vacuum.c", Line: 1443)
--
-- this is because the autostat ANALYZE happens before the tuple beging
-- flushed, fixed by forceing the flush.
create temp table mars (c1 int, c2 int) using mars;
insert into mars values (1, 2);
drop table mars;

\set tname test_mars_varlen_type_as_tag
\set tnamestr '''' :tname ''''

select gp_debug_set_create_table_default_numsegments(1);

--start_ignore
drop table if exists :tname;

create table :tname(c0 text, c1 int) using mars
 with(global_order_col_="{c0}") distributed by(c1);
--end_ignore

insert into :tname select i/3, i from generate_series(0, 12) i;

select ctid, * from :tname;

explain (costs off) select * from :tname where c0 > '2';

select * from :tname where c0 > '2';

select * from :tname where c0 > '2'::varchar;

select * from :tname where c0 > '2'::text;

--start_ignore
drop table if exists :tname;

create table :tname(c0 interval, c1 numeric) using mars
 with(global_order_col_="{c0, c1}") distributed by(c0);
--end_ignore

insert into :tname select i/5 * '1 hour'::interval, i/3 from generate_series(0, 12) i;

select ctid, * from :tname;

explain (costs off) select * from :tname where c0 >= make_interval(hours => 2);

select * from :tname where c0 >= make_interval(hours => 2);

explain (costs off) select * from :tname where c1 >= 3::int8;

select * from :tname where c1 >= 3::int8;

explain (costs off) select * from :tname where c0 >= make_interval(hours => 2) and c1 >= 3;

select * from :tname where c0 >= make_interval(hours => 2) and c1 >= 3;

--start_ignore
drop table if exists :tname;

create table :tname(c0 varchar, c1 name) using mars
 with(global_order_col_="{c0, c1}") distributed by(c0);
--end_ignore

insert into :tname select (i/5)::varchar, (i/3)::name from generate_series(0, 12) i;

select ctid, * from :tname;

explain (costs off) select * from :tname where c0 >= '2';

select * from :tname where c0 >= '2';

explain (costs off) select * from :tname where c1 >= '3';

select * from :tname where c1 >= '3';

explain (costs off) select * from :tname where c0 >= '2' and c1 >= '3';

select * from :tname where c0 >= '2' and c1 >= '3';

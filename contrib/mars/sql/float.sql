--
-- float numbers have many specially designed behavior:
--
-- * NaN is greater than all other values, including Inf;
--
-- mars should follow the behavior.
--

\set float4_min '-3.402823466e+38::float4'
\set float4_max '+3.402823466e+38::float4'
\set float4_nan '''NAN''::float4'
\set float4_inf '''INF''::float4'

\set float8_min '-1.7976931348623158e+308::float8'
\set float8_max '+1.7976931348623158e+308::float8'
\set float8_nan '''NAN''::float8'
\set float8_inf '''INF''::float8'

\set tname test_mars_float_t1

drop table if exists :tname;

create table :tname (f4 float4, f8 float8, seg int default 0, id serial)
 using mars
 distributed by (seg);

insert into :tname
values ( +:float4_inf, +:float4_inf )
     , ( -:float4_inf, -:float4_inf )
     , ( +:float4_nan, +:float4_nan )
     , ( -:float4_nan, -:float4_nan )
;

insert into :tname values ( +:float4_inf, +:float4_inf );
insert into :tname values ( -:float4_inf, -:float4_inf );
insert into :tname values ( +:float4_nan, +:float4_nan );
insert into :tname values ( -:float4_nan, -:float4_nan );

select id, f4, f8 from :tname;

select min(f4) as f4_min, max(f4) as f4_max
     , min(f8) as f8_min, max(f8) as f8_max
  from :tname
;

select id, f4, f8 from :tname where f4 < :float4_inf;
select id, f4, f8 from :tname where f4 < :float4_nan;
select id, f4, f8 from :tname where f4 = :float4_inf;
select id, f4, f8 from :tname where f4 = :float4_nan;
select id, f4, f8 from :tname where f4 > :float4_inf;
select id, f4, f8 from :tname where f4 > :float4_nan;

select id, f4, f8 from :tname where f8 < :float8_inf;
select id, f4, f8 from :tname where f8 < :float8_nan;
select id, f4, f8 from :tname where f8 = :float8_inf;
select id, f4, f8 from :tname where f8 = :float8_nan;
select id, f4, f8 from :tname where f8 > :float8_inf;
select id, f4, f8 from :tname where f8 > :float8_nan;

select id, f4, f8 from :tname where f4 < :float8_inf;
select id, f4, f8 from :tname where f4 < :float8_nan;
select id, f4, f8 from :tname where f4 = :float8_inf;
select id, f4, f8 from :tname where f4 = :float8_nan;
select id, f4, f8 from :tname where f4 > :float8_inf;
select id, f4, f8 from :tname where f4 > :float8_nan;

select id, f4, f8 from :tname where f8 < :float4_inf;
select id, f4, f8 from :tname where f8 < :float4_nan;
select id, f4, f8 from :tname where f8 = :float4_inf;
select id, f4, f8 from :tname where f8 = :float4_nan;
select id, f4, f8 from :tname where f8 > :float4_inf;
select id, f4, f8 from :tname where f8 > :float4_nan;

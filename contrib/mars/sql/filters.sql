--
-- when a scankey is unsupported by mars, mars silently ignores it, the scankey
-- is stilled checked by the scan node, so the result is still correct.
--
-- mars tries its best to optimize the scan with the supported scankeys,
-- however we cannot verify this in the tests.
--

\set tname test_mars_filters_t1

drop table if exists :tname;
create table :tname
     ( i4 int4
     , i8 int8
     , f4 float4
     , f8 float8
     , ts timestamp
     , vc varchar
     , xs numeric
     , kv jsonb)
 using mars
  with ( group_col_ = '{i4, i8}'
       , global_order_col_ = '{i4, i8}'
       , local_order_col_ = '{i8}'
       )
 distributed by (i4)
;

insert into :tname
select i / 4 as i4
     , i % 4 as i8
     , i * 0.1 as f4
     , i * 0.1 as f8
     , '2000-01-01'::timestamp + i::text::interval as ts
     , i as vc
     , i * 0.1 as xs
     , ('{ "key1": ' || i || ', "key2": ' || (i * 0.1) || ' }')::jsonb as kv
  from generate_series(0, 15) i
;

-- below tests may or may not contain filters unsupported by mars, the results
-- should always be correct.

select * from :tname where i4 = 2::int2;
select * from :tname where i4 = 2::int4;
select * from :tname where i4 = 2::int8;
select * from :tname where i4 <> 2;
select * from :tname where i4 > 2.0;
select * from :tname where i4::int4 = 2::int4;
select * from :tname where i4::int8 = 2::int8;
select * from :tname where i4::int8 = 2::int4;
select * from :tname where i4 = 2 or i4 = 0;
select * from :tname where i4 in (2::int8, 0::int8);
select * from :tname where i4 = 2 and (i8 < 1 or i8 > 2);
select * from :tname where i4 = i8;
select * from :tname where i4 + i8 < 4;
select * from :tname where i4 + 1 = 2;
select * from :tname where i4 = 2 and i4 = 2;
select * from :tname where i4 = 2 and true;
select * from :tname where -i4 = -2;
select * from :tname where abs(i4) = 2;
select * from :tname where i4 = abs(2);
select * from :tname where 1 = 1;
select * from :tname where 1 < 2;
select * from :tname where true;
select * from :tname where i4 = '2';
select * from :tname where i4 = ' 2 ';
select * from :tname where i4 = +2;
select * from :tname where i4 = 2 and i8 between 0 and 2;

-- var on right side
select * from :tname where 2 = i4;
select * from :tname where 2 > i4;
select * from :tname where 2 >= i4;

-- null testers
select * from :tname where i4 is not null;
select * from :tname where i4 is null;
select * from :tname where i4 = null;
select * from :tname where i4 <> null;

-- not exprs
select * from :tname where not i4 <> 2;
select * from :tname where not i4 = 2;
select * from :tname where not not i4 = 2;
select * from :tname where not i4 is not null;
select * from :tname where not i4 is null;
select * from :tname where not i4 = null;
select * from :tname where not i4 <> null;

select * from :tname where ts = '2000-01-01'::timestamp;
select * from :tname where ts = '2000-01-01'::timestamp::timestamptz::timestamp;
select * from :tname where ts = '2000-01-01'::date;
select * from :tname where ts < '2000-01-01 00:00:10';
select * from :tname where ts < '2000-01-01'::timestamp + '10s'::interval;
select * from :tname where ts + '10s'::interval < '2000-01-01 00:00:20';

select * from :tname where vc = 2;
select * from :tname where vc = '2';
select * from :tname where vc = ' 2 ';
select * from :tname where vc between '2' and '8';
select * from :tname where vc in ('2', '3', '4', '5');
select * from :tname where vc like '%2';
select * from :tname where vc = '2' || '';
select * from :tname where vc = '' || 2;

select * from :tname where xs = 1;
select * from :tname where xs = 1::int4;
select * from :tname where xs = 1::float4;
select * from :tname where xs = 1::float8;
select * from :tname where xs = 1::numeric;
select * from :tname where xs = 1::numeric(10,2);
select * from :tname where xs > 1.0;
select * from :tname where scale(xs) = 1;

select * from :tname where kv = kv;
select * from :tname where kv = '{"key1":10,"key2":1.0}';
select * from :tname where kv = '{  "key2"  :  1.0  ,  "key1"  :  10  }';
select * from :tname where kv = '{"key1":10,"key2":1.0}'::jsonb;
select * from :tname where kv ? 'key1';
select * from :tname where kv->>'key1' = '10';
select * from :tname where kv->>'key1' = 10::text;
select * from :tname where (kv->>'key1')::int = 10;
select * from :tname where kv->>'key2' = '1.0';
select * from :tname where kv->>'key2' = 1.0::text;
select * from :tname where (kv->>'key2')::float = 1.0;

-- system attributes
select 1 from :tname where gp_segment_id = 0 limit 1;
select 1 from :tname where ctid < '(2,0)' limit 1;

-- agg queries
select count(i4), sum(i4), sum(i8) from :tname where abs(i4) = 2;
select count(i4), sum(i4), sum(i8) from :tname where i4 = abs(2);
select count(i4), sum(i4), sum(i8) from :tname where i4 + i8 < 4;
select count(i4), sum(i4), sum(i8) from :tname where i4 + 1 = 2;
select count(i4), sum(i4), sum(i8) from :tname where i4 is null;
select count(i4), sum(i4), sum(i8) from :tname where i4 <> null;
select count(i4), sum(i4), sum(i8) from :tname where not i4 <> 2;
select count(i4), sum(i4), sum(i8) from :tname where not i4 = 2;
select count(i4), sum(i4), sum(i8) from :tname where not not i4 = 2;
select count(i4), sum(i4), sum(i8) from :tname where scale(xs) = 1;
select count(i4), sum(i4), sum(i8) from :tname where ts < '2000-01-01'::timestamp + '10s'::interval;
select count(i4), sum(i4), sum(i8) from :tname where ts + '10s'::interval < '2000-01-01 00:00:20';
select count(i4), sum(i4), sum(i8) from :tname where vc between '2' and '8';
select count(i4), sum(i4), sum(i8) from :tname where vc like '%2';
select count(i4), sum(i4), sum(i8) from :tname where vc = '2' || '';
select count(i4), sum(i4), sum(i8) from :tname where vc = '' || 2;

-- vi: et :

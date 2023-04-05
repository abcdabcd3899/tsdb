-- start_ignore
create extension if not exists mars;
-- end_ignore

create table t1 ( c1 int
                , c2 bigint
                , c3 float4
                , c4 float8
                , c5 timestamp
                , c6 timestamptz
                )
 using mars
 distributed by (c1)
;

create table t2 ( c1 int
                , c2 bigint
                , c3 float4
                , c4 float8
                , c5 timestamp
                , c6 timestamptz
                )
 using mars
 distributed by (c1)
;

-- verify reading on empty tables
select * from t1;

insert into t1
     select i * 1 as c1
          , i * 2 as c2
          , i * 3 as c3
          , i * 4 as c4
          , '2000-01-01'::timestamp + (i * 5)::text::interval as c5
          , '2000-01-01'::timestamptz + (i * 6)::text::interval as c6
       from generate_series(1, 5) i;

insert into t2
     select i * 1 as c1
          , i * 2 as c2
          , i * 3 as c3
          , i * 4 as c4
          , '2000-01-01'::timestamp + (i * 5)::text::interval as c5
          , '2000-01-01'::timestamptz + (i * 6)::text::interval as c6
       from generate_series(1, 10000) i;

-- select multi times to ensure that the scan is repeatable.
select * from t1;
select * from t1;
select * from t1;
select * from t1;

-- select in a different session should also work.
\connect
select * from t1;
select * from t1;
select * from t1;
select * from t1;

-- select single or partial columns
select c2 from t1;
select c1, c3 from t1;

-- select with filters
select c2 from t1 where c1 = 1;
select c1, c3 from t1 where c1 = 1 and c2 = 2;

-- select from a table with multiple parquet row groups
select sum(c1) as c1
     , sum(c2) as c2
     , sum(c3::float8) as c3  -- convert to float8 to preserve precision
     , sum(c4) as c4
     , max(c5) as c5
     , max(c6) as c6
  from t2;

-- for unsupported exprs, we should fallback to seqscan filters automatically.
select * from t1 where c1 = 1 or c1 = 3;
select * from t1 where (c1, c2) = (1, 2);
select * from t1 where c1 is not null;
select * from t1 where c1 = 1.0;
select * from t1 where abs(c1) = 1;
select * from t1 where c1 in (1.0, 2.0);

-- bfv: we used to throw an error on unsupported operator, for example:
--
--     select * from t1 where c1 <> 1;
--     ERROR:  operator 518 is not a member of opfamily
--
-- fixed by silently ignoring it, it can be handled by the scan node
select * from t1 where c1 <> 1;

-- bfv: we used to throw an error on unsupported scankey type:
--
--     select * from t1 where c1 = 1::int2;
--     ERROR:  mars: type is not supported yet (ScanKey.cc:37)
--
-- fixed by silently ignoring it, it can be handled by the scan node
select * from t1 where c1 = 1::int2;

-- bfv: we do not allow a scankey to have different arg types at the moment,
-- suppose c1 is an int8 column, then "where c1 = 0::int4" would give wrong
-- result (to be accurate, only when c1 has values outside the int4
-- representable range).
--
-- for now we will silently ignore it and let the scan node handle it, however
-- this would cause very bad performance.
select * from t1 where c2 = 2::int4;

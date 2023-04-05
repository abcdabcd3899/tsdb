-- start_ignore
create extension if not exists mars;
-- end_ignore

select gp_debug_set_create_table_default_numsegments(1);
--
-- pushdown of sum() aggs.
--

\set tname test_mars_customscan_sum_t1
\set tmpname test_mars_customscan_sum_t2

\set int4_min 'x''80000000''::int4'
\set int4_max 'x''7fffffff''::int4'

\set int8_min 'x''8000000000000000''::int8'
\set int8_max 'x''7fffffffffffffff''::int8'

\set float4_min '-3.402823466e+38::float4'
\set float4_max '+3.402823466e+38::float4'
\set float4_nan '''NAN''::float4'
\set float4_inf '''INF''::float4'

\set float8_min '-1.7976931348623158e+308::float8'
\set float8_max '+1.7976931348623158e+308::float8'
\set float8_nan '''NAN''::float8'
\set float8_inf '''INF''::float8'

drop table if exists :tname;

-- we dispatch by batch, so the tuples of the same batch are on the same seg,
-- this is important to ensure the per-block sum.
create table :tname (batch int, i4 int4, i8 int8, f4 float4, f8 float8, id serial)
	using mars
	with (group_col_ = "{batch}", group_param_col_ = "{id in 1000}")
	distributed by (batch);

-- a new era
truncate :tname;

-- sum on empty tables should return nothing.
\i sql/sum.template

-- a new era
truncate :tname;

-- batch 0: sum(float4) should overflow because the result type is the same
-- with the arg type.
insert into :tname values
  (0, null, null, :float4_min, null)
, (0, null, null, :float4_min, null)
;

\i sql/sum.template

-- a new era
truncate :tname;

-- batch 0: sum(float4) should overflow because the result type is the same
-- with the arg type.
insert into :tname values
  (0, null, null, :float4_max, null)
, (0, null, null, :float4_max, null)
;

\i sql/sum.template

-- a new era
truncate :tname;

-- batch 0: sum(float8) should overflow because the result type is the same
-- with the arg type.
insert into :tname values
  (0, null, null, null, :float8_min)
, (0, null, null, null, :float8_min)
;

\i sql/sum.template

-- a new era
truncate :tname;

-- batch 0: sum(float8) should overflow because the result type is the same
-- with the arg type.
insert into :tname values
  (0, null, null, null, :float8_max)
, (0, null, null, null, :float8_max)
;

\i sql/sum.template

-- a new era
truncate :tname;

-- now we verify the cases that should succeed, first let's do them via the
-- content scan of the custom scan.
insert into :tname values
-- batch 0: nothing, just a placeholder
\set batch 0
  (:batch, 0, 0, 0, 0)

-- batch 10: (NULL + NULL) -> NULL
\set batch 10
, (:batch, null, null, null, null)
, (:batch, null, null, null, null)

-- batch 11: (NULL + x + y) -> (x + y)
\set batch 11
, (:batch, null, null, null, null)
, (:batch, 1, 2, .3, .4)
, (:batch, 10, 20, .03, .04)

-- batch 20: sum(int4) or sum(int8) won't overflow because the result type is
-- upgraded accordingly.
\set batch 20
, (:batch, :int4_max, :int8_max, null, null)
, (:batch, :int4_max, :int8_max, null, null)

-- batch 30: (NAN + any) -> NAN
\set batch 30
, (:batch, null, null, :float4_nan, :float8_nan)
, (:batch, null, null, 1, 1)

-- batch 31: (NAN + INF) -> NAN
\set batch 31
, (:batch, null, null, :float4_nan, :float8_nan)
, (:batch, null, null, :float4_inf, :float8_inf)

-- batch 32: (-NAN + -NAN) -> NAN
\set batch 32
, (:batch, null, null, -:float4_nan, -:float8_nan)
, (:batch, null, null, -:float4_nan, -:float8_nan)

-- batch 33: (NAN - NAN) -> NAN
\set batch 33
, (:batch, null, null, +:float4_nan, +:float8_nan)
, (:batch, null, null, -:float4_nan, -:float8_nan)

-- batch 40: (INF + value) -> INF
\set batch 40
, (:batch, null, null, :float4_inf, :float8_inf)
, (:batch, null, null, 1, 1)

-- batch 41: (INF + INF) -> INF
\set batch 41
, (:batch, null, null, :float4_inf, :float8_inf)
, (:batch, null, null, :float4_inf, :float8_inf)

-- batch 42: (-INF + -INF) -> -INF
\set batch 42
, (:batch, null, null, -:float4_inf, -:float8_inf)
, (:batch, null, null, -:float4_inf, -:float8_inf)

-- batch 43: (INF - INF) -> NAN
\set batch 43
, (:batch, null, null, +:float4_inf, +:float8_inf)
, (:batch, null, null, -:float4_inf, -:float8_inf)

-- batch 44: (INF + NAN) -> NAN
\set batch 44
, (:batch, null, null, :float4_inf, :float8_inf)
, (:batch, null, null, :float4_nan, :float8_nan)

-- batch 50: sum(value), nothing special
\set batch 50
, (:batch, 1, 2, .3, .4)
, (:batch, 10, 20, .03, .04)

-- end of the insert
;

\i sql/sum.template

-- then let's do them with the block scan of the custom scan, we need to insert
-- each batch separately.
create temp table :tmpname (like :tname) distributed by (batch);
insert into :tmpname select * from :tname order by batch;
truncate :tname;

do language plpgsql $$
    declare
        batch_ int;
    begin
        for batch_ in select distinct batch
                        from test_mars_customscan_sum_t2
                       order by 1
        loop
            insert into test_mars_customscan_sum_t1
                 select *
                   from test_mars_customscan_sum_t2
                  where batch = batch_;
        end loop;
    end;
$$;

\i sql/sum.template

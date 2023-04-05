--
-- pushdown of min(), max() aggs.
--

-- start_ignore
create extension if not exists gp_debug_numsegments;
create extension if not exists mars;
-- end_ignore

\set tname test_mars_customscan_min_max_t1

\set int4_min 'x''80000000''::int4'
\set int4_max 'x''7fffffff''::int4'

\set int8_min 'x''8000000000000000''::int8'
\set int8_max 'x''7fffffffffffffff''::int8'

\set float4_min '1.175494351e-38::float4'
\set float4_max '3.402823466e+38::float4'
\set float4_inf '''INF''::float4'
\set float4_nan '''NAN''::float4'

\set float8_min '2.2250738585072014e-308::float8'
\set float8_max '1.7976931348623158e+308::float8'
\set float8_inf '''INF''::float8'
\set float8_nan '''NAN''::float8'

drop table if exists :tname;

select gp_debug_set_create_table_default_numsegments(1);

create table :tname (batch int, i4 int4, i8 int8, f4 float4, f8 float8, id serial)
 using mars
  with (group_col_ = "{batch}", group_param_col_ = "{id in 1000}")
 distributed by (batch);

select gp_debug_reset_create_table_default_numsegments();

-- a new era
truncate :tname;

-- min/max on empty tables should return nothing.
\i sql/min_max.template

-- a new era
truncate :tname;

-- min(null) or max(null) should return null.
insert into :tname values
  (0, null, null, null, null)
, (0, null, null, null, null)
;

\i sql/min_max.template

-- a new era
truncate :tname;

insert into :tname values
-- nothing, just a placeholder
\set batch 0
  (:batch, 0, 0, 0, 0)

-- only nulls
\set batch 10
, (:batch, null, null, null, null)
, (:batch, null, null, null, null)

-- nulls and normal values
\set batch 20
, (:batch, 2, 2, .2, .3)
, (:batch, null, null, null, null)
, (:batch, 3, 3, .3, .3)
, (:batch, 4, 4, .4, .4)
, (:batch, null, null, null, null)
, (:batch, null, null, null, null)
, (:batch, 5, 5, .5, .5)
, (:batch, 6, 6, .6, .6)

-- normal values
\set batch 30
, (:batch, +1, +1, +.1, +.1)
, (:batch, +2, +2, +.2, +.2)
, (:batch, -1, -1, -.1, -.1)
, (:batch, -2, -2, -.2, -.2)

-- zeros
\set batch 40
, (:batch,  0,  0,  0.,  0.)
, (:batch, +0, +0, +0., +0.)
, (:batch, -0, -0, -0., -0.)

\set batch 41
, (:batch,  0,  0,  0.,  0.)
, (:batch,  0,  0,  0.,  0.)

\set batch 42
, (:batch, +0, +0, +0., +0.)
, (:batch, +0, +0, +0., +0.)

\set batch 43
, (:batch, -0, -0, -0., -0.)
, (:batch, -0, -0, -0., -0.)

-- NAN
\set batch 50
, (:batch, null, null, +:float4_nan, +:float8_nan)
, (:batch, null, null, -:float4_nan, -:float8_nan)

-- INF
\set batch 60
, (:batch, null, null, +:float4_inf, +:float8_inf)
, (:batch, null, null, -:float4_inf, -:float8_inf)

-- min and max values
\set batch 70
, (:batch, :int4_min, :int8_min, :float4_min, :float8_min)
, (:batch, :int4_max, :int8_max, :float4_max, :float8_max)

-- end of insert
;

\i sql/min_max.template

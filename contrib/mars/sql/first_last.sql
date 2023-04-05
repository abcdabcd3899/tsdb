--
-- pushdown of first() and last() aggs.
--

-- start_ignore
create extension if not exists gp_debug_numsegments;
create extension if not exists mars;
create extension if not exists matrixts;
-- end_ignore

\set tname test_mars_customscan_first_last_t1

drop table if exists :tname;

select gp_debug_set_create_table_default_numsegments(1);
create table :tname ( tag int
                    , ts timestamp
                    , i4 int4
                    , i8 int8
                    , f4 float4
                    , f8 float8
                    )
       using mars
        with ( group_col_ = "{tag}"
             , group_param_col_ = "{ts in 1 day}"
             )
 distributed by (tag)
;
select gp_debug_reset_create_table_default_numsegments();

insert into :tname
     select i / 10000 as tag
          , '2000-01-01'::timestamp + (i % 10000)::text::interval as ts
          , i as i4
          , -i as i8
          , i / 10. as f4
          , -i / 10. as f8
       from generate_series(0, 99999) i
;

-- simulate min()/max() with first()/last().

explain (costs off)
select min  (tag     ) as tag_min
     , max  (tag     ) as tag_max
     , first(tag, tag) as tag_first
     , last (tag, tag) as tag_last
     , min  ( ts     ) as ts_min
     , max  ( ts     ) as ts_max
     , first( ts,  ts) as ts_first
     , last ( ts,  ts) as ts_last
     , min  ( i4     ) as i4_min
     , max  ( i4     ) as i4_max
     , first( i4,  i4) as i4_first
     , last ( i4,  i4) as i4_last
     , min  ( i8     ) as i8_min
     , max  ( i8     ) as i8_max
     , first( i8,  i8) as i8_first
     , last ( i8,  i8) as i8_last
     , min  ( f4     ) as f4_min
     , max  ( f4     ) as f4_max
     , first( f4,  f4) as f4_first
     , last ( f4,  f4) as f4_last
     , min  ( f8     ) as f8_min
     , max  ( f8     ) as f8_max
     , first( f8,  f8) as f8_first
     , last ( f8,  f8) as f8_last
  from :tname
;

\x on
select min  (tag     ) as tag_min
     , max  (tag     ) as tag_max
     , first(tag, tag) as tag_first
     , last (tag, tag) as tag_last
     , min  ( ts     ) as ts_min
     , max  ( ts     ) as ts_max
     , first( ts,  ts) as ts_first
     , last ( ts,  ts) as ts_last
     , min  ( i4     ) as i4_min
     , max  ( i4     ) as i4_max
     , first( i4,  i4) as i4_first
     , last ( i4,  i4) as i4_last
     , min  ( i8     ) as i8_min
     , max  ( i8     ) as i8_max
     , first( i8,  i8) as i8_first
     , last ( i8,  i8) as i8_last
     , min  ( f4     ) as f4_min
     , max  ( f4     ) as f4_max
     , first( f4,  f4) as f4_first
     , last ( f4,  f4) as f4_last
     , min  ( f8     ) as f8_min
     , max  ( f8     ) as f8_max
     , first( f8,  f8) as f8_first
     , last ( f8,  f8) as f8_last
  from :tname
;
\x off

\set col ts
\i sql/first_last.template

\set col i4
\i sql/first_last.template

\set col i8
\i sql/first_last.template

\set col f4
\i sql/first_last.template

\set col f8
\i sql/first_last.template

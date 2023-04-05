-- start_ignore
create extension if not exists gp_debug_numsegments;
-- end_ignore

select gp_debug_set_create_table_default_numsegments(1);

\set compresslevel 1
\set nrows 1000
\set fulltype half
\set datatype i4

\set kvtype json
 \set compresstype none \i sql/mxkv/compress.template
 \set compresstype lz4  \i sql/mxkv/compress.template
 \set compresstype zstd \i sql/mxkv/compress.template
\set kvtype jsonb
 \set compresstype none \i sql/mxkv/compress.template
 \set compresstype lz4  \i sql/mxkv/compress.template
 \set compresstype zstd \i sql/mxkv/compress.template
\set kvtype mxkv_text
 \set compresstype none \i sql/mxkv/compress.template
 \set compresstype lz4  \i sql/mxkv/compress.template
 \set compresstype zstd \i sql/mxkv/compress.template
\set kvtype mxkv_int4
 \set compresstype none \i sql/mxkv/compress.template
 \set compresstype lz4  \i sql/mxkv/compress.template
 \set compresstype zstd \i sql/mxkv/compress.template
\set kvtype mxkv_float4
 \set compresstype none \i sql/mxkv/compress.template
 \set compresstype lz4  \i sql/mxkv/compress.template
 \set compresstype zstd \i sql/mxkv/compress.template
\set kvtype mxkv_float8
 \set compresstype none \i sql/mxkv/compress.template
 \set compresstype lz4  \i sql/mxkv/compress.template
 \set compresstype zstd \i sql/mxkv/compress.template
\set kvtype 'mxkv_float4(2)'
 \set compresstype none \i sql/mxkv/compress.template
 \set compresstype lz4  \i sql/mxkv/compress.template
 \set compresstype zstd \i sql/mxkv/compress.template
\set kvtype 'mxkv_float8(2)'
 \set compresstype none \i sql/mxkv/compress.template
 \set compresstype lz4  \i sql/mxkv/compress.template
 \set compresstype zstd \i sql/mxkv/compress.template

-- check the sizes, do not treat them as diffs
-- start_ignore
select relname
     , pg_total_relation_size(oid) as total_bytes
     , pg_size_pretty(pg_total_relation_size(oid)) as total_size
  from pg_class c
 where relname like 'test_mxkv_compress_%'
 order by relname;
-- end_ignore

-- vi: et :

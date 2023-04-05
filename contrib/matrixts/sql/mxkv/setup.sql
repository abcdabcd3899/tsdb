-- mxkv tests can be run after the matrixts tests or separately, so we have to
-- ensure that the extension is loaded.
create extension if not exists matrixts;

\set tname test_mxkv_settings
\set nkeys 10000

-- the mxkv tests requires the mxkv_keys enum to be empty, we used to ensure
-- this by dropping and re-creating the mxkv extension, but now as we are part
-- of the matrixts extension, we should no longer do this, we simply verify it.
select * from pg_enum where enumtypid = 'mxkv_keys'::regtype;

-- generate the full int4 kv records
\out /dev/null
select '''''''{'
    || string_agg(format('"k%s": %s', i, i), ' ,')
    || '}''''''' as full_records_i4
  from generate_series(1, :nkeys - 1) i
;
\out
\gset

-- generate the half int4 kv records
\out /dev/null
select '''''''{'
    || string_agg(format('"k%s": -%s', i, i), ' ,')
    || '}''''''' as half_records_i4
  from generate_series(2, :nkeys - 1, 2) i
;
\out
\gset

-- generate the full float4 kv records
\out /dev/null
select '''''''{'
    || string_agg(format('"k%s": %s', i, i / 100.), ' ,')
    || '}''''''' as full_records_f4
  from generate_series(1, :nkeys - 1) i
;
\out
\gset

-- generate the half float4 kv records
\out /dev/null
select '''''''{'
    || string_agg(format('"k%s": -%s', i, i / 100.), ' ,')
    || '}''''''' as half_records_f4
  from generate_series(2, :nkeys - 1, 2) i
;
\out
\gset

-- generate the full float8 kv records
\out /dev/null
select '''''''{'
    || string_agg(format('"k%s": %s', i, i / 100.), ' ,')
    || '}''''''' as full_records_f8
  from generate_series(1, :nkeys - 1) i
;
\out
\gset

-- generate the half float8 kv records
\out /dev/null
select '''''''{'
    || string_agg(format('"k%s": -%s', i, i / 100.), ' ,')
    || '}''''''' as half_records_f8
  from generate_series(2, :nkeys - 1, 2) i
;
\out
\gset

-- save the settings
drop table if exists :tname;
create table :tname
    as select :nkeys::int as nkeys
            , :full_records_i4::text as full_records_i4
            , :half_records_i4::text as half_records_i4
            , :full_records_f4::text as full_records_f4
            , :half_records_f4::text as half_records_f4
            , :full_records_f8::text as full_records_f8
            , :half_records_f8::text as half_records_f8
         from generate_series(1, 1)
    distributed randomly;

-- load the settings
\out /dev/null
select * from :tname;
\out
\gset

-- import the keys
select mxkv_import_keys(:full_records_i4, false /* dryrun */);

-- vi: et :

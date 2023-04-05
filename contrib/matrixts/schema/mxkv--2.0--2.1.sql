
--
-- MXKV, only enabled in enterprise build
--

create type mxkv_keys as enum ();

--
-- helper function to extract and import unknown keys.
--
-- usage:
--
--      -- list and check which keys will be imported
--      select * from mxkv_import_keys('{"c1": 1, "c2": 2}');
--
--      -- actually import the keys
--      select * from mxkv_import_keys('{"c1": 1, "c2": 2}', false);
--
--      -- import into a non-default keyset
--      select * from mxkv_import_keys('{"c1": 1, "c2": 2}', false, 'keyset1');
--

-- internal function to register one key
create or replace function
matrixts_internal.__mxkv_learn_key(key text, keyset regtype)
returns void as $$
  begin
    execute 'alter type ' || keyset::name || ' add value if not exists ' || quote_literal(key);
  end;
$$ language plpgsql strict execute on master;

-- this form import keys from a json string directly.
--
--      select mxkv_import_keys('{"c1":1, "c2":2}'::json);
--
create or replace function
mxkv_import_keys(input json,
                 dryrun bool default false,
                 keyset regtype default 'mxkv_keys')
returns table(imported_keys text) as $$
  declare
    key         text;
  begin
    -- must lock the table to prevent concurrent updates
    lock table pg_enum in exclusive mode;

    -- list all the unknown keys
    for key in select t1.key from json_object_keys(input) as t1(key)
                where t1.key not in (select enumlabel from pg_enum
                                      where enumtypid = keyset::oid)
    loop
        if not dryrun then
            -- register the key
            perform matrixts_internal.__mxkv_learn_key(key, keyset);
        end if;

        -- show them
        return query select key;
    end loop;
  end;
$$ language plpgsql strict execute on master;

-- this form import keys from a column of a table.
--
--      select mxkv_import_keys('t1', 'kv');
--
-- the second argument "col" should be of type "name", however then we could
-- not call it with mxkv_import_keys('t1','kv') because neither argument
-- matches the arg types directly, we have to either
-- mxkv_import_keys('t1'::regclass,'kv') or mxkv_import_keys('t1','kv'::name),
-- so to make life easier we declare the second arg as "text".
create or replace function
mxkv_import_keys(rel regclass,
                 col text,
                 dryrun bool default false,
                 keyset regtype default 'mxkv_keys')
returns table(imported_keys text) as $$
  declare
    key         text;
  begin
    -- must lock the table to prevent concurrent updates
    lock table pg_enum in exclusive mode;

    -- list all the unknown keys
    for key in execute
        'select t.key from (select distinct jsonb_object_keys(t.' || quote_ident(col) || '::jsonb) as key' ||
        '                   from ' || rel::name || ' as t) as t' ||
        ' where t.key not in (select enumlabel from pg_enum'
        '                      where enumtypid = ' || keyset::oid || ')'
    loop
        if not dryrun then
            -- register the key
            perform matrixts_internal.__mxkv_learn_key(key, keyset);
        end if;

        -- show them
        return query select key;
    end loop;
  end;
$$ language plpgsql strict execute on master;

--
-- mxkv_text is for text and other varlen values, no encoding.
--

create type mxkv_text;

create or replace function mxkv_text_in(cstring, oid, int) returns mxkv_text
as 'MODULE_PATHNAME', 'mxkv_text_in' language c immutable strict;

create or replace function mxkv_out(mxkv_text) returns cstring
as 'MODULE_PATHNAME', 'mxkv_text_out' language c immutable strict;

create type mxkv_text
    ( input = mxkv_text_in
    , output = mxkv_out
    , internallength = variable
    , storage = extended
    );

create or replace function mxkv_text(json) returns mxkv_text
as 'MODULE_PATHNAME', 'mxkv_text_from_json' language c immutable strict;

create cast (json as mxkv_text)
  with function mxkv_text(json)
    as implicit;

create or replace function mxkv_text(jsonb) returns mxkv_text
as 'MODULE_PATHNAME', 'mxkv_text_from_jsonb' language c immutable strict;

create cast (jsonb as mxkv_text)
  with function mxkv_text(jsonb)
    as implicit;

create or replace function mxkv_lookup(mxkv_text, text) returns text
as 'MODULE_PATHNAME', 'mxkv_text_lookup' language c immutable strict;

create operator ->
    ( function = mxkv_lookup
    , leftarg = mxkv_text
    , rightarg = text
    );

create operator ->>
    ( function = mxkv_lookup
    , leftarg = mxkv_text
    , rightarg = text
    );

create or replace function mxkv_exists(mxkv_text, text) returns bool
as 'MODULE_PATHNAME', 'mxkv_text_exists' language c immutable strict;

create operator ?
    ( function = mxkv_exists
    , leftarg = mxkv_text
    , rightarg = text
    );

--
-- mxkv_int4 is for int4 values, delta encoded.
--

create type mxkv_int4;

create or replace function mxkv_int4_in(cstring, oid, int) returns mxkv_int4
as 'MODULE_PATHNAME', 'mxkv_int4_in' language c immutable strict;

create or replace function mxkv_out(mxkv_int4) returns cstring
as 'MODULE_PATHNAME', 'mxkv_int4_out' language c immutable strict;

create type mxkv_int4
    ( input = mxkv_int4_in
    , output = mxkv_out
    , internallength = variable
    , storage = extended
    );

create or replace function mxkv_int4(json) returns mxkv_int4
as 'MODULE_PATHNAME', 'mxkv_int4_from_json' language c immutable strict;

create cast (json as mxkv_int4)
  with function mxkv_int4(json)
    as implicit;

create or replace function mxkv_int4(jsonb) returns mxkv_int4
as 'MODULE_PATHNAME', 'mxkv_int4_from_jsonb' language c immutable strict;

create cast (jsonb as mxkv_int4)
  with function mxkv_int4(jsonb)
    as implicit;

create or replace function mxkv_lookup(mxkv_int4, text) returns int4
as 'MODULE_PATHNAME', 'mxkv_int4_lookup' language c immutable strict;

create operator ->
    ( function = mxkv_lookup
    , leftarg = mxkv_int4
    , rightarg = text
    );

create operator ->>
    ( function = mxkv_lookup
    , leftarg = mxkv_int4
    , rightarg = text
    );

create or replace function mxkv_exists(mxkv_int4, text) returns bool
as 'MODULE_PATHNAME', 'mxkv_text_exists' language c immutable strict;

create operator ?
    ( function = mxkv_exists
    , leftarg = mxkv_int4
    , rightarg = text
    );

--
-- mxkv_float4 is for float4 values, no encoding at present.
--

create type mxkv_float4;

create or replace function mxkv_float4_in(cstring, oid, int) returns mxkv_float4
as 'MODULE_PATHNAME', 'mxkv_float4_in' language c immutable strict;

create or replace function mxkv_out(mxkv_float4) returns cstring
as 'MODULE_PATHNAME', 'mxkv_float4_out' language c immutable strict;

create or replace function mxkv_float4_typmod_in(cstring[]) returns int
as 'MODULE_PATHNAME', 'mxkv_float4_typmod_in' language c immutable strict;

create or replace function mxkv_float4_typmod_out(int) returns cstring
as 'MODULE_PATHNAME', 'mxkv_float4_typmod_out' language c immutable strict;

create type mxkv_float4
    ( input = mxkv_float4_in
    , output = mxkv_out
    , typmod_in = mxkv_float4_typmod_in
    , typmod_out = mxkv_float4_typmod_out
    , internallength = variable
    , storage = extended
    );

create or replace function mxkv_float4(mxkv_float4, int) returns mxkv_float4
as 'MODULE_PATHNAME', 'mxkv_float4_scale' language c immutable strict;

create cast (mxkv_float4 as mxkv_float4)
  with function mxkv_float4(mxkv_float4, int)
    as implicit;

create or replace function mxkv_float4(json) returns mxkv_float4
as 'MODULE_PATHNAME', 'mxkv_float4_from_json' language c immutable strict;

create cast (json as mxkv_float4)
  with function mxkv_float4(json)
    as implicit;

create or replace function mxkv_float4(jsonb) returns mxkv_float4
as 'MODULE_PATHNAME', 'mxkv_float4_from_jsonb' language c immutable strict;

create cast (jsonb as mxkv_float4)
  with function mxkv_float4(jsonb)
    as implicit;

create or replace function mxkv_lookup(mxkv_float4, text) returns float4
as 'MODULE_PATHNAME', 'mxkv_float4_lookup' language c immutable strict;

create operator ->
    ( function = mxkv_lookup
    , leftarg = mxkv_float4
    , rightarg = text
    );

create operator ->>
    ( function = mxkv_lookup
    , leftarg = mxkv_float4
    , rightarg = text
    );

create or replace function mxkv_exists(mxkv_float4, text) returns bool
as 'MODULE_PATHNAME', 'mxkv_text_exists' language c immutable strict;

create operator ?
    ( function = mxkv_exists
    , leftarg = mxkv_float4
    , rightarg = text
    );

--
-- mxkv_float8 is for float8 values, no encoding at present.
--

create type mxkv_float8;

create or replace function mxkv_float8_in(cstring, oid, int) returns mxkv_float8
as 'MODULE_PATHNAME', 'mxkv_float8_in' language c immutable strict;

create or replace function mxkv_out(mxkv_float8) returns cstring
as 'MODULE_PATHNAME', 'mxkv_float8_out' language c immutable strict;

create or replace function mxkv_float8_typmod_in(cstring[]) returns int
as 'MODULE_PATHNAME', 'mxkv_float8_typmod_in' language c immutable strict;

create or replace function mxkv_float8_typmod_out(int) returns cstring
as 'MODULE_PATHNAME', 'mxkv_float8_typmod_out' language c immutable strict;

create type mxkv_float8
    ( input = mxkv_float8_in
    , output = mxkv_out
    , typmod_in = mxkv_float8_typmod_in
    , typmod_out = mxkv_float8_typmod_out
    , internallength = variable
    , storage = extended
    );

create or replace function mxkv_float8(mxkv_float8, int) returns mxkv_float8
as 'MODULE_PATHNAME', 'mxkv_float8_scale' language c immutable strict;

create cast (mxkv_float8 as mxkv_float8)
  with function mxkv_float8(mxkv_float8, int)
    as implicit;

create or replace function mxkv_float8(json) returns mxkv_float8
as 'MODULE_PATHNAME', 'mxkv_float8_from_json' language c immutable strict;

create cast (json as mxkv_float8)
  with function mxkv_float8(json)
    as implicit;

create or replace function mxkv_float8(jsonb) returns mxkv_float8
as 'MODULE_PATHNAME', 'mxkv_float8_from_jsonb' language c immutable strict;

create cast (jsonb as mxkv_float8)
  with function mxkv_float8(jsonb)
    as implicit;

create or replace function mxkv_lookup(mxkv_float8, text) returns float8
as 'MODULE_PATHNAME', 'mxkv_float8_lookup' language c immutable strict;

create operator ->
    ( function = mxkv_lookup
    , leftarg = mxkv_float8
    , rightarg = text
    );

create operator ->>
    ( function = mxkv_lookup
    , leftarg = mxkv_float8
    , rightarg = text
    );

create or replace function mxkv_exists(mxkv_float8, text) returns bool
as 'MODULE_PATHNAME', 'mxkv_text_exists' language c immutable strict;

create operator ?
    ( function = mxkv_exists
    , leftarg = mxkv_float8
    , rightarg = text
    );

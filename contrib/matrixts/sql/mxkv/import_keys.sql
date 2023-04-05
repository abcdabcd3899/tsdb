\set tname test_mxkv_import_keys_t1
\set tnamestr '''' test_mxkv_import_keys_t1 ''''

\set keyset test_mxkv_import_keys_enum1
\set keysetstr '''' :keyset ''''

drop type if exists :keyset cascade;
create type :keyset as enum ('c1', 'c2', 'c3');

-- allow empty object
select mxkv_import_keys('{}');
select mxkv_import_keys('{}', true);
select mxkv_import_keys('{}', true, :keysetstr);

-- allow null input
select mxkv_import_keys(null);

-- list unknown keys in dryrun mode
select mxkv_import_keys('{"c1":1, "c2":2, "c4":4}', true, :keysetstr);
select mxkv_import_keys('{"c1":1, "c2":2, "c4":4}', true, :keysetstr);

-- negative: disallow non-object json string
select mxkv_import_keys('["c1", "c2", "c4"]');
select mxkv_import_keys('"c1"');
select mxkv_import_keys('null');
select mxkv_import_keys('');
select mxkv_import_keys('1');
select mxkv_import_keys(1);

-- actually import the unknown keys
select mxkv_import_keys('{"c1":1, "c2":2, "c4":4}', false, :keysetstr);

-- verify that the keys are really imported in dryrun mode
select mxkv_import_keys('{"c1":1, "c2":2, "c4":4}', true, :keysetstr);

-- allow special characters
select mxkv_import_keys('{"Key with '' and \" quotes.": 1}', false, :keysetstr);

--
-- import from a table can be done with the 2nd form of the function
--

drop table if exists :tname;
create table :tname (id int, kv jsonb) distributed by (id);
insert into :tname select i, '{"c1":1, "c10":10}' from generate_series(1, 2) i;

select mxkv_import_keys(:tnamestr, 'kv', true, :keysetstr);
select mxkv_import_keys(:tnamestr, 'kv', false, :keysetstr);

-- vi: et :

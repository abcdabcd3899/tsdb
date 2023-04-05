--
-- pg_dump should be able to dump mxkv columns as well as the registered key.
--

\set tname test_mxkv_utils_t1

drop table if exists :tname;
create table :tname (tag int, kv mxkv_text) distributed by (tag);

insert into :tname
values (10, '{}')
     , (11, '{    }')
     , (20, '{"k1": 1, "k2": -2, "k3": null}')
     -- below keys with special characters are imported in mxkv/syntax.sql
     , (30, '{ "a\"b''c{}() .,:d\\e\u0001f\r\n\tg": 1 }')
     , (31, '{ "a\"b''c{}() .,:d\\e\u0002f\r\n\tg": 1 }')
;

-- generate a dump
\! pg_dump -d contrib_regression -t test_mxkv_utils_t1 -F p -f /tmp/test_mxkv_utils_dump.sql;

-- verify that the dump can be loaded back to the original db
drop table :tname;
\i /tmp/test_mxkv_utils_dump.sql;
-- some settings are modified by loading the dump, such as search_path, so we
-- reconnect to reset the settings.
\c
select * from :tname order by tag;

-- todo: verify that the dump can be loaded in a new db

-- vi: et :

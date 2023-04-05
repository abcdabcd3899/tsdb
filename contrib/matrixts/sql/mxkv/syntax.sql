\out /dev/null
select * from test_mxkv_settings;
\out
\gset

--
-- mxkv_text allow any type of values
--

-- convert from text, then convert back to text
select '{ "k101": "foobar", "k102": 10, "k103": 12.5 }'::mxkv_text;

-- allow empty records
select '{ }'::mxkv_text;

-- the records are sorted by key order
select '{ "k103": 12.5, "k101": "foobar", "k102": 10 }'::mxkv_text;

-- null value is allowed, however it is not stored
select '{ "k101": null }'::mxkv_text;

-- convert repeatedly should work
select :full_records_i4::mxkv_text::text::mxkv_text->'k9876';

-- negative: only allow registered keys
select '{ "no such key": 100 }'::mxkv_text;

-- negative: do not allow duplicate keys
select '{ "k101": 1, "k101": 2 }'::mxkv_text;
select '{ "k101": 1, "k101": 1 }'::mxkv_text;
select '{ "k101": null, "k101": 1 }'::mxkv_text;
select '{ "k101": 1, "k101": null }'::mxkv_text;
select '{ "k101": null, "k101": null }'::mxkv_text;

-- negative: do not allow nesting
select '{ "k101: { "k102": 2 } }'::mxkv_text;
select '{ "k101: [ 1, 2, 3 ] }'::mxkv_text;

-- negative: must be a json object
select '10'::mxkv_text;
select '"abc"'::mxkv_text;
select '[1, 2, 3]'::mxkv_text;
select 'null'::mxkv_text;

--
-- mxkv_int4 allow int4 values
--

-- convert from text, then convert back to text
select '{ "k101": -1, "k102": 10, "k103": 100 }'::mxkv_int4;

-- allow empty records
select '{ }'::mxkv_int4;

-- allow anything that can be fully converted to an integer
select '{ "k101": "10" }'::mxkv_int4;
select '{ "k101": "  10  " }'::mxkv_int4;

-- convert repeatedly should work
select :full_records_i4::mxkv_int4::text::mxkv_int4->'k9876';

-- null value is allowed, however it is not stored
select '{ "k101": null }'::mxkv_int4;

-- negative: only allow int4 values
select '{ "k101": 10.2 }'::mxkv_int4;
select '{ "k101": "" }'::mxkv_int4;
select '{ "k101": "10 abc" }'::mxkv_int4;
select '{ "k101": "abc" }'::mxkv_int4;

-- negative: do not allow scientific notation
select '{ "k101": 10e3 }'::mxkv_int4;

--
-- mxkv_float4 allow float4 values
--

-- convert from text, then convert back to text
select '{ "k101": -1.1, "k102": 10.2, "k103": 100.00003 }'::mxkv_float4;

-- allow empty records
select '{ }'::mxkv_float4;

-- allow anything that can be fully converted to an integer
select '{ "k101": "10.101" }'::mxkv_float4;
select '{ "k101": "  10.101  " }'::mxkv_float4;

-- null value is allowed, however it is not stored
select '{ "k101": null }'::mxkv_float4;

-- integer values are also allowed
select '{ "k101": 123 }'::mxkv_float4;

-- allow scientific notation
select '{ "k101": 10e3 }'::mxkv_float4;
select '{ "k101": 10e-3 }'::mxkv_float4;

-- convert repeatedly should work
select :full_records_i4::mxkv_float4::text::mxkv_float4->'k9876';

-- negative: only allow int/float values
select '{ "k101": "" }'::mxkv_float4;
select '{ "k101": "10 abc" }'::mxkv_float4;
select '{ "k101": "abc" }'::mxkv_float4;

--
-- mxkv_float8 allow float8 values
--

-- convert from text, then convert back to text
select '{ "k101": -1.1, "k102": 10.2, "k103": 100.00003 }'::mxkv_float8;

-- allow empty records
select '{ }'::mxkv_float8;

-- allow anything that can be fully converted to an integer
select '{ "k101": "10.101" }'::mxkv_float8;
select '{ "k101": "  10.101  " }'::mxkv_float8;

-- null value is allowed, however it is not stored
select '{ "k101": null }'::mxkv_float8;

-- integer values are also allowed
select '{ "k101": 123 }'::mxkv_float8;

-- allow scientific notation
select '{ "k101": 10e3 }'::mxkv_float8;
select '{ "k101": 10e-3 }'::mxkv_float8;

-- convert repeatedly should work
select :full_records_i4::mxkv_float8::text::mxkv_float8->'k9876';

-- negative: only allow int/float values
select '{ "k101": "" }'::mxkv_float8;
select '{ "k101": "10 abc" }'::mxkv_float8;
select '{ "k101": "abc" }'::mxkv_float8;

--
-- mxkv_float4(scale) allow scaled float4 values
--

-- convert from text, then convert back to text
select '{ "k101": -1.1, "k102": 10.2, "k103": 100.00003 }'::mxkv_float4(5);

-- allow empty records
select '{ }'::mxkv_float4(2);

-- allow anything that can be fully converted to an integer
select '{ "k101": "10.101" }'::mxkv_float4(3);
select '{ "k101": "  10.101  " }'::mxkv_float4(3);

-- null value is allowed, however it is not stored
select '{ "k101": null }'::mxkv_float4(3);

-- integer values are also allowed
select '{ "k101": 123 }'::mxkv_float4(3);

-- allow scientific notation
select '{ "k101": 10e3 }'::mxkv_float4(3);
select '{ "k101": 10e-3 }'::mxkv_float4(3);

-- convert repeatedly should work
select :full_records_i4::mxkv_float4::text::mxkv_float4(3)->'k9876';

-- negative: only allow int/float values
select '{ "k101": "" }'::mxkv_float4(3);
select '{ "k101": "10 abc" }'::mxkv_float4(3);
select '{ "k101": "abc" }'::mxkv_float4(3);

-- convert between different scales should work
select '{ "k101": 12.34, "k102": -56.78 }'::mxkv_float4(2)::mxkv_float4::mxkv_float4(4)::mxkv_float4(3);

-- auto round to the specific scale
select '{ "k101": 12.34 }'::mxkv_float4(1);

-- min & max at different scales
-- note: the real range for mxkv_float4(0) is [-2147483648,2147483647], however
-- the input process is mxkv_in(typmod=-1) -> mxkv_scale(typmod), the precision
-- is lost during the first step, the range becomes
-- [-2.1474836e+09,2.1474836e+09], it is out of range in the step, so in
-- practice the valid range is [-2.1474835e+09,2.1474835e+09].
select '{ "k1": -2147483500, "k2": 2147483500 }'::mxkv_float4(0);
select '{ "k1": -214748350.0, "k2": 214748350.0 }'::mxkv_float4(1);
select '{ "k1": -21474835.00, "k2": 21474835.00 }'::mxkv_float4(2);
select '{ "k1": -2147483.500, "k2": 2147483.500 }'::mxkv_float4(3);
select '{ "k1": -214748.3500, "k2": 214748.3500 }'::mxkv_float4(4);
select '{ "k1": -21474.83500, "k2": 21474.83500 }'::mxkv_float4(5);
select '{ "k1": -2147.483500, "k2": 2147.483500 }'::mxkv_float4(6);
select '{ "k1": -214.7483500, "k2": 214.7483500 }'::mxkv_float4(7);
select '{ "k1": -21.47483500, "k2": 21.47483500 }'::mxkv_float4(8);
select '{ "k1": -2.147483500, "k2": 2.147483500 }'::mxkv_float4(9);
select '{ "k1": -0.2147483500, "k2": 0.2147483500 }'::mxkv_float4(10);

-- allow storing integers with scale 0
select '{ "k101": 12 }'::mxkv_float4(0);

-- trim ending zeros
select '{ "k101": 12.00 }'::mxkv_float4(0);
select '{ "k101": 12.300 }'::mxkv_float4(1);

-- a value can be stored with higher scales
select '{ "k101": 12.034 }'::mxkv_float4(3);
select '{ "k101": 12.034 }'::mxkv_float4(4);
select '{ "k101": 12.034 }'::mxkv_float4(5);

select :full_records_i4::mxkv_float4(0)->'k100';
select :half_records_i4::mxkv_float4(0)->'k100';
select :full_records_f4::mxkv_float4(2)->'k100';
select :half_records_f4::mxkv_float4(2)->'k100';

--
-- mxkv_float8(scale) allow scaled float8 values
--

-- convert from text, then convert back to text
select '{ "k101": -1.1, "k102": 10.2, "k103": 100.00003 }'::mxkv_float8(5);

-- allow empty records
select '{ }'::mxkv_float8(2);

-- allow anything that can be fully converted to an integer
select '{ "k101": "10.101" }'::mxkv_float8(3);
select '{ "k101": "  10.101  " }'::mxkv_float8(3);

-- null value is allowed, however it is not stored
select '{ "k101": null }'::mxkv_float8(3);

-- integer values are also allowed
select '{ "k101": 123 }'::mxkv_float8(3);

-- allow scientific notation
select '{ "k101": 10e3 }'::mxkv_float8(3);
select '{ "k101": 10e-3 }'::mxkv_float8(3);

-- convert repeatedly should work
select :full_records_i4::mxkv_float8::text::mxkv_float8(3)->'k9876';

-- negative: only allow int/float values
select '{ "k101": "" }'::mxkv_float8(3);
select '{ "k101": "10 abc" }'::mxkv_float8(3);
select '{ "k101": "abc" }'::mxkv_float8(3);

-- convert between different scales should work
select '{ "k101": 12.34, "k102": -56.78 }'::mxkv_float8(2)::mxkv_float8::mxkv_float8(4)::mxkv_float8(3);

-- auto round to the specific scale
select '{ "k101": 12.34 }'::mxkv_float8(1);

-- min & max at different scales
-- note: the real range for mxkv_float8(0) is
-- [-9223372036854775808,9223372036854775807], however the input process is
-- mxkv_in(typmod=-1) -> mxkv_scale(typmod), the precision is lost during the
-- first step, the range becomes
-- [-9.223372036854776e+18,9.223372036854776e+18], it is out of range in the
-- step, so in practice the valid range is
-- [-9.223372036854774e+18,9.223372036854774e+18].
select '{ "k1": -9223372036854774000, "k2": 9223372036854774000 }'::mxkv_float8(0);
select '{ "k1": -922337203685477400.0, "k2": 922337203685477400.0 }'::mxkv_float8(1);
select '{ "k1": -92233720368547740.00, "k2": 92233720368547740.00 }'::mxkv_float8(2);
select '{ "k1": -9223372036854774.000, "k2": 9223372036854774.000 }'::mxkv_float8(3);
select '{ "k1": -922337203685477.4000, "k2": 922337203685477.4000 }'::mxkv_float8(4);
select '{ "k1": -92233720368547.74000, "k2": 92233720368547.74000 }'::mxkv_float8(5);
select '{ "k1": -9223372036854.774000, "k2": 9223372036854.774000 }'::mxkv_float8(6);
select '{ "k1": -922337203685.4774000, "k2": 922337203685.4774000 }'::mxkv_float8(7);
select '{ "k1": -92233720368.54774000, "k2": 92233720368.54774000 }'::mxkv_float8(8);
select '{ "k1": -9223372036.854774000, "k2": 9223372036.854774000 }'::mxkv_float8(9);
select '{ "k1": -922337203.6854774000, "k2": 922337203.6854774000 }'::mxkv_float8(10);
select '{ "k1": -92233720.36854774000, "k2": 92233720.36854774000 }'::mxkv_float8(11);
select '{ "k1": -9223372.036854774000, "k2": 9223372.036854774000 }'::mxkv_float8(12);
select '{ "k1": -922337.2036854774000, "k2": 922337.2036854774000 }'::mxkv_float8(13);
select '{ "k1": -92233.72036854774000, "k2": 92233.72036854774000 }'::mxkv_float8(14);
select '{ "k1": -9223.372036854774000, "k2": 9223.372036854774000 }'::mxkv_float8(15);
select '{ "k1": -922.3372036854774000, "k2": 922.3372036854774000 }'::mxkv_float8(16);
select '{ "k1": -92.23372036854774000, "k2": 92.23372036854774000 }'::mxkv_float8(17);
select '{ "k1": -9.223372036854774000, "k2": 9.223372036854774000 }'::mxkv_float8(18);
select '{ "k1": -0.9223372036854774000, "k2": 0.9223372036854774000 }'::mxkv_float8(19);

-- allow storing integers with scale 0
select '{ "k101": 12 }'::mxkv_float8(0);

-- trim ending zeros
select '{ "k101": 12.00 }'::mxkv_float8(0);
select '{ "k101": 12.300 }'::mxkv_float8(1);

-- a value can be stored with higher scales
select '{ "k101": 12.034 }'::mxkv_float8(3);
select '{ "k101": 12.034 }'::mxkv_float8(4);
select '{ "k101": 12.034 }'::mxkv_float8(5);

select :full_records_i4::mxkv_float8(0)->'k100';
select :half_records_i4::mxkv_float8(0)->'k100';
select :full_records_f4::mxkv_float8(2)->'k100';
select :half_records_f4::mxkv_float8(2)->'k100';

--
-- some json like operators are supported, and are more effective
--

select :full_records_i4::mxkv_int4->'k100';
select :full_records_i4::mxkv_int4->>'k100';

-- "->" looks up by key, but unlike json, we return text for mxkv_text
with t1 as (select :full_records_i4::mxkv_text as kv)
  select kv->'k100' as k100, pg_typeof(kv->'k100') as type from t1;
-- or int4 for mxkv_int4
with t1 as (select :full_records_i4::mxkv_int4 as kv)
  select kv->'k100' as k100, pg_typeof(kv->'k100') as type from t1;
-- or float4 for mxkv_float4
with t1 as (select :full_records_f4::mxkv_float4 as kv)
  select kv->'k100' as k100, pg_typeof(kv->'k100') as type from t1;
-- or float8 for mxkv_float8
with t1 as (select :full_records_f8::mxkv_float8 as kv)
  select kv->'k100' as k100, pg_typeof(kv->'k100') as type from t1;

-- "->>" looks up by key, but unlike json, we return text for mxkv_text
with t1 as (select :full_records_i4::mxkv_text as kv)
  select kv->>'k100' as k100, pg_typeof(kv->>'k100') as type from t1;
-- or int4 for mxkv_int4
with t1 as (select :full_records_i4::mxkv_int4 as kv)
  select kv->>'k100' as k100, pg_typeof(kv->>'k100') as type from t1;
-- or float4 for mxkv_float4
with t1 as (select :full_records_f4::mxkv_float4 as kv)
  select kv->>'k100' as k100, pg_typeof(kv->>'k100') as type from t1;
-- or float8 for mxkv_float8
with t1 as (select :full_records_f8::mxkv_float8 as kv)
  select kv->>'k100' as k100, pg_typeof(kv->>'k100') as type from t1;

-- "?" tests the existence of the key, only jsons supports it
with t1 as (select :half_records_i4::mxkv_text as kv)
  select kv->'k100' from t1 where kv ? 'k300';
with t1 as (select :half_records_i4::mxkv_int4 as kv)
  select kv->'k100' from t1 where kv ? 'k300';
with t1 as (select :half_records_f4::mxkv_float4 as kv)
  select kv->'k100' from t1 where kv ? 'k300';
with t1 as (select :half_records_f8::mxkv_float8 as kv)
  select kv->'k100' from t1 where kv ? 'k300';
-- 'k301' does not exists in :half_records_{i4,f8}
with t1 as (select :half_records_i4::mxkv_text as kv)
  select kv->'k100' from t1 where kv ? 'k301';
with t1 as (select :half_records_i4::mxkv_int4 as kv)
  select kv->'k100' from t1 where kv ? 'k301';
with t1 as (select :half_records_f4::mxkv_float4 as kv)
  select kv->'k100' from t1 where kv ? 'k301';
with t1 as (select :half_records_f8::mxkv_float8 as kv)
  select kv->'k100' from t1 where kv ? 'k301';

-- this behaves similar with "?", but not fully the same
with t1 as (select :half_records_i4::mxkv_text as kv)
  select kv->'k100' from t1 where kv->'k300' is not null;
with t1 as (select :half_records_i4::mxkv_int4 as kv)
  select kv->'k100' from t1 where kv->'k300' is not null;
with t1 as (select :half_records_f4::mxkv_float4 as kv)
  select kv->'k100' from t1 where kv->'k300' is not null;
with t1 as (select :half_records_f8::mxkv_float8 as kv)
  select kv->'k100' from t1 where kv->'k300' is not null;
-- 'k301' does not exists in :half_records_{i4,f8}
with t1 as (select :half_records_i4::mxkv_text as kv)
  select kv->'k100' from t1 where kv->'k301' is not null;
with t1 as (select :half_records_i4::mxkv_int4 as kv)
  select kv->'k100' from t1 where kv->'k301' is not null;
with t1 as (select :half_records_f4::mxkv_float4 as kv)
  select kv->'k100' from t1 where kv->'k301' is not null;
with t1 as (select :half_records_f8::mxkv_float8 as kv)
  select kv->'k100' from t1 where kv->'k301' is not null;

-- filter by value is also possible, but note the type
with t1 as (select :full_records_i4::mxkv_text as kv)
  select kv->'k100' from t1 where kv->'k300' = '300';
with t1 as (select :full_records_i4::mxkv_text as kv)
  select kv->'k100' from t1 where (kv->'k300')::int = 300;

-- filter by value is easier with typed mxkv variants
with t1 as (select :full_records_i4::mxkv_int4 as kv)
  select kv->'k100' from t1 where kv->'k300' = 300;
with t1 as (select :full_records_f4::mxkv_float4 as kv)
  select kv->'k100' from t1 where kv->'k300' = 3.00;
with t1 as (select :full_records_f8::mxkv_float8 as kv)
  select kv->'k100' from t1 where kv->'k300' = 3.00;

-- you can do anything on the values
with t1 as (select :full_records_i4::mxkv_int4 as kv)
  select min(kv->'k100'), max(kv->'k200'), sum(kv->'k300') from t1;
with t1 as (select :full_records_f4::mxkv_float4 as kv)
  select min(kv->'k100'), max(kv->'k200'), sum(kv->'k300') from t1;
with t1 as (select :full_records_f8::mxkv_float8 as kv)
  select min(kv->'k100'), max(kv->'k200'), sum(kv->'k300') from t1;

-- the operators allow non-registered keys
select '{ "k1": 1 }'::mxkv_int4->'no such key';
select '{ "k1": 1 }'::mxkv_int4->>'no such key';
select '{ "k1": 1 }'::mxkv_int4 ? 'no such key';

-- the operators work on empty mxkv
select '{}'::mxkv_text->'k1';
select '{}'::mxkv_int4->'k1';
select '{}'::mxkv_float4->'k1';
select '{}'::mxkv_float8->'k1';
select '{}'::mxkv_text->>'k1';
select '{}'::mxkv_int4->>'k1';
select '{}'::mxkv_float4->>'k1';
select '{}'::mxkv_float8->>'k1';
select '{}'::mxkv_text ? 'k1';
select '{}'::mxkv_int4 ? 'k1';
select '{}'::mxkv_float4 ? 'k1';
select '{}'::mxkv_float8 ? 'k1';

-- the operators also allow empty key
select '{ "k1": 1 }'::mxkv_text->'';
select '{ "k1": 1 }'::mxkv_int4->'';
select '{ "k1": 1 }'::mxkv_float4->'';
select '{ "k1": 1 }'::mxkv_float8->'';
select '{ "k1": 1 }'::mxkv_text->>'';
select '{ "k1": 1 }'::mxkv_int4->>'';
select '{ "k1": 1 }'::mxkv_float4->>'';
select '{ "k1": 1 }'::mxkv_float8->>'';
select '{ "k1": 1 }'::mxkv_text ? '';
select '{ "k1": 1 }'::mxkv_int4 ? '';
select '{ "k1": 1 }'::mxkv_float4 ? '';
select '{ "k1": 1 }'::mxkv_float8 ? '';

--
-- type casting
--

select :full_records_i4::text::mxkv_text->'k9000';
select :full_records_i4::text::mxkv_int4->'k9000';
select :full_records_i4::text::mxkv_float4->'k9000';
select :full_records_i4::text::mxkv_float8->'k9000';

select :full_records_i4::json::mxkv_text->'k9000';
select :full_records_i4::json::mxkv_int4->'k9000';
select :full_records_i4::json::mxkv_float4->'k9000';
select :full_records_i4::json::mxkv_float8->'k9000';

select :full_records_i4::jsonb::mxkv_text->'k9000';
select :full_records_i4::jsonb::mxkv_int4->'k9000';
select :full_records_i4::jsonb::mxkv_float4->'k9000';
select :full_records_i4::jsonb::mxkv_float8->'k9000';

--
-- special characters handling
--

-- the hint for unregistered keys should be quoted
select '{ "a\"b''c{}() .,:d\\e\u0001f\r\n\tg": 1 }'::mxkv_int4;
select '{ "a\"b''c{}() .,:d\\e\u0002f\r\n\tg": 1 }'::mxkv_int4;

-- import keys with special characters
select mxkv_import_keys('{ "a\"b''c{}() .,:d\\e\u0001f\r\n\tg": 1 }'::json, false);
-- alter type should also work
alter type mxkv_keys add value E'a\"b''c{}() .,:d\\e\x02f\r\n\tg';

-- double check the keys are successfully imported
select '{ "a\"b''c{}() .,:d\\e\u0001f\r\n\tg": 1 }'::mxkv_int4;
select '{ "a\"b''c{}() .,:d\\e\u0002f\r\n\tg": 1 }'::mxkv_int4;

-- convert keys with special characters to mxkv and back to text
select '{ "a\"b''c{}() .,:d\\e\u0001f\r\n\tg": 1 }'::mxkv_int4;
-- and again
select '{ "a\"b''c{}() .,:d\\e\u0001f\r\n\tg": 1 }'::mxkv_int4::text::mxkv_int4;

-- lookup with keys with special characters
select '{ "a\"b''c{}() .,:d\\e\u0001f\r\n\tg": 1 }'::mxkv_int4 ? E'a\"b''c{}() .,:d\\e\x01f\r\n\tg';
select '{ "a\"b''c{}() .,:d\\e\u0001f\r\n\tg": 1 }'::mxkv_int4->>E'a\"b''c{}() .,:d\\e\x01f\r\n\tg';

-- values with special characters
select '{ "k1": "a\"b''c{}() .,:d\\e\u0001f\r\n\tg" }'::mxkv_text;

-- the looked up value should be quoted as text
select '{ "k1": "a\"b''c{}() .,:d\\e\u0001f\r\n\tg" }'::mxkv_text->>'k1';
-- -> behaves the same with ->>, this is different with json/jsonb
select '{ "k1": "a\"b''c{}() .,:d\\e\u0001f\r\n\tg" }'::mxkv_text->'k1';

-- vi: et :

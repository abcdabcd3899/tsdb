The MatriX database Key-Value type, aka MXKV or mxkv, is an optimized type to
store large scale of key-value records.  Compared with the existing key-value
types, json, jsonb, and hstore, mxkv is better in compression ratio, loading
and looking up performance.

Installation
============

MXKV is part of the matrixts extension, but it is only enabled in enterprise
build, so make sure to configure with `--enable-enterprise`.

Note that `matrixts.so` is in the preload list by default, so every time you
rebuild `matrixts.so` you need to restart the cluster.

Usage
=====

MXKV can be used via some types and operators, they are registered by loading
the matrixts extension, again, MXKV is only enabled in enterprise build.

    -- create the extension
    create extension matrixts;

    -- verify that mxkv is enabled
    select '{}'::mxkv_text;

Note that MXKV is introduced since matrixts-2.1, so when matrixts-2.0 is being
used, update to 2.1 to use MXKV.

    -- suppose we are using matrixts-2.0
    create extension matrixts with version '2.0';

    -- update to 2.1 explicitly
    alter extension matrixts update to '2.1';

    -- verify that mxkv is enabled
    select '{}'::mxkv_text;

Key importing
-------------

In the previous section we verify the mxkv feature with an empty KV set, if you
try with a non-empty KV set, you are likely to encounter below error:

    select '{"a": 1, "b": 2}'::mxkv_text;
    psql: ERROR:  unknown key "a"
    LINE 1: select '{"a": 1, "b": 2}'::mxkv_text;
                   ^
    DETAIL:  The key is not imported yet
    HINT:  Import the keys with the mxkv_import_keys() function

This is because you haven't import the keys yet.  MXKV stores keys separately,
this is important to make it small and fast.  Keys can be imported as below:

    -- import the unknown keys
    select mxkv_import_keys('{"a": 1, "b": 2}');
     mxkv_import_keys
    ------------------
     a
     b
    (2 rows)

    -- it won't re-import known keys
    select mxkv_import_keys('{"a": 1, "b": 2}');
     mxkv_import_keys
    ------------------
    (0 rows)

    -- list the keys to be imported, but do not actually import,
    -- the second argument 'true' turns on dryrun mode.
    select mxkv_import_keys('{"c": 1, "d": 2}', true);
    -----------------
     c
     d
    (2 rows)

Now the examples should succeed:

    select '{"a": 1, "b": 2}'::mxkv_text;
          mxkv_text
    ----------------------
     {"a": "1", "b": "2"}
    (1 row)

The `mxkv_import_keys()` function has below arguments:

- `input json`: the input in json format, it must be a json object, only the
  keys are imported; so far we do not support importing a json array, which
  is useful and might be supported in the future;
- `dryrun bool default false`: an optional argument, when it is true the
  function only lists the keys to be imported, but will not actually import
  them, this gives the user a chance to examine whether they are importing the
  right keys; we provide this dry run mode because there is no way to retire an
  imported key, so we want to be careful;
- `keyset regtype default 'mxkv_keys'::regtype`: an optional argument, MXKV
  manages keys via the postgres enum type, this argument specifies the enum
  type to import the keys, the default is `mxkv_keys`, which is used by all the
  pre-defined MXKV types;

A commonly seen scenario is that the user already has a table and stores
key-value data in a json column, so to import the existing keys he/she/it
might do it like below:

    -- this does not work
    select mxkv_import_keys(kv_json, false) from t2;

Unfortunately this won't work, the function is executed on segments, however
importing keys to an enum type must be done on the master.

To support this we have a second form of the function:

    select mxkv_import_keys('t2'::regclass, 'kv_json');

This form has below arguments:

- `rel regclass`: the relation name or oid, it is very convenient to use the
  `::regclass` helper;
- `col text`: the column name that contains the key-value data, it must be a
  text, json or jsonb column;
- `dryrun bool default false`: the same with the first form;
- `keyset regtype default 'mxkv_keys'::regtype`: the same with the first form;

A reminder is that this two forms might not always be selected automatically,
in such a case an explicit type casting is needed, for example:

	-- this does not work
	select mxkv_import_keys('t2', 'kv_json');
	psql: ERROR:  function mxkv_import_keys(unknown, unknown) is not unique
	LINE 1: select mxkv_import_keys('t2', 'kv_json');
				   ^
	HINT:  Could not choose a best candidate function. You might need to add explicit type casts.

	-- either of below works
    select mxkv_import_keys('t2'::regclass, 'kv_json');
    select mxkv_import_keys('t2', 'kv_json'::name);

MXKV types
----------

MXKV requires a key-value set to have the same type of all the values, this
type is also called the key-value type, or KV type.  Many MXKV types are
provided for each kind of KV type, at present we have:

- `mxkv_int4` for `int4` / `int` 32-bit integer values
- `mxkv_float4` for `float4` / `real` 32-bit float values
- `mxkv_float8` for `float8` / `float` / `double precision` 64-bit float values
- `mxkv_text` for `text` values or values of mixed types

Whenever possible, pick the mxkv type from the top to the bottom of the above
list for your key-value set, `mxkv_int4` usually has better performance and
compression ratio than `mxkv_text`.

    -- the KV type can be int, float or text, so pick mxkv_int4.
    select '{"a": 1, "b": 2}'::mxkv_int4;

    -- the KV type can be float or text, so pick mxkv_float4.
    select '{"a": 1.2, "b": 2}'::mxkv_float4;

    -- the KV type can only be text, so pick mxkv_text.
    select '{"a": "hello", "b": 2}'::mxkv_text;

If your key-value records have mixed KV types, it is strongly recommended to
group them by KV types, and store in different columns.

    create table t1 ( tag int, ts timestamp
                    , kv1 mxkv_int4
                    , kv2 mxkv_float4
                    , kv3 mxkv_text
                    );

All the MXKV types can be converted from text, in the json object syntax, and
they can be converted back to text.

    select '{"a": 1, "b": 2}'::mxkv_text::text::mxkv_int4;

Scaled float types
------------------

In practice the float values usually have a fixed-length of digits after the
point, for example, the body temperature is usually something like 36.3, 37.0,
with only one digit after the point, in MXKV we can store them in the scaled
float types, such as `mxkv_float4(1)`, which indicates that there is only one
digit after the point.

- `mxkv_float4(scale)`, where `scale >= 0`, the value range at different scale:
  - `mxkv_float4(0)`: [-2147483500, 2147483500]
  - `mxkv_float4(2)`: [-21474835.00, 21474835.00]
  - `mxkv_float4(4)`: [-214748.3500, 214748.3500]
  - etc.
- `mxkv_float8(scale)`, where `scale >= 0`, the value range at different scale:
  - `mxkv_float8(0)`: [-9223372036854775000, 9223372036854775000]
  - `mxkv_float4(2)`: [-92233720368547750.00, 92233720368547750.00]
  - `mxkv_float8(4)`: [-922337203685477.5000, 922337203685477.5000]
  - etc.

The scaled float types usually have better compression ratio and looking up
performance, so use them instead of non-scaled float types whenever possible.

MXKV operators
--------------

MXKV also provides some json/jsonb like operators to access the key-value data
more efficiently.

`->` and `->>` are json/jsonb operators to lookup the value of a specific key,
MXKV also supports them, but has different return types with json/jsonb, for
example:

    -- this returns 1 of json type
    select '{"a": 1, "b": 2}'::json->'a';
    -- this returns 1 of text type
    select '{"a": 1, "b": 2}'::json->>'a';

    -- both return 1 of int4 type
    select '{"a": 1, "b": 2}'::mxkv_int4->'a';
    select '{"a": 1, "b": 2}'::mxkv_int4->>'a';

    -- both return 1 of text type
    select '{"a": 1, "b": 2}'::mxkv_text->'a';
    select '{"a": 1, "b": 2}'::mxkv_text->>'a';

When there is no match both the operators return `NULL`.

`?` is a jsonb operator to test whether a specific key exists in the key-value
set or not, MXKV also supports it, for example:

    -- this returns true
    select '{"a": 1, "b": 2}'::jsonb ? 'a';
    -- this returns false
    select '{"a": 1, "b": 2}'::jsonb ? 'c';

    -- mxkv types behave exactly the same as jsonb
    select '{"a": 1, "b": 2}'::mxkv_int4 ? 'a';
    select '{"a": 1, "b": 2}'::mxkv_text ? 'a';
    select '{"a": 1, "b": 2}'::mxkv_int4 ? 'c';
    select '{"a": 1, "b": 2}'::mxkv_text ? 'c';

MXKV key mapping
================

MXKV translates key names to key ids during the parsing of the json format
input string, the keys ids are stored as integers.  When output the keys, they
are converted back to key names.  This is called the key mapping.

Key mapping are done via the postgres enum type, an enum type `mxkv_keys` are
defined for this purpose, bi-direction translation can be done efficiently via
the indices and syscaches on the underlying `pg_enum` catalog table.

`pg_enum` records an oid for each key name, we use this oid as the key id.

MXKV encoding
=============

Values can be encoded to use less bytes, and encoded values are usually easier
to compress.

At the moment only intergers, such as the keys and the int4 values, are
encoded, in the future we might also support encoding of float values.

We use delta+varlen encoding for integers, that means we first calculate the
delta of 2 neighbour integers, and store the delta as a variable-length value.

For example, for values [12, 12, 15]:

- the first value 12 is always stored directly, and we store it in native
  byte-order, so it is little-endian on PC, so the first 4 bytes are `0x0c`,
  `0x00`, `0x00`, `0x00`;
- the second value is also 12, the delta to the previous value is `12-12=0`,
  in our varlen schema 0 is stored in only one bit `b0`;
- the third value is 15, the delta to the previous value is `15-12=3`, in our
  varlen schema 3 is stored with a leader code of two bits `b10`, then six bits
  for the body `b000011`;
- so totally we need 4 bytes and 1+8=9 bits to store the 3 integers, after
  a padding that is 6 bytes in all;

An optimization is that the first value can be stored as a delta to 0, so the
first 12 can be stored as a leader code of 2 bits `b10` and a body of 6 bits
`b001100`, in such a case only 8+1+8=17 bits, padded to 3 bytes are needed to
store the 3 values.

For different KV types we use different varlen schemas.

int4 varlen schema
------------------

- value=0: 1 bit leader code `b0`, no body;
- value in [-31, 32]: 2 bits leader code `b10`, body is 6 bits;
- value in [-255, 256]: 3 bits leader code `b110`, body is 9 bits;
- value in [-2047, 2048]: 4 bits leader code `b1110`, body is 12 bits;
- otherwise: 4 bits leader code `b1111`, body is 32 bits;

int8 varlen schema
------------------

- value=0: 1 bit leader code `b0`, no body;
- value in [-31, 32]: 2 bits leader code `b10`, body is 6 bits;
- value in [-4095, 4096]: 3 bits leader code `b110`, body is 13 bits;
- value in [-524287, 524288]: 4 bits leader code `b1110`, body is 20 bits;
- otherwise: 4 bits leader code `b1111`, body is 64 bits;

MXKV scaled floats
------------------

Scaled floats are stored as int4/int8 accordingly, the values are also encoded
as integers.

MXKV clustering
===============

Delta encoding helps to reduce storage size, and increases compress ratio,
however it makes it slow to random seek, we have to decode all the
n values to know the nth.

To solve this problem, we cluster the KV records into small clusters, and
stores an extra cluster index for key lookuping.  To make the cluster index
small and efficient, we sort all the KV records by the keys, so clusters are
also sorted by the keys, and the key ranges do not overlap, so we only need to
store the min key of each cluster.

MXKV disk layout
================

The MXKV on-disk format is based on the postgres `bytea` type, the only
requirement is that the first 4 bytes indicates the total length of the data,
including these 4 bytes.

MXKV on-disk header
-------------------

The MXKV on-disk header is like below, it can be 12 or 16 bytes.

             00       01       02       03
         ┌───────────────────────────────────┐           ┌────────┐
    0000 │     vl_len_: the total length     │    flags: │TCVKOOWW│
         ├───────────────────────────────────┤           └────────┘
    0004 │   nrecords: the # of KV records   │    - WW: key_width code
         ├────────┬────────┬────────┬────────┤    - OO: offset_width code
    0008 │version │C.shift │ flags  │padding │    - K : keys_encoded
         ├────────┴────────┴────────┴────────┤    - V : values_encoded
    000C │     typmod: only if flags.T=1     │    - C : clustered
         └───────────────────────────────────┘    - T : has_typmod

- `C.shift` is the shift of cluster capacity, for example if it is 6, then
  a cluster can contain at most `1 << 6 = 64` KV records;
- `typmod` is the value typmod, it only exists if the typmod is specified for
  the mxkv type, such as `mxkv_float4(0)`, `mxkv_float8(2)`, in such a case the
  `flags.T` is also set to 1, the meaning of typmod is specific to each mxkv
  type;

Have to mention the byte-order of the fields:

- the `vl_len_` is in network byte-order, that is big-endian, this is defined
  by postgres;
- the `nrecords` and `typmod`, as well as other multi-byte fields of the MXKV
  on-disk layout, are all in native byte-order, it is little-endian on PC, this
  is a risk if we want to support cross-architecture clusters;

MXKV on-disk cluster index
--------------------------

When `flags.C=1`, the KV records are clustered, a cluster index is stored after
the header for fast seeking.  The type of the keys and offsets are defined by
`flags.WW` and `flags.OO` codes, which are defined as below:

- code=0: the type is 1-byte width
- code=1: the type is 2-byte width
- code=2: the type is 4-byte width
- code=3: the type is 8-byte width

For example, if the `flags.OO=2` then an offset is represented in uint16, so
the max offset cannot exceed 65535.  However at the moment we only support
code=3 for both `flags.WW` and `flags.OO`, so keys and offsets are both 4-byte
width.

So the MXKV on-disk cluster index is like below (again: `flags.WW=3` and
`flags.OO=3`):

         ┌───────────────────────────────────┐
         │   maxkey: maxkey of all clusters  │
         ├───────────────────────────────────┤
         │ minkey 0  : minkey of cluster 0   │
         │ minkey 1  : minkey of cluster 1   │
         │           :                       │
         │ minkey N-1: minkey of cluster N-1 │
         ├───────────────────────────────────┤
         │ offset 0  : offset of cluster 0   │
         │ offset 1  : offset of cluster 1   │
         │           :                       │
         │ offset N-1: offset of cluster N-1 │
         └───────────────────────────────────┘

- the cluster index is never encoded;
- there are total `N=nclusters` clusters, `nclusters` is calculated with
  `nrecords` and `C.shift`, all the clusters must be full except the last;
- when `nrecords=0`, `nclusters` is also 0, if `flags.C=1` the cluster index
  contains only the `maxkey`, which is meaningless anyway, but usually
  `flags.C=0` is set for such a case;
- the offset of a cluster is the offset based on the begin of cluster 0, so
  `offset 0` is always 0 for cluster 0, and `offset 1` is always the length of
  cluster 0, etc.;

MXKV on-disk clusters
---------------------

When `flags.C=1`, the KV records are clustered, N clusters are stored after the
cluster index, each cluster is as below:

         ┌───────────────────────────────────┐
         │               key 0               │
         │               key 1               │
         │                   :               │
         │               key N-1             │
         ├───────────────────────────────────┤
         │             value 0               │
         │             value 1               │
         │                   :               │
         │             value N-1             │
         └───────────────────────────────────┘

- when `nclusters=0`, no cluster is stored at all;
- when `flags.K=0`, keys are not encoded, each key is stored in a fixed-width
  type, which is defined by `flags.WW`, as explained in [MXKV on-disk cluster
  index](MXKV-on-disk-cluster-index), but at the moment we do not support this
  yet;
- when `flags.K=1`, keys are encoded as int4 (at the moment), in such a case
  they are not aligned to 4 bytes as above diagram;
- values are actually type dependent, for `mxkv_int4` they are 4-byte integers,
  for `mxkv_float8` they are 8-byte floats, for `mxkv_text` they are just 0
  terminated strings, and how they are actually stored are controlled by more
  flags:
  - for `mxkv_text` they are always stored as 0 terminated strings, one after
    another, but in the future we could support fixed-length text, such as
    `mxkv_text(10)`, then they should be stored in a fixed-length format, not
    defined yet;
  - for `mxkv_int4` we force `flags.V=1` so the values are always encoded, in
    the future we may allow the user to turned off it for better performance;
  - for `mxkv_float{4,8}` we force `flags.V=0` because we do not support float
    encoding yet, however `flags.T=1`, `typmod` is defined, in such a case they
    are scaled floats, the values are stored as int4 or int8 accordingly, and
    are also encoded (should respect `flags.V`);

When `flags.C=0`, all the KV records are put in one cluster unless when
`nrecords=0`, in which case `nclusters=0` so no cluster is stored.

 vi: et autoindent :

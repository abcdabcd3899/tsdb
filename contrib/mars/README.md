The Matrix Append-optimized Resilient Storage, aka MARS, is an advanced
columnar storage engine with below features:

- columnar storage;
- support data encoding and compression;
- support aggregation pushdown;
- optimized scan without indices;

Installation
============

To install MARS we first need to install the Apache Parquet C++ 3.0.
You could follow the official [Apache Arrow Installation Guide][1], we need the
development files for both arrow and parquet:

    # on ubuntu, first enable the official repo by following the install guide,
    # then:
    sudo apt install -y libarrow-dev libparquet-dev
    
    # on centos7, first enable the official repo by following the install guide,
    # then:
    sudo yum install -y --enablerepo=epel arrow-devel parquet-devel
    
    # on macos:
    brew install apache-arrow

Then compile and install the MARS extension:

    cd /path/to/matrixdb.git/contrib/mars
    make install

> - __NOTICE__: on centos we usually compile matrixdb with scl gcc-7 or higher,
>   however the official parquet rpm is compiled with the default gcc-4.8, if
>   we compile MARS with gcc-7 or higher, it might cause runtime crash due to
>   ABI incompatiblity of STL, so we must compile MARS with the same gcc
>   version used by parquet:
>
>   - if you use the official parquet rpm, compile MARS with below command:
>
>       # force using gcc-4.8
>       cd /path/to/matrixdb.git/contrib/mars
>       make install CC=/usr/bin/gcc CXX=/usr/bin/g++
>
>   - or you could compile parquet from source code, just make sure that you
>     use the same gcc version with matrixdb.

Then we can load the extension in your matrixdb database:

    CREATE EXTENSION mars;

[1]: https://arrow.apache.org/install/

Usage
=====

Create a MARS table
-------------------

To create a table using the MARS storage engine, simply specify it with the
`USING` keyword:

    CREATE TABLE t1 (ts TIMESTAMP, tag INT, val FLOAT) USING mars;

At the moment, only below types are supported:

- `INT4` / `INT`: 32-bit integers
- `INT8` / `BIGINT`: 64-bit integers
- `FLOAT4` / `REAL`: 32-bit floats
- `FLOAT8` / `FLOAT` / `DOUBLE PRECISION`: 64-bit floats
- `TIMESTAMP` / `TIMESTAMP WITHOUT TIME ZONE`: timestamp without timezone
- `TIMESTAMPTZ` / `TIMESTAMP WITH TIME ZONE`: timestamp with timezone
- all the variable-length types, such as `TEXT`, `JSON`, `JSONB`,
  `VARCHAR(10)`, `NUMERIC(12,2)`, etc.

Specify the compression method
------------------------------

MARS also supports compression, at the moment we can specify it by column:

```sql
CREATE TABLE t1
     ( ts   TIMESTAMP ENCODING (compresstype=rle_type)
     , tag  INT       ENCODING (compresstype=rle_type)
     , val1 FLOAT     ENCODING (compresstype=lz4)
     , val2 FLOAT     ENCODING (compresstype=zstd)
     ) USING mars;
```

Below encoding/compression methods are supported:

- `rle_type`: the [Run-Length Encoding][2], which is suitable for columns of
  slowly or small-step changed integers or timestamps;
- `lz4`: the [LZ4 compression algorithm][3], which is fast in compression and
  decompression;
- `zstd`: the [Zstandard compression algorithm][4], which has better
  compression ratio than `lz4`, but slower than it in compression and
  decompression;

[2]: https://en.wikipedia.org/wiki/Run-length_encoding
[3]: https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)
[4]: https://en.wikipedia.org/wiki/Zstandard

Simple storage options for hint
-------------------------------

```sql
CREATE TABLE t1
	 ( ts   TIMESTAMP
	 , tag  INT
	 , val1 FLOAT
	 , val2 FLOAT
	 ) USING mars WITH (tagkey="tag", timekey="ts", timebucket="1 mins");
```

MARS is designed for time series native storage. For each `Insert` command, MARS packaging
several tuples into one row group. There are three options to describe an MARS relation
data ordering and slicing strategy.
- `tagkey` indicates device unique tag column. The column requires a fixed-length type.
- `timekey` indicates a timestamp column which represents when the tuple information
  is gathered, as well as, user can manually give an interval value for `timebucket`
  option. The column requires `int` or `timestamp` type.
- `timebucket` (optional). if it is set, MARS continues gather all tuples which in
  a same time bucket until the time bucket is changed, then package these tuples in
  one row group. Instead, if the `time_bucket` is not set, MARS packaging data if any
  `tag` or `time` column value is changed.
>- __NOTICE__: So far, MARS does not automatically sort import data. user need to manually
> promise data order if the option is set.
>- __NOTICE__: MARS table packaging by tuple numbers if both `tagkey` and `timekey` is not set.

Accurate storage options for hint
---------------------------------
Except simple storage option, for general data requirement, MARS also have accurate
storage options. For both sets of options can give hint for MARS, but mixed using them
are unaccepted.

```sql
    CREATE TABLE t1
    ( ts   TIMESTAMP
    , tag  INT
    , val1 FLOAT
    , val2 FLOAT
    ) 
    USING mars 
    WITH (group_col_="{tag}", group_param_col_="{ts in 1 mins}",
          global_order_col_= "{tag, ts}", local_order_col_="{ts}");
```

As above example, the options declaration using accurate storage option, which give the
same as simple option example. So far, MARS have four accurate storage options. They
can describe all data ordering and slicing strategy for MARS storage, but we deprecate
them before user does not have a well-rounded knowledge for MARS. Unlike simple storage
option, all accurate storage option is organized as string array. Each element in an
array is delimited by comma, and requires surrounding by brace.

- `group_col_`: Data slicing array. Each element represents a column name. When data
  batch insert, any these column value change will trigger data packaging.
- `group_param_col_`: Basically is same as `group_col_`, but require an interval parameter for
  each element. The column name and interval are separated by `in`. For the column value change
  does not directly trigger data slicing. Instead, the column value need to calculate `time_bucket`
  result, and the result become data slicing trigger.
- `global_order_col_`: indicate table level data order
- `local_order_col_`: indicate slice level data order

> - __NOTICE__: the order is not ensured by the MARS, you have to ensure the data order manually.

Order hints
-----------
For the previous example we specify that the data are ordered by `(tag, ts)`

It is allowed to specify different order on table level and block level, for
example:

    CREATE TABLE t1 (ts TIMESTAMP, tag INT, val FLOAT)
     USING mars
      WITH (local_order_col_="{val, tag}",
            global_order_col_= "ts");

- in this example the blocks are ordered by (ts), but tuples inside a block are
  ordered by (tag, ts);
- again, the data order must be ensured manually;

Queries can have better performance with these order hints automatically:

    SELECT * FROM t1
     WHERE tag IN (1, 3);
    
    SELECT * FROM t1
     WHERE ts >= '2000-01-01 12:00:00'
       AND ts <  '2000-01-01 18:00:00';
    
    SELECT * FROM t1
     WHERE tag IN (1, 3)
       AND ts >= '2000-01-01 12:00:00'
       AND ts <  '2000-01-01 18:00:00';
    
    SELECT * FROM t1
     WHERE tag = 3
     ORDER BY ts
     LIMIT 1;

Best Practice
=============

Grouping and Ordering
---------------------

Grouping size
-------------

One-level and two-level layouts
-------------------------------

TODO

Design
======

> - __NOTICE__: the design is likely to be improved continuously.

MARS is based on the Apache Parquet storage format.

Block Versions
--------------

At present we have developed 2 block formats for MARS, v0.1 and v1.0, which
can be distinguished with the `created_by()` attribute of each Parquet file,
aka MARS block.  Here is the mapping of the values and the versions:

- `MatrixDB`: v0.1
- `MARS 0.1`: v0.1
- `MARS 1.0`: v1.0

It is allowed to store blocks of different versions in the same MARS table.
Suppose a MARS table is created with an old version of MARS, all the blocks are
stored in v0.1 format, then we updaed the MARS extension, we could then insert
blocks to the table in v1.0 format, and we could still scan all the blocks out.

The default format is v1.0, but it is possible to specify it manually via a
GUC `mars.default_file_format`, it has 3 values at present:

- `v0.1`: insert in v0.1 format;
- `v1.0`: insert in v1.0 format;
- `auto`: the same as `v1.0` at present;

Note that this GUC only affect writing format.  The reading is always
controlled by the actual version string in each block.

Data Layout
-----------

### Block Layout

The logical data layout is as below.  A table contains several blocks, each
block contains several tuples.  As a columnar storage, the data of the same
column are stored together.

    ┌────────────────────────╥────────────────────────┐
    │         block 1        ║         block 2        │
    │                        ║                        │
    │ ┌──────╥──────╥──────┐ ║ ┌──────╥──────╥──────┐ │
    │ │column║column║column│ ║ │column║column║column│ │
    │ │      ║      ║      │ ║ │      ║      ║      │ │
    │ │0 ts  ║0 tag ║0 val │ ║ │0 ts  ║0 tag ║0 val │ │
    │ │1 ts  ║1 tag ║1 val │ ║ │1 ts  ║1 tag ║1 val │ │
    │ │2 ts  ║2 tag ║2 val │ ║ │2 ts  ║2 tag ║2 val │ │
    │ └──────╨──────╨──────┘ ║ └──────╨──────╨──────┘ │
    └────────────────────────╨────────────────────────┘
    
    ┌────────────────────────┐
    │         block 1        │
    │                        │
    │ ┌──────╥──────╥──────┐ │
    │ │column║column║column│ │
    │ │      ║      ║      │ │
    │ │0 ts  ║0 tag ║0 val │ │ <───> row 0 of block 1
    │ │1 ts  ║1 tag ║1 val │ │ <───> row 1 of block 1
    │ │2 ts  ║2 tag ║2 val │ │ <───> row 2 of block 1
    │ └──────╨──────╨──────┘ │
    └────────────────────────┘

### Column Encoder

MARS 1.0 introduced "Column Encoder" to support per-column encoding.  All the
columns are encoded in MARS 1.0, the float4 and float8 columns are encoded with
the Facebook Gorilla encoding to improve the compression ratio, all the others
are encoded with the plain encoding, which stores the original data directly.
Later we might implement more encodings.  Encoded data are always stored in
Parquet BytesArray format.

       ┌────────────────────────┐
       │         block 1        │
       │                        │
       │ ┌──────╥──────╥──────┐ │
       │ │column║column║column│ │
       │ │      ║      ║      │ │
       │ │0 ts  ║0 tag ║0 val │ │
       │ │1 ts  ║1 tag ║1 val │ │
       │ │2 ts  ║2 tag ║2 val │ │
       │ └──────╨──────╨──────┘ │
       │  ↑      ↑      ↑       │
       │  E1      E2     E3     │<───> Encoder 1:1 bound at Column
       └────────────────────────┘

The encoded data, no matter it is encoded by the plain encoder or a real
encoder, has a encoding header, it describes how the data is encoded, so it is
possible to decode it with a higher version of MARS.  The physical layout is as
below:

    struct pg_attribute_packed() PhysicalHeader_V1_0 {
        int32 vl_len_; //<! the bytea header, always in 4B format

        uint8 has_nulls  : 1; //<! has null values or not
        uint8 has_values : 1; //<! has non-null values or not
        uint8 encoded    : 1; //<! encoded or not
        uint8 uncompressed_datalen : 1; //<! whether the original data length is saved
        uint8 padding    : 4; //<! padding bits

        varatt total_count; //<! total count, including nulls

        if (encoded) {
            varatt nencodings; //<! # of encodings, >= 1 when present
            varatt encoding; //<! encoding method, can be NONE
        }

        /*!
         * The "nulls" section, it is skipped in all-null or no-null case.
         */
        if (has_nulls && has_values) {
            varatt null_count; //<! null count

            uint8 bitmap[(total_count + 7) / 8]; //<! null bitmap
        }

        /*!
         * The "values" section, it is skipped in all-null case.
         *
         * The format and length are encoding specific.
         */
        if (has_values) {
            char encoded_values[];
        }
    }

### Parquet Layout

Here is how we represent this layout with Apache Parquet.

A parquet file is consisted by a `DATA` and a `FOOTER`:

- the `DATA` is the data, compressed or uncompressed, depends on the settings;
- the `FOOTER` tracks kinds of information, such as the data schema, the file
  position of the data, the columns, the statistics, etc.;

In MARS we store all the `DATA` in the relation data file, and store all the
`FOOTER` in the auxiliary file of the relation.  Each time we `INSERT` a batch
of tuples, we append the `DATA` to the data file, and append the `FOOTER` to
the auxiliary file.  So it is like this:

             data file                          auxiliary file
    ┌────────────────────────┐            ┌────────────────────────┐
    │                        │   ┌────────│         FOOTER         │
    │          DATA          │<──┘        ├────────────────────────┤
    │                        │      ┌─────│         FOOTER         │
    ├────────────────────────┤      │     ├────────────────────────┤
    │                        │      │  ┌──│         FOOTER         │
    │          DATA          │<─────┘  │  └────────────────────────┘
    │                        │         │
    ├────────────────────────┤         │
    │                        │         │
    │          DATA          │<────────┘
    │                        │
    └────────────────────────┘

The `DATA` and the corresponding `FOOTER` consist a logical `PARQUET FILE`, but
in our code we usually call it `FOOTER` for short, in most parts of this
document, we will also refer to a `PARQUET FILE` as a `FOOTER`.

Each `FOOTER` contains several `ROW GROUP`s, each `ROW GROUP` contains several
`COLUMN CHUNK`s, each `COLUMN CHUNK` contains several `DATA POINT`s, it maps to
our logical block layout like this:

           physical layout                            logical layout
    ┌────────────────────────────┐              ┌────────────────────────┐
    │          FOOTER 1          │<────────────>│         block 1        │
    │                            │              │                        │
    │ ┌────────────────────────┐ │              │                        │
    │ │      ROW GROUP 0       │ │              │                        │
    │ │                        │ │              │                        │
    │ │ ┌──────╥──────╥──────┐ │ │              │ ┌──────╥──────╥──────┐ │
    │ │ │COLUMN║COLUMN║COLUMN│ │ │<────────────>│ │column║column║column│ │
    │ │ │CHUNK ║CHUNK ║CHUNK │ │ │              │ │      ║      ║      │ │
    │ │ │      ║      ║      │ │ │              │ │      ║      ║      │ │
    │ │ │0 ts  ║0 tag ║0 val │ │ │              │ │0 ts  ║0 tag ║0 val │ │
    │ │ │1 ts  ║1 tag ║1 val │ │ │              │ │1 ts  ║1 tag ║1 val │ │
    │ │ │2 ts  ║2 tag ║2 val │ │ │              │ │2 ts  ║2 tag ║2 val │ │
    │ │ └──────╨──────╨──────┘ │ │              │ │      ║      ║      │ │
    │ ╞════════════════════════╡ │              │ │      ║      ║      │ │
    │ │      ROW GROUP 1       │ │              │ │      ║      ║      │ │
    │ │                        │ │              │ │      ║      ║      │ │
    │ │ ┌──────╥──────╥──────┐ │ │              │ │      ║      ║      │ │
    │ │ │COLUMN║COLUMN║COLUMN│ │ │              │ │      ║      ║      │ │
    │ │ │CHUNK ║CHUNK ║CHUNK │ │ │              │ │      ║      ║      │ │
    │ │ │      ║      ║      │ │ │              │ │      ║      ║      │ │
    │ │ │3 ts  ║3 tag ║3 val │ │ │              │ │3 ts  ║3 tag ║3 val │ │
    │ │ │4 ts  ║4 tag ║4 val │ │ │              │ │4 ts  ║4 tag ║4 val │ │
    │ │ │5 ts  ║5 tag ║5 val │ │ │              │ │5 ts  ║5 tag ║5 val │ │
    │ │ └──────╨──────╨──────┘ │ │              │ └──────╨──────╨──────┘ │
    │ └────────────────────────┘ │              │                        │
    └────────────────────────────┘              └────────────────────────┘

At present we store only one `ROW GROUP` in each `FOOTER`, so the layout is
actually like below:

           physical layout                            logical layout
    ┌────────────────────────────┐              ┌────────────────────────┐
    │          FOOTER 1          │<────────────>│         block 1        │
    │                            │              │                        │
    │ ┌────────────────────────┐ │              │                        │
    │ │      ROW GROUP 0       │ │              │                        │
    │ │                        │ │              │                        │
    │ │ ┌──────╥──────╥──────┐ │ │              │ ┌──────╥──────╥──────┐ │
    │ │ │COLUMN║COLUMN║COLUMN│ │ │<────────────>│ │column║column║column│ │
    │ │ │CHUNK ║CHUNK ║CHUNK │ │ │              │ │      ║      ║      │ │
    │ │ │      ║      ║      │ │ │              │ │      ║      ║      │ │
    │ │ │0 ts  ║0 tag ║0 val │ │ │              │ │0 ts  ║0 tag ║0 val │ │
    │ │ │1 ts  ║1 tag ║1 val │ │ │              │ │1 ts  ║1 tag ║1 val │ │
    │ │ │2 ts  ║2 tag ║2 val │ │ │              │ │2 ts  ║2 tag ║2 val │ │
    │ │ │3 ts  ║3 tag ║3 val │ │ │              │ │3 ts  ║3 tag ║3 val │ │
    │ │ │4 ts  ║4 tag ║4 val │ │ │              │ │4 ts  ║4 tag ║4 val │ │
    │ │ │5 ts  ║5 tag ║5 val │ │ │              │ │5 ts  ║5 tag ║5 val │ │
    │ │ └──────╨──────╨──────┘ │ │              │ └──────╨──────╨──────┘ │
    │ └────────────────────────┘ │              │                        │
    └────────────────────────────┘              └────────────────────────┘

- a `FOOTER` represents a block;
- a `COLUMN CHUNK` represents the column data in an `INSERT` batch;
- the `ROW GROUP` concept is actually not used presently;



Insertion
---------

For insertion, we care about two problems. The first is how to store data in a single
insertion command. The second is that how to satisfy transaction requirements(ACID).

So far, MARS has an assumption that all data is cold time-series data, which means for
the imported data is ordered. In table creation command, if user does not give any
hint of data package strategy. MARS package every 10240 tuples into one row group.
Otherwise, MARS compares time bucket value at each input tuple. If current input
tuple's time bucket is same as previous', store current tuple into data buffer until
the time bucket is changed. Then, package one rowgroup and flush to disk. The original
parquet file is designed  for HDFS-like file system, so every Parquet file hold data
in serveral rowgroups, after all data in one input branch written down, Parquet
serialize a footer binary attach at the end of the file. The footer is a row group
level index, which record each row group file offset. Next time, before data input,
HDFS need to create a new file to store. In MatrixDB, since our physical storage can
random read-write, we append new data into old file instead of creation. For the
footer binary data, MARS package by each row group, so every row group in MatrixDB
have its private footer. Furthermore, in MatrixDB, the footers is not stored in data
file, instead, MARS use an Auxiliary table to manage them.

For the second problem, MARS use Auxiliary table (aux table) to deal with MVCC.
Each MARS relation in MatrixDB segment maintain an unique aux table which is a Heap
table( The aux table detail is introduced by next chapter). Since each tuple in
Heap table contains MVCC information, so footer binary visibility is managed
by MatrixDB.

So far, MARS does not support multi-session insertion yet. Each MARS relation only
have one data file for each segment.

Auxiliary table
===============

Every MARS relation has a auxiliary table, aka aux table, it stores the preagg
information of all the columns, such as the count, min, max, sum, the parquet
file metadata is also stored in the aux table.

Mapping
-------

The `mx_mars` catalog table can be used to lookup the aux table of a MARS
table, for example below query lists all the MARS tables and the corresponding
aux tables:

    select relid::regclass as relname
         , segrelid::regclass as auxname
      from mx_mars;

Aux table versions
------------------

There are also multiple versions of aux table schemas, at present there are V1
and V2.  The version information is not stored in any catalog table, however
we could detect the aux version by checking the tupdesc.

By default all the new MARS aux tables are created in the latest version, V2 at
present, but it is allowed to explicitly specify it via the GUC
`mars.default_auxfile_format`, there are 3 possible values:

- `v1`: create aux table in V1 schema;
- `v2`: create aux table in V2 schema;
- `auto`: the same as `v1` at present, and is the default;

Aux table V1
------------

    pg_aoseg.<oid>_footer (
     segno          bigint
     batch          bigint
     content        bytea
     rowgroupcount  integer
     rowcount       bigint[]    -- bigint[nrowgroups]
     nullcount      bytea[]     -- bigint[nrowgroups][ncolumns]
     sum            bytea[]     -- any128[nrowgroups][ncolumns]
     min_at         bytea[]     -- integer[nrowgroups][ncolumns]
     max_at         bytea[]     -- integer[nrowgroups][ncolumns]
     min            bytea[]     -- any64[nrowgroups][ncolumns]
     max            bytea[]     -- any64[nrowgroups][ncolumns]
    )

* `segno` indicates which physical file current tuple belonged.
* `batch` For every `segno` file, its unique key for a footer tuple. The value start
  from 0, and automatically growth one for every insert command.
* `content` The parquet footer binary data.Before read raw data, the content
   value is needed to `ParquetReader` class.
* `rowgroupcount` indicates the number of row groups is managed by this footer.
  So far, the value is always 1.
* `rowcount` indecate the tuple count information for each row group. Each field
   is a bigint in the array
* `sum`  Each field in the array is a binary value to store all columns sum in a row-
  group for current footer. Each sum size is 16 bytes to prevent sum value overflow.
* `min` like `min`, the field size is 8 bytes.
* `max` like `max`, the field size is 8 bytes.

Aux table V2
------------

    pg_aoseg.<oid>_footer (
     segno          bigint
     batch          bigint
     content        bytea
     rowgroupcount  integer
     rowcount       bigint
     nullcount      bytea       -- bigint[ncolumns]
     sum            bytea       -- any128[ncolumns]
     min_at         bytea       -- integer[ncolumns]
     max_at         bytea       -- integer[ncolumns]
     min            bytea       -- any64[ncolumns]
     max            bytea       -- any64[ncolumns]

     -- below only exist in V2

     physical_edges bytea       -- any64[ncolumns * 2]
    )

The major difference with V1 is that in V2 we drop the capability to store
multiple parquet row groups in one parquet file.  In MARS we only store one
parquet row group in one parquet file, so this change does not make any
behavior or structure change.  An V2 aux table is smaller than V1, both on-disk
and in-memory, and can be loaded more efficiently.

In V2 also change the storage type of the varlen columns from `extended` to
`external`.  See the next subsection for the purpose.

In V2 we also introduced a new aux column, `physical_edges`, which stores the
first & last values in physical store order.

Data layout of min/max preagg
-----------------------------

For `min` and `max`, if data type is variable length type and
it supports comparison operator, we append the value of `min`/`max`
into `min`/`max` bytea. Meanwhile, we store the offset of `min`/`max` value
in the corresponding position(bytea[i]).

Here is an illustrative example based on V2:

    attributes: int32      text      interval  int64
          data: min_int32  offset1   offset2   min_int64  min_interval  padding    min_interval
                ^                                         ^             ^          ^
                position 0                                offset1       alignment  offset2

and this is a real case(`max`) in hex:

    int32           text            varchar(100)    int64
    0200000000000000200000000000000024000000000000000c00000000000000823600008233
    ^               ^               ^               ^               ^   ^   ^
    max=2, 8B       offset1=0x20    offset2=0x24    max=12          p1  p2  p3

* p1 offset is 0x20, and it represents text '6' (ASCII 0x36)
* p2 offset is 0x22, and it's padding for alignment of next value
* p3 offset is 0x24, and it represents varchar '3' (ASCII 0x33)

Lazy detoasting
---------------

The aux table contains several varlen columns, they can be toasted depends on
the # of columns of the MARS table itself.  When loading the aux table rows we
used to load all the columns into memory, which means all the toasted varlen
columns are detoasted immediately, however depends on the query not all of the
columns are used, for example in a custom agg scan we usually don't scan the
data, so the `content` column, the parquet file metadata, is useless, it is a
waste of cpu to detoast them.

So we introduced the aux table lazy detoasting mechanism, when loading the
toasted aux table columns we only store the toast header, and do not detoast
until being used.

A tuple scanned from a aux table, no matter V1 or V2, can be in several
formats:

- raw data, neither compressed nor toasted, we must copy the data out
  immediately because the data might be stored in a buffer page, which can be
  inaccessible once we move to the next row;
- toasted, the actual data is stored in the corresponding toast table, the
  tuple is only a toast header, a pointer to the actual data; we could store
  the toast header itself, which is usually small enough, say 20 bytes, and do
  the detoasting when actually being used;
- in-place compressed, the actual data is still stored in the aux table itself,
  just being compressed; like raw data, we must copy the data out immediately
  otherwise it is inaccessible when we move to the next row, however we have
  two options: a) we could copy the compressed data out, and do a lazy
  decompression when being used, or b) we could decompress immediately; the
  option a needs one extra copy compared to option b, consider that the
  compressed data can usually be hundreds or thousands of bytes, or even more,
  the copying can be expansive, so we would rather choose option b, decompress
  immediately.

We can see that in-place compressed data is always expansive to load, they are
usually large, and can not be lazy decompressed efficiently.  So in aux table
V2 the storage type of all the varlen columns are changed from `extended` to
`external`, the former can be in-place compressed or toasted, the latter can
only be toasted.

Aux index
---------

### Columns for index
Since user can specify `global_order_col_`, `group_col_` and `group_param_col_` while creating, these info also should be stored in auxiliay table if exists to accelerate the query, so some columns will be added besides the above columns.

Let `N` is the attribute number in data table, here is the naming convention:
|       type        | original name   | name in aux table            |
|-------------------|-----------------|------------------------------|
| global_order_col_ | `c_global`      | `attrN__min__c_global`       |
| global_order_col_ | `c_global`      | `attrN__max__c_global`       |
| group_col_        | `c_group`       | `attrN__group_key__c_group`  |
| group_param_col_  | `c_group_param` | `attrN__group_param_key__c_` |

For `global_order_col_`, `min` and `max` will be stored two different columns.
For `group_param_col_`, we will evaluate the value by `time_bucket` then store it in auxiliary table.

These additional columns will be added to the auxiliary table in the order of `global_order_col_`, `group_col_` and `group_param_col_`. If there are multiple columns in `global_order_col_`, `min` and `max` columns of the same column in data table will be bound together, and the order of different columns is the order in `global_order_col_` declaration.

If the original column name is too long so that the length of new name exceeds `NAMEDATALEN`, the name will be truncated by multibyte character boundary, such as last line as example.

Finally, talk about their types. They are consistent with the types of columns corresponding to the original data table.

Here is an example:
```sql
    CREATE TABLE t1
    ( ts   TIMESTAMP
    , tag  INT
    , val1 FLOAT
    , val2 FLOAT
    ) 
    USING mars 
    WITH (group_col_="{tag}", group_param_col_="{ts in 1 mins}",
          global_order_col_= "{tag, ts}", local_order_col_="{ts}");
```

The following columns will be appended to auxiliary table.
|           Column           |            Type             |
|----------------------------|-----------------------------|
| attr2__min__tag            | integer                     |
| attr2__max__tag            | integer                     |
| attr1__min__ts             | timestamp without time zone |
| attr1__max__ts             | timestamp without time zone |
| attr2__group_key__tag      | integer                     |
| attr1__group_param_key__ts | timestamp without time zone |

### Indexes
Base on these columns, we will create several kinds of indexes:

```c
enum IndexType
{
	INVALID_INDEX_TYPE,

	// for group / order keys
	ORDER_KEY_MIN_FIRST,
	ORDER_KEY_MAX_FIRST,
	GROUP_KEY,
	GROUP_PARAM_KEY,

	// for ctid seeking
	BATCH,
};
```

Similar with columns name, we also have naming convention for index name. Let `oid` stands for oid of data table and digit `T` denotes index type, so the index name is `idxT__oid_footer`.

We will create two different indexes if global order column exists. The difference between them is that one uses the minimum value as the first index key(Type: `ORDER_KEY_MIN_FIRST`), and the other uses the maximum value as the first index key(Type: `ORDER_KEY_MAX_FIRST`).

If group key has already stored in aux table, we also create an index for it. If group param key exists, it will be added into the index described above(Type: `GROUP_KEY`)

Similarly, if group param key exists, we will append group keys and then create an index for param key(Type: `GROUP_PARAM_KEY`).

In the example mentioned last section, the following 4 indexes will be created on auxiliary table.
```
Indexes:
    "idx1__474924_footer" btree (attr2__min__tag, attr2__max__tag, attr1__min__ts, attr1__max__ts)
    "idx2__474924_footer" btree (attr2__max__tag, attr2__min__tag, attr1__max__ts, attr1__min__ts)
    "idx3__474924_footer" btree (attr2__group_key__tag, attr1__group_param_key__ts)
    "idx4__474924_footer" btree (attr1__group_param_key__ts, attr2__group_key__tag)
```

Optimized Data Scanning
=======================

The MARS is scanned via the SeqScan node, which is expected to returned all the
visible tuples, one by one, without order.

However MARS tables contain the per-block min/max information of each column,
these actually form a range index, like the BRIN, so it is possible to optimize
the scanning with these information, for example, we can decide whether a block
matches the `WHERE` conditions, and skip it if not.  And we can do even better.

Depends on the avaliable information, we can use several different optimization
techniques.

Column Extraction
-----------------

When a query only cares about a subset of the columns, we can skipping loading
of the other columns.  We can know this information by checking the projection
list, the targetlists, and the `WHERE` conditions, the quals.

The Basic Filtering
-------------------

The min/max information are always available, so we can always do a block
filtering with the `WHERE` conditions.

At the beginning of the SeqScan, we have a chance to check the quals, a set of
ScanKey are extracted from the `WHERE` conditions, however let's call them
filters at this stage, we can check whether a block or tuple matches a filter,
nothing more.  Some of the filters will be upgraded to scankeys, we will cover
this in later sections.

A filter describes a condition like `c1 > 0`, `c2 = 1`, `c3 <= 2`, etc., there
can be multiple filters, they are considered as `AND`ed, which means that, if
any of the filters is unsastified, the matching result is `MISMATCH`.

Not every `WHERE` condition translates to a filter.  The unsupported
conditions, such as `c1 != 1`, can be simply ignored.  Every tuple returned by
MARS will also be re-checked at the SeqScan level, it will run the full
checking with all the `WHERE` conditions.

For a tuple, we simply compare the corresponding column with the filter.  Only
when a tuple passes all the filters, it is returned.

Before that we can also use the filter on the blocks.  We know the min/max
information of every column in a block, so we can use them against a filter:

- <, <=, >, >=
  - if either `min` or `max` matches the filter, it is a `MAYBE MATCH`;
  - if both `min` and `max` match the filter, it is a `FULL MATCH`;
  - otherwise it is a `MISMATCH`;
- =
  - if either `min <= filter.arg` or `filter.arg <= max`, it is a `MAYBE MATCH`;
  - if both `min <= filter.arg` and `filter.arg <= max`, it is a `FULL MATCH`;
  - otherwise it is a `MISMATCH`;

We match a block with all the filters:

- if any of them is `MISMATCH`, we can skip the block;
- if all of them are `FULL MATCH`, we know that all the tuples in the block
  pass all the filters, so actually we do not need to run the filters again on
  the tuples;
- otherwise we know that some of the tuples of the block can pass the filters,
  we need to run the filters on every tuple later;

> - __DISCUSSION__: we know that SeqScan will recheck the tuples returned by
    MARS, so what is the value of running the filters on the tuples by MARS
    itself?  A short answer is: MARS is near to the data, and running filters
    on warm data is usually more efficient than on cold data, especially when
    MARS is a columnar store.

The Ordered Scanning
--------------------

Sometimes the data can be stored in order on one or more columns, for example:

- time series data are usually ordered by timestamp by nature;
- data can be reordered during data retention or even during data loading, for
  better compression and query performance;

We can let MARS know the storing order by creating the tables with the
`table_order_attr*` and `block_order_attr*` options, we can also let MARS know
the query order with the `mars.scan_order_desc*` GUCs.  MARS can scan more
efficiently with these information.

> - __NOTICE__: in below description we assume that the scan is in the forward
>   direction, from the first to the last, and the data is stored in ascending
>   order.

Suppose a block contains an int column, the values are `1, 1, 2, 2, 3, 3`,
let's refer to them as `X0, X1, ..., X5`.

| row | X |
|-----|---|
| 0   | 1 |
| 1   | 1 |
| 2   | 2 |
| 3   | 2 |
| 4   | 3 |
| 5   | 3 |

Filters on such an ordered column is called scankeys, they can be used more
efficiently than normal filters.

First let's begin with a scankey `X = 2`.  Instead of checking from X0 to X5
one by one, we can run a binary search and seek to the first occurrence of
value 2, at X2, with log2(N) compares, and return X2, X3, in order; when we
come to X4, the value 3 does not match the scankey, we know that there will be
no more match in this block, so we do not need to check X5, and leave the block
immediately.

This process can be described in a more general way:
1. seek the first matching tuple with the scankeys;
2. report the tuples one by one, until;
3. end of the block, or;
4. the scankeys can no longer be matched;

Here we are further spliting the scankeys as `begin keys` and `end keys`.  Let
us understand them with some examples:

- `X < 2`
  - there is no `begin key`, we can simply scan from X0;
  - the `end key` is `X >= 2`, so once we come to X2, the block is done;
- `X <= 2`
  - there is no `begin key`, we can simply scan from X0;
  - the `end key` is `X > 2`, so once we come to X4, the block is done;
- `X = 2`
  - the `begin key` is `X >= 2`, we scan from X2;
  - the `end key` is `X > 2`, so once we come to X4, the block is done;
- `X >= 2`
  - the `begin key` is also `X >= 2`, we scan from X2;
  - there is no `end key`, we need to scan to end of the block;
- `X > 2`
  - the `begin key` is `X > 2`, we scan from X4;
  - there is no `end key`, we need to scan to end of the block;

When there are multiple scankeys on a column we can merge the `begin keys` and
`end keys` separately, let us also check some examples:

- `X < 2 and X = 1`
  - the `begin key` is `X >= 1`;
  - the `end key` is `X >= 2`;
- `X >= 2 and X < 3`
  - the `begin key` is `X >= 2`;
  - the `end key` is `X >= 3`;

With the scankeys we can seek to the matching range efficiently, and stop
scanning early.  Filters can also be checked on the tuples.

So far we are only seeing one ordering key.  Things can be more complicated
when there are multiple ordering keys.  For example, now suppose we have two
columns, X and Y, ordered by (X, Y):

| row | X | Y |
|-----|---|---|
| 0   | 1 | 1 |
| 1   | 1 | 2 |
| 2   | 2 | 1 |
| 3   | 2 | 2 |
| 4   | 3 | 1 |
| 5   | 3 | 2 |

- if there are filters on X, they can be upgraded to scankeys;
- if there are filters on both X and Y, they can be upgraded to scankeys;
- if there are only filters on Y, but not X, they can not be upgraded;

When there are scankeys on both X and Y, we have to decide the `begin keys`
and `end keys` more carefully, for example:

- `X > 1 and Y < 2`
  - the `begin key` is `X > 1`;
  - no `end key`;
- `X > 1 and Y = 1`
  - the `begin key` is `X > 1 and Y >= 1`;
  - no `end key`;
- `X > 1 and Y > 1`
  - the `begin key` is `X > 1 and Y > 1`;
  - no `end key`;
- `X < 1 and Y > 1`
  - no `begin key`;
  - the `end key` is `X >= 1`;
- `X < 1 and Y < 1`
  - no `begin key`;
  - the `end key` is `X >= 1 and Y >= 1`;

We are not listing all the possible combinations, the general policy is that a
filter/scankey on Y is only a real scankey when there is suitable scankey on X.

Scankeys can also be used on blocks, in a similar way.  This makes it possible
to support millions of blocks, or even more, the scan can be efficient as long
as there are suitable scankeys.

Index denotes that we have the logical order on blocks, and btree index is able to help us to find blocks meeting conditions, so besides the store order, we also use these index info to create a mars sortscan path to accelerate the query.

For group param key, there is a little difference. We store the result of function `time_bucket` against original data not the original data themselves, so we cannot guarantee that the original data is in order. Therefore, whether we can leverage the index on group param key or not depends on paramter in query expression. It is fine if and only if it is an multiple of the parameter specified when the table is built.

The Paradox Conditions
----------------------

Sometimes there can be paradox `WHERE` conditions, which means two keys
conflict with each other.  For example, `X < 2 and X > 3`, in such a case we
know that no match is possible.

Most paradox conditions can be optimized by the planner automatically, however
there are still cases that we need to handle them manually, an example is `c1 <
2 and c1 in (1, 2, 3)`, see the next section on how is the `IN` expression
handled, and why it leads to paradox conditions.

The IN Expressions
------------------

A `IN` expression is like `X in (1, 2, 3)`, depends on whether it is on an
ordered column, (to be accurate, the primary ordered column), it is an array
key or an array filter.

When the `IN` expression is on the primary order column, it is an array key, to
handle it efficiently, we run the scanning 3 rounds, the first round we scan
with `X = 1`, then `X = 2`, and finally `X = 3`, in every round we could use
the array key as a normal scankey, and do fast seeking with it.  We will detect
for paradox conditions every round, and skip that round if possible.

When the `IN` expression is on a non-primary ordered column, we can not apply
above optimization, we can only use the `IN` expression as a normal filter, we
call it an array filter.  Array filters are not always useful, because the
SeqScan node, the caller of MARS, will run the array filter again on every
reported tuple.  However when a block-level reordering is needed, the array
filters can be very useful, by filtering out unmatched tuples before the
sorting, we can make the sorting faster.

Optimized Custom Sort Scan
=======================

The Result Order
----------------

So far we are only talking about how to speed up the scanning with the order
information, in fact it is also possible to optimize the plan with the order
information.  For example:

    SELECT x FROM t1 ORDER BY x LIMIT 1;

The plan is like this by default:

    Limit
      ->  Sort
            Sort Key: x
            ->  Seq Scan on t1

It sorts all the data just to find out the min value.  However if we know the
data storing order is already (x), then the planner will try to reduce the
unnecessary SORT node like:

    Limit
      ->  Seq Scan on t1

A seqscan path cannot provide a pathkeys (the order info inside postgres), so
we build a custom path to provide the order pathkeys. The plan finally looks
like:

    Limit
      -> Custom Scan (MarsSortScan) on t1

This kind of optimization is usually only applied on a table with btree index,
because btree indexscan is ordered by nature.  MARS can support this without an
index.

Now, Mars can only provide two kinds of sort info:
1) the table-level sort info
2) the time_bucket expression upon table-level sort info

planner now is indicated by GUC:

    SET mars.scan_order_desc1 TO 1;

We need a better way to let the planner know the order information.

To note, Mars will try the best to keep the order in the table-level even we have
a MARS table having different order on blocks and tuples inside each block, for
example, suppose the blocks are ordered by (X) and tuples inside each block is
ordered by (Y, X), for example (the table schema is (X, Y), too):

    block 1: (1,1) (2,1) (1,2) (2,2)
    block 2: (3,1) (4,1) (3,2) (4,2)

If we simply scan in the store order, the tuples are returned in below order:

    (1,1)
    (2,1)
    (1,2)
    (2,2)
    
    (3,1)
    (4,1)
    (3,2)
    (4,2)

The result is not ordered by (X), does this mean that a full table sort is
needed?  Actually, no.  Notice that the blocks are ordered by (X), which means
that all the X in block 1 <= the X in the block 2, so if we sort block 1 and
block 2 separately on (X), the overall order is then ordered by (X):

    (1,1)            (1,1)
    (2,1)    sort    (1,2)
    (1,2)  ───────>  (2,1)
    (2,2)            (2,2)
    
    (3,1)            (3,1)
    (4,1)    sort    (3,2)
    (3,2)  ───────>  (4,1)
    (4,2)            (4,2)

Is this more efficient than sorting the whole table?  Yes.  Suppose there are M
blocks, each contains N tuples:

- sort `M * N` tuples together, the complexity is `M * N * log2(M * N)`; while
  sorting M blocks separately, the complexity is `M * (N * log2(N)) = M * N *
  log2(N)`, so the MARS way is a win;
- and a block is usually small enough to fit the memory, so a in-memory sorting
  can be performed, while sorting the whole table might need spilling to the
  disk;
- and when the query is a `ORDER BY LIMIT`, it is very likely that only the
  first block is actually sorted in the MARS way;

> - __NOTICE__: this optimization is only correct when blocks do not overlap,
    for example, if the X min/max is (1,3) in block 1, and (2,4) in block 2,
    then sorting the blocks separately is not enough to produce an overall
    ordered result;

When there are scankeys and filters, too, we can run them before sorting the
block, so only a subset of the tuples will be sorted, which makes it more
efficient.

Optimized Custom Aggregate Scan
=======================
Mars now provides pre-aggregate results like SUM, MIN, MAX within the footers,
we now provide a brand new customized path called `MarsAggScan` to take the
advantage of the footer metadata.

for example:

    select count(*) from t1 group by time.

Mars table `t1` is ordered on column `time`, comparing to the traditional
plan:

    Finalize HashAggregate
       -> Gather Motion
          -> Partial HashAggregate
             -> Sort
                -> Seq Scan on t1

A `MarsAggScan` plan looks like

    Finalize HashAggregate
       -> Gather Motion
          -> Custom Scan (MarsAggScan) on t1
 
The Internal of the Custom agg scan looks like:

    Custom Scan (MarsAggScan)
    Internal Plan:
    -> Block Header Scan (Footer Scan)
    -> GroupAggregate
       -> Block Scan

For each block of the mars table, if the block match:

1) all tuples match the filter quals.
2) all tuples are unique on the group keys.

We construct the aggregate output directly from the footer without reading
the block content at all. For mismatched blocks, the tuples within are
aggregated as normal. The overall performance of MarsAggScan depends on the
rate that the Footer Scan path is hit.

The MarsAggScan now is only triggered when:
1) all aggregate functions in the query are supported by Mars
2) plain agg or
3) groupkeys contain within the pathkeys generated by Mars Sort Scan.


 vi: et autoindent :

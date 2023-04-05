GORILLA encoding
====================

This module implements the Fackbook GORILLA [1] encoding, which includes Gorilla
and Delta-Delta encode algorithm.
### Gorilla
Gorilla aims to improve the compression ratio of floating-point numbers.

Ordinary compression algorithm doesn't work effectively on floating-point
numbers due to thefloating encoding format, IEEE-754, which spreads meaningful
bits in several small fields in every number.  The gorilla encoding groups most
of the meaningful bits together, and stores them in a variable-length format,
so it takes less bits, and can be better compressed.

### Delta-Delta
Delta-Delta is suitable for continuous integer values which means the
distribution of the differences of the time intervals are close.
Obviously, the time-series case especially meets this characteristic.
For time series data, rather than storing timestamps in their entirety, store
delta of deltas is more efficient.

[1]: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf

Installation
------------

Gorilla and Delta-Delta is installed with Mars and as MARS's internal algorithm.

    create extension mars;

Usage
-----

In MARS, both Gorilla and Delta-Delta is not visible to users.
They work as an implicit option for float type.

##### Gorilla
When float data inserted into a MARS table, Gorilla automatically encode the
float data column.

    create table t1
        (c1 float4, c2 float8)
    using mars;

Gorilla can work with other compression algorithms.

    create table t2
        (c1 float4 ENCODING (compresstype=zstd),
         c2 float  ENCODING (compresstype=lz4))
    using mars;
    
##### Delta-Delta
When a column type is int4, int8, timestamp or timestamptz type, Delta-Delta
will encode automatically this column.

    create table t3
        (c1 int4, c2 timestamp)
    using mars;
    
Delta-Delta can also work with other compression algorithms just like the norm.

    create table t3
        (c1 int4 ENCODING (compresstype=zstd),
         c2 timestamp ENCODING (compresstype=lz4))
    using mars;
    
Best Practice
-----

- Both Gorilla and Delta-Delta are suitable for continuous and regular numbers.
Gorilla is efficient for floating-number and Delta-Delta is for integer number.
In turn, random numbers, are not efficient for them.

- Gorilla and Delta-Delta work with other compression applications can get a
high compression ratio.

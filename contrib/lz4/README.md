The LZ4 compression
===================

This module provides the LZ4 compression, which is very fast in compression and
decompression speed, but with worse compression ratio compared to zlib or zstd.

Installation
------------

Simply `make install` under the module dir to install it.

It is recommended to create a new cluster after the installation, so the LZ4
compression method is available in all the databases.

Usage
-----

The LZ4 method can be used anywhere where the zlib or zstd is useable, for
example:

    create table t1 (c1 int) with ( appendonly=true
                                  , orientation=column
                                  , compressmethod=lz4
                                  , compresslevel=1);

Note on the `compresslevel`, it has different meaning with the other
compression methods:

- for zlib or zstd this property specifies the compression level, higher levels
  usually lead to better compression ratio, at the cost of higher memory usage
  and slower compression speed;
- for lz4, this property is actually the acceleration level, higher levels lead
  to faster compression speed, at the cost of worse compression ratio;

The `compresslevel` can be within the range [1, 65537].

Compression ratio
-----------------

The LZ4 compression ratio does not always <= 1.0, it can even be > 1.0, which
means the compressed size can be greater than the uncompressed size due to the
LZ4 header.  So the module provides a check:

- when the compressed size < uncompressed size: store the compressed data;
- otherwise: store the uncompressed data;

A 4-byte header (not the LZ4 header) is stored to indicates the data is
compressed or uncompressed.

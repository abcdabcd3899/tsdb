/*-------------------------------------------------------------------------
 *
 * contrib/zstd/zstd_compression.h
 *	  The wrapper of zstd compression lib.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_ZSTD_COMPRESSION_H
#define MATRIXDB_ZSTD_COMPRESSION_H

#include "postgres.h"

extern int32 zstd_compress_data(const char *source, uint32 source_size,
								char *dest, uint32 dst_size);

extern int32 zstd_decompress_data(const char *source, uint32 source_size,
								  char *dest, uint32 dst_size);

extern uint32 zstd_compress_bound(uint32 source_size);

#endif /* MATRIXDB_ZSTD_COMPRESSION_H */

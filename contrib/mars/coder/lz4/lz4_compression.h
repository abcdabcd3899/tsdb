/*-------------------------------------------------------------------------
 *
 * contrib/lz4/lz4_compression.h
 *	  The wrapper of lz4 compression lib.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_LZ4_COMPRESSION_H
#define MATRIXDB_LZ4_COMPRESSION_H

#include "postgres.h"

extern int32 lz4_compress_data(const char *source, uint32 source_size,
							   char *dest, uint32 dst_size);

extern int32 lz4_decompress_data(const char *source, uint32 source_size,
								 char *dest, uint32 dst_size);

extern uint32 lz4_compress_bound(uint32 source_size);

#endif /* MATRIXDB_LZ4_COMPRESSION_H */

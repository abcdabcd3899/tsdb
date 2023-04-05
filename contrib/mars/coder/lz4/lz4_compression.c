/*-------------------------------------------------------------------------
 *
 * contrib/lz4/lz4_compression.c
 *	  The wrapper of lz4 compression lib.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#include "lz4_compression.h"

#include "lz4.h"

int32 lz4_compress_data(const char *source, uint32 source_size,
						char *dest, uint32 dst_size)
{
	return LZ4_compress_default(source, dest, source_size, dst_size);
}

int32 lz4_decompress_data(const char *source, uint32 source_size,
						  char *dest, uint32 dst_size)
{
	return LZ4_decompress_safe(source, dest, source_size, dst_size);
}

uint32
lz4_compress_bound(uint32 source_size)
{
	return LZ4_compressBound(source_size);
}

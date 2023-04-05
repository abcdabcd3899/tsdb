/*-------------------------------------------------------------------------
 *
 * contrib/zstd/zstd_compression.c
 *	  The wrapper of zstd compression lib.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#include "zstd_compression.h"

#include <zstd.h>

int32 zstd_compress_data(const char *source, uint32 source_size,
						 char *dest, uint32 dst_size)
{
	size_t code = ZSTD_compress(dest,
								dst_size,
								source,
								source_size,
								0 /* use default compression level */);

	if (ZSTD_isError(code))
	{
		elog(ERROR, "fail to compress data using zstd. %s", ZSTD_getErrorName(code));
	}

	return code;
}

int32 zstd_decompress_data(const char *source, uint32 source_size,
						   char *dest, uint32 dst_size)
{
	return ZSTD_decompress(dest, dst_size, source, source_size);
}

uint32
zstd_compress_bound(uint32 source_size)
{
	return ZSTD_compressBound(source_size);
}

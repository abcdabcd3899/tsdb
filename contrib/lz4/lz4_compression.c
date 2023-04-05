/*---------------------------------------------------------------------
 *
 * lz4_compression.c
 *
 * IDENTIFICATION
 *	    contrib/lz4/lz4_compression.c
 *
 *---------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "catalog/pg_compression.h"
#include "fmgr.h"
#include "storage/gp_compress.h"
#include "utils/builtins.h"

#include <lz4.h>

Datum		lz4_constructor(PG_FUNCTION_ARGS);
Datum		lz4_destructor(PG_FUNCTION_ARGS);
Datum		lz4_compress(PG_FUNCTION_ARGS);
Datum		lz4_decompress(PG_FUNCTION_ARGS);
Datum		lz4_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(lz4_constructor);
PG_FUNCTION_INFO_V1(lz4_destructor);
PG_FUNCTION_INFO_V1(lz4_compress);
PG_FUNCTION_INFO_V1(lz4_decompress);
PG_FUNCTION_INFO_V1(lz4_validator);

#ifndef UNIT_TESTING
PG_MODULE_MAGIC;
#endif

/* Internal state for lz4 */
typedef struct lz4_state
{
	int			level;			/* Compression level */
	bool		compress;		/* Compress if true, decompress otherwise */

	void	   *compressState;	/* lz4 compression state */
} lz4_state;

/* on-disk header for the compressed data */
typedef struct lz4_header
{
	/*
	 * - compressed == 0: uncompressed data is stored
	 * - compressed == 1: compressed lz4 data is stored
	 *
	 * FIXME: we need to design the header more carefully, it can not be
	 * changed in a stable relase version.
	 */
	int			compressed;

	char		data[0];
} lz4_header;

Datum
lz4_constructor(PG_FUNCTION_ARGS)
{
	/* PG_GETARG_POINTER(0) is TupleDesc that is currently unused. */

	StorageAttributes *sa = (StorageAttributes *) PG_GETARG_POINTER(1);
	CompressionState *cs = palloc0(sizeof(CompressionState));
	lz4_state  *state = palloc(sizeof(lz4_state));
	bool		compress = PG_GETARG_BOOL(2);

	if (!PointerIsValid(sa->comptype))
		elog(ERROR, "lz4_constructor called with no compression type");

	cs->opaque = (void *) state;
	cs->desired_sz = NULL;

	if (sa->complevel == 0)
		sa->complevel = 1;

	state->level = sa->complevel;
	state->compress = compress;

	if (compress)
	{
		state->compressState = palloc(LZ4_sizeofState());

		if (!state->compressState)
			elog(ERROR, "lz4: out of memory");
	}
	else
		state->compressState = NULL;

	PG_RETURN_POINTER(cs);
}

Datum
lz4_destructor(PG_FUNCTION_ARGS)
{
	CompressionState *cs = (CompressionState *) PG_GETARG_POINTER(0);

	if (cs != NULL && cs->opaque != NULL)
	{
		lz4_state  *state = (lz4_state *) cs->opaque;

		if (state->compressState)
			pfree(state->compressState);

		pfree(state);
	}

	PG_RETURN_VOID();
}

/*
 * lz4 compression implementation
 *
 * Note that when compression fails due to algorithm inefficiency,
 * dst_used is set so src_sz, but the output buffer contents are left unchanged
 */
Datum
lz4_compress(PG_FUNCTION_ARGS)
{
	const void *src = PG_GETARG_POINTER(0);
	int32		src_sz = PG_GETARG_INT32(1);
	void	   *dst = PG_GETARG_POINTER(2);
	int32		dst_sz = PG_GETARG_INT32(3);
	int32	   *dst_used = (int32 *) PG_GETARG_POINTER(4);
	CompressionState *cs = (CompressionState *) PG_GETARG_POINTER(5);
	lz4_state  *state = (lz4_state *) cs->opaque;

	/*
	 * XXX: the "acceleration level" in LZ4 is actually an opposite concept to
	 * the usual "compression level", a higher acceleration results in faster
	 * compression & decompression, but lesser compression ratio.
	 */
	int			acceleration = state->level;
	int32		dst_length_used;
	lz4_header *header = dst;

	dst_length_used = LZ4_compress_fast_extState(state->compressState,
												 src,
												 header->data,
												 src_sz,
												 dst_sz - sizeof(*header),
												 acceleration);

	if (dst_length_used == 0 ||		/* the dst buffer is not large enough, or */
		dst_length_used >= src_sz)	/* the compression has no effect */
	{
		/* store the uncompressed data instead */
		memcpy(header->data, src, src_sz);
		dst_length_used = src_sz + sizeof(*header);
		header->compressed = 0;
	}
	else
	{
		dst_length_used += sizeof(*header);
		header->compressed = 1;
	}

	*dst_used = (int32) dst_length_used;

	PG_RETURN_VOID();
}

Datum
lz4_decompress(PG_FUNCTION_ARGS)
{
	const void *src = PG_GETARG_POINTER(0);
	int32		src_sz = PG_GETARG_INT32(1);
	void	   *dst = PG_GETARG_POINTER(2);
	int32		dst_sz = PG_GETARG_INT32(3);
	int32	   *dst_used = (int32 *) PG_GETARG_POINTER(4);

	int32		dst_length_used;
	const lz4_header *header = src;

	if (src_sz <= 0)
		elog(ERROR, "invalid source buffer size %d", src_sz);
	if (dst_sz <= 0)
		elog(ERROR, "invalid destination buffer size %d", dst_sz);

	if (header->compressed)
	{
		dst_length_used = LZ4_decompress_safe(header->data,
											  dst,
											  src_sz - sizeof(*header),
											  dst_sz);

		if (dst_length_used <= 0)
			elog(ERROR, "lz4_decompress(): failed to decompress");
	}
	else
	{
		if (dst_sz < src_sz - sizeof(*header))
			elog(ERROR,
				 "lz4_decompress(): the dst buffer is not large enough,"
				 " %d bytes provided, %zd bytes required",
				 dst_sz, src_sz - sizeof(*header));

		memcpy(dst, header->data, src_sz - sizeof(*header));
		dst_length_used = src_sz - sizeof(*header);
	}

	*dst_used = (int32) dst_length_used;

	PG_RETURN_VOID();
}

Datum
lz4_validator(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

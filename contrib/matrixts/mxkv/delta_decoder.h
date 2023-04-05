/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/delta_decoder.h
 *	  Delta and varlen decoder.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_DELTA_DECODER_H
#define MXKV_DELTA_DECODER_H

#include "postgres.h"

#include "mxkv/bits_reader.h"
#include "mxkv/bits_writer.h"

typedef struct DeltaDecoder
{
	BitsReader reader;
	int64 autoinc; /* autoinc, see comments in delta_encoder_begin() */
} DeltaDecoder;

typedef struct DeltaDecoderLookup
{
	int32 key; /* the key to lookup during the decoding */
	int nth;   /* the index of the matching key, -1 if not found */
} DeltaDecoderLookup;

extern char *delta_decode_into_array(const char *data, BitsWriterWidth width,
									 int64 base, int64 autoinc, char *xs, int n,
									 DeltaDecoderLookup *lookup);

#endif /* MXKV_DELTA_DECODER_H */

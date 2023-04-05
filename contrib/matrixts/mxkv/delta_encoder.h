/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/delta_encoder.h
 *	  Delta and varlen encoder.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_DELTA_ENCODER_H
#define MXKV_DELTA_ENCODER_H

#include "postgres.h"

#include "mxkv/bits_writer.h"
#include "mxkv/bits_writer.h"

typedef struct DeltaEncoder
{
	BitsWriter *writer; /* the bits writer */
	BitsWriterWidth width;

	int64 last;	   /* the last value */
	int64 autoinc; /* autoinc, see comments in delta_encoder_begin() */
} DeltaEncoder;

extern void delta_encoder_begin(DeltaEncoder *encoder, BitsWriter *writer,
								BitsWriterWidth width,
								int64 base, int64 autoinc);
extern void delta_encoder_end(DeltaEncoder *encoder);
extern void delta_encoder_push(DeltaEncoder *encoder, int64 x);

#endif /* MXKV_DELTA_ENCODER_H */

/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/delta_coding.h
 *	  Common parts of the delta encoder and decoder.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_DELTA_CODING_H
#define MXKV_DELTA_CODING_H

#include "postgres.h"

typedef struct DeltaCodingSchema
{
	uint8 leader_bits;
	int8 leader_nbits;
	int8 body_nbits;
	int64 min;
	int64 max;
	int64 sub;
} DeltaCodingSchema;

extern const DeltaCodingSchema *delta_coding_schema32_get_by_leader(uint8 leader);
extern const DeltaCodingSchema *delta_coding_schema32_get_by_value(int32 x);
extern const DeltaCodingSchema *delta_coding_schema64_get_by_leader(uint8 leader);
extern const DeltaCodingSchema *delta_coding_schema64_get_by_value(int64 x);

#endif /* MXKV_DELTA_CODING_H */

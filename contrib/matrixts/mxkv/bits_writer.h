/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/bits_writer.h
 *	  BitsWriter writes to a StringInfo bit by bit.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_BITS_WRITER_H
#define MXKV_BITS_WRITER_H

#include "postgres.h"

#include "lib/stringinfo.h"

/*
 * A short code to indicate the width of a type.
 *
 * It is not used by BitsWriter itself, it is put here simply because all the
 * users include this header directly or indirectly.
 */
typedef enum
{
	BITS_WRITER_WIDTH_INVALID = -1,

	BITS_WRITER_WIDTH_1B = 0,
	BITS_WRITER_WIDTH_2B = 1,
	BITS_WRITER_WIDTH_4B = 2,
	BITS_WRITER_WIDTH_8B = 3,
} BitsWriterWidth;

typedef struct BitsWriter
{
	StringInfo bytes;
	int nbits; /* # of unwritten bits in the current byte */
} BitsWriter;

/*
 * Generate a bit mask with n '1' bits on the LSB side.
 *
 * The n must be within [0, 64].
 */
static inline uint64 pg_attribute_unused()
	bits_writer_mask(int nbits)
{
	Assert(nbits >= 0 && nbits <= 64);

	return (1LLU << nbits) - 1;
}

extern void bits_writer_init(BitsWriter *writer, StringInfo str);
extern void bits_writer_pad(BitsWriter *writer);
extern void bits_writer_push_bit(BitsWriter *writer, uint8 bit);
extern void bits_writer_push_bits(BitsWriter *writer, uint32 bits, int nbits);
extern void bits_writer_push_bits64(BitsWriter *writer, uint64 bits, int nbits);

static inline uint8 pg_attribute_unused()
	bits_writer_width_from_code(uint8 code)
{
	switch (code)
	{
	case BITS_WRITER_WIDTH_1B:
		return 1;
	case BITS_WRITER_WIDTH_2B:
		return 2;
	case BITS_WRITER_WIDTH_4B:
		return 4;
	case BITS_WRITER_WIDTH_8B:
		return 8;
	default:
		pg_unreachable();
	}
}

#endif /* MXKV_BITS_WRITER_H */

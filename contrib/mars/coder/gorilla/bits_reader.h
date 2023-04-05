/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/bits_reader.h
 *	  BitsWriter reads a bit stream.
 *
 * All the functions are inlined, otherwise the code is 30~50% slower.
 *
 * TODO: this file is copied from mxkv, later we should find a way to share
 * the code instead of copying.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_BITS_READER_H
#define MXKV_BITS_READER_H

#include "postgres.h"

/*
 * Define this to preload at most 32 bits, this could lead to at most 4 bytes
 * over accessing after the end of the data, the benefit is the code is about
 * ~5% faster.
 */
#if 0
#define MXKV_DELTA_DECODER_PRELOAD
#endif

typedef struct BitsReader
{
	const uint8 *bytes; /* points to the first unloaded byte */
	uint64 bits;		/* bits buffer, loaded from bytes */
	int nbits;			/* # of bits in the bits buffer */
} BitsReader;

static inline void pg_attribute_unused()
	bits_reader_init(BitsReader *reader, const char *data)
{
	reader->bytes = (const uint8 *)data;
	reader->bits = 0;
	reader->nbits = 0;
}

/*
 * Get the position of the next byte.
 *
 * The next byte is the first byte that contains no read bit.
 */
static inline char *pg_attribute_unused()
	bits_reader_next_byte(BitsReader *reader)
{
#ifdef USE_ASSERT_CHECKING
	while (reader->nbits >= 8)
	{
		/*
		 * this happens when we seeked more bits than needed, for example we
		 * have 3 bits in the buffer, to read a leader we need to seek 4 bits,
		 * so we have to load the next byte, so there are 11 bits in the
		 * buffer, then if the leader is 0, then we only need to consume 1 bit,
		 * so there are still 10 bits left in the buffer.
		 */
		reader->bytes--;
		Assert(reader->bytes[0] == (reader->bits & 0xff));

		reader->bits >>= 8;
		reader->nbits -= 8;
	}
#else  /* USE_ASSERT_CHECKING */
	if (reader->nbits >= 8)
	{
		int nbytes = reader->nbits / 8;

		reader->bytes -= nbytes;
		reader->bits >>= nbytes * 8;
		reader->nbits -= nbytes * 8;
	}
#endif /* USE_ASSERT_CHECKING */

	return (char *)reader->bytes;
}

/*
 * Ensure that there are at least n bits in the bits buffer.
 */
static inline void pg_attribute_unused()
	bits_reader_ensure_bits(BitsReader *reader, int nbits)
{
	int need;

	Assert(nbits >= 0 && nbits <= 32);
	need = nbits - reader->nbits;

#ifdef MXKV_DELTA_DECODER_PRELOAD
	/* preload some more bits */
	if (need > 0)
	{
		/*
		 * load 4 bytes in big-endian order, the bytes might not be aligned
		 * as uint32, but modern compilers and processors should be able to
		 * handle this, otherwise we could write it as below:
		 *
		 *		reader->bits |= (((uint64) reader->bytes[0] << 24) |
		 *						 ((uint64) reader->bytes[1] << 16) |
		 *						 ((uint64) reader->bytes[2] <<  8) |
		 *						 ((uint64) reader->bytes[3] <<  0));
		 */
		reader->bits <<= 32;
		reader->bits |= ntohl(*(uint32 *)reader->bytes);
		reader->nbits += 32;
		reader->bytes += 4;
	}
#else  /* MXKV_DELTA_DECODER_PRELOAD */
	if (need > 24)
	{
		reader->bits <<= 32;
		reader->bits |= (((uint64)reader->bytes[0] << 24) |
						 ((uint64)reader->bytes[1] << 16) |
						 ((uint64)reader->bytes[2] << 8) |
						 ((uint64)reader->bytes[3] << 0));
		reader->nbits += 32;
		reader->bytes += 4;
	}
	else if (need > 16)
	{
		reader->bits <<= 24;
		reader->bits |= (((uint64)reader->bytes[0] << 16) |
						 ((uint64)reader->bytes[1] << 8) |
						 ((uint64)reader->bytes[2] << 0));
		reader->nbits += 24;
		reader->bytes += 3;
	}
	else if (need > 8)
	{
		reader->bits <<= 16;
		reader->bits |= (((uint64)reader->bytes[0] << 8) |
						 ((uint64)reader->bytes[1] << 0));
		reader->nbits += 16;
		reader->bytes += 2;
	}
	else if (need > 0)
	{
		reader->bits <<= 8;
		reader->bits |= (uint64)reader->bytes[0];
		reader->nbits += 8;
		reader->bytes += 1;
	}
#endif /* MXKV_DELTA_DECODER_PRELOAD */
}

/*
 * Peek n bits, but do not consume them.
 *
 * The peeked bits must be consumed in time by calling
 * bits_reader_consume_bits().
 */
static inline uint32 pg_attribute_unused()
	bits_reader_peek_bits(BitsReader *reader, int nbits)
{
	bits_reader_ensure_bits(reader, nbits);

	return reader->bits >> (reader->nbits - nbits);
}

/*
 * Consume n bits.
 *
 * This is usually called after the bits_reader_peek_bits().
 */
static inline void pg_attribute_unused()
	bits_reader_consume_bits(BitsReader *reader, int nbits)
{
	Assert(reader->nbits >= nbits);

	reader->nbits -= nbits;
	reader->bits &= (1LLU << reader->nbits) - 1;
}

/*
 * Read at most 32 bits.
 */
static inline uint64 pg_attribute_unused()
	bits_reader_read_bits(BitsReader *reader, int nbits)
{
	uint64 bits;
	int shift;

	bits_reader_ensure_bits(reader, nbits);

	shift = reader->nbits - nbits;
	bits = reader->bits >> shift;  /* read the target bits */
	reader->bits ^= bits << shift; /* clear the read bits */
	reader->nbits = shift;

	return bits;
}

/*
 * Read at most 64 bits.
 */
static inline uint64 pg_attribute_unused()
	bits_reader_read_bits64(BitsReader *reader, int nbits)
{
	if (unlikely(nbits > 32))
		return (bits_reader_read_bits(reader, nbits - 32) << 32) |
			   bits_reader_read_bits(reader, 32);
	else
		return bits_reader_read_bits(reader, nbits);
}

#endif /* MXKV_BITS_READER_H */

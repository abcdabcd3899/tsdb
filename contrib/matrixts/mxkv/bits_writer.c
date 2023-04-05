/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/bits_writer.c
 *	  BitsWriter writes to a StringInfo bit by bit.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "mxkv/bits_writer.h"

/*
 * Initialize a BitsWriter with the given StringInfo.
 *
 * The bits writer will append bits to the str.
 */
void bits_writer_init(BitsWriter *writer, StringInfo str)
{
	writer->bytes = str;
	writer->nbits = 0;
}

/*
 * Pad all the unwritten bits with '0' bits in the current byte.
 */
void bits_writer_pad(BitsWriter *writer)
{
	writer->nbits = 0;
}

/*
 * Ensure that there are at least 1 unwritten bit in the current byte.
 *
 * If the current byte is full then move to the next byte.
 */
static void
bits_writer_ensure_space(BitsWriter *writer)
{
	if (unlikely(writer->nbits == 0))
	{
		appendStringInfoCharMacro(writer->bytes, 0);
		writer->nbits = 8;
	}
}

/*
 * Get the cursor to write.
 *
 * The cursor is ensured to have 1~8 unwritten bits.
 */
static uint8 *
bits_writer_cursor(BitsWriter *writer)
{
	bits_writer_ensure_space(writer);

	return (uint8 *)&writer->bytes->data[writer->bytes->len - 1];
}

/*
 * Push 1 bit to the bits writer.
 *
 * The bit is on the LSB side of "bit".
 */
void bits_writer_push_bit(BitsWriter *writer, uint8 bit)
{
	uint8 *cursor = bits_writer_cursor(writer);

	*cursor |= (bit & 1) << (writer->nbits - 1);
	writer->nbits -= 1;
}

/*
 * Push n bits to the bits writer.
 *
 * The bits are on the LSB side of "bits".
 */
void bits_writer_push_bits(BitsWriter *writer, uint32 bits, int nbits)
{
	while (nbits > 0)
	{
		uint8 *cursor = bits_writer_cursor(writer);
		int n = Min(nbits, writer->nbits);

		/* push the top n bits at once */
		*cursor |= (((bits >> (nbits - n)) & bits_writer_mask(n)) << (writer->nbits - n));

		writer->nbits -= n;
		nbits -= n;
	}
}

void bits_writer_push_bits64(BitsWriter *writer, uint64 bits, int nbits)
{
	if (unlikely(nbits > 32))
	{
		bits_writer_push_bits(writer, bits >> 32, nbits - 32);

#ifdef USE_ASSERT_CHECKING
		bits &= 0xffffffffLU;
#endif /* USE_ASSERT_CHECKING */
		nbits = 32;
	}

	bits_writer_push_bits(writer, bits, nbits);
}

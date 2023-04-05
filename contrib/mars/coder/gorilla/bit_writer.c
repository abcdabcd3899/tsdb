/*-------------------------------------------------------------------------
 *
 * contrib/mars/coder/gorilla/bit_writer.h
 *	  Bit writer.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "bit_writer.h"

void bitwriter_construct(BitWriter *bit_writer, char *begin, char *end)
{
	bit_writer->dest_begin = begin;
	bit_writer->dest_current = begin;
	bit_writer->dest_end = end;
	bit_writer->bits_buffer = 0;
	bit_writer->bits_count = 0;
	bit_writer->buffer_overflow = false;
}

static void
bitwriter_do_flush(BitWriter *bit_writer)
{
	size_t available_size;
	size_t write_size;

	available_size = bit_writer->dest_end - bit_writer->dest_current;
	write_size = 1;

	/*
	 * No more space left, set buffer_overflow to true.
	 * Abandon flush operation.
	 */
	if (available_size < write_size)
	{
		bit_writer->buffer_overflow = true;
		return;
	}

	*(bit_writer->dest_current++) = bit_writer->bits_buffer;
	bit_writer->bits_count = 0;
	bit_writer->bits_buffer = 0;
}

void bitwriter_flush(BitWriter *bit_writer)
{
	if (bit_writer->bits_count != 0)
		bitwriter_do_flush(bit_writer);
}

void bitwriter_write_bits(BitWriter *bit_writer, uint8 bits_size, uint64 value)
{
	Assert(bits_size > 0);
	value <<= sizeof(value) * 8 - bits_size;

	while (bits_size > 0)
	{
		uint8 nbits;
		uint8 write_bits;

		if (bit_writer->bits_count == 8)
		{
			/*
			 * No more space left, set buffer_overflow to true.
			 * Abandon write operation.
			 */
			if (bit_writer->dest_end <= bit_writer->dest_current + 1)
			{
				bit_writer->buffer_overflow = true;
				return;
			}

			*(bit_writer->dest_current++) = bit_writer->bits_buffer;
			bit_writer->bits_count = 0;
			bit_writer->bits_buffer = 0;
		}

		nbits = Min(8 - bit_writer->bits_count, bits_size);
		write_bits = value >> (sizeof(value) * 8 - nbits);
		write_bits <<= (8 - bit_writer->bits_count - nbits);

		bit_writer->bits_buffer |= write_bits;
		bit_writer->bits_count += nbits;

		bits_size -= nbits;
		value <<= nbits;
	}
}

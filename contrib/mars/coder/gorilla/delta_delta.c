/*-------------------------------------------------------------------------
 *
 * contrib/gorilla/delta_delta.h
 *	  delta delta encoding method.
 *
 * This implements the Facebook Gorilla [1] encoding.
 *
 * [1]: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "port/pg_bitutils.h"

#include "delta_delta.h"

#include "bit_writer.h"
#include "bits_reader.h"

typedef struct WriteSpec
{
	uint8 prefix_bits;
	uint8 prefix;
	uint8 data_bits;
} WriteSpec;

/*
 * delta size prefix and data lengths based on
 * few high bits peeked from binary stream
 */
const WriteSpec WRITE_SPEC_LUT[32] = {
	// 0b0 - 1-bit prefix, no data to read
	/* 00000 */ {1, 0b0, 0},
	/* 00001 */ {1, 0b0, 0},
	/* 00010 */ {1, 0b0, 0},
	/* 00011 */ {1, 0b0, 0},
	/* 00100 */ {1, 0b0, 0},
	/* 00101 */ {1, 0b0, 0},
	/* 00110 */ {1, 0b0, 0},
	/* 00111 */ {1, 0b0, 0},
	/* 01000 */ {1, 0b0, 0},
	/* 01001 */ {1, 0b0, 0},
	/* 01010 */ {1, 0b0, 0},
	/* 01011 */ {1, 0b0, 0},
	/* 01100 */ {1, 0b0, 0},
	/* 01101 */ {1, 0b0, 0},
	/* 01110 */ {1, 0b0, 0},
	/* 01111 */ {1, 0b0, 0},

	// 0b10 - 2 bit prefix, 7 bits of data
	/* 10000 */ {2, 0b10, 7},
	/* 10001 */ {2, 0b10, 7},
	/* 10010 */ {2, 0b10, 7},
	/* 10011 */ {2, 0b10, 7},
	/* 10100 */ {2, 0b10, 7},
	/* 10101 */ {2, 0b10, 7},
	/* 10110 */ {2, 0b10, 7},
	/* 10111 */ {2, 0b10, 7},

	// 0b110 - 3 bit prefix, 9 bits of data
	/* 11000 */ {3, 0b110, 9},
	/* 11001 */ {3, 0b110, 9},
	/* 11010 */ {3, 0b110, 9},
	/* 11011 */ {3, 0b110, 9},

	// 0b1110 - 4 bit prefix, 12 bits of data
	/* 11100 */ {4, 0b1110, 12},
	/* 11101 */ {4, 0b1110, 12},

	// 5-bit prefixes
	/* 11110 */ {5, 0b11110, 32},
	/* 11111 */ {5, 0b11111, 64},
};

static void
getDeltaWriteSpec(WriteSpec *write_spec, int64_t dd)
{
	if (dd > -63 && dd < 64)
		*write_spec = (WriteSpec){.prefix_bits = 2, .prefix = 0b10, .data_bits = 7};
	else if (dd > -255 && dd < 256)
		*write_spec = (WriteSpec){.prefix_bits = 3, .prefix = 0b110, .data_bits = 9};
	else if (dd > -2047 && dd < 2048)
		*write_spec = (WriteSpec){.prefix_bits = 4, .prefix = 0b1110, .data_bits = 12};
	else if (dd > INT32_MIN && dd < INT32_MAX)
		*write_spec = (WriteSpec){.prefix_bits = 5, .prefix = 0b11110, .data_bits = 32};
	else
		*write_spec = (WriteSpec){.prefix_bits = 5, .prefix = 0b11111, .data_bits = 64};
}

int32 delta_delta_compress_data(const char *source, uint32 source_size,
								char *dest, uint32 dst_size)
{
	const char *source_end;
	char *dest_start;
	Datum prev_value;
	Datum curr_value;
	Datum prev_delta;
	Datum curr_delta;
	Datum delta_delta;
	uint32 items_count;
	BitWriter bit_writer;

	/*
	 * calculate total count,
	 * write total count.
	 */
	{
		dest_start = dest;
		source_end = source + source_size;
		items_count = source_size / sizeof(Datum);

		memcpy(dest, &items_count, sizeof(items_count));
		dest += sizeof(items_count);
	}

	/*
	 * read first value,
	 * write first value.
	 */
	if (source < source_end)
	{
		memcpy(&prev_value, source, sizeof(prev_value));
		memcpy(dest, &prev_value, sizeof(prev_value));

		source += sizeof(prev_value);
		dest += sizeof(prev_value);
	}

	/*
	 * read second value,
	 * write first delta.
	 */
	if (source < source_end)
	{
		memcpy(&curr_value, source, sizeof(curr_value));
		source += sizeof(curr_value);

		prev_delta = curr_value - prev_value;
		memcpy(dest, &prev_delta, sizeof(prev_delta));

		dest += sizeof(prev_delta);
		prev_value = curr_value;
	}

	/*
	 * iterate read values,
	 * write delta delta.
	 */
	bitwriter_construct(&bit_writer, dest, dest + dst_size);
	while (source < source_end)
	{
		memcpy(&curr_value, source, sizeof(curr_value));
		source += sizeof(curr_value);

		curr_delta = curr_value - prev_value;
		delta_delta = curr_delta - prev_delta;

		prev_delta = curr_delta;
		prev_value = curr_value;

		if (delta_delta == 0)
		{
			bitwriter_write_bits(&bit_writer, 1, 0);
		}
		else
		{
			/*
			 * sign: if delta_delta < 0: then -1
			 *       else              : then  1
			 */
			int8 sign = delta_delta < 0;
			long long abs_value = llabs(delta_delta) - 1;

			WriteSpec write_spec;
			getDeltaWriteSpec(&write_spec, (int64_t)delta_delta);

			bitwriter_write_bits(&bit_writer, write_spec.prefix_bits,
								 write_spec.prefix);
			bitwriter_write_bits(&bit_writer, 1, sign);
			bitwriter_write_bits(&bit_writer, write_spec.data_bits - 1,
								 abs_value);
		}
	}

	bitwriter_flush(&bit_writer);

	return bit_writer.dest_current - dest_start;
}

uint32
delta_delta_decompress_data(const char *source, uint32 source_size, char *dest)
{
	const char *source_end;
	char *dest_start;
	uint32 items_count;
	uint32 items_read;
	Datum prev_value;
	Datum curr_value;
	Datum prev_delta;
	Datum curr_delta;
	BitsReader bit_reader;
	WriteSpec write_spec;
	uint8 sign;

	dest_start = dest;
	source_end = source + source_size;

	/*
	 * if nothing to read, try early end.
	 */
	if (source + sizeof(uint32) > source_end)
		return 0;

	/*
	 * read total count.
	 */
	memcpy(&items_count, source, sizeof(items_count));
	source += sizeof(items_count);

	/*
	 * if total count == 0, try early end.
	 */
	if (source + sizeof(Datum) > source_end || items_count < 1)
		return 0;

	/*
	 * read first value.
	 */
	{
		memcpy(&prev_value, source, sizeof(prev_value));
		memcpy(dest, &prev_value, sizeof(prev_value));

		source += sizeof(prev_value);
		dest += sizeof(prev_value);
	}

	/*
	 * if total count == 1, try early end.
	 */
	if (source + sizeof(Datum) > source_end || items_count < 2)
		return dest - dest_start;

	/*
	 * read second value,
	 * calculate first delta.
	 */
	{
		memcpy(&prev_delta, source, sizeof(prev_delta));
		prev_value = prev_value + prev_delta;
		memcpy(dest, &prev_value, sizeof(prev_value));

		source += sizeof(prev_delta);
		dest += sizeof(prev_value);
	}

	/*
	 * iterate each delta delta,
	 * calculate delta and value.
	 */
	bits_reader_init(&bit_reader, source);
	for (items_read = 2; items_read < items_count; ++items_read)
	{
		Datum delta_delta = 0;

		Assert(sizeof(WRITE_SPEC_LUT) / sizeof(WRITE_SPEC_LUT[0]) == 32);
		write_spec = WRITE_SPEC_LUT[bits_reader_peek_bits(&bit_reader, 5)];

		bits_reader_consume_bits(&bit_reader, write_spec.prefix_bits);
		if (write_spec.data_bits != 0)
		{
			sign = bits_reader_read_bits(&bit_reader, 1);
			delta_delta = bits_reader_read_bits64(&bit_reader, write_spec.data_bits - 1) + 1;

			if (sign)
			{
				delta_delta = -delta_delta;
			}
		}

		curr_delta = delta_delta + prev_delta;
		curr_value = prev_value + curr_delta;
		memcpy(dest, &curr_value, sizeof(curr_value));
		dest += sizeof(curr_value);

		prev_delta = curr_value - prev_value;
		prev_value = curr_value;
	}
	return dest - dest_start;
}

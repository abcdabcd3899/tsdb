/*-------------------------------------------------------------------------
 *
 * contrib/gorilla/gorilla.c
 *	  The gorilla encoding method.
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

#include "gorilla.h"

#include "bit_writer.h"
#include "bits_reader.h"

/*
 * Swap any type pointers.
 */
#define swap_ptr(ptr1, ptr2) \
	do                       \
	{                        \
		void *tmp;           \
		tmp = ptr1;          \
		ptr1 = ptr2;         \
		ptr2 = tmp;          \
	} while (0);

typedef struct BinaryValueInfo
{
	/*
	 * For 4bytes data, the length of meaningful bits is between 0 ~ 64.
	 */
	int8 leading_zero_size;
	int8 data_size;
	int8 trailing_zero_size;
} BinaryValueInfo;

typedef uint64 ValueType;

#define VALUE_TYPE_WIDTH_IN_BYTES sizeof(ValueType)
#define VALUE_TYPE_WIDTH_IN_BITS (VALUE_TYPE_WIDTH_IN_BYTES * 8)

/*
 * 1-byte value is 8 bits, and we need 4 bits to represent 8 : 1000,
 * 2-byte         16 bits        =>    5
 * 4-byte         32 bits        =>    6
 * 8-byte         64 bits        =>    7
 */
#define DATA_BIT_LENGTH 7

/*
 * -1 since there must be at least 1 non-zero bit.
 */
#define LEADING_ZEROES_BIT_LENGTH (DATA_BIT_LENGTH - 1)

/*
 * The gorilla data header.
 *
 * All the fields are in native byte-order, that is little-endian on PC.
 */
typedef struct pg_attribute_packed() _Header
{
	uint64 count; /* items count */

	ValueType first_value[]; /* the first value, optional */
}
_Header;

static void
getLeadingAndTrailingBits(ValueType binary_value, BinaryValueInfo *binary_info)
{
	binary_info->leading_zero_size = binary_value != 0 ? 63 - pg_leftmost_one_pos64(binary_value) : 63;
	binary_info->trailing_zero_size = binary_value != 0 ? pg_rightmost_one_pos64(binary_value) : 1;
	binary_info->data_size = 64 - binary_info->leading_zero_size - binary_info->trailing_zero_size;
}

/*
 * store count of leading zeros and trailing zeros.
 */
static void
binary_value_info_init(BinaryValueInfo *binaryValueInfo)
{
	binaryValueInfo->leading_zero_size = 0;
	binaryValueInfo->data_size = 0;
	binaryValueInfo->trailing_zero_size = 0;
}

/*
 * ret: -1    buffer overflow, fail to compress.
 */
int32 gorilla_compress_data(const char *source, uint32 source_size,
							char *dest, uint32 dst_size)
{
	const char *source_end;
	char *dest_start = dest;
	char *dest_end;
	uint64 items_count;
	ValueType prev_value;
	ValueType curr_value;
	ValueType xor_value;
	BitWriter bit_writer;
	BinaryValueInfo prev_xored_info0;
	BinaryValueInfo curr_xored_info0;
	BinaryValueInfo *prev_xored_info = &prev_xored_info0;
	BinaryValueInfo *curr_xored_info = &curr_xored_info0;

	source_end = source + source_size;
	dest_end = dest + dst_size;
	items_count = source_size / sizeof(ValueType);

	/*
	 * store count of {source items}
	 */
	memcpy(dest, &items_count, sizeof(items_count));
	dest += sizeof(items_count);

	binary_value_info_init(prev_xored_info);
	binary_value_info_init(curr_xored_info);

	/*
	 * Store first item with no compression.
	 */
	if (source < source_end)
	{
		memcpy(&prev_value, source, VALUE_TYPE_WIDTH_IN_BYTES);
		memcpy(dest, &prev_value, VALUE_TYPE_WIDTH_IN_BYTES);

		source += sizeof(prev_value);
		dest += sizeof(prev_value);
	}
	/*
	 * begin to compress
	 */
	bitwriter_construct(&bit_writer, dest, dest_end);

	while (source < source_end)
	{
		memcpy(&curr_value, source, VALUE_TYPE_WIDTH_IN_BYTES);
		source += sizeof(curr_value);

		xor_value = prev_value ^ curr_value;
		getLeadingAndTrailingBits(xor_value, curr_xored_info);

		/*
		 * case1: same value
		 */
		if (xor_value == 0)
			bitwriter_write_bits(&bit_writer, 1, 0);
		/*
		 * case2: falls, store -> {1,0}, {meaningful xor}
		 */
		else if (prev_xored_info->data_size != 0 &&
				 prev_xored_info->leading_zero_size <= curr_xored_info->leading_zero_size &&
				 prev_xored_info->trailing_zero_size <= curr_xored_info->trailing_zero_size)
		{
			/*
			 * example:
			 *      xor_value: 0010 1100
			 *
			 *      sign:             10
			 *      cut xor trailing zeros
			 *      then       0000 1011
			 *
			 *      final:     **10 1011
			 */
			bitwriter_write_bits(&bit_writer, 2, 0x2);
			bitwriter_write_bits(&bit_writer, prev_xored_info->data_size,
								 xor_value >> prev_xored_info->trailing_zero_size);
		}
		/*
		 * case3: not falls, store -> {leading_zero}, {meaningful xor}
		 */
		else
		{
			/*
			 * example:
			 *      xor_value: 1110 1100
			 *
			 *      sign:             11
			 *      leading zeros: DATA_BIT_LENGTH - 1 = 4
			 *                      0000
			 *      data size:     DATA_BIT_LENGTH = 5
			 *                     00110
			 *
			 *      cut xor trailing zeros
			 *      then xor               111011
			 *      final     1100 0000 1011 1011
			 *
			 */
			bitwriter_write_bits(&bit_writer, 2, 0x3);
			bitwriter_write_bits(&bit_writer, LEADING_ZEROES_BIT_LENGTH,
								 curr_xored_info->leading_zero_size);
			bitwriter_write_bits(&bit_writer, DATA_BIT_LENGTH,
								 curr_xored_info->data_size);
			bitwriter_write_bits(&bit_writer, curr_xored_info->data_size,
								 xor_value >> curr_xored_info->trailing_zero_size);
			swap_ptr(prev_xored_info, curr_xored_info);
		}

		if (bit_writer.buffer_overflow)
			return -1;

		/* move to next series data window */
		prev_value = curr_value;
	}
	bitwriter_flush(&bit_writer);
	return bit_writer.dest_current - dest_start;
}

static uint32
gorilla_read_leader(BitsReader *reader)
{
	uint32 leader = bits_reader_peek_bits(reader, 2);

	if (leader < 0x02)
		bits_reader_consume_bits(reader, 1);
	else
		bits_reader_consume_bits(reader, 2);

	return leader;
}

uint32
gorilla_decompress_data(const char *source, uint32 source_size, char *dest)
{
	_Header *header;
	BitsReader bit_reader;
	char *dest_start;
	const char *source_end;
	uint64 items_count;
	uint64 items_read;
	BinaryValueInfo prev_xored_info0;
	BinaryValueInfo curr_xored_info0;
	BinaryValueInfo *prev_xored_info = &prev_xored_info0;
	BinaryValueInfo *curr_xored_info = &curr_xored_info0;
	ValueType prev_value;

	if (source_size < sizeof(header))
		/* no enought source data.  TODO: should this be an error? */
		return 0;

	dest_start = dest;
	source_end = source + source_size;

	/*
	 * read the header, the source data might not be properly aligned, however
	 * modern compilers and cpus should be able to handle this.
	 */
	header = (_Header *)source;
	source += sizeof(header);

	if (header->count == 0)
		/* empty, done */
		return 0;
	else
		/* non-empty, the first_value is available */
		source += sizeof(header->first_value[0]);

	items_count = header->count;
	prev_value = header->first_value[0];

	/* output the first value */
	memcpy(dest, &prev_value, sizeof(prev_value));
	dest += sizeof(prev_value);

	/* time to read the other values */
	bits_reader_init(&bit_reader, source);

	binary_value_info_init(prev_xored_info);
	binary_value_info_init(curr_xored_info);

	for (items_read = 1; items_read < items_count; ++items_read)
	{
		uint32 leader = gorilla_read_leader(&bit_reader);
		ValueType curr_value = prev_value;
		ValueType xored_data;

		swap_ptr(curr_xored_info, prev_xored_info);

		if (leader >= 0x02)
		{
			/*
			 * if sign == 0b11
			 * then: decompress curr_xored_info
			 */
			if (leader == 0x03)
			{
				/* preload the two sized for better performance */
				bits_reader_ensure_bits(&bit_reader,
										LEADING_ZEROES_BIT_LENGTH + DATA_BIT_LENGTH);

				curr_xored_info->leading_zero_size =
					bits_reader_read_bits(&bit_reader, LEADING_ZEROES_BIT_LENGTH);
				curr_xored_info->data_size =
					bits_reader_read_bits(&bit_reader, DATA_BIT_LENGTH);
				curr_xored_info->trailing_zero_size =
					VALUE_TYPE_WIDTH_IN_BITS - curr_xored_info->leading_zero_size - curr_xored_info->data_size;
			}

			/*
			 * if sign == 0b10
			 * then: use prev_xored_info
			 */
			if (unlikely(curr_xored_info->leading_zero_size == 0 &&
						 curr_xored_info->data_size == 0 &&
						 curr_xored_info->trailing_zero_size == 0))
				elog(ERROR, "invalid gorilla-encoded data");

			xored_data = bits_reader_read_bits64(&bit_reader, curr_xored_info->data_size);
			xored_data <<= curr_xored_info->trailing_zero_size;
			curr_value = prev_value ^ xored_data;
		}

		/*
		 * else: sign == 0b0
		 * then: use prev_value
		 */
		memcpy(dest, &curr_value, sizeof(curr_value));
		dest += sizeof(curr_value);

		swap_ptr(prev_xored_info, curr_xored_info);
		prev_value = curr_value;
	}

	return dest - dest_start;
}

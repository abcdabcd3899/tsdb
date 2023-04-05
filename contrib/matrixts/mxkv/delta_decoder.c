/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/delta_decoder.c
 *	  Delta and varlen decoder.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "mxkv/bits_reader.h"
#include "mxkv/bits_writer.h"
#include "mxkv/delta_coding.h"
#include "mxkv/delta_decoder.h"

/*
 * Initialize a delta decoder.
 *
 * See delta_encoder_begin() for "autoinc".
 */
static void
delta_decoder_init(DeltaDecoder *decoder, const char *data, int64 autoinc)
{
	bits_reader_init(&decoder->reader, data);
	decoder->autoinc = autoinc;
}

/*
 * Read the leader code.
 *
 * The "max" and "sub" are used to convert a value to negative:
 *
 *		if (x > max)
 *			x -= sub;
 *
 * XXX: must keep in sync with delta_coding_schemas.
 */
static uint8
delta_decoder_read_leader32(DeltaDecoder *decoder,
							uint8 *body_nbits, int32 *max, int32 *sub)
{
	/*
	 * we don't know the length of the leader code until we read them, intead
	 * of reading them bit by bit we seek 4 bits blindly, the max length of
	 * leader codes, and consume the actual count once we know it.
	 */
	uint8 leader = bits_reader_peek_bits(&decoder->reader, 4);

	Assert(leader <= 0x0f);

	switch (leader)
	{
	case 0x0 ... 0x7: /* "b0" */
		bits_reader_consume_bits(&decoder->reader, 1);
		*body_nbits = 1;
		*max = 0;
		*sub = 0;
		return 0x0;
	case 0x8 ... 0xb: /* "b10" */
		bits_reader_consume_bits(&decoder->reader, 2);
		*body_nbits = 6;
		*max = 32;
		*sub = 64;
		return 0x2;
	case 0xc ... 0xd: /* "b110" */
		bits_reader_consume_bits(&decoder->reader, 3);
		*body_nbits = 9;
		*max = 256;
		*sub = 512;
		return 0x6;
	case 0xe: /* "b1110" */
		bits_reader_consume_bits(&decoder->reader, 4);
		*body_nbits = 12;
		*max = 2048;
		*sub = 4096;
		return 0xe;
	default: /* "b1111" */
		Assert(leader == 0x0f);
		bits_reader_consume_bits(&decoder->reader, 4);
		*body_nbits = 32;
		*max = PG_INT32_MAX;
		*sub = 0;
		return 0xf;
	}
}

/*
 * Read the leader code.
 *
 * The "max" and "sub" are used to convert a value to negative:
 *
 *		if (x > max)
 *			x -= sub;
 *
 * XXX: must keep in sync with delta_coding_schemas.
 */
static uint8
delta_decoder_read_leader64(DeltaDecoder *decoder,
							uint8 *body_nbits, int64 *max, int64 *sub)
{
	/*
	 * we don't know the length of the leader code until we read them, intead
	 * of reading them bit by bit we seek 4 bits blindly, the max length of
	 * leader codes, and consume the actual count once we know it.
	 */
	uint8 leader = bits_reader_peek_bits(&decoder->reader, 4);

	Assert(leader <= 0x0f);

	switch (leader)
	{
	case 0x0 ... 0x7: /* "b0" */
		bits_reader_consume_bits(&decoder->reader, 1);
		*body_nbits = 1;
		*max = 0;
		*sub = 0;
		return 0x0;
	case 0x8 ... 0xb: /* "b10" */
		bits_reader_consume_bits(&decoder->reader, 2);
		*body_nbits = 6;
		*max = 32;
		*sub = 64;
		return 0x2;
	case 0xc ... 0xd: /* "b110" */
		bits_reader_consume_bits(&decoder->reader, 3);
		*body_nbits = 13;
		*max = 4096;
		*sub = 8192;
		return 0x6;
	case 0xe: /* "b1110" */
		bits_reader_consume_bits(&decoder->reader, 4);
		*body_nbits = 20;
		*max = 524288;
		*sub = 1048576;
		return 0xe;
	default: /* "b1111" */
		Assert(leader == 0x0f);
		bits_reader_consume_bits(&decoder->reader, 4);
		*body_nbits = 64;
		*max = PG_INT64_MAX;
		*sub = 0;
		return 0xf;
	}
}

/*
 * Read a varlen int32.
 */
static uint32
delta_decoder_read_varint32(DeltaDecoder *decoder)
{
	uint8 nbits;
	int32 max;
	int32 sub;
	uint8 leader = delta_decoder_read_leader32(decoder,
											   &nbits, &max, &sub);
	uint32 bits;

	if (leader == 0)
		return 0; /* fast path, leader code "b0" always means value 0 */

	bits = bits_reader_read_bits(&decoder->reader, nbits);

	/* need to extend the sign bit */
	if (bits > max)
		/* yes, it is negative */
		bits -= sub;

	return bits;
}

/*
 * Read a varlen int64.
 */
static uint64
delta_decoder_read_varint64(DeltaDecoder *decoder)
{
	uint8 nbits;
	int64 max;
	int64 sub;
	uint8 leader = delta_decoder_read_leader64(decoder,
											   &nbits, &max, &sub);
	uint64 bits;

	if (leader == 0)
		return 0; /* fast path, leader code "b0" always means value 0 */

	bits = bits_reader_read_bits64(&decoder->reader, nbits);

	/* need to extend the sign bit */
	if (bits > max)
		/* yes, it is negative */
		bits -= sub;

	return bits;
}

/*
 * Decode n int32/64 values from data.
 *
 * The data must be large enough.
 *
 * If "lookup" is not NULL, the index of the given key will be recorded during
 * the decoding.
 */
char *
delta_decode_into_array(const char *data, BitsWriterWidth width,
						int64 base, int64 autoinc, char *xs, int n,
						DeltaDecoderLookup *lookup)
{
	DeltaDecoder decoder;
	int64 last = base - autoinc;

	if (lookup)
		lookup->nth = -1;

	Assert(n > 0);
	delta_decoder_init(&decoder, data, autoinc);

	/*
	 * we want to make this function fast, so we do not put the switch case
	 * inside the for loop.
	 */
	switch (width)
	{
	case BITS_WRITER_WIDTH_4B:
	{
		int32 *xs32 = (int32 *)xs;

		for (int i = 0; i < n; ++i, ++xs32)
		{
			int32 delta = delta_decoder_read_varint32(&decoder);

			*xs32 = delta + last + decoder.autoinc;
			last = *xs32;

			if (lookup && lookup->key == last)
				lookup->nth = i;
		}
	}
	break;

	case BITS_WRITER_WIDTH_8B:
	{
		int64 *xs64 = (int64 *)xs;

		for (int i = 0; i < n; ++i, ++xs64)
		{
			int64 delta = delta_decoder_read_varint64(&decoder);

			*xs64 = delta + last + decoder.autoinc;
			last = *xs64;

			if (lookup && lookup->key == last)
				lookup->nth = i;
		}
	}
	break;

	default:
		/* not supported yet */
		pg_unreachable();
	}

	/* drop all the padding bits */
	return bits_reader_next_byte(&decoder.reader);
}

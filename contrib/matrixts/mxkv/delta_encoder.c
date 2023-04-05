/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/delta_encoder.c
 *	  Delta encoder.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "mxkv/delta_coding.h"
#include "mxkv/delta_encoder.h"

/*
 * Initialize and begin a delta encoder.
 *
 * "autoinc" specifies the automatic increment, the increment is subtracted
 * from the delta.
 *
 * for example, for a batch of keys, which are unique and sorted, the delta
 * between the neighbors are at least 1, suppose the keys are 1,2,3,5, the
 * deltas are 1,1,2, by setting autoinc=1, the deltas become 0,0,1, which can
 * be encoded into less bits.
 */
void delta_encoder_begin(DeltaEncoder *encoder, BitsWriter *writer,
						 BitsWriterWidth width,
						 int64 base, int64 autoinc)
{
	encoder->writer = writer;
	encoder->width = width;
	/* if the first value is passed as base, we want the first delta be 0 */
	encoder->last = base - autoinc;
	encoder->autoinc = autoinc;
}

/*
 * End of a delta encoder.
 *
 * All the unwritten bits of the writer cursor are padded with "0" bits.
 */
void delta_encoder_end(DeltaEncoder *encoder)
{
	bits_writer_pad(encoder->writer);

#ifdef USE_ASSERT_CHECKING
	encoder->width = BITS_WRITER_WIDTH_INVALID;
#endif /* USE_ASSERT_CHECKING */
}

/*
 * Push an integer into the encoder.
 *
 * Both signed and unsigned intergers are accepted.
 */
void delta_encoder_push(DeltaEncoder *encoder, int64 x)
{
	const DeltaCodingSchema *schema;
	int64 last = x;

	x = x - encoder->last - encoder->autoinc;
	encoder->last = last;

	if (x == 0)
	{
		/* fast path, 0 is simply a bit "b0" */
		bits_writer_push_bit(encoder->writer, 0);
		return;
	}

	switch (encoder->width)
	{
	case BITS_WRITER_WIDTH_4B:
		schema = delta_coding_schema32_get_by_value(x);
		Assert(schema);
		Assert(schema->min <= (int32)x && (int32)x <= schema->max);
		break;
	case BITS_WRITER_WIDTH_8B:
		schema = delta_coding_schema64_get_by_value(x);
		Assert(schema);
		Assert(schema->min <= x && x <= schema->max);
		break;
	default:
		/* not supported yet */
		pg_unreachable();
	}

	/* leader bits */
	bits_writer_push_bits(encoder->writer,
						  schema->leader_bits, schema->leader_nbits);

	/* body bits */
	bits_writer_push_bits64(encoder->writer, x, schema->body_nbits);
}

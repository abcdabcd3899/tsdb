/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/types.c
 *	  The mxkv types.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/numeric.h"

#include "mxkv/types.h"

/*
 * Define this to parse scaled floats in the high precision but slow way, by
 * using numeric.
 *
 * The in() operation of scaled floats is 30% faster by turning high precision
 * off.
 */
#if 0
#define MXKV_HIGH_PRECISON_SCALED_FLOATS
#endif

void mxkv_vtype_deduce(MxkvVType *vtype)
{
	switch (vtype->typid)
	{
	case MXKV_VTYPE_VARLEN_TYPID:
	case INT4OID:
		vtype->width = -1;
		vtype->scale = 0;
		break;
	case FLOAT4OID:
	case FLOAT8OID:
		/* TODO: should limit the scale */
		vtype->width = vtype->typmod;
		vtype->scale = 0;
		if (vtype->width >= 0)
		{
			vtype->scale = 1;
			for (int i = 0; i < vtype->width; ++i)
				vtype->scale *= 10;
		}
		break;
	default:
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid mxkv type: %u", vtype->typid)));
	}
}

/*
 * Read the float value and scale it to an integer.
 *
 * It does the same thing as this:
 *
 *		return (int) round(float_value * scale);
 *
 * But it is done via numeric to prevent losing precision during the process.
 */
int64 mxkv_vtype_read_scaled_float(const MxkvVType *vtype, const char *data)
{
#ifdef MXKV_HIGH_PRECISON_SCALED_FLOATS
	/* typmod for numeric(30,width) */
	int32 num_typmod = (30 << 16) + vtype->width + VARHDRSZ;
	Numeric num;

	num = DatumGetNumeric(DirectFunctionCall3(numeric_in,
											  CStringGetDatum(data),
											  NUMERICOID,
											  Int32GetDatum(num_typmod)));
	if (vtype->scale > 1)
	{
		Numeric scale;

		scale = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
													Int64GetDatum(vtype->scale)));
		num = numeric_mul_opt_error(num, scale, NULL /* have_error */);
	}

	switch (vtype->typid)
	{
	case FLOAT4OID:
		return numeric_int4_opt_error(num, NULL /* have_error */);
	case FLOAT8OID:
		return DatumGetInt64(DirectFunctionCall1(numeric_int8,
												 NumericGetDatum(num)));
	default:
		pg_unreachable();
	}
#else  /* MXKV_HIGH_PRECISON_SCALED_FLOATS */
	double float_value = atof(data);

	if (vtype->scale > 1)
		float_value *= vtype->scale;

	return round(float_value);
#endif /* MXKV_HIGH_PRECISON_SCALED_FLOATS */
}

void mxkv_vtype_serialize(const MxkvVType *vtype, DeltaEncoder *encoder,
						  const char *value)
{
	StringInfo bin = encoder->writer->bytes;

	/*
	 * for int/float types, we do not check the tokentype, so a text is
	 * allowed as long as it can be fully converted to the real type.
	 */

	if (mxkv_vtype_is_varlen(vtype))
	{
		/*
		 * '\0' terminated strings
		 *
		 * can a json key or text value contains a '\0' in the body?
		 * We believe it is not.  For example:
		 *
		 *     -- this is valid, and is true
		 *     select '{"a": "\u0001"}'::json->>'a' = E'\x01';
		 *     -- "\u0000" is not accepted as json
		 *     select '{"a": "\u0000"}'::json->>'a';
		 *     -- E'\x00' is not accepted as sql
		 *     select E'\x00';
		 *
		 * in either json or sql a text cannot contain '\0' in the
		 * body, so it is safe to assume that '\0' is always the endo
		 * of a string.
		 */
		appendBinaryStringInfo(bin, value, strlen(value) + 1);
	}
	else if (mxkv_vtype_is_scaled(vtype))
	{
		int64 v;

		Assert(vtype->typid == FLOAT4OID ||
			   vtype->typid == FLOAT8OID);

		v = mxkv_vtype_read_scaled_float(vtype, value);
		delta_encoder_push(encoder, v);
	}
	else if (vtype->typid == INT4OID)
	{
		int32 v = mxkv_int32_from_string(value);

		delta_encoder_push(encoder, v);
	}
	else if (vtype->typid == FLOAT4OID)
	{
		float4 v = mxkv_float4_from_string(value);

		/* float4 is not encoded */
		appendBinaryStringInfo(bin, (char *)&v, vtype->typlen);
	}
	else if (vtype->typid == FLOAT8OID)
	{
		float8 v = mxkv_float8_from_string(value);

		/* float8 is not encoded */
		appendBinaryStringInfo(bin, (char *)&v, vtype->typlen);
	}
	else
		pg_unreachable();
}

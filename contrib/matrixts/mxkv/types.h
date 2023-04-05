/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/types.h
 *	  The mxkv types.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_TYPES_H
#define MXKV_TYPES_H

#include "postgres.h"

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/float.h"

#include "mxkv/delta_encoder.h"

#define MXKV_VTYPE_VARLEN_TYPID 0
#define MXKV_VTYPE_VARLEN_TYPLEN 0

/* when byval=off, the values point to an array of string pointers */
#define MXKV_VTYPE_BYPTR_TYPID MXKV_VTYPE_VARLEN_TYPID
#define MXKV_VTYPE_BYPTR_TYPLEN sizeof(char *)

#define mxkv_init_vtype(_typid, _typlen, _typmod, _byval) \
	{                                                     \
		.typid = _typid,                                  \
		.typlen = _typlen,                                \
		.typmod = _typmod,                                \
		.width = -1,                                      \
		.scale = -1,                                      \
		.byval = _byval,                                  \
	}

#define mxkv_text_init_vtype(_typmod)                                  \
	mxkv_init_vtype(MXKV_VTYPE_VARLEN_TYPID, MXKV_VTYPE_VARLEN_TYPLEN, \
					_typmod, true /* byval */)
#define mxkv_int4_init_vtype(_typmod) \
	mxkv_init_vtype(INT4OID, sizeof(int32), _typmod, true /* byval */)
#define mxkv_float4_init_vtype(_typmod) \
	mxkv_init_vtype(FLOAT4OID, sizeof(float4), _typmod, true /* byval */)
#define mxkv_float8_init_vtype(_typmod) \
	mxkv_init_vtype(FLOAT8OID, sizeof(float8), _typmod, true /* byval */)

typedef struct MxkvVType
{
	uint32 typid;
	int32 typlen;

	/* below are only meaningful in "in()" functions */

	int32 typmod;
	int32 width; /* width of the scale */
	int64 scale; /* for numeric and scaled floats */
	bool byval;	 /* false if pointers are stored instead of values */
} MxkvVType;

extern void mxkv_vtype_deduce(MxkvVType *vtype);
extern int64 mxkv_vtype_read_scaled_float(const MxkvVType *vtype,
										  const char *data);
extern void mxkv_vtype_serialize(const MxkvVType *vtype, DeltaEncoder *encoder,
								 const char *value);

static inline bool pg_attribute_unused()
	mxkv_vtype_is_varlen(const MxkvVType *vtype)
{
	AssertImply(vtype->typid == MXKV_VTYPE_VARLEN_TYPID,
				vtype->typlen == MXKV_VTYPE_VARLEN_TYPLEN);

	return vtype->typlen == MXKV_VTYPE_VARLEN_TYPLEN;
}

static inline bool pg_attribute_unused()
	mxkv_vtype_is_scaled(const MxkvVType *vtype)
{
	AssertImply(vtype->scale > 0,
				vtype->width >= 0);

	return vtype->scale > 0;
}

static inline bool pg_attribute_unused()
	mxkv_vtype_is_byval(const MxkvVType *vtype)
{
	/* at the moment only the varlen vtype is allowed to be non-byval */
	AssertImply(!vtype->byval,
				mxkv_vtype_is_varlen(vtype));

	return vtype->byval;
}

static inline bool pg_attribute_unused()
	mxkv_vtype_supports_encoding(const MxkvVType *vtype)
{
	/* currently only int4 supports encoding */
	if (vtype->typid == INT4OID)
		return true;

	return false;
}

static inline bool pg_attribute_unused()
	mxkv_vtype_has_typmod(const MxkvVType *vtype)
{
	return vtype->typmod >= 0;
}

static inline int32 pg_attribute_unused()
	mxkv_vtype_load_int32(const MxkvVType *vtype, const char **pdata)
{
	int32 v = *(const int32 *)*pdata;

	*pdata += sizeof(int32);

	return v;
}

static inline float4 pg_attribute_unused()
	mxkv_vtype_load_float4(const MxkvVType *vtype, const char **pdata)
{
	float4 v;

	if (mxkv_vtype_is_scaled(vtype))
	{
		v = *((const int32 *)*pdata) / (float4)vtype->scale;
		*pdata += sizeof(int32);
	}
	else
	{
		v = *(const float4 *)*pdata;
		*pdata += sizeof(float4);
	}

	return v;
}

static inline float8 pg_attribute_unused()
	mxkv_vtype_load_float8(const MxkvVType *vtype, const char **pdata)
{
	float8 v;

	if (mxkv_vtype_is_scaled(vtype))
	{
		v = *((const int64 *)*pdata) / (float8)vtype->scale;
		*pdata += sizeof(int64);
	}
	else
	{
		v = *(const float8 *)*pdata;
		*pdata += sizeof(float8);
	}

	return v;
}

static inline void pg_attribute_unused()
	mxkv_int32_to_string(StringInfo result, int32 v)
{
	char s[12]; /* see int4out() */

	/* we want to behave the same with int4out() */
	pg_ltoa(v, s);
	appendStringInfoString(result, s);
}

static inline void pg_attribute_unused()
	mxkv_float4_to_string(StringInfo result, float4 v)
{
	char *s;

	/* we want to behave the same with float4out() */
	s = DatumGetCString(DirectFunctionCall1(float4out, Float4GetDatum(v)));

	appendStringInfoString(result, s);
}

static inline void pg_attribute_unused()
	mxkv_float8_to_string(StringInfo result, float8 v)
{
	char *s;

	/* we want to behave the same with float8out() */
	s = float8out_internal(v);

	appendStringInfoString(result, s);
}

static inline int32 pg_attribute_unused()
	mxkv_int32_from_string(const char *data)
{
	/* we want to behave the same with int4in() */
	int32 v = pg_strtoint32(data);

	return v;
}

static inline float4 pg_attribute_unused()
	mxkv_float4_from_string(const char *data)
{
	Datum arg;
	float4 v;

	/* we want to behave the same with float4in() */
	arg = CStringGetDatum(data);
	v = DatumGetFloat4(DirectFunctionCall1(float4in, arg));

	return v;
}

static inline float8 pg_attribute_unused()
	mxkv_float8_from_string(const char *data)
{
	/* we want to behave the same with float8in() */
	float8 v = float8in_internal((char *)data,
								 NULL /* endptr_p */,
								 "float8",
								 data);

	return v;
}

#endif /* MXKV_TYPES_H */

/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/module.c
 *	  mxkv module.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"

#include "mxkv/mxkv.h"
#include "mxkv/mxkv_in.h"

PG_FUNCTION_INFO_V1(mxkv_text_in);
PG_FUNCTION_INFO_V1(mxkv_text_out);
PG_FUNCTION_INFO_V1(mxkv_text_from_json);
PG_FUNCTION_INFO_V1(mxkv_text_from_jsonb);
PG_FUNCTION_INFO_V1(mxkv_text_lookup);
PG_FUNCTION_INFO_V1(mxkv_text_exists);

PG_FUNCTION_INFO_V1(mxkv_int4_in);
PG_FUNCTION_INFO_V1(mxkv_int4_out);
PG_FUNCTION_INFO_V1(mxkv_int4_from_json);
PG_FUNCTION_INFO_V1(mxkv_int4_from_jsonb);
PG_FUNCTION_INFO_V1(mxkv_int4_lookup);
PG_FUNCTION_INFO_V1(mxkv_int4_exists);

PG_FUNCTION_INFO_V1(mxkv_float4_in);
PG_FUNCTION_INFO_V1(mxkv_float4_out);
PG_FUNCTION_INFO_V1(mxkv_float4_typmod_in);
PG_FUNCTION_INFO_V1(mxkv_float4_typmod_out);
PG_FUNCTION_INFO_V1(mxkv_float4_scale);
PG_FUNCTION_INFO_V1(mxkv_float4_from_json);
PG_FUNCTION_INFO_V1(mxkv_float4_from_jsonb);
PG_FUNCTION_INFO_V1(mxkv_float4_lookup);
PG_FUNCTION_INFO_V1(mxkv_float4_exists);

PG_FUNCTION_INFO_V1(mxkv_float8_in);
PG_FUNCTION_INFO_V1(mxkv_float8_out);
PG_FUNCTION_INFO_V1(mxkv_float8_typmod_in);
PG_FUNCTION_INFO_V1(mxkv_float8_typmod_out);
PG_FUNCTION_INFO_V1(mxkv_float8_scale);
PG_FUNCTION_INFO_V1(mxkv_float8_from_json);
PG_FUNCTION_INFO_V1(mxkv_float8_from_jsonb);
PG_FUNCTION_INFO_V1(mxkv_float8_lookup);
PG_FUNCTION_INFO_V1(mxkv_float8_exists);

static const MxkvVType mxkv_text_default_vtype = mxkv_text_init_vtype(-1);
static const MxkvVType mxkv_int4_default_vtype = mxkv_int4_init_vtype(-1);
static const MxkvVType mxkv_float4_default_vtype = mxkv_float4_init_vtype(-1);
static const MxkvVType mxkv_float8_default_vtype = mxkv_float8_init_vtype(-1);

/*
 * Input function for mxkv_text type.
 *
 * mxkv_text <- (cstring).
 *
 * The argument should be a unnested json object, for example:
 *
 *		{ "a": 1, "b": 2 }
 */
Datum mxkv_text_in(PG_FUNCTION_ARGS)
{
	char *json = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid typelem = PG_GETARG_OID(1);
	int32 typmod = PG_GETARG_INT32(2);
#endif
	bytea *bytes = mxkv_in(json, &mxkv_text_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Output function for mxkv_text type.
 *
 * cstring <- (mxkv_text)
 */
Datum mxkv_text_out(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);

	PG_RETURN_CSTRING(mxkv_out(bytes, &mxkv_text_default_vtype));
}

/*
 * Cast mxkv_text from json.
 *
 * mxkv_text <- (json).
 */
Datum mxkv_text_from_json(PG_FUNCTION_ARGS)
{
	char *json = TextDatumGetCString(PG_GETARG_DATUM(0));
	bytea *bytes = mxkv_in(json, &mxkv_text_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Cast mxkv_text from jsonb.
 *
 * mxkv_text <- (jsonb).
 */
Datum mxkv_text_from_jsonb(PG_FUNCTION_ARGS)
{
	Jsonb *jsonb = PG_GETARG_JSONB_P(0);
	char *json = JsonbToCString(NULL, &jsonb->root, VARSIZE(jsonb));
	bytea *bytes = mxkv_in(json, &mxkv_text_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Get the value of the given key name.
 *
 * text <- (mxkv_text, text)
 *
 * TODO: support look up with given key id.
 */
Datum mxkv_text_lookup(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	text *keytext = PG_GETARG_TEXT_P(1);
	const char *value = mxkv_lookup(bytes, keytext, &mxkv_text_default_vtype);

	if (value)
		PG_RETURN_TEXT_P(CStringGetTextDatum(value));
	else
		PG_RETURN_NULL();
}

/*
 * Test whether the given key exists in a mxkv.
 *
 * bool <- (mxkv_text, text)
 *
 * TODO: support look up with given key id.
 */
Datum mxkv_text_exists(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	text *keytext = PG_GETARG_TEXT_P(1);

	PG_RETURN_BOOL(mxkv_exists(bytes, keytext, &mxkv_text_default_vtype));
}

/*
 * Input function for mxkv_int4 type.
 *
 * mxkv_int4 <- (cstring).
 *
 * The argument should be a unnested json object, for example:
 *
 *		{ "a": 1, "b": 2 }
 */
Datum mxkv_int4_in(PG_FUNCTION_ARGS)
{
	char *json = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid typelem = PG_GETARG_OID(1);
	int32 typmod = PG_GETARG_INT32(2);
#endif
	bytea *bytes = mxkv_in(json, &mxkv_int4_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Output function for mxkv_int4 type.
 *
 * cstring <- (mxkv_int4)
 */
Datum mxkv_int4_out(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);

	PG_RETURN_CSTRING(mxkv_out(bytes, &mxkv_int4_default_vtype));
}

/*
 * Cast mxkv_int4 from json.
 *
 * mxkv_int4 <- (json).
 */
Datum mxkv_int4_from_json(PG_FUNCTION_ARGS)
{
	char *json = TextDatumGetCString(PG_GETARG_DATUM(0));
	bytea *bytes = mxkv_in(json, &mxkv_int4_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Cast mxkv_int4 from jsonb.
 *
 * mxkv_int4 <- (jsonb).
 */
Datum mxkv_int4_from_jsonb(PG_FUNCTION_ARGS)
{
	Jsonb *jsonb = PG_GETARG_JSONB_P(0);
	char *json = JsonbToCString(NULL, &jsonb->root, VARSIZE(jsonb));
	bytea *bytes = mxkv_in(json, &mxkv_int4_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Get the value of the given key name.
 *
 * int4 <- (mxkv_int4, text)
 *
 * TODO: support look up with given key id.
 */
Datum mxkv_int4_lookup(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	text *keytext = PG_GETARG_TEXT_P(1);
	const char *value = mxkv_lookup(bytes, keytext, &mxkv_int4_default_vtype);

	if (value)
		PG_RETURN_INT32(*(int32 *)value);
	else
		PG_RETURN_NULL();
}

/*
 * Test whether the given key exists in a mxkv.
 *
 * bool <- (mxkv_int4, text)
 *
 * TODO: support look up with given key id.
 */
Datum mxkv_int4_exists(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	text *keytext = PG_GETARG_TEXT_P(1);

	PG_RETURN_BOOL(mxkv_exists(bytes, keytext, &mxkv_int4_default_vtype));
}

/*
 * Input function for mxkv_float4 type.
 *
 * mxkv_float4 <- (cstring).
 *
 * The argument should be a unnested json object, for example:
 *
 *		{ "a": 1, "b": 2 }
 */
Datum mxkv_float4_in(PG_FUNCTION_ARGS)
{
	char *json = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid typelem = PG_GETARG_OID(1);
#endif
	int32 typmod = PG_GETARG_INT32(2);
	MxkvVType vtype = mxkv_float4_init_vtype(typmod);
	bytea *bytes = mxkv_in(json, &vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Output function for mxkv_float4 type.
 *
 * cstring <- (mxkv_float4)
 */
Datum mxkv_float4_out(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);

	PG_RETURN_CSTRING(mxkv_out(bytes, &mxkv_float4_default_vtype));
}

/*
 * Typmod input function for mxkv_float4 type.
 *
 * int <- (cstring[]).
 */
Datum mxkv_float4_typmod_in(PG_FUNCTION_ARGS)
{
	ArrayType *strs = PG_GETARG_ARRAYTYPE_P(0);
	int32 *vals;
	int n;

	vals = ArrayGetIntegerTypmods(strs, &n);
	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid mxkv_float4 type modifier")));

	PG_RETURN_INT32(vals[0]);
}

/*
 * Typmod output function for mxkv_float4 type.
 *
 * cstring <- (int)
 */
Datum mxkv_float4_typmod_out(PG_FUNCTION_ARGS)
{
	int32 typmod = PG_GETARG_INT32(0);
	char *str = palloc(32);

	if (typmod >= 0)
		snprintf(str, 32, "(%d)", typmod);

	PG_RETURN_CSTRING(str);
}

/*
 * Cast mxkv_float4 from mxkv_float4, the purpose is to change the scale.
 *
 * mxkv_float4 <- (mxkv_float4).
 */
Datum mxkv_float4_scale(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	int32 typmod = PG_GETARG_INT32(1);
	MxkvVType vtype = mxkv_float4_init_vtype(typmod);

	bytes = mxkv_scale(bytes, &vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Cast mxkv_float4 from json.
 *
 * mxkv_float4 <- (json).
 */
Datum mxkv_float4_from_json(PG_FUNCTION_ARGS)
{
	char *json = TextDatumGetCString(PG_GETARG_DATUM(0));
	bytea *bytes = mxkv_in(json, &mxkv_float4_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Cast mxkv_float4 from jsonb.
 *
 * mxkv_float4 <- (jsonb).
 */
Datum mxkv_float4_from_jsonb(PG_FUNCTION_ARGS)
{
	Jsonb *jsonb = PG_GETARG_JSONB_P(0);
	char *json = JsonbToCString(NULL, &jsonb->root, VARSIZE(jsonb));
	bytea *bytes = mxkv_in(json, &mxkv_float4_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Get the value of the given key name.
 *
 * float4 <- (mxkv_float4, text)
 *
 * TODO: support look up with given key id.
 */
Datum mxkv_float4_lookup(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	text *keytext = PG_GETARG_TEXT_P(1);
	const char *value = mxkv_lookup(bytes, keytext, &mxkv_float4_default_vtype);

	if (value)
		PG_RETURN_FLOAT4(*(float4 *)value);
	else
		PG_RETURN_NULL();
}

/*
 * Test whether the given key exists in a mxkv.
 *
 * bool <- (mxkv_float4, text)
 *
 * TODO: support look up with given key id.
 */
Datum mxkv_float4_exists(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	text *keytext = PG_GETARG_TEXT_P(1);

	PG_RETURN_BOOL(mxkv_exists(bytes, keytext, &mxkv_float4_default_vtype));
}

/*
 * Input function for mxkv_float8 type.
 *
 * mxkv_float8 <- (cstring).
 *
 * The argument should be a unnested json object, for example:
 *
 *		{ "a": 1, "b": 2 }
 */
Datum mxkv_float8_in(PG_FUNCTION_ARGS)
{
	char *json = PG_GETARG_CSTRING(0);
#ifdef NOT_USED
	Oid typelem = PG_GETARG_OID(1);
#endif
	int32 typmod = PG_GETARG_INT32(2);
	MxkvVType vtype = mxkv_float8_init_vtype(typmod);
	bytea *bytes = mxkv_in(json, &vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Output function for mxkv_float8 type.
 *
 * cstring <- (mxkv_float8)
 */
Datum mxkv_float8_out(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);

	PG_RETURN_CSTRING(mxkv_out(bytes, &mxkv_float8_default_vtype));
}

/*
 * Typmod input function for mxkv_float8 type.
 *
 * int <- (cstring[]).
 */
Datum mxkv_float8_typmod_in(PG_FUNCTION_ARGS)
{
	ArrayType *strs = PG_GETARG_ARRAYTYPE_P(0);
	int32 *vals;
	int n;

	vals = ArrayGetIntegerTypmods(strs, &n);
	if (n != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid mxkv_float8 type modifier")));

	PG_RETURN_INT32(vals[0]);
}

/*
 * Typmod output function for mxkv_float8 type.
 *
 * cstring <- (int)
 */
Datum mxkv_float8_typmod_out(PG_FUNCTION_ARGS)
{
	int32 typmod = PG_GETARG_INT32(0);
	char *str = palloc(32);

	if (typmod >= 0)
		snprintf(str, 32, "(%d)", typmod);

	PG_RETURN_CSTRING(str);
}

/*
 * Cast mxkv_float8 from mxkv_float8, the purpose is to change the scale.
 *
 * mxkv_float8 <- (mxkv_float8).
 */
Datum mxkv_float8_scale(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	int32 typmod = PG_GETARG_INT32(1);
	MxkvVType vtype = mxkv_float8_init_vtype(typmod);

	bytes = mxkv_scale(bytes, &vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Cast mxkv_float8 from json.
 *
 * mxkv_float8 <- (json).
 */
Datum mxkv_float8_from_json(PG_FUNCTION_ARGS)
{
	char *json = TextDatumGetCString(PG_GETARG_DATUM(0));
	bytea *bytes = mxkv_in(json, &mxkv_float8_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Cast mxkv_float8 from jsonb.
 *
 * mxkv_float8 <- (jsonb).
 */
Datum mxkv_float8_from_jsonb(PG_FUNCTION_ARGS)
{
	Jsonb *jsonb = PG_GETARG_JSONB_P(0);
	char *json = JsonbToCString(NULL, &jsonb->root, VARSIZE(jsonb));
	bytea *bytes = mxkv_in(json, &mxkv_float8_default_vtype);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * Get the value of the given key name.
 *
 * float8 <- (mxkv_float8, text)
 *
 * TODO: support look up with given key id.
 */
Datum mxkv_float8_lookup(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	text *keytext = PG_GETARG_TEXT_P(1);
	const char *value = mxkv_lookup(bytes, keytext, &mxkv_float8_default_vtype);

	if (value)
		PG_RETURN_FLOAT8(*(float8 *)value);
	else
		PG_RETURN_NULL();
}

/*
 * Test whether the given key exists in a mxkv.
 *
 * bool <- (mxkv_float8, text)
 *
 * TODO: support look up with given key id.
 */
Datum mxkv_float8_exists(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	text *keytext = PG_GETARG_TEXT_P(1);

	PG_RETURN_BOOL(mxkv_exists(bytes, keytext, &mxkv_float8_default_vtype));
}

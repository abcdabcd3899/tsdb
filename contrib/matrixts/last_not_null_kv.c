/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/last_not_null_kv.c
 *	  A last_not_null varint for KV types.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqcomm.h"
#include "libpq/pqformat.h"
#include "nodes/value.h"
#include "utils/datum.h"
#include "utils/fmgrprotos.h"
#include "utils/json.h"
#include "utils/jsonapi.h"
#include "utils/hashutils.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"

#define __DESTROY_MISCS
/*
 * enable below macros to free more in-time and more deeply, so the memory
 * usage can be lower, at the cost of performance downgrade.
 */
#if 0
#define __DESTROY_STATES
#define __DESTROY_BUFFERS
#endif

typedef struct KvBuffer
{
	char *data;
	int size;
	int capacity;
} KvBuffer;

#ifdef __DESTROY_BUFFERS
static void
kv_buffer_release(KvBuffer *buf)
{
	if (buf->data)
		pfree(buf->data);
}
#endif /* __DESTROY_BUFFERS */

static KvBuffer *
kv_buffer_bind(KvBuffer *buf, char *data, int size)
{
	buf->data = data;
	buf->size = size;
	buf->capacity = size;
	return buf;
}

static KvBuffer *
kv_buffer_bind_empty(KvBuffer *buf)
{
	return kv_buffer_bind(buf, NULL, 0);
}

static KvBuffer *
kv_buffer_bind_bytea(KvBuffer *buf, void *data)
{
	return kv_buffer_bind(buf, data, VARSIZE(data));
}

static uint32
kv_buffer_hash(const KvBuffer *buf)
{
	return DatumGetUInt32(hash_any((const unsigned char *)buf->data,
								   buf->size));
}

static bool
kv_buffer_equal(const KvBuffer *a, const KvBuffer *b)
{
	return (a->size == b->size &&
			memcmp(a->data, b->data, a->size) == 0);
}

static void
kv_buffer_copy(MemoryContext mcxt,
			   KvBuffer *dst, const KvBuffer *src, bool as_str)
{
	AssertImply(as_str, src->data);

	if (dst->capacity < src->size + as_str)
	{
		if (dst->data)
			pfree(dst->data);

		dst->capacity = src->size + as_str; /* str stores an extra '\0' */
		dst->data = MemoryContextAlloc(mcxt, dst->capacity);
	}

	dst->size = src->size;

	if (src->size > 0)
	{
		memcpy(dst->data, src->data, src->size);

		if (as_str)
			dst->data[dst->size] = '\0';
	}
}

typedef struct KvRecord
{
	KvBuffer key;
	KvBuffer value;
	uint32 hash;
	Datum time;
	JsonTokenType value_type;
	char status;
} KvRecord;

#define SH_PREFIX kvset
#define SH_ELEMENT_TYPE KvRecord
#define SH_KEY_TYPE KvBuffer
#define SH_SCOPE static pg_attribute_unused()
#define SH_DECLARE

#define SH_KEY key
#define SH_EQUAL(table, a, b) kv_buffer_equal(&a, &b)
#define SH_HASH_KEY(table, key) kv_buffer_hash(&key)
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a) a->hash
#define SH_DEFINE

#include "lib/simplehash.h"

PG_FUNCTION_INFO_V1(mx_last_not_null_kv_sfunc);
PG_FUNCTION_INFO_V1(mx_last_not_null_kv_finalfunc);
PG_FUNCTION_INFO_V1(mx_last_not_null_kv_value_finalfunc);
PG_FUNCTION_INFO_V1(mx_last_not_null_kv_combinefunc);
PG_FUNCTION_INFO_V1(mx_last_not_null_kv_serializefunc);
PG_FUNCTION_INFO_V1(mx_last_not_null_kv_deserializefunc);

typedef struct TimeOps
{
	Oid typid;
	bool byval;
	int16 typlen;

	bool direct_compare;

	FmgrInfo compare_flinfo;
	Oid compare_collation;
	FmgrInfo output_flinfo;
	FmgrInfo serialize_flinfo;
	FmgrInfo deserialize_flinfo;
	Oid deserialize_ioparam;
} TimeOps;

typedef struct KvState
{
	MemoryContext mcxt;
	struct kvset_hash *records;
	Oid kv_typid;
	Oid time_typid;
	TimeOps timeops;
} KvState;

static void
timeops_init(TimeOps *ops, MemoryContext mcxt, Oid typid, Oid collation)
{
	ops->typid = typid;
	get_typlenbyval(typid, &ops->typlen, &ops->byval);

	ops->direct_compare = (ops->typid == TIMESTAMPOID ||
						   ops->typid == TIMESTAMPTZOID ||
						   ops->typid == TIMEOID ||
						   ops->typid == DATEOID ||
						   ops->typid == INT2OID ||
						   ops->typid == INT4OID ||
						   ops->typid == INT8OID);

	/* compare function */
	{
		List *opname = list_make1(makeString((char *)"<"));
		Oid oper = OpernameGetOprid(opname, typid, typid);
		Oid opcode = get_opcode(oper);

		fmgr_info_cxt(opcode, &ops->compare_flinfo, mcxt);
		ops->compare_collation = collation;
#ifdef __DESTROY_MISCS
		list_free_deep(opname);
#endif /* __DESTROY_MISCS */
	}

	/* output function */
	{
		Oid opcode;
		bool varlen;

		getTypeOutputInfo(typid, &opcode, &varlen);
		fmgr_info_cxt(opcode, &ops->output_flinfo, mcxt);
	}

	/* serialize function */
	{
		Oid opcode;
		bool varlen;

		getTypeBinaryOutputInfo(typid, &opcode, &varlen);
		fmgr_info_cxt(opcode, &ops->serialize_flinfo, mcxt);
	}

	/* deserialize function */
	{
		Oid opcode;

		getTypeBinaryInputInfo(typid, &opcode, &ops->deserialize_ioparam);
		fmgr_info_cxt(opcode, &ops->deserialize_flinfo, mcxt);
	}
}

static Datum
timeops_copy(TimeOps *ops, Datum datum)
{
	return datumCopy(datum, ops->byval, ops->typlen);
}

static bool
timeops_compare(TimeOps *ops, Datum a, Datum b)
{
	if (likely(ops->direct_compare))
		return a < b;
	else
		return (a != b &&
				DatumGetBool(FunctionCall2Coll(&ops->compare_flinfo,
											   ops->compare_collation, a, b)));
}

static char *
timeops_output(TimeOps *ops, Datum datum)
{
	return OutputFunctionCall(&ops->output_flinfo, datum);
}

static bytea *
timeops_serialize(TimeOps *ops, Datum datum)
{
	return SendFunctionCall(&ops->serialize_flinfo, datum);
}

static Datum
timeops_deserialize(TimeOps *ops, StringInfo buf)
{
	Oid typmod = InvalidOid;

	return ReceiveFunctionCall(&ops->deserialize_flinfo, buf,
							   ops->deserialize_ioparam, typmod);
}

static KvState *
state_create(MemoryContext mcxt, Oid collation, Oid kv_typid, Oid time_typid)
{
	KvState *state = MemoryContextAlloc(mcxt, sizeof(KvState));
	uint32 nelements = 8192; /* the initialize capacity of the kvset */

	state->mcxt = mcxt;
	state->records = kvset_create(mcxt, nelements, NULL /* private_data */);
	state->kv_typid = kv_typid;
	state->time_typid = time_typid;

	timeops_init(&state->timeops, mcxt, time_typid, collation);

	return state;
}

static void
state_destroy(KvState *state)
{
#ifdef __DESTROY_STATES
#ifdef __DESTROY_BUFFERS
	TimeOps *timeops = &state->timeops;
	KvRecord *record;
	kvset_iterator iter;

	kvset_start_iterate(state->records, &iter);
	while ((record = kvset_iterate(state->records, &iter)))
	{
		if (!timeops->byval)
			pfree(DatumGetPointer(record->time));

		kv_buffer_release(&record->key);
		kv_buffer_release(&record->value);
	}
#endif /* __DESTROY_BUFFERS */

	kvset_destroy(state->records);
	pfree(state);
#endif /* __DESTROY_STATES */
}

static void
state_insert_record(KvState *state, Datum time, JsonTokenType value_type,
					const KvBuffer *key, const KvBuffer *value)
{
	TimeOps *timeops = &state->timeops;
	KvRecord *record;
	bool as_str = value_type == JSON_TOKEN_STRING;
	bool found;

	Assert(value_type != JSON_TOKEN_NULL);

	record = kvset_insert(state->records, *key, &found);
	if (!found)
	{
		/* this key is first seen */
		record->time = time;
		record->value_type = value_type;

		/* copy the value */
		kv_buffer_bind_empty(&record->value);
		kv_buffer_copy(state->mcxt, &record->value, value, as_str);

		/* the key must also be duplicated */
		kv_buffer_bind_empty(&record->key);
		kv_buffer_copy(state->mcxt, &record->key, key, true /* as_str */);
	}
	else if (timeops_compare(timeops, record->time, time))
	{
		/* the arg is newer than the record */
		record->time = time;
		record->value_type = value_type;
		kv_buffer_copy(state->mcxt, &record->value, value, as_str);
	}
}

/*
 * (state internal, value jsonb, time "any") => internal
 *
 * The kv pairs are handled key by key, all the null values are stripped.
 */
Datum mx_last_not_null_kv_sfunc(PG_FUNCTION_ARGS)
{
	KvState *state;
	Jsonb *jsonb;
	JsonbIterator *it;
	JsonbIteratorToken type;
	JsonbValue v;
	Datum time;
	KvBuffer key;
	KvBuffer value;
	MemoryContext aggcontext;
	MemoryContext oldcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "%s called in non-aggregate context", __func__);

	if (PG_ARGISNULL(1) || PG_ARGISNULL(2))
	{
		/* null values / times are ignored by last_not_null() */
		if (PG_ARGISNULL(0))
			PG_RETURN_NULL();
		else
			PG_RETURN_POINTER(PG_GETARG_POINTER(0));
	}

	if (PG_ARGISNULL(0))
		state = state_create(aggcontext, fcinfo->fncollation,
							 get_fn_expr_argtype(fcinfo->flinfo, 1),
							 get_fn_expr_argtype(fcinfo->flinfo, 2));
	else
		state = (KvState *)PG_GETARG_POINTER(0);

	jsonb = PG_GETARG_JSONB_P(1);
	time = PG_GETARG_DATUM(2);

	it = JsonbIteratorInit(&jsonb->root);

	/* only need to dupliate the time once */
	oldcontext = MemoryContextSwitchTo(aggcontext);
	time = timeops_copy(&state->timeops, time);
	MemoryContextSwitchTo(oldcontext);

	/* validate the first node separately */
	type = JsonbIteratorNext(&it, &v, false);
	if (type != WJB_BEGIN_OBJECT)
		elog(ERROR, "the value is not a json object");

	while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		if (type == WJB_KEY)
			kv_buffer_bind(&key, v.val.string.val, v.val.string.len);
		else if (type != WJB_VALUE && type != WJB_ELEM)
			continue;
		else if (v.type == jbvString)
			state_insert_record(state, time, JSON_TOKEN_STRING, &key,
								kv_buffer_bind(&value,
											   v.val.string.val,
											   v.val.string.len));
		else if (v.type == jbvNumeric)
			/* pass the numeric vlena directly */
			state_insert_record(state, time, JSON_TOKEN_NUMBER, &key,
								kv_buffer_bind_bytea(&value, v.val.numeric));
		else if (v.type == jbvBool)
			state_insert_record(state, time,
								v.val.boolean ? JSON_TOKEN_TRUE : JSON_TOKEN_FALSE,
								&key, kv_buffer_bind_empty(&value));
		else
			continue; /* jbvNull and composite types are ignored */
	}

	return PointerGetDatum(state);
}

static Datum
finalfunc(FunctionCallInfo fcinfo, bool valueonly)
{
	KvState *state = (KvState *)PG_GETARG_POINTER(0);
	TimeOps *timeops = &state->timeops;
	KvRecord *record;
	kvset_iterator iter;
	JsonbParseState *jstate = NULL;
	JsonbValue *jroot = NULL;
	Jsonb *jsonb;
	MemoryContext aggcontext;
	MemoryContext oldcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "%s called in non-aggregate context", __func__);

	AssertImply(!PG_ARGISNULL(0), PG_GETARG_POINTER(0));

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	jroot = pushJsonbValue(&jstate, WJB_BEGIN_OBJECT, NULL /* jbval */);

	kvset_start_iterate(state->records, &iter);
	while ((record = kvset_iterate(state->records, &iter)))
	{
		JsonbValue v;

		/* the key is always a quoted string */
		v.type = jbvString;
		v.val.string.len = record->key.size;
		v.val.string.val = record->key.data;
		jroot = pushJsonbValue(&jstate, WJB_KEY, &v);

		/* the value */
		switch (record->value_type)
		{
		case JSON_TOKEN_STRING:
			v.type = jbvString;
			v.val.string.len = record->value.size;
			v.val.string.val = record->value.data;
			break;

		case JSON_TOKEN_NUMBER:
			v.type = jbvNumeric;
			v.val.numeric = DatumGetNumeric(record->value.data);
			break;

		case JSON_TOKEN_TRUE:
			v.type = jbvBool;
			v.val.boolean = true;
			break;

		case JSON_TOKEN_FALSE:
			v.type = jbvBool;
			v.val.boolean = false;
			break;

		default:
			pg_unreachable(); /* should never happen */
		}

		if (valueonly)
			/* output only the value, as a scalar */
			jroot = pushJsonbValue(&jstate, WJB_VALUE, &v);
		else
		{
			/* output both the value and the time, as an array */
			char *val = timeops_output(timeops, record->time);
			JsonbValue vt;

			/* the time is always a string */
			vt.type = jbvString;
			vt.val.string.len = strlen(val);
			vt.val.string.val = val;

			jroot = pushJsonbValue(&jstate, WJB_BEGIN_ARRAY, NULL /* jbval */);
			jroot = pushJsonbValue(&jstate, WJB_ELEM, &v);
			jroot = pushJsonbValue(&jstate, WJB_ELEM, &vt);
			jroot = pushJsonbValue(&jstate, WJB_END_ARRAY, NULL /* jbval */);
		}
	}

	jroot = pushJsonbValue(&jstate, WJB_END_OBJECT, NULL /* jbval */);

	oldcontext = MemoryContextSwitchTo(aggcontext);
	jsonb = JsonbValueToJsonb(jroot);
	MemoryContextSwitchTo(oldcontext);

	state_destroy(state);
	PG_RETURN_POINTER(jsonb);
}

/*
 * (state internal) => jsonb
 */
Datum mx_last_not_null_kv_finalfunc(PG_FUNCTION_ARGS)
{
	return finalfunc(fcinfo, false /* valueonly */);
}

/*
 * (state internal) => jsonb
 */
Datum mx_last_not_null_kv_value_finalfunc(PG_FUNCTION_ARGS)
{
	return finalfunc(fcinfo, true /* valueonly */);
}

/*
 * (state_a internal, state_b internal) => internal
 *
 * The value type is expected to be jsonb, the kv pairs are handled key by key.
 */
Datum mx_last_not_null_kv_combinefunc(PG_FUNCTION_ARGS)
{
	/*
	 * the old one will be merged into the new one, then the old one will be
	 * released, the new one will be returned.
	 */
	KvState *oldstate = (KvState *)PG_GETARG_POINTER(0);
	KvState *newstate = (KvState *)PG_GETARG_POINTER(1);
	KvRecord *record;
	kvset_iterator iter;

	AssertImply(!PG_ARGISNULL(0), PG_GETARG_POINTER(0));
	AssertImply(!PG_ARGISNULL(1), PG_GETARG_POINTER(1));

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();
	else if (PG_ARGISNULL(0))
		PG_RETURN_POINTER(PG_GETARG_POINTER(1));
	else if (PG_ARGISNULL(1))
		PG_RETURN_POINTER(PG_GETARG_POINTER(0));

	kvset_start_iterate(oldstate->records, &iter);
	while ((record = kvset_iterate(oldstate->records, &iter)))
	{
		/* TODO: transfer the ownership of key, time, and value */
		state_insert_record(newstate, record->time, record->value_type,
							&record->key, &record->value);
	}

	state_destroy(oldstate);
	return PointerGetDatum(newstate);
}

/*
 * (state internal) => bytea
 */
Datum mx_last_not_null_kv_serializefunc(PG_FUNCTION_ARGS)
{
	KvState *state = (KvState *)PG_GETARG_POINTER(0);
	TimeOps *timeops = &state->timeops;
	KvRecord *record;
	kvset_iterator iter;
	StringInfoData buf;

	Assert(!PG_ARGISNULL(0));

	pq_begintypsend(&buf);

	/* send the kv type */
	pq_sendint32(&buf, state->kv_typid);
	/* send the time type */
	pq_sendint32(&buf, state->time_typid);

	/* send the # of records */
	pq_sendint32(&buf, state->records->members);

	kvset_start_iterate(state->records, &iter);
	while ((record = kvset_iterate(state->records, &iter)))
	{
		/* send the key */
		pq_sendint32(&buf, record->key.size);
		pq_sendbytes(&buf, record->key.data, record->key.size);

		/* send the time */
		{
			bytea *bytes = timeops_serialize(timeops, record->time);
			int len = VARSIZE(bytes) - VARHDRSZ;

			pq_sendint32(&buf, len);
			pq_sendbytes(&buf, VARDATA(bytes), len);
		}

		/* send value_type */
		pq_sendbyte(&buf, record->value_type);

		/* send value, only if necessary */
		switch (record->value_type)
		{
		case JSON_TOKEN_STRING:
		case JSON_TOKEN_NUMBER:
			pq_sendint32(&buf, record->value.size);
			pq_sendbytes(&buf, record->value.data, record->value.size);
			break;

		default:
			/* nothing to do for JSON_TOKEN_TRUE / JSON_TOKEN_FALSE */
			break;
		}
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * (bytea, internal) => internal
 */
Datum mx_last_not_null_kv_deserializefunc(PG_FUNCTION_ARGS)
{
	KvState *state;
	TimeOps *timeops;
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	Oid kv_typid;
	Oid time_typid;
	int nrecords;
	StringInfoData buf;
	MemoryContext aggcontext;
	MemoryContext oldcontext;

	Assert(!PG_ARGISNULL(0));

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		elog(ERROR, "%s called in non-aggregate context", __func__);

	/*
	 * build string info wrapper for the bytes, so we can deserialize it using
	 * the receive functions.
	 */
	buf.data = (char *)bytes;
	buf.len = VARSIZE(bytes);
	buf.maxlen = buf.len;
	buf.cursor = VARHDRSZ; /* skip the bytea header */

	/* recv the kv type */
	kv_typid = pq_getmsgint(&buf, sizeof(int32));
	/* recv the time type */
	time_typid = pq_getmsgint(&buf, sizeof(int32));
	/* make an empty state */
	state = state_create(aggcontext, fcinfo->fncollation, kv_typid, time_typid);
	timeops = &state->timeops;

	/* recv the # of records */
	nrecords = pq_getmsgint(&buf, sizeof(int32));

	for (unsigned int i = 0; i < nrecords; ++i)
	{
		KvBuffer key;
		KvBuffer value;
		Datum time;
		int len;
		JsonTokenType value_type;

		/* recv the key */
		{
			len = pq_getmsgint(&buf, sizeof(int32));
			kv_buffer_bind(&key, &buf.data[buf.cursor], len);
			buf.cursor += len;
		}

		/* recv the time */
		{
			int len pg_attribute_unused() =
				pq_getmsgint(&buf, sizeof(int32));

			oldcontext = MemoryContextSwitchTo(aggcontext);
			time = timeops_deserialize(timeops, &buf);
			MemoryContextSwitchTo(oldcontext);
		}

		/* recv value_type */
		value_type = pq_getmsgbyte(&buf);

		/* recv value */
		switch (value_type)
		{
		case JSON_TOKEN_STRING:
		case JSON_TOKEN_NUMBER:
			len = pq_getmsgint(&buf, sizeof(int32));
			kv_buffer_bind(&value, &buf.data[buf.cursor], len);
			buf.cursor += len;
			break;

		default:
			/* nothing to do for JSON_TOKEN_TRUE / JSON_TOKEN_FALSE */
			kv_buffer_bind_empty(&value);
			break;
		}

		state_insert_record(state, time, value_type, &key, &value);
	}

#ifdef __DESTROY_MISCS
	PG_FREE_IF_COPY(bytes, 0);
#endif /* __DESTROY_MISCS */
	PG_RETURN_POINTER(state);
}

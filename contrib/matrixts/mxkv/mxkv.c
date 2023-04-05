/*-------------------------------------------------------------------------
 *
 * contrib/mxkv/mxkv.c
 *	  mxkv, a type for large set of key-value records.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_enum.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/float.h"
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/syscache.h"

#include "mxkv/config.h"
#include "mxkv/delta_decoder.h"
#include "mxkv/mxkv.h"
#include "mxkv/mxkv_in.h"
#include "mxkv/mxkv_iter.h"

Oid mxkv_enum_typid(void)
{
	static Oid typid = InvalidOid;

	if (unlikely(!OidIsValid(typid)))
	{
		/*
		 * when callbed by pg_dump the search_path is empty, so the type cannot
		 * be found.  We have to ensure that "public" exists in search_path.
		 *
		 * FIXME: this is rarely seen in the code, any better way to do the
		 * job?
		 */
		char *old_search_path = namespace_search_path;

		namespace_search_path = "public";
		assign_search_path(namespace_search_path, NULL);
		typid = TypenameGetTypid(MXKV_ENUM_NAME);
		namespace_search_path = old_search_path;

		if (!OidIsValid(typid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("type %s is undefined", MXKV_ENUM_NAME),
					 errhint("Recreate the extension %s", "matrixts")));
	}

	return typid;
}

char *
mxkv_key_name(uint32 id)
{
	HeapTuple tup;
	char *name = NULL;

	tup = SearchSysCache1(ENUMOID, ObjectIdGetDatum(id));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_enum en = (Form_pg_enum)GETSTRUCT(tup);

		name = pstrdup(NameStr(en->enumlabel));

		ReleaseSysCache(tup);
	}

	if (name == NULL)
		/*
		 * this happens only if the key was ever imported, but then got retired
		 * due to some reason.  there is no user interface to retire a key at
		 * all, so we do not know how this happens, maybe the catalog is
		 * corrupted, or maybe due to some manually hacks.  this is not a
		 * recoverable error, but we really have no other choice than raising
		 * an error.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("unknown key id %u", id)));

	return name;
}

/*
 * Convert a key name to key id.
 *
 * The key id is the enumsortorder, not oid.  So key id counts from 0, and is
 * easy to sort.  However this makes it slow to convert key id back to key
 * name;
 */
int32 mxkv_key_id(const char *name, bool allow_missing)
{
	Oid typid = MXKV_ENUM_TYPID;
	HeapTuple tup;
	int32 id = -1;

	tup = SearchSysCache2(ENUMTYPOIDNAME,
						  ObjectIdGetDatum(typid),
						  CStringGetDatum(name));
	if (HeapTupleIsValid(tup))
	{
		/* the name already exists */
		Form_pg_enum en = (Form_pg_enum)GETSTRUCT(tup);

		/* we assume that the enum contains only int values */
		id = (int32)en->oid;

		ReleaseSysCache(tup);
	}
	else if (!allow_missing)
	{
		StringInfoData str;

		initStringInfo(&str);
		escape_json(&str, name);

		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("unknown key %s", str.data),
				 errdetail("The key is not imported yet"),
				 errhint("Import the keys with the mxkv_import_keys() function")));
	}

	return id;
}

static char *
mxkv_parse_int_array(const char **pdata, BitsWriterWidth width, int n,
					 int64 base, int64 autoinc, MxkvLookup *lookup)
{
	char *result;

	result = palloc(bits_writer_width_from_code(width) * n);
	*pdata = delta_decode_into_array(*pdata, width, base, autoinc, result, n,
									 (DeltaDecoderLookup *)lookup);

	return result;
}

/*
 * Parse the MXKV on-disk layout.
 *
 * This only parse the header, so it is a lightweight operation.
 */
void mxkv_parse(Mxkv *mxkv, const char *data, const MxkvVType *vtype)
{
	mxkv->data = data;
	mxkv->datalen = VARSIZE(data);

	mxkv_config_from_header(&mxkv->config,
							(const MxkvDiskHeader *)data,
							vtype);
	data += sizeof(MxkvDiskHeader);

	if (mxkv->config.has_typmod)
		data += sizeof(int32);

	/* at the moment we only support int32 as key & value types */
	Assert(mxkv->config.key_width == sizeof(int32));
	Assert(mxkv->config.offset_width == sizeof(int32));

	if (mxkv->config.clustered)
	{
		mxkv->max = *(uint32 *)data;
		data += mxkv->config.key_width;

		mxkv->mins = (uint32 *)data;
		data += mxkv->config.key_width * mxkv->config.nclusters;

		mxkv->offs = (uint32 *)data;
		data += mxkv->config.offset_width * mxkv->config.nclusters;
	}
	else
	{
		mxkv->max = 0;
		mxkv->mins = NULL;
		mxkv->offs = NULL;
	}

	mxkv->cluster_data = data;
}

/*
 * Parse the nth cluster.
 *
 * In non-clustered mode the "nth" is ignored.
 *
 * If "lookup" is provided, the index of the given key will be recorded during
 * the parsing.
 */
void mxkv_parse_cluster(const Mxkv *mxkv, MxkvCluster *cluster, int nth,
						MxkvLookup *lookup)
{
	const MxkvConfig *config = &mxkv->config;
	const char *data;

	if (config->clustered)
		data = mxkv->cluster_data + mxkv->offs[nth];
	else
		data = mxkv->cluster_data;

	cluster->mxkv = mxkv;
	cluster->nrecords = mxkv_decide_cluster_nrecords(config, nth);
	cluster->raw_keys = data;

	cluster->keys = (uint32 *)
		mxkv_parse_int_array(&data,
							 config->key_width_code,
							 cluster->nrecords,
							 /* see mxkv_serialize_keys() */
							 config->clustered ? mxkv->mins[nth] : 0,
							 MXKV_AUTOINC_KEY,
							 lookup);

	cluster->raw_values = data;

	/*
	 * TODO: maybe we could take the chance to record the value offsets for
	 * non-fixed-width mxkv variants.
	 */

	if (mxkv_vtype_is_varlen(&config->vtype))
		cluster->values = data;
	else if (mxkv_vtype_is_scaled(&config->vtype))
	{
		Assert(config->vtype.typid == FLOAT4OID ||
			   config->vtype.typid == FLOAT8OID);

		/*
		 * the scaled floats are represented as ints, we do not convert them
		 * back to floats until returning them to the caller.
		 */
		cluster->values = (const char *)
			mxkv_parse_int_array(&data,
								 config->value_width_code,
								 cluster->nrecords,
								 0 /* base value */,
								 MXKV_AUTOINC_VALUE,
								 NULL /* lookup */);
	}
	else if (config->vtype.typid == INT4OID)
	{
		/* int4 is delta encoded */
		cluster->values = (const char *)
			mxkv_parse_int_array(&data,
								 config->key_width_code,
								 cluster->nrecords,
								 0 /* base value */,
								 MXKV_AUTOINC_VALUE,
								 NULL /* lookup */);
	}
	else if (config->vtype.typid == FLOAT4OID ||
			 config->vtype.typid == FLOAT8OID)
	{
		/* float4/float8 is not encoded */
		cluster->values = data;
		data += config->vtype.typlen * cluster->nrecords;
	}
	else
		pg_unreachable();
}

/*
 * Return the cluster index that contains the key, or -1 if not found.
 */
static int
mxkv_find_cluster_guts(const Mxkv *mxkv, uint32 key)
{
	const uint32 *mins = mxkv->mins;
	uint32 max = mxkv->max;
	int L = 0;
	int R = mxkv->config.nclusters - 1;

	if (L > R)
		/* fast path: empty ranges */
		return -1;

	if (key < mins[L] || max < key)
		/* fast path: key is not within the global [min, max] */
		return -1;

	/* find the first cluster whose min <= key */
	while (L < R)
	{
		int M = L + (R - L) / 2;

		Assert(M < R);

		if (mins[M] <= key && key < mins[M + 1])
			/* there is no overlap of the ranges, so this is the only match */
			return M;
		else if (mins[M] < key)
			L = M + 1;
		else
			R = M - 1;
	}

	/* it is possible for "R < L", or even "R < 0", but L is always safe. */
	Assert(L >= 0 && L < mxkv->config.nclusters);

	if (mins[L] <= key)
		return L;
	else
		return -1;
}

/*
 * Return the cluster index that contains the key, or -1 if not found.
 */
int mxkv_find_cluster(const Mxkv *mxkv, uint32 key)
{
	if (mxkv->config.clustered)
		return mxkv_find_cluster_guts(mxkv, key);

	/* in non-clustered mode we should search the records directly */
	return 0;
}

/*
 * Return the index of the key, or -1 if not found
 *
 * It is a simply binary search, we could use bsearch(), but we don't want to
 * pay the cost of function calls to the cmp() function, so we do it manually.
 */
int mxkv_cluster_find_key(const MxkvCluster *cluster, uint32 key)
{
	int L = 0;
	int R = cluster->nrecords - 1;

	if (cluster->nrecords == 0)
		/* fast path: empty cluster */
		return -1;

	if (key < cluster->keys[L] || cluster->keys[R] < key)
		/* fast path: key is not within the [min, max], unlikely to happen */
		return -1;

	while (L < R)
	{
		int M = L + (R - L) / 2;

		if (cluster->keys[M] == key)
			/* keys are unique, so this is the only match */
			return M;
		else if (cluster->keys[M] < key)
			L = M + 1;
		else
			R = M - 1;
	}

	/* it is possible for "R < L", or even "R < 0", but L is always safe. */
	Assert(L >= 0 && L < cluster->nrecords);

	if (cluster->keys[L] == key)
		return L;
	else
		return -1;
}

const char *
mxkv_cluster_get_value(const MxkvCluster *cluster, int nth)
{
	MxkvClusterIter iter;

	Assert(nth >= 0 && nth < cluster->nrecords);

	/*
	 * XXX: this is slow, however this function is not supposed to be heavily
	 * called, for example it is only called once by the '->' operator, so it
	 * is not a big issue.
	 */
	mxkv_cluster_iter_begin(&iter, cluster);
	mxkv_cluster_iter_seek(&iter, nth);

	return mxkv_cluster_iter_get_value(&iter);
}

const char *
mxkv_lookup(bytea *bytes, text *keytext, const MxkvVType *vtype)
{
	Mxkv mxkv;
	MxkvCluster cluster;
	MxkvLookup lookup;
	int32 key;
	int nth;

	key = mxkv_key_id(text_to_cstring(keytext), true /* allow_missing */);
	if (key < 0)
		return NULL;

	mxkv_parse(&mxkv, (char *)bytes, vtype);
	if (mxkv.config.nrecords == 0)
		return NULL;

	nth = mxkv_find_cluster(&mxkv, key);
	if (nth < 0)
		return NULL;

	lookup.key = key;
	mxkv_parse_cluster(&mxkv, &cluster, nth, &lookup);
	if (lookup.nth < 0)
		return NULL;

	return mxkv_cluster_get_value(&cluster, lookup.nth);
}

bool mxkv_exists(bytea *bytes, text *keytext, const MxkvVType *vtype)
{
	Mxkv mxkv;
	MxkvCluster cluster;
	MxkvLookup lookup;
	int32 key;
	int nth;

	key = mxkv_key_id(text_to_cstring(keytext), true /* allow_missing */);
	if (key < 0)
		return false;

	mxkv_parse(&mxkv, (char *)bytes, vtype);
	if (mxkv.config.nrecords == 0)
		return false;

	nth = mxkv_find_cluster(&mxkv, key);
	if (nth < 0)
		return false;

	if (mxkv.config.clustered && mxkv.mins[nth] == key)
		return true;

	lookup.key = key;
	mxkv_parse_cluster(&mxkv, &cluster, nth, &lookup);

	return lookup.nth >= 0;
}

static void
mxkv_to_string(const Mxkv *mxkv, StringInfo result)
{
	MxkvIter iter;

	/* the result is a json object like {"key1": value1, "key2": value2} */
	appendStringInfoCharMacro(result, '{');

	mxkv_foreach_record_fast(iter, mxkv)
	{
		if (!mxkv_iter_is_begin_record(&iter))
		{
			appendStringInfoCharMacro(result, ',');
			appendStringInfoCharMacro(result, ' ');
		}

		escape_json(result, mxkv_key_name(mxkv_iter_get_key(&iter)));
		appendStringInfoCharMacro(result, ':');
		appendStringInfoCharMacro(result, ' ');

		mxkv_iter_value_to_string(&iter, result);
	}

	appendStringInfoCharMacro(result, '}');
}

/*
 * Convert mxkv bytea to cstring.
 */
char *
mxkv_out(bytea *bytes, const MxkvVType *vtype)
{
	Mxkv mxkv;
	StringInfoData result;

	mxkv_parse(&mxkv, (char *)bytes, vtype);

	initStringInfoOfSize(&result, mxkv.datalen);

	mxkv_to_string(&mxkv, &result);

	return result.data;
}

bytea *
mxkv_scale(bytea *bytes, const MxkvVType *vtype)
{
	Mxkv mxkv;
	MxkvConfig config;
	MxkvIter iter;

	/* first parse the src mxkv */
	mxkv_parse(&mxkv, (char *)bytes, vtype);
	mxkv_iter_begin(&iter, &mxkv);

	/* fill the output config */
	{
		mxkv_config_init(&config, mxkv.config.nrecords, vtype);

		/* turn off clustering if no enough records to fill a cluster */
		if (config.nrecords <= config.cluster_capacity)
		{
			config.clustered = false;
			mxkv_config_deduce(&config);
		}
	}

	return mxkv_serialize(&iter, &config);
}

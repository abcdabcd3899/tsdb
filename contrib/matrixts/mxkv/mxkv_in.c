/*-------------------------------------------------------------------------
 *
 * contrib/mxkv/mxkv_in.c
 *	  Convert text to mxkv bytea.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

/*
 * XXX: a dirty hack to enable 64-bit bms, see the comments of
 * BITS_PER_BITMAPWORD on how it works.
 *
 * mxkv uses bms for duplicate keys detection, key ids are of type uint32,
 * while 32-bit bms uses int32 for offsets, so once a key id >= 0x80000000 it
 * is considered as a negative bms offset, which will cause runtime crash, so
 * we must enable 64-bit bms in mxkv.
 */
#undef false
#define false 1
#include "nodes/bitmapset.h"
#undef false
#define false 0

#if BITS_PER_BITMAPWORD < 64
#error "mxkv: fail to enable 64-bit bitmapset"
#endif

#include "catalog/pg_type.h"
#include "nodes/bitmapset.h"
#include "utils/builtins.h"
#include "utils/float.h"
#include "utils/jsonapi.h"

#include "mxkv/bits_writer.h"
#include "mxkv/config.h"
#include "mxkv/delta_encoder.h"
#include "mxkv/delta_decoder.h"
#include "mxkv/mxkv.h"
#include "mxkv/mxkv_in.h"
#include "mxkv/mxkv_iter.h"

typedef struct MxkvJsonState
{
	JsonLexContext *lex;

	const char *value;
	JsonTokenType tokentype;

	int nrecords;
	StringInfoData keys;   /* keys are stored as int32 */
	StringInfoData values; /* values are stored as pointers */

	Bitmapset *seen;
} MxkvJsonState;

static void
_on_json_array(void *state)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			 errmsg("cannot convert an array to mxkv")));
}

static void
_on_json_object(void *state)
{
	MxkvJsonState *_state = state;

	if (_state->lex->lex_level != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("cannot convert nesting objects to mxkv")));
}

static void
_on_json_scalar(void *state, char *token, JsonTokenType tokentype)
{
	MxkvJsonState *_state = state;

	if (_state->lex->lex_level == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("cannot convert an scalar to mxkv")));

	_state->value = token;
	_state->tokentype = tokentype;
}

static void
_on_json_field_start(void *state, char *fname, bool isnull)
{
	/* nothing to do */
}

static void
_on_json_field_end(void *state, char *fname, bool isnull)
{
	MxkvJsonState *_state = state;
	int32 key = mxkv_key_id(fname, false /* allow_missing */);

	if (bms_is_member(key, _state->seen))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE),
				 errmsg("cannot insert duplicate keys to mxkv")));

	_state->seen = bms_add_member(_state->seen, key);

	if (!isnull)
	{
		char *value = pstrdup(_state->value);

		appendBinaryStringInfo(&_state->keys, (char *)&key, sizeof(key));
		appendBinaryStringInfo(&_state->values, (char *)&value,
							   sizeof(value));
		_state->nrecords += 1;
	}

	_state->value = NULL;
	_state->tokentype = JSON_TOKEN_INVALID;
}

static MxkvJsonState *
mxkv_parse_json(char *json)
{
	MxkvJsonState *state;
	JsonLexContext *lex;
	JsonSemAction *sem;

	lex = makeJsonLexContextCstringLen(json, strlen(json),
									   true /* need_escapes */);

	state = palloc0(sizeof(*state));
	state->lex = lex;

	/*
	 * we cannot predict how many key-value records will there be, so we
	 * blindly choose an initialize size, it benefits the case that there are
	 * only a small amount of key-value records.
	 */
	initStringInfoOfSize(&state->keys, 1024);
	initStringInfoOfSize(&state->values, 1024);

	sem = palloc0(sizeof(*sem));
	sem->semstate = state;
	sem->array_start = _on_json_array;
	sem->object_start = _on_json_object;
	sem->scalar = _on_json_scalar;
	sem->object_field_start = _on_json_field_start;
	sem->object_field_end = _on_json_field_end;

	pg_parse_json(lex, sem);

	/*
	 * the bms is only used during parsing for duplicates detection, and it can
	 * be very large, so we must free it asap, other it can cause oom during
	 * analyze, which does not use a per-tuple memory content, so the memleaks
	 * can be accumulated.
	 */
	bms_free(state->seen);
	state->seen = NULL;

	/* as well as the lex */
	if (state->lex->strval)
	{
		pfree(state->lex->strval->data);
		pfree(state->lex->strval);
	}

	pfree(state->lex);
	state->lex = NULL;

	/* and the sem */
	pfree(sem);

	return state;
}

static void
_mxkv_free_parse_state(MxkvJsonState *state)
{
	char **values = (char **)state->values.data;

	for (int i = 0; i < state->nrecords; ++i)
		pfree(values[i]);

	pfree(state->keys.data);
	pfree(state->values.data);
	pfree(state);
}

static int
_mxkv_qsort_cmp(const MxkvJsonState *state, int L, int R)
{
	const uint32 *keys = (uint32 *)state->keys.data;

	return keys[L] - keys[R];
}

static void
_mxkv_qsort_swap(MxkvJsonState *state, int L, int R)
{
	uint32 *keys = (uint32 *)state->keys.data;
	Datum *datums = (Datum *)state->values.data;
	uint32 key;
	Datum datum;

	key = keys[L];
	keys[L] = keys[R];
	keys[R] = key;

	datum = datums[L];
	datums[L] = datums[R];
	datums[R] = datum;
}

/*
 * A custom qsort() for mxkv json records.
 *
 * Some ideas are stolen from pg_qsort().
 */
static void
_mxkv_qsort(MxkvJsonState *state, int left, int n)
{
#define cmp(a, b) _mxkv_qsort_cmp(state, a, b)
#define swap(a, b) _mxkv_qsort_swap(state, a, b)

	int right = left + n;
	int pa,
		pb,
		pc,
		pd,
		pm;
	size_t d1,
		d2;

loop:
	if (n < 7)
	{
		for (int m = left + 1; m < right; ++m)
			for (int l = m; l > left && cmp(l - 1, l) > 0; --l)
				swap(l, l - 1);
		return;
	}

	{
		bool presorted = true;

		for (int m = left + 1; m < right; ++m)
			if (cmp(m - 1, m) > 0)
			{
				presorted = false;
				break;
			}

		if (presorted)
			return;
	}
	pm = left + n / 2;
	swap(left, pm);
	pa = pb = left + 1;
	pc = pd = right - 1;
	for (;;)
	{
		int r;

		while (pb <= pc && (r = cmp(pb, left)) <= 0)
		{
			if (r == 0)
			{
				swap(pa, pb);
				++pa;
			}
			++pb;
		}
		while (pb <= pc && (r = cmp(pc, left)) >= 0)
		{
			if (r == 0)
			{
				swap(pc, pd);
				--pd;
			}
			--pc;
		}
		if (pb > pc)
			break;
		swap(pb, pc);
		++pb;
		--pc;
	}

	d1 = Min(pa - left, pb - pa);
	if (d1 > 0)
		swap(left, pb - d1);
	d1 = Min(pd - pc, right - pd - 1);
	if (d1 > 0)
		swap(pb, right - d1);
	d1 = pb - pa;
	d2 = pd - pc;
	if (d1 <= d2)
	{
		/* Recurse on left partition, then iterate on right partition */
		if (d1 > 1)
			_mxkv_qsort(state, left, d1);
		if (d2 > 1)
		{
			/* Iterate rather than recurse to save stack space */
			/* pg_qsort(right - d2, d2 / es, es, cmp); */
			left = right - d2;
			n = d2;
			goto loop;
		}
	}
	else
	{
		/* Recurse on right partition, then iterate on left partition */
		if (d2 > 1)
			_mxkv_qsort(state, right - d2, d2);
		if (d1 > 1)
		{
			/* Iterate rather than recurse to save stack space */
			/* pg_qsort(a, d1 / es, es, cmp); */
			n = d1;
			goto loop;
		}
	}

#undef cmp
#undef swap
}

/*
 * Sort the records in key id order.
 *
 * The keys and values are stored separately, so we have to implement a custom
 * qsort() manually.
 */
static void
mxkv_sort_json_records(MxkvJsonState *state)
{
	_mxkv_qsort(state, 0, state->nrecords);
}

static uint32
mxkv_estimate_bytea_length(const Mxkv *mxkv, const MxkvConfig *config)
{
	uint32 length;

	/* calculate the total bytes, first the common parts */
	length = sizeof(MxkvDiskHeader); /* the disk layout header */

	length += sizeof(int32); /* the optional typmod */

	if (config->clustered)
	{
		/* when it is clustered, the cluster index is stored */
		length += config->key_width;						/* the max key */
		length += config->key_width * config->nclusters;	/* the min keys */
		length += config->offset_width * config->nclusters; /* the offsets */
	}

	length += config->key_width * config->nrecords; /* the keys in total */

	if (mxkv_vtype_is_varlen(&config->vtype))
	{
		/*
		 * it is not easy to iterate all the values and accumulate the accurate
		 * string lengths, the mxkv_foreach_record() does not work if mxkv is a
		 * fake one; and even if it works, it is too slow, so we only make an
		 * estimation, it can grow automatically if the estimation is not large
		 * enough.
		 */
		length += 16 * mxkv->config.nrecords;
	}
	else
		/* the values are fixed-length */
		length += config->vtype.typlen * mxkv->config.nrecords;

	return length;
}

static void
mxkv_serialize_header(StringInfo bin, const MxkvConfig *config)
{
	MxkvDiskHeader header;

	mxkv_config_to_header(config, &header);

	/* the vl_len_ is not known yet, we will update it later */
	appendBinaryStringInfo(bin, (char *)&header, sizeof(header));

	if (config->has_typmod)
		appendBinaryStringInfo(bin, (char *)&config->vtype.typmod,
							   sizeof(config->vtype.typmod));
}

static void
mxkv_serialize_keys(StringInfo bin, MxkvIter *iter,
					const MxkvConfig *config, int first, int last)
{
	BitsWriter writer;
	DeltaEncoder encoder;

	bits_writer_init(&writer, bin);

	/* the keys, auto increase by 1 */
	delta_encoder_begin(&encoder, &writer, config->key_width_code,
						/*
						 * for non-clustered case, the first key's delta is
						 * calculated with 0, otherwise it is calculated with
						 * itself.
						 */
						config->clustered ? mxkv_iter_get_key(iter) : 0,
						MXKV_AUTOINC_KEY);
	{
		for (int i = first; i <= last; ++i)
		{
			delta_encoder_push(&encoder, mxkv_iter_get_key(iter));

			Assert(!mxkv_iter_is_end_record(iter));
			mxkv_iter_next_record(iter);
		}
	}
	delta_encoder_end(&encoder);
}

static void
mxkv_serialize_values(StringInfo bin, MxkvIter *iter,
					  const MxkvConfig *config, int first, int last)
{
	BitsWriter writer;
	DeltaEncoder encoder;

	bits_writer_init(&writer, bin);

	/*
	 * the values.  a delta encoder is prepared for the variants that needs it,
	 * and it is cheap and safe even if it does not.
	 */
	delta_encoder_begin(&encoder, &writer, config->value_width_code,
						0 /* base value */, MXKV_AUTOINC_VALUE);
	{
		for (int i = first; i <= last; ++i)
		{
			if (mxkv_vtype_is_varlen(&iter->mxkv->config.vtype))
			{
				const char *text = mxkv_iter_get_value(iter);

				mxkv_vtype_serialize(&config->vtype, &encoder, text);
			}
			else
			{
				StringInfoData tmp;

				initStringInfoOfSize(&tmp, 32);
				mxkv_iter_value_to_string(iter, &tmp);

				mxkv_vtype_serialize(&config->vtype, &encoder, tmp.data);
				pfree(tmp.data);
			}

			Assert(!mxkv_iter_is_end_record(iter));
			mxkv_iter_next_record(iter);
		}
	}
	delta_encoder_end(&encoder);
}

static void
mxkv_serialize_records(StringInfo bin, MxkvIter *iter,
					   const MxkvConfig *config, int first, int last)
{
	/*
	 * we have to iterate twice, once for keys, and once for values, so we
	 * have to backup the iter first.
	 */
	MxkvIter iter0 = *iter;

	mxkv_serialize_keys(bin, iter, config, first, last);

	/* now restore the iter to iterate the values */
	*iter = iter0;

	mxkv_serialize_values(bin, iter, config, first, last);
}

static void
mxkv_serialize_clusters(StringInfo bin, MxkvIter *iter,
						const MxkvConfig *config)
{
	uint32 mins_offset;
	uint32 offs_offset;
	uint32 clusters_offset;

	/*
	 * first comes the min keys and offsets of the clusters, but we do not know
	 * them yet, so put some placeholders, we will fill them later.
	 *
	 * TODO: maybe we want to adjust bin.len directly.
	 */
	{
		uint64 max = 0;

		/*
		 * the max key, we initialize it to 0, so when nclusters=0 we do not
		 * store __garbage__ data for the field.  however usually we should
		 * set flags.clustered to off to skip the cluster index.
		 */
		appendBinaryStringInfo(bin, (char *)&max, config->key_width);

		mins_offset = bin->len; /* record where the min keys are */
		appendStringInfoSpaces(bin, config->key_width * config->nclusters);

		offs_offset = bin->len; /* record where the offsets are */
		appendStringInfoSpaces(bin, config->offset_width * config->nclusters);
	}

	clusters_offset = bin->len; /* record where the clusters are */

	/* the clusters, one by one */
	for (int nth = 0; nth < config->nclusters; ++nth)
	{
		uint32 first;
		uint32 last;

		mxkv_decide_cluster_records(config, nth, &first, &last);

		/* now we know all the range information of the cluster, fill it */
		{
			/*
			 * the ranges pointer must be re-calculated every time, in case the
			 * bin.data gets repalloc'ed.
			 */
			uint32 *mins = (uint32 *)(bin->data + mins_offset);
			uint32 *offs = (uint32 *)(bin->data + offs_offset);

			mins[nth] = mxkv_iter_get_key(iter);
			offs[nth] = bin->len - clusters_offset;
		}

		/* write the records */
		mxkv_serialize_records(bin, iter, config, first, last);
	}

	/*
	 * fill mins[-1], the max key of all the clusters, we do this after the
	 * full iteration, so the iter->cluster is still valid, and is the last
	 * cluster, so we have a chance to access the last key.
	 */
	{
		const MxkvCluster *cluster = &iter->cluster;
		uint32 *mins = (uint32 *)(bin->data + mins_offset);

		mins[-1] = cluster->keys[cluster->nrecords - 1];
	}
}

bytea *
mxkv_serialize(MxkvIter *iter, const MxkvConfig *config)
{
	StringInfoData bin;
	uint32 vl_len;
	bytea *result;

	/* at the moment we only support int32 as key & value types */
	Assert(config->key_width == sizeof(int32));
	Assert(config->offset_width == sizeof(int32));

	/*
	 * prepare the result buffer, with 8 more bytes, just in case.
	 *
	 * we want to avoid resizing the string info, but do not rely on it,
	 * the following logic must work even if the resizing happens.
	 */
	vl_len = mxkv_estimate_bytea_length(iter->mxkv, config);
	initStringInfoOfSize(&bin, vl_len + 8);

	/* the header */
	mxkv_serialize_header(&bin, config);

	if (config->clustered)
		/* the records are clustered */
		mxkv_serialize_clusters(&bin, iter, config);
	else
		/* the records are not clustered */
		mxkv_serialize_records(&bin, iter, config, 0, config->nrecords - 1);

	/* set the actual var size */
	result = (bytea *)bin.data;
	SET_VARSIZE(result, bin.len);

	return result;
}

bytea *
mxkv_in(char *json, const MxkvVType *vtype)
{
	MxkvJsonState *state;
	MxkvConfig config_out;
	MxkvConfig *config = &config_out;
	Mxkv mxkv_in;
	MxkvIter iter;
	bytea *result;

	/* parse the input json string */
	state = mxkv_parse_json(json);
	/* sort by key id */
	mxkv_sort_json_records(state);

	/*
	 * add a mxkv shell for the records, so we can access them with the
	 * iterators.
	 */
	{
		/* the json parse results are always text */
		MxkvVType vtype_in = mxkv_text_init_vtype(-1);
		MxkvCluster *cluster = &iter.cluster;

		vtype_in.byval = false; /* turn off byval */

		memset(&mxkv_in, 0, sizeof(mxkv_in));
		mxkv_config_init(&mxkv_in.config, state->nrecords, &vtype_in);

		mxkv_in.config.clustered = false;
		mxkv_in.config.nclusters = 1;
		mxkv_config_deduce(&mxkv_in.config);

		mxkv_iter_init(&iter, &mxkv_in);

		cluster->mxkv = &mxkv_in;
		cluster->nrecords = state->nrecords;
		cluster->raw_keys = NULL;
		cluster->raw_values = NULL;
		cluster->keys = (uint32 *)state->keys.data;
		cluster->values = state->values.data;

		mxkv_cluster_iter_begin(&iter.citer, cluster);
	}

	/* fill the output config */
	{
		mxkv_config_init(config, state->nrecords, vtype);

		/* turn off clustering if no enough records to fill a cluster */
		if (config->nrecords <= config->cluster_capacity)
		{
			config->clustered = false;
			mxkv_config_deduce(config);
		}
	}

	result = mxkv_serialize(&iter, config);

	/*
	 * free the parse state, see the bms comments in mxkv_parse_json() for
	 * details.
	 */
	_mxkv_free_parse_state(state);

	return result;
}

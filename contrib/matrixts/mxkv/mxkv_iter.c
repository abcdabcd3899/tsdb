/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/mxkv_iter.c
 *	  mxkv iterators
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/json.h"

#include "mxkv/mxkv_iter.h"

void mxkv_cluster_iter_load_value(MxkvClusterIter *iter)
{
	const MxkvConfig *config = &iter->cluster->mxkv->config;

	/* byval=off is only possible for varlen values */
	AssertImply(!mxkv_vtype_is_byval(&config->vtype),
				mxkv_vtype_is_varlen(&config->vtype));

	if (!mxkv_vtype_is_byval(&config->vtype))
		iter->value = ((const char **)iter->cluster->values)[iter->nth];
	else if (mxkv_vtype_is_varlen(&config->vtype))
		iter->vlen = strlen(iter->value) + 1;
	else if (!mxkv_vtype_is_scaled(&config->vtype))
		Assert((config->vtype.typid == INT4OID && iter->vlen == sizeof(int32)) ||
			   (config->vtype.typid == FLOAT4OID && iter->vlen == sizeof(float4)) ||
			   (config->vtype.typid == FLOAT4OID && iter->vlen == sizeof(float4)) ||
			   (config->vtype.typid == FLOAT8OID && iter->vlen == sizeof(float8)));
	else if (config->vtype.typid == FLOAT4OID)
	{
		const char *value = iter->cluster->values + sizeof(int32) * iter->nth;
		float4 *p = (float4 *)iter->buf0;

		*p = mxkv_vtype_load_float4(&config->vtype, &value);
		Assert(iter->vlen == sizeof(float4));
	}
	else if (config->vtype.typid == FLOAT8OID)
	{
		const char *value = iter->cluster->values + sizeof(int64) * iter->nth;
		float8 *p = (float8 *)iter->buf0;

		*p = mxkv_vtype_load_float8(&config->vtype, &value);
		Assert(iter->vlen == sizeof(float8));
	}
	else
		pg_unreachable();
}

void mxkv_cluster_iter_value_to_string(const MxkvClusterIter *iter, StringInfo str)
{
	const MxkvConfig *config = &iter->cluster->mxkv->config;
	const char *v = mxkv_cluster_iter_get_value(iter);

	if (mxkv_vtype_is_varlen(&config->vtype))
	{
		/*
		 * show the value as a json string, this will also show digit values as
		 * strings, for example {"a":1} is displayed as {"a":"1"}, but this
		 * does not matter a lot, we could still convert it to mxkv_int4
		 * successfully.
		 *
		 * FIXME: however this might be the expected behavior of the users, so
		 * later we might still have to distinguish 1 and "1".
		 */
		escape_json(str, v);
	}
	else if (config->vtype.typid == INT4OID)
		mxkv_int32_to_string(str, *(int32 *)v);
	else if (config->vtype.typid == FLOAT4OID)
		mxkv_float4_to_string(str, *(float4 *)v);
	else if (config->vtype.typid == FLOAT8OID)
		mxkv_float8_to_string(str, *(float8 *)v);
	else
		pg_unreachable();
}

void mxkv_cluster_iter_seek(MxkvClusterIter *iter, int nth)
{
	const MxkvConfig *config = &iter->cluster->mxkv->config;

	Assert(nth <= iter->cluster->nrecords);

	if (!mxkv_vtype_is_byval(&config->vtype))
		iter->value = iter->cluster->values + iter->vlen * nth;
	else if (mxkv_vtype_is_varlen(&config->vtype))
	{
		if (iter->nth > nth)
			/* rewind from the beginning */
			mxkv_cluster_iter_begin(iter, iter->cluster);

		while (iter->nth < nth)
			mxkv_cluster_iter_next(iter);
	}
	else if (!mxkv_vtype_is_scaled(&config->vtype))
		iter->value = iter->cluster->values + iter->vlen * nth;

	iter->nth = nth;

	if (!mxkv_cluster_iter_is_end(iter))
		mxkv_cluster_iter_load_value(iter);
}

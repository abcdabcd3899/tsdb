/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/mxkv_iter.h
 *	  mxkv iterators
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_MXKV_ITER_H
#define MXKV_MXKV_ITER_H

#include "postgres.h"

#include "mxkv/mxkv.h"

typedef struct MxkvClusterIter
{
	const MxkvCluster *cluster;
	const char *value;
	int vlen;
	int nth;
	char *buf;
	char buf0[sizeof(Datum)];
} MxkvClusterIter;

#define mxkv_cluster_foreach_record(iter, cluster)  \
	for (mxkv_cluster_iter_begin(&(iter), cluster); \
		 !mxkv_cluster_iter_is_end(&(iter));        \
		 mxkv_cluster_iter_next(&(iter)))

extern void mxkv_cluster_iter_load_value(MxkvClusterIter *iter);
extern void mxkv_cluster_iter_value_to_string(const MxkvClusterIter *iter,
											  StringInfo str);
extern void mxkv_cluster_iter_seek(MxkvClusterIter *iter, int nth);

static inline bool pg_attribute_unused()
	mxkv_cluster_iter_is_end(const MxkvClusterIter *iter)
{
	return iter->nth >= iter->cluster->nrecords;
}

static inline bool pg_attribute_unused()
	mxkv_cluster_iter_is_begin(const MxkvClusterIter *iter)
{
	return iter->nth == 0;
}

static inline void pg_attribute_unused()
	mxkv_cluster_iter_begin(MxkvClusterIter *iter, const MxkvCluster *cluster)
{
	const MxkvConfig *config = &cluster->mxkv->config;

	iter->cluster = cluster;
	/* var varlen vtypes the real vlen is decided in load_value() */
	iter->vlen = config->vtype.typlen;
	iter->nth = 0;
	iter->buf = NULL;

	if (mxkv_vtype_is_scaled(&config->vtype))
		/* for scaled types the result is decoded into the buffer */
		iter->value = iter->buf0;
	else
		/* otherwise the result is stored in place in the values */
		iter->value = cluster->values;

	if (!mxkv_cluster_iter_is_end(iter))
		mxkv_cluster_iter_load_value(iter);
}

static inline void pg_attribute_unused()
	mxkv_cluster_iter_next(MxkvClusterIter *iter)
{
	const MxkvConfig *config = &iter->cluster->mxkv->config;

	Assert(!mxkv_cluster_iter_is_end(iter));

	iter->nth += 1;

	if (unlikely(mxkv_cluster_iter_is_end(iter)))
		return;

	if (!mxkv_vtype_is_byval(&config->vtype))
		/* nothing to do */
		;
	else if (!mxkv_vtype_is_scaled(&config->vtype))
		iter->value += iter->vlen;

	mxkv_cluster_iter_load_value(iter);
}

static inline uint32 pg_attribute_unused()
	mxkv_cluster_iter_get_key(const MxkvClusterIter *iter)
{
	Assert(!mxkv_cluster_iter_is_end(iter));
	return iter->cluster->keys[iter->nth];
}

static inline const char *pg_attribute_unused()
	mxkv_cluster_iter_get_value(const MxkvClusterIter *iter)
{
	Assert(!mxkv_cluster_iter_is_end(iter));
	return iter->value;
}

/**************************************************************************/

typedef struct MxkvIter
{
	const Mxkv *mxkv;
	int nth;

	MxkvCluster cluster;
	MxkvClusterIter citer;
} MxkvIter;

#define mxkv_foreach_cluster(iter, mxkv)     \
	for (mxkv_iter_begin(&(iter), mxkv);     \
		 !mxkv_iter_is_end_cluster(&(iter)); \
		 mxkv_iter_next_cluster(&(iter)))

#define mxkv_foreach_record(iter, mxkv)     \
	for (mxkv_iter_begin(&(iter), mxkv);    \
		 !mxkv_iter_is_end_record(&(iter)); \
		 mxkv_iter_next_record(&(iter)))

#define mxkv_foreach_record_fast(iter, mxkv) \
	mxkv_foreach_cluster(iter, mxkv)         \
		mxkv_cluster_foreach_record((iter).citer, &(iter).cluster)

static inline bool pg_attribute_unused()
	mxkv_iter_is_end_cluster(const MxkvIter *iter)
{
	const MxkvConfig *config = &iter->mxkv->config;

	/* in non-clustered mode there is only zero or one cluster */
	AssertImply(!config->clustered, config->nclusters <= 1);

	return iter->nth >= config->nclusters;
}

static inline bool pg_attribute_unused()
	mxkv_iter_is_last_cluster(const MxkvIter *iter)
{
	const MxkvConfig *config = &iter->mxkv->config;

	/* in non-clustered mode there is only zero or one cluster */
	AssertImply(!config->clustered, config->nclusters <= 1);

	return iter->nth == config->nclusters - 1;
}

static inline bool pg_attribute_unused()
	mxkv_iter_is_begin_cluster(const MxkvIter *iter)
{
	return iter->nth == 0;
}

static inline void pg_attribute_unused()
	mxkv_iter_next_cluster(MxkvIter *iter)
{
	Assert(!mxkv_iter_is_end_cluster(iter));

	iter->nth += 1;

	if (unlikely(mxkv_iter_is_end_cluster(iter)))
		return;

	mxkv_parse_cluster(iter->mxkv, &iter->cluster, iter->nth,
					   NULL /* lookup */);
	mxkv_cluster_iter_begin(&iter->citer, &iter->cluster);
}

static inline bool pg_attribute_unused()
	mxkv_iter_is_end_record(const MxkvIter *iter)
{
	return (mxkv_iter_is_end_cluster(iter) ||
			(mxkv_iter_is_last_cluster(iter) &&
			 mxkv_cluster_iter_is_end(&iter->citer)));
}

static inline bool pg_attribute_unused()
	mxkv_iter_is_begin_record(const MxkvIter *iter)
{
	return (mxkv_iter_is_begin_cluster(iter) &&
			mxkv_cluster_iter_is_begin(&iter->citer));
}

/*
 * Init a mxkv iter, but do not begin it.
 *
 * The cluster and ther cluster iter are uninitialized at all, the caller is
 * responsible to fill them.
 *
 * This is useful when the caller wants to setup the cluster manually, like in
 * mxkv_in().
 */
static inline void pg_attribute_unused()
	mxkv_iter_init(MxkvIter *iter, const Mxkv *mxkv)
{
	iter->mxkv = mxkv;
	iter->nth = 0;
}

static inline void pg_attribute_unused()
	mxkv_iter_begin(MxkvIter *iter, const Mxkv *mxkv)
{
	iter->mxkv = mxkv;
	iter->nth = 0;

	if (!mxkv_iter_is_end_cluster(iter))
	{
		mxkv_parse_cluster(iter->mxkv, &iter->cluster, iter->nth,
						   NULL /* lookup */);
		mxkv_cluster_iter_begin(&iter->citer, &iter->cluster);
	}
}

static inline void pg_attribute_unused()
	mxkv_iter_next_record(MxkvIter *iter)
{
	Assert(!mxkv_iter_is_end_record(iter));

	if (likely(!mxkv_cluster_iter_is_end(&iter->citer)))
		mxkv_cluster_iter_next(&iter->citer);

	if (unlikely(mxkv_cluster_iter_is_end(&iter->citer)))
		mxkv_iter_next_cluster(iter);
}

static inline const MxkvCluster *pg_attribute_unused()
	mxkv_iter_get_cluster(const MxkvIter *iter)
{
	Assert(!mxkv_iter_is_end_cluster(iter));
	return &iter->cluster;
}

static inline const MxkvClusterIter *pg_attribute_unused()
	mxkv_iter_get_cluster_iter(const MxkvIter *iter)
{
	Assert(!mxkv_iter_is_end_cluster(iter));
	return &iter->citer;
}

static inline uint32 pg_attribute_unused()
	mxkv_iter_get_key(const MxkvIter *iter)
{
	Assert(!mxkv_iter_is_end_record(iter));
	return mxkv_cluster_iter_get_key(&iter->citer);
}

static inline const char *pg_attribute_unused()
	mxkv_iter_get_value(const MxkvIter *iter)
{
	Assert(!mxkv_iter_is_end_record(iter));
	return mxkv_cluster_iter_get_value(&iter->citer);
}

static inline void pg_attribute_unused()
	mxkv_iter_value_to_string(const MxkvIter *iter, StringInfo str)
{
	Assert(!mxkv_iter_is_end_record(iter));
	mxkv_cluster_iter_value_to_string(&iter->citer, str);
}

#endif /* MXKV_MXKV_ITER_H */

/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/mxkv.h
 *	  mxkv, a type for large set of key-value records.
 *
 * The disk layout:
 *
 *		struct
 *		{
 *			// first 4 bytes are the varlena length, required by postgres
 *			int32		vl_len_;
 *
 *			// # of key-value records
 *			uint32		nrecords;
 *
 *			// the version of the disk layout
 *			uint8		version;
 *
 *			// (1 << cluster_size_shift) is the # of records per cluster
 *			uint8		cluster_size_shift;
 *
 *			using cluster_capacity = 1 << cluster_size_shift;
 *			using nclusters = (nrecords + cluster_capacity - 1) / cluster_capacity;
 *
 *			// switches:
 *			// - flags & 0x03: key_width: 0->1, 1->2, 2->4
 *			// - flags & 0x0c: offset_width: 0->1, 1->2, 2->4
 *			// - flags & 0x10: keys_encoded
 *			// - flags & 0x20: values_encoded
 *			// - flags & 0x40: clustered
 *			// - flags & 0x80: has_typmod
 *			uint8		flags;
 *
 *			// for future use
 *			uint8		padding;
 *
 *			// decide key type according to key_width
 *			using KeyType = uint{8,16,32};
 *
 *			// decide offset type according to offset_width
 *			using OffsetType = uint{8,16,32};
 *
 *			// the typmod is optional
 *			if (has_typmod)
 *			{
 *				int32		typmod;
 *			}
 *
 *			// the cluster index
 *			if (clustered)
 *			{
 *				// the max key of all the clusters
 *				KeyType		max;
 *
 *				// the min keys of clusters
 *				KeyType		mins[nclusters];
 *
 *				// the offsets of clusters
 *				OffsetType	offs;
 *			};
 *
 *			// the clusters
 *			struct
 *			{
 *				// the keys, are encoded if keys_encoded is true
 *				KeyType		keys[count];
 *
 *				// the values, the format depends on the value type,
 *				// are encoded if values_encoded is true
 *				char		values[];
 *			} clusters[nclusters];
 *		};
 *
 * All the numbers are stored in native byte order.
 *
 * FIXME: the bitmap of nulls should also be stored.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_MXKV_H
#define MXKV_MXKV_H

#include "postgres.h"

#include "mxkv/config.h"

#define MXKV_ENUM_NAME "mxkv_keys"
#define MXKV_ENUM_TYPID mxkv_enum_typid()

#define MXKV_AUTOINC_KEY 2
#define MXKV_AUTOINC_VALUE 0

/*
 * Mxkv in-memory layout.
 */
typedef struct Mxkv
{
	MxkvConfig config;

	const char *data;		  /* raw data */
	uint32 datalen;			  /* data length */
	uint32 max;				  /* the max key of all the clusters */
	const uint32 *mins;		  /* the min keys of clusters */
	const uint32 *offs;		  /* the offsets of clusters */
	const char *cluster_data; /* the raw data of clusters */
} Mxkv;

/*
 * A cluster contains encoded key-value records, and can be fast seeked
 * according to the ranges.
 */
typedef struct MxkvCluster
{
	const Mxkv *mxkv;

	uint32 nrecords; /* # of key-value records */

	const char *raw_keys;	/* raw data of keys */
	const char *raw_values; /* raw data of values */

	const uint32 *keys; /* the keys */
	const char *values; /* the values */
} MxkvCluster;

typedef struct MxkvLookup
{
	uint32 key;
	int nth;
} MxkvLookup;

extern Oid mxkv_enum_typid(void);

extern char *mxkv_key_name(uint32 id);
extern int32 mxkv_key_id(const char *name, bool allow_missing);

extern void mxkv_parse(Mxkv *mxkv, const char *data, const MxkvVType *vtype);
extern void mxkv_parse_cluster(const Mxkv *mxkv, MxkvCluster *cluster, int nth,
							   MxkvLookup *lookup);
extern int mxkv_find_cluster(const Mxkv *mxkv, uint32 key);

extern int mxkv_cluster_find_key(const MxkvCluster *cluster, uint32 key);
extern const char *mxkv_cluster_get_value(const MxkvCluster *cluster, int nth);

extern const char *mxkv_lookup(bytea *bytes, text *keytext,
							   const MxkvVType *vtype);
extern bool mxkv_exists(bytea *bytes, text *keytext, const MxkvVType *vtype);
extern char *mxkv_out(bytea *bytes, const MxkvVType *vtype);
extern bytea *mxkv_scale(bytea *bytes, const MxkvVType *vtype);

#endif /* MXKV_MXKV_H */

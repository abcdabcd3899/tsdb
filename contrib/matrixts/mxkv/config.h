/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/config.h
 *	  The mxkv configuration.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MXKV_CONFIG_H
#define MXKV_CONFIG_H

#include "postgres.h"

#include "mxkv/bits_writer.h"
#include "mxkv/types.h"

/* the MXKV version code */
#define MXKV_LAYOUT_VERSION 1

/* the default cluster capacity shift, which means 64 kv records per cluster */
#define MXKV_CLUSTER_CAPACITY_SHIFT 6

/*
 * The MXKV on-disk layout header.
 *
 * TODO: the vl_len_ is stored in network byte-order, the big-endian; other
 * fields are stored in native byte-order, it is little-endian on PC.  this is
 * to save some cpu cycles when reading/writing, but it would cause problem
 * when we want to support cross-architecture clusters, or debug a filedump of
 * a different architecture.
 */
typedef struct pg_attribute_packed() MxkvDiskHeader
{
	/*
	 * the total length, must be the first, must be get/set with
	 * VARSIZE()/SET_VARSIZE().
	 */
	int32 vl_len_;

	uint32 nrecords; /* # of records */

	uint8 version;				  /* the version code */
	uint8 cluster_capacity_shift; /* cluster capacity = 1 << shift */
	uint8 flags;				  /* flag bits, see MXKV_FLAGS_* macros */
	uint8 padding;				  /* padding byte */

	/* typmod is only stored in the header when flags.has_typmod is true */
	int32 typmod[];
}
pg_attribute_packed() MxkvDiskHeader;

/*
 * The MXKV in-memory header.
 *
 * All the fields are in native byte-order.
 */
typedef struct MxkvConfig
{
	/*
	 * the in-memory layout of MxkvDiskHeader, all these fields have a
	 * corresponding part in MxkvDiskHeader.
	 *
	 * these must be filled, and can be adjusted, just remember to call
	 * mxkv_config_deduce() after the adjustment.
	 */

	uint32 nrecords;				   /* MxkvDiskHeader.nrecords */
	uint8 version;					   /* MxkvDiskHeader.version */
	uint8 cluster_capacity_shift;	   /* .cluster_capacity_shift */
	BitsWriterWidth key_width_code;	   /* .flags.key_width */
	BitsWriterWidth offset_width_code; /* .flags.offset_width */
	bool keys_encoded;				   /* .flags.keys_encoded */
	bool clustered;					   /* .flags.clustered */
	bool has_typmod;				   /* .flags.has_typmod */

	/* provided by the UDF */
	MxkvVType vtype;
	BitsWriterWidth value_width_code;

	/* these are deduced from above information */
	uint32 cluster_capacity;
	uint32 nclusters;
	uint8 key_width;
	uint8 offset_width;
	bool values_encoded;
} MxkvConfig;

extern void mxkv_config_init(MxkvConfig *config, uint32 nrecords,
							 const MxkvVType *vtype);
extern void mxkv_config_deduce(MxkvConfig *config);
extern void mxkv_config_to_header(const MxkvConfig *config,
								  MxkvDiskHeader *header);
extern void mxkv_config_from_header(MxkvConfig *config,
									const MxkvDiskHeader *header,
									const MxkvVType *vtype);

extern void mxkv_decide_cluster_records(const MxkvConfig *config, int nth,
										uint32 *first, uint32 *last);

extern uint32 mxkv_decide_cluster_nrecords(const MxkvConfig *config, int nth);

#endif /* MXKV_CONFIG_H */

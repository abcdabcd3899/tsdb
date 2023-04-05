/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/config.c
 *	  The mxkv configuration.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"

#include "mxkv/mxkv.h"
#include "mxkv/config.h"

/*
 * The key width code, see BitsWriterWidth, only 4B is supported at present.
 */
#define MXKV_FLAGS_MASK_KEY_WIDTH 0x03
#define MXKV_FLAGS_SHIFT_KEY_WIDTH 0

/*
 * The offset width code, see BitsWriterWidth, only 4B is supported at present.
 */
#define MXKV_FLAGS_MASK_OFFSET_WIDTH 0x0c
#define MXKV_FLAGS_SHIFT_OFFSET_WIDTH 2

/* this big is set if the keys are encoded */
#define MXKV_FLAGS_MASK_KEYS_ENCODED 0x10
#define MXKV_FLAGS_SHIFT_KEYS_ENCODED 4

/* this big is set if the values are encoded */
#define MXKV_FLAGS_MASK_VALUES_ENCODED 0x20
#define MXKV_FLAGS_SHIFT_VALUES_ENCODED 5

/* this big is set if the kv records are clustered */
#define MXKV_FLAGS_MASK_CLUSTERED 0x40
#define MXKV_FLAGS_SHIFT_CLUSTERED 6

/* this big is set if the typmod is stored in the header */
#define MXKV_FLAGS_MASK_HAS_TYPMOD 0x80
#define MXKV_FLAGS_SHIFT_HAS_TYPMOD 7

#define __MXKV_FLAGS_ISSET(flags, mask, shift) \
	((flags) & (mask))
#define __MXKV_FLAGS_GET(flags, mask, shift) \
	(((flags) & (mask)) >> (shift))
#define __MXKV_FLAGS_SET(flags, v, mask, shift) \
	(flags = ((flags) & ~(mask)) | (((v) << (shift)) & (mask)))
#define __MXKV_FLAGS_UNSET(flags, mask, shift) \
	(flags = ((flags) & ~(mask)))

#define MXKV_FLAGS_ISSET(flags, name) \
	__MXKV_FLAGS_ISSET(flags, MXKV_FLAGS_MASK_##name, MXKV_FLAGS_SHIFT_##name)
#define MXKV_FLAGS_GET(flags, name) \
	__MXKV_FLAGS_GET(flags, MXKV_FLAGS_MASK_##name, MXKV_FLAGS_SHIFT_##name)
#define MXKV_FLAGS_SET(flags, v, name) \
	__MXKV_FLAGS_SET(flags, v, MXKV_FLAGS_MASK_##name, MXKV_FLAGS_SHIFT_##name)
#define MXKV_FLAGS_UNSET(flags, name) \
	__MXKV_FLAGS_UNSET(flags, MXKV_FLAGS_MASK_##name, MXKV_FLAGS_SHIFT_##name)

static uint32
mxkv_decide_nclusters(uint32 nrecords, uint32 cluster_capacity)
{
	return (nrecords + cluster_capacity - 1) / cluster_capacity;
}

/*
 * Fill the config with mostly default settings.
 */
void mxkv_config_init(MxkvConfig *config, uint32 nrecords, const MxkvVType *vtype)
{
	config->nrecords = nrecords;
	config->version = MXKV_LAYOUT_VERSION;
	config->cluster_capacity_shift = MXKV_CLUSTER_CAPACITY_SHIFT;
	config->key_width_code = BITS_WRITER_WIDTH_4B;
	config->offset_width_code = BITS_WRITER_WIDTH_4B;
	config->keys_encoded = true;
	config->clustered = true;

	config->vtype = *vtype;
	switch (vtype->typlen)
	{
	case 4:
		config->value_width_code = BITS_WRITER_WIDTH_4B;
		break;
	case 8:
		config->value_width_code = BITS_WRITER_WIDTH_8B;
		break;
	default:
		config->value_width_code = BITS_WRITER_WIDTH_INVALID;
		break;
	}

	mxkv_config_deduce(config);
}

void mxkv_config_deduce(MxkvConfig *config)
{
	mxkv_vtype_deduce(&config->vtype);

	config->cluster_capacity = 1 << config->cluster_capacity_shift;
	config->key_width = bits_writer_width_from_code(config->key_width_code);
	config->offset_width = bits_writer_width_from_code(config->offset_width_code);
	config->values_encoded = mxkv_vtype_supports_encoding(&config->vtype);
	config->has_typmod = mxkv_vtype_has_typmod(&config->vtype);

	if (config->clustered)
		config->nclusters = mxkv_decide_nclusters(config->nrecords,
												  config->cluster_capacity);
	else if (config->nrecords > 0)
		config->nclusters = config->nrecords > 0 ? 1 : 0;
}

void mxkv_config_to_header(const MxkvConfig *config, MxkvDiskHeader *header)
{
	header->vl_len_ = 0; /* the total length is not known yet */
	header->nrecords = config->nrecords;
	header->version = config->version;
	header->cluster_capacity_shift = config->cluster_capacity_shift;
	header->flags = 0;
	header->padding = 0;

	MXKV_FLAGS_SET(header->flags, config->key_width_code, KEY_WIDTH);
	MXKV_FLAGS_SET(header->flags, config->offset_width_code, OFFSET_WIDTH);
	MXKV_FLAGS_SET(header->flags, config->keys_encoded, KEYS_ENCODED);
	MXKV_FLAGS_SET(header->flags, config->values_encoded, VALUES_ENCODED);
	MXKV_FLAGS_SET(header->flags, config->clustered, CLUSTERED);
	MXKV_FLAGS_SET(header->flags, config->has_typmod, HAS_TYPMOD);
}

void mxkv_config_from_header(MxkvConfig *config,
							 const MxkvDiskHeader *header,
							 const MxkvVType *vtype)
{
	uint32 nrecords = header->nrecords;

	mxkv_config_init(config, nrecords, vtype);

	config->key_width_code = MXKV_FLAGS_GET(header->flags, KEY_WIDTH);
	config->offset_width_code = MXKV_FLAGS_GET(header->flags, OFFSET_WIDTH);
	config->keys_encoded = MXKV_FLAGS_GET(header->flags, KEYS_ENCODED);
	config->values_encoded = MXKV_FLAGS_GET(header->flags, VALUES_ENCODED);
	config->clustered = MXKV_FLAGS_GET(header->flags, CLUSTERED);
	config->has_typmod = MXKV_FLAGS_GET(header->flags, HAS_TYPMOD);

	if (config->has_typmod)
		config->vtype.typmod = header->typmod[0];
	else
		config->vtype.typmod = -1;

	mxkv_config_deduce(config);
}

/*
 * Decide the indices of the first & last records in the nth cluster.
 */
void mxkv_decide_cluster_records(const MxkvConfig *config, int nth,
								 uint32 *first, uint32 *last)
{
	*first = config->cluster_capacity * nth;
	*last = Min(*first + config->cluster_capacity - 1,
				config->nrecords - 1);
}

/*
 * Decide how many records should be put in the nth cluster.
 */
uint32
mxkv_decide_cluster_nrecords(const MxkvConfig *config, int nth)
{
	if (config->nrecords == 0)
		return 0;
	else if (nth < config->nclusters - 1)
		/* not the last cluster, must be full */
		return config->cluster_capacity;
	else if (config->nrecords % config->cluster_capacity == 0)
		/* the last cluster is also full */
		return config->cluster_capacity;
	else
		/* the last cluster is not full */
		return config->nrecords % config->cluster_capacity;
}

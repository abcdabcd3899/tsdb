/*-------------------------------------------------------------------------
 *
 * contrib/matrixts/mxkv/delta_coding.c
 *	  Common parts of the delta encoder and decoder.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "mxkv/delta_coding.h"

/* the delte coding schema for int32 */
static const DeltaCodingSchema schemas32[] =
	{
		{
			.leader_bits = 0,
			.leader_nbits = 0,
			.body_nbits = 1,
			.min = 0,
			.max = 0,
			.sub = 0,
		},
		{
			.leader_bits = 0x02,
			.leader_nbits = 2,
			.body_nbits = 6,
			.min = -31,
			.max = 32,
			.sub = 64,
		},
		{
			.leader_bits = 0x06,
			.leader_nbits = 3,
			.body_nbits = 9,
			.min = -255,
			.max = 256,
			.sub = 512,
		},
		{
			.leader_bits = 0x0e,
			.leader_nbits = 4,
			.body_nbits = 12,
			.min = -2047,
			.max = 2048,
			.sub = 4096,
		},
		{
			.leader_bits = 0x0f,
			.leader_nbits = 4,
			.body_nbits = 32,
			.min = PG_INT32_MIN,
			.max = PG_INT32_MAX,
			.sub = 0,
		},
};

const DeltaCodingSchema *
delta_coding_schema32_get_by_leader(uint8 leader)
{
	static const DeltaCodingSchema *(tab[256]) =
		{
			[0x00] = &schemas32[0],
			[0x02] = &schemas32[1],
			[0x06] = &schemas32[2],
			[0x0e] = &schemas32[3],
			[0x0f] = &schemas32[4],
		};

	return tab[leader];
}

const DeltaCodingSchema *
delta_coding_schema32_get_by_value(int32 x)
{
	for (int i = 0; i < ARRAY_SIZE(schemas32); ++i)
	{
		const DeltaCodingSchema *schema = &schemas32[i];

		if (schema->min <= x && x <= schema->max)
			return schema;
	}

	return NULL;
}

/* the delte coding schema for int64 */
static const DeltaCodingSchema schemas64[] =
	{
		{
			.leader_bits = 0,
			.leader_nbits = 0,
			.body_nbits = 1,
			.min = 0,
			.max = 0,
			.sub = 0,
		},
		{
			.leader_bits = 0x02,
			.leader_nbits = 2,
			.body_nbits = 6,
			.min = -31,
			.max = 32,
			.sub = 64,
		},
		{
			.leader_bits = 0x06,
			.leader_nbits = 3,
			.body_nbits = 13,
			.min = -4095,
			.max = 4096,
			.sub = 8192,
		},
		{
			.leader_bits = 0x0e,
			.leader_nbits = 4,
			.body_nbits = 20,
			.min = -524287,
			.max = 524288,
			.sub = 1048576,
		},
		{
			.leader_bits = 0x0f,
			.leader_nbits = 4,
			.body_nbits = 64,
			.min = PG_INT64_MIN,
			.max = PG_INT64_MAX,
			.sub = 0,
		},
};

const DeltaCodingSchema *
delta_coding_schema64_get_by_leader(uint8 leader)
{
	static const DeltaCodingSchema *(tab[256]) =
		{
			[0x00] = &schemas64[0],
			[0x02] = &schemas64[1],
			[0x06] = &schemas64[2],
			[0x0e] = &schemas64[3],
			[0x0f] = &schemas64[4],
		};

	return tab[leader];
}

const DeltaCodingSchema *
delta_coding_schema64_get_by_value(int64 x)
{
	for (int i = 0; i < ARRAY_SIZE(schemas64); ++i)
	{
		const DeltaCodingSchema *schema = &schemas64[i];

		if (schema->min <= x && x <= schema->max)
			return schema;
	}

	return NULL;
}

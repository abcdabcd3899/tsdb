/*----------------------------------------------------------------------------
 *
 * sortheap_freepages.h
 * 		Generic routines to manage freepages in sort heap
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap_freepages.h
 *
 *----------------------------------------------------------------------------
 */
#ifndef SORTHEAP_FREEPAGES_H
#define SORTHEAP_FREEPAGES_H

typedef struct FreePageLeaf
{
	int16 leafIndex;
	uint64 fastBitmap; /* fast decision for a block range */
	uint64 maps[1];
} FreePageLeaf;

/* ~1000 maps by default , ~64000 blocks (~500MB) per page by default */
#define NUMBLOCKS_PERMAP (sizeof(uint64) * 8)
#define NUMMAPS_PERLEAF ((BLCKSZ - sizeof(FreePageLeaf) - PAGE_RESERVED_SPACE) / sizeof(uint64))
#define NUMBLOCKS_PERLEAF (NUMBLOCKS_PERMAP * NUMMAPS_PERLEAF)
#define NUMMAPS_PERFASTBIT (NUMMAPS_PERLEAF / (sizeof(uint64) * 8) + 1)

typedef struct FreePageLeafRef
{
	bool hasFreePages;
	BlockNumber blkno;
} FreePageLeafRef;

typedef struct FreePageRoot
{
	int16 rootIndex;
	int16 numLeaves;
	bool hasFreePages;
	FreePageLeafRef leaves[1];
} FreePageRoot;

#define NUMLEAVES_PERROOT ((BLCKSZ - sizeof(FreePageLeaf) - PAGE_RESERVED_SPACE) / sizeof(FreePageLeafRef))
#define NUMBLOCKS_PERROOT (NUMBLOCKS_PERLEAF * NUMLEAVES_PERROOT)

#endif /* //SORTHEAP_FREEPAGES_H */

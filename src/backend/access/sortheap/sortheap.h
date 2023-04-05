/*-------------------------------------------------------------------------
 *
 * sortheap.h
 *	  POSTGRES Sort Heap access method definitions.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SORTHEAPAM_H
#define SORTHEAPAM_H

#include "sortheap_common.h"

/* Magic number to identify a Sort Heap Page */
#define SORTHEAP_MAGIC 0x20200615

#define SORTHEAP_METABLOCK 0

/* reserved space in a sort heap page */
#define PAGE_RESERVED_SPACE (MAXALIGN(sizeof(PageHeaderData)) + MAXALIGN(sizeof(SortHeapPageOpaqueData)))

typedef enum SortHeapPageType
{
	SORTHEAP_METAPAGE = 1,
	SORTHEAP_TAPESETSMETAPAGE,
	SORTHEAP_TAPEMETAPAGE,
	SORTHEAP_RUNMETAPAGE,
	SORTHEAP_DATAPAGE,
	SORTHEAP_BTPAGE,
	SORTHEAP_BRINPAGE,
	SORTHEAP_ALLOCATEDPAGE,
	SORTHEAP_FREESPACEPAGE
} SortHeapPageType;

/* Structure of the special space of a sort heap page */
typedef struct SortHeapPageOpaqueData
{
	SortHeapPageType type;
	BlockNumber next;
	uint32 range;
} SortHeapPageOpaqueData;

/*
 * Descriptor for fetches from sortheap via an index.
 */
typedef struct IndexFetchSortHeapData
{
	IndexFetchTableData xs_base; /* AM independent part of the descriptor */

	Buffer xs_cbuf; /* current heap buffer in scan, if any */
	/* NB: if xs_cbuf is not InvalidBuffer, we hold a pin on that buffer */

	SortHeapState *sortstate;

	/* Fields to project out unneeded attributes */
	AttrNumber *proj_atts;
	int n_proj_atts;

	/* Fields for the compressed tuple */
	HeapTupleData columnstore_tuple;
	MemoryContext columnchunkcontext;
	TupleDesc columnstore_desc;
	TupleDesc *columnchunk_descs;

	bool cached;
	Datum **datum_cache;
	bool **isnull_cache;
	int cur_cache;
	int size_cache;
} IndexFetchSortHeapData;

/*
 * Descriptor for sort heap table scans.
 */
typedef struct SortHeapScanDescData
{
	HeapScanDescData hdesc; /* AM independent part of the descriptor */
	SortHeapState *sortstate;
	bool keeporder; /* Indicate whether the scan should keep order */
	int activedsets;
	AttrNumber *proj_atts;
	int n_proj_atts;
	IndexScanState *iss;
	ExprState *qual; /* quals excluded the scan keys */
	ExprContext *qualcontext;
} SortHeapScanDescData;

typedef struct SortHeapScanDescData *SortHeapScanDesc;

/*
 * Descriptor for sort heap table bitmap scans.
 */
typedef struct SortHeapBitmapScanDescData
{
	SortHeapScanDescData shdesc; /* AM independent part of the descriptor */

	/* Fields for the compressed tuple */
	MemoryContext columnchunkcontext;
	TupleDesc columnstore_desc;
	TupleDesc *columnchunk_descs;

	int nindexkeys;
	bool cached;
	Datum **datum_cache;
	bool **isnull_cache;
	int cur_cache;
	int size_cache;
} SortHeapBitmapScanDescData;
typedef struct SortHeapBitmapScanDescData *SortHeapBitmapScanDesc;

/*
 * The metadata of the Sort Heap Table, it is always the first block of the
 * underlying file
 */
typedef struct SortHeapMeta
{
	uint32 meta_magic; /* should contain SORTHEAP_MAGIC */

	/* the first freepages root page */
	BlockNumber firstFreePagesRoot;

	/*
	 * File size tracking.  nBlocksWritten is the size of the underlying file,
	 * in BLCKSZ blocks.  nBlocksAllocated is the number of blocks allocated
	 * by sortheap_get_freeblock(), and it is always greater than or equal to
	 * nBlocksWritten. Blocks between nBlocksAllocated and nBlocksWritten are
	 * blocks that have been allocated for a tape, but have not been written
	 * to the underlying file yet.
	 */
	long nBlocksAllocated;
	long nBlocksWritten;

	/* The prefered tapeset to merge */
	int mergets;
	/* The prefered tapeset to insert */
	int insertts;

	/*
	 * We have two logical tape sets to support concurrent INSERT and MERGE,
	 * tapesets array stores the blocknum stores the metadata of a tape sets.
	 */
	BlockNumber tapesets[MAX_TAPESETS];
} SortHeapMeta;

SortHeapState *sortheap_beginsort(Relation relation, Snapshot snap,
								  Relation indexrel, SortHeapOP op,
								  void *args);
bool sortheap_endsort(SortHeapState *sortstate);
bool sortheap_advance_next_tapesets(SortHeapState *shstate);
bool sortheap_try_lock_merge(Relation relation, LOCKMODE mode);
void sortheap_unlock_merge(Relation relation, LOCKMODE mode);
void sortheap_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
					 int options, TransactionId xid);
void sortheap_insert_finish(Relation relation);
bool sortheap_getnextslot(TableScanDesc sscan, ScanDirection direction,
						  TupleTableSlot *slot);
void sortheap_extend(SortHeapState *shstate, BlockNumber target);

/* XLog related functions */
XLogRecPtr log_full_page_update(Buffer curbuf);

#endif /* //SORTHEAPAM_H */

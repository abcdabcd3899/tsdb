/*-------------------------------------------------------------------------
 *
 * sortheap_common.h
 *	  common headers and common routines for sort heap.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap_common.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SORTHEAP_COMMON_H
#define SORTHEAP_COMMON_H

#include "c.h"
#include "postgres.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "funcapi.h"
#include "math.h"
#include "access/heapam_xlog.h"
#include "access/sortheapam.h"
#include "access/distributedlog.h"
#include "access/hio.h"
#include "access/htup.h"
#include "access/itup.h"
#include "access/htup_details.h"
#include "access/hash.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/rmgr.h"
#include "access/sdir.h"
#include "access/tuptoaster.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "catalog/pg_amop_d.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_compression.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "commands/defrem.h"
#include "commands/vacuum.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "nodes/extensible.h"
#include "optimizer/plancat.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/cost.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/gp_compress.h"
#include "storage/lmgr.h"
#include "storage/block.h"
#include "storage/smgr.h"
#include "storage/procarray.h"
#include "tcop/tcopprot.h"
#include "utils/datum.h"
#include "utils/extract_column.h"
#include "utils/faultinjector.h"
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "cdb/cdbsortheapxlog.h"
#include "cdb/cdbutil.h"

typedef struct SortHeapState SortHeapState;
typedef struct TapeSetsMeta TapeSetsMeta;
typedef struct TapeInsertState TapeInsertState;
typedef struct TapeReadState TapeReadState;
typedef struct SortHeapLogicalTape SortHeapLogicalTape;
typedef struct ExternalSortMethods ExternalSortMethods;
typedef struct SortHeapScanDescData *SortHeapScanDesc;
typedef struct TapeScanDescData *TapeScanDesc;

#define COMPRESS_MIN_GROUPSIZE_THRESHOLD (8 * BLCKSZ) /* 64 KB */
#define COMPRESS_MIN_NTUPLES_THRESHOLD 200
#define COMPRESS_MAX_NTUPLES_THRESHOLD 1000

#define IsColumnStoreTuple(tup)                            \
	(((tup)->t_data->t_infomask & HEAP_HASOID_OLD) != 0 && \
	 ((tup)->t_len != ENDRUNMARKER_LEN))

#define IsRunEndMarker(tup)                                \
	(((tup)->t_data->t_infomask & HEAP_HASOID_OLD) != 0 && \
	 ((tup)->t_len == ENDRUNMARKER_LEN))

BlockNumber sortheap_get_freeblock(SortHeapState *shstate);
void sortheap_recycle_block(Relation relation, BlockNumber blkno);

/* Helper functions for minimum heap */
void minheap_insert(SortHeapState *state,
					SortTuple *memtuples,
					int *memtupcount,
					SortTuple *tuple);
void minheap_replace_top(SortHeapState *state,
						 SortTuple *memtuples,
						 int *memtupcount,
						 SortTuple *tuple);
void minheap_delete_top(SortHeapState *state,
						SortTuple *memtuples,
						int *memtupcount);

Datum get_sorttuple_cache(SortHeapState *shstate,
						  SortTuple *stup,
						  int idx,
						  bool *isnull);
bool sortheap_match_scankey_quals(Relation relation, TapeScanDesc scandesc,
								  SortTuple *stup, TupleTableSlot *slot,
								  bool *continuescan, bool comparescankey,
								  bool comparequal);
#endif /* //SORTHEAP_COMMON_H */

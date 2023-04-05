/*-------------------------------------------------------------------------
 *
 * toin.h
 *	  TODO file description
 *
 *
 * Copyright (c) 2020-Present TODO
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef TOIN_H
#define TOIN_H

#include "postgres.h"

#include "access/genam.h"
#include "access/brin_revmap.h"
#include "access/nbtree.h"
#include "executor/tuptable.h"
#include "utils/relcache.h"

/*
 * To support index scan we need to use the btree order (compare) function, we
 * use the last brin optional procnum, 15, for it, however there is the risk to
 * conflict with other customized am.
 */
#define TOIN_ORDER_PROC			BRIN_LAST_OPTIONAL_PROCNUM

#define ToinScanPosIsValid(scanpos) \
( \
	BlockNumberIsValid((scanpos).currPage) \
)
#define ToinScanPosInvalidate(scanpos) \
	do { \
		(scanpos).currPage = InvalidBlockNumber; \
	} while (0);


/*
 * Copied from src/backend/access/brin/brin.c.
 *
 * Struct used as "opaque" during index scans
 */
typedef struct BrinOpaque
{
	BlockNumber bo_pagesPerRange;
	BrinRevmap *bo_rmAccess;
	BrinDesc   *bo_bdesc;
} BrinOpaque;

/*
 * Derived from BTScanPosItem.
 *
 * BTScanOpaqueData is the btree-private state needed for an indexscan.
 * This consists of preprocessed scan keys (see _bt_preprocess_keys() for
 * details of the preprocessing), information about the current location
 * of the scan, and information about the marked location, if any.  (We use
 * BTScanPosData to represent the data needed for each of current and marked
 * locations.)	In addition we can remember some known-killed index entries
 * that must be marked before we can move off the current page.
 *
 * Index scans work a page at a time: we pin and read-lock the page, identify
 * all the matching items on the page and save them in BTScanPosData, then
 * release the read-lock while returning the items to the caller for
 * processing.  This approach minimizes lock/unlock traffic.  Note that we
 * keep the pin on the index page until the caller is done with all the items
 * (this is needed for VACUUM synchronization, see nbtree/README).  When we
 * are ready to step to the next page, if the caller has told us any of the
 * items were killed, we re-lock the page to mark them killed, then unlock.
 * Finally we drop the pin and step to the next page in the appropriate
 * direction.
 *
 * If we are doing an index-only scan, we save the entire IndexTuple for each
 * matched item, otherwise only its heap TID and offset.  The IndexTuples go
 * into a separate workspace array; each BTScanPosItem stores its tuple's
 * offset within that array.
 */

typedef struct ToinScanPosItem	/* what we remember about each match */
{
	ItemPointerData heapTid;	/* TID of referenced heap item */
} ToinScanPosItem;

typedef struct ToinScanPosData
{
	BlockNumber currPage;		/* page referenced by items array */

	/*
	 * moreLeft and moreRight track whether we think there may be matching
	 * index entries to the left and right of the current page, respectively.
	 * We can clear the appropriate one of these flags when _bt_checkkeys()
	 * returns continuescan = false.
	 */
	bool		moreLeft;
	bool		moreRight;

	/*
	 * The items array is always ordered in index order (ie, increasing
	 * indexoffset).  When scanning backwards it is convenient to fill the
	 * array back-to-front, so we start at the last slot and fill downwards.
	 * Hence we need both a first-valid-entry and a last-valid-entry counter.
	 * itemIndex is a cursor showing which entry was last returned to caller.
	 */
	int			firstItem;		/* first valid index in items[] */
	int			lastItem;		/* last valid index in items[] */
	int			itemIndex;		/* current index in items[] */

	ToinScanPosItem items[MaxIndexTuplesPerPage]; /* MUST BE LAST */
} ToinScanPosData, *ToinScanPos;

/*
 * Derived from BTScanOpaqueData.
 */
typedef struct ToinScanOpaqueData
{
	/* these fields are set by _bt_preprocess_keys(): */
	bool		qual_ok;		/* false if qual can never be satisfied */
	int			numberOfKeys;	/* number of preprocessed scan keys */
	ScanKey		keyData;		/* array of preprocessed scan keys */

	/* workspace for SK_SEARCHARRAY support */
	ScanKey		arrayKeyData;	/* modified copy of scan->keyData */
	int			numArrayKeys;	/* number of equality-type array keys (-1 if
								 * there are any unsatisfiable array keys) */
	BTArrayKeyInfo *arrayKeys;	/* info about each equality-type array key */
	MemoryContext arrayContext; /* scan-lifespan context for array data */

	/* keep these last in struct for efficiency */
	ToinScanPosData currPos;		/* current position data */

	/* Toin specific field, used for heap scan */
	TupleTableSlot *slot;
	IndexFetchTableData *heapfetch;
} ToinScanOpaqueData;

typedef ToinScanOpaqueData *ToinScanOpaque;

/*
 * Toin combines both brin and btscan, to reuse their code we create both
 * opaque pointers of them, during bitmap scan we let the brin be the scan
 * opaque, during index scan we let the btscan be the scan opaque, so we need
 * a special way to get the toin itself.  So we let the toin struct be an
 * extension of the scan desc.
 *
 * Note, the btscan is a hacked version, not the original one, but we still
 * call it btscan.
 */
typedef struct Toin
{
	IndexScanDescData scan;

	/* The brin opaque for bitmap scan */
	BrinOpaque *brin;

	/* The hacked version of btree scan opaque for index scan */
	ToinScanOpaque so;

	bool		counted;
	BlockNumber	firstBlock;
	BlockNumber	lastBlock;
} Toin;

/* toin.c */
extern IndexScanDesc toinbeginscan(Relation r, int nkeys, int norderbys);
extern void toinendscan(IndexScanDesc scan);

/* toin_nbtree.c */
extern bool toin_gettuple(IndexScanDesc scan, ScanDirection dir);
extern void toin_beginscan(IndexScanDesc scan, Relation rel,
						   int nkeys, int norderbys);
extern void toin_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						ScanKey orderbys, int norderbys);
extern void toin_endscan(IndexScanDesc scan);

/* toin_nbtsearch.c */
extern bool _toin_first(IndexScanDesc scan, ScanDirection dir);
extern bool _toin_next(IndexScanDesc scan, ScanDirection dir);

/* toin_nbtutils.c */
extern void _toin_preprocess_array_keys(IndexScanDesc scan);
extern void _toin_start_array_keys(IndexScanDesc scan, ScanDirection dir);
extern bool _toin_advance_array_keys(IndexScanDesc scan, ScanDirection dir);
extern void _toin_preprocess_keys(IndexScanDesc scan);
extern bool _toin_checkkeys(IndexScanDesc scan,
							int tupnatts, ScanDirection dir,
							bool *continuescan);

/* toin_brin.c */
extern List *_toin_make_tlist_from_scankeys(IndexScanDesc scan);
extern Relation _toin_get_heap_relation(IndexScanDesc scan);
extern void _toin_get_heap_nblocks(IndexScanDesc scan);
extern OffsetNumber _toin_get_heap_block_ntuples(IndexScanDesc scan,
												 BlockNumber blkno,
												 OffsetNumber *firstOff,
												 OffsetNumber *lastOff);
extern int32 _toin_compare_brin(IndexScanDesc scan,
								BTScanInsert key, BrinMemTuple *dtup);
extern BlockNumber _toin_search_brin(IndexScanDesc scan, BTScanInsert key);
extern bool _toin_get_heap_tuple(IndexScanDesc scan, BlockNumber blkno,
								 OffsetNumber off);
extern int32 _toin_compare_heap(IndexScanDesc scan, BTScanInsert key);
extern OffsetNumber _toin_search_heap(IndexScanDesc scan, BTScanInsert key,
									  BlockNumber blkno);

#endif   /* TOIN_H */

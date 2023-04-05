/*-------------------------------------------------------------------------
 *
 * sortheap_btree.h
 *	  POSTGRES Sort Heap btree index routines.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap_btree.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SORTHEAP_BTREE_H
#define SORTHEAP_BTREE_H

/*
 * Status record for a btree page being built.  We have one of these
 * for each active tree level.
 *
 * The reason we need to store a copy of the minimum key is that we'll
 * need to propagate it to the parent node when this page is linked
 * into its parent.  However, if the page is not a leaf page, the first
 * entry on the page doesn't need to contain a key, so we will not have
 * stored the key itself on the page.  (You might think we could skip
 * copying the minimum key on leaf pages, but actually we must have a
 * writable copy anyway because we'll poke the page's address into it
 * before passing it up to the parent...)
 */
typedef struct BTPageState
{
	Buffer btps_buf;
	Page btps_page;				   /* workspace for page building */
	BlockNumber btps_blkno;		   /* block # to write this page at */
	IndexTuple btps_minkey;		   /* copy of minimum key (first item) on page */
	OffsetNumber btps_lastoff;	   /* last item offset loaded */
	uint32 btps_level;			   /* tree level (0 = leaf) */
	Size btps_full;				   /* "full" if less than this much free space */
	struct BTPageState *btps_next; /* link to parent level, if any */
} BTPageState;

void sortheap_bt_buildadd(Relation relation,
						  TapeInsertState *insertstate,
						  BTPageState *state,
						  IndexTuple itup);
BTPageState *sortheap_bt_pagestate(Relation relation,
								   TapeInsertState *insertstate,
								   uint32 level);

BlockNumber sortheap_bt_uppershutdown(Relation relation,
									  TapeInsertState *insertstate,
									  int curRun);

bool sortheap_bt_first(IndexScanDesc scan, ScanDirection dir);

bool sortheap_bt_checkkeys(Relation heaprel,
						   IndexScanDesc scan,
						   SortTuple *stup,
						   ScanDirection dir,
						   bool *continuescan);

#endif /* //SORTHEAP_BTREE_H */

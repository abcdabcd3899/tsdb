/* ------------------------------------------------------------------
 * sortheap_xlog.c
 *		Generalized codes to xlog.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_xlog.c
 * ------------------------------------------------------------------
 */
#include "sortheap.h"
#include "sortheap_tapesets.h"
#include "sortheap_freepages.h"
#include "cdb/cdbsortheapxlog.h"

static void
sortheap_xlog_dump(XLogReaderState *record)
{
	Buffer buffer;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page page;
		XLogRecPtr lsn = record->EndRecPtr;
		xl_sortheap_dump *xlrec = (xl_sortheap_dump *)XLogRecGetData(record);
		TapeSetsMeta *metad;

		page = BufferGetPage(buffer);
		metad = (TapeSetsMeta *)PageGetContents(page);

		metad->destTape = xlrec->newDestTape;
		metad->currentRun = xlrec->newCurrentRun;
		metad->lastInsertXid = xlrec->lastInsertXid;
		metad->tapes[xlrec->destTape].runs = xlrec->newRuns;
		metad->tapes[xlrec->destTape].dummy = xlrec->newDummy;

		MarkBufferDirty(buffer);
		PageSetLSN(page, lsn);
	}

	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
sortheap_xlog_newheader(XLogReaderState *record)
{
	int tapenum;
	Buffer metabuf;
	Page headerpage;
	Buffer headerbuf;
	BlockNumber headerblk;
	XLogRecPtr lsn = record->EndRecPtr;
	HeapTapeMeta *tapemetad;
	SortHeapPageOpaqueData *opaque;

	tapenum = *(int *)XLogRecGetData(record);

	/* Re-init the new header page */
	headerbuf = XLogInitBufferForRedo(record, 1);
	headerpage = BufferGetPage(headerbuf);
	headerblk = BufferGetBlockNumber(headerbuf);

	PageInit(headerpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
	tapemetad = (HeapTapeMeta *)PageGetContents(headerpage);

	/* Initialize the tape meta page */
	tapemetad->nprealloc = 0;
	tapemetad->prealloc_size = 0;
	tapemetad->lastdatablock = InvalidBlockNumber;
	tapemetad->numblocks = 0;

	opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(headerpage);
	opaque->type = SORTHEAP_TAPEMETAPAGE;
	opaque->next = InvalidBlockNumber;

	/*
	 * Set pd_lower just past the end of the metadata.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader)headerpage)->pd_lower =
		((char *)tapemetad + sizeof(HeapTapeMeta)) - (char *)headerpage;

	PageSetLSN(headerpage, lsn);
	MarkBufferDirty(headerbuf);
	UnlockReleaseBuffer(headerbuf);

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page metapage;
		TapeSetsMeta *metad;

		metapage = BufferGetPage(metabuf);
		metad = (TapeSetsMeta *)PageGetContents(metapage);

		metad->tapes[tapenum].header = headerblk;
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

static void
sortheap_xlog_record_allocated(XLogReaderState *record)
{
	bool curbufset;
	bool nextbufset;
	int nprealloc;
	long nallocated;
	Buffer metabuf;
	Buffer curbuf;
	Buffer nextbuf;
	Page curpage;
	Index idx;
	BlockNumber *allocated;
	BlockNumber newalloc;
	BlockNumber nextblkno;
	XLogRecPtr lsn = record->EndRecPtr;

	curbufset = *(bool *)XLogRecGetData(record);
	nextbufset = *(bool *)(XLogRecGetData(record) + sizeof(bool));
	nprealloc = *(int *)(XLogRecGetData(record) + 2 * sizeof(bool));
	nallocated = *(long *)(XLogRecGetData(record) + 2 * sizeof(bool) +
						   sizeof(int));
	newalloc = *(BlockNumber *)(XLogRecGetData(record) +
								2 * sizeof(bool) +
								sizeof(int) +
								sizeof(long));

	idx = (nallocated - 1) % ALLOCATEDBLOCKPERAUXBLOCK;

	if (nextbufset)
	{
		Page nextpage;
		SortHeapPageOpaqueData *opaque;

		nextbuf = XLogInitBufferForRedo(record, 1);
		nextblkno = BufferGetBlockNumber(nextbuf);
		nextpage = BufferGetPage(nextbuf);
		PageInit(nextpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
		allocated = (BlockNumber *)PageGetContents(nextpage);
		memset(allocated, InvalidBlockNumber, ALLOCATEDBLOCKPERAUXBLOCK * sizeof(BlockNumber));
		opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(nextpage);
		opaque->next = InvalidBlockNumber;

		/* Mark the new block as allocated */
		allocated[idx] = newalloc;

		/*
		 * Set pd_lower just past the end of the metadata.  This is essential,
		 * because without doing so, metadata will be lost if xlog.c
		 * compresses the page.
		 */
		((PageHeader)nextpage)->pd_lower =
			((char *)allocated + ALLOCATEDBLOCKPERAUXBLOCK * sizeof(BlockNumber)) - (char *)nextpage;

		MarkBufferDirty(nextbuf);
		PageSetLSN(nextpage, lsn);
		UnlockReleaseBuffer(nextbuf);

		/* update the tape metadata */
		if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
		{
			Page metapage;
			HeapTapeMeta *metad;

			metapage = BufferGetPage(metabuf);
			metad = (HeapTapeMeta *)PageGetContents(metapage);
			metad->numblocks = nallocated;
			metad->nprealloc = nprealloc;

			metad->currentauxblock = nextblkno;

			if (!curbufset)
				metad->firstauxblock = nextblkno;

			PageSetLSN(metapage, lsn);
			MarkBufferDirty(metabuf);
		}

		if (BufferIsValid(metabuf))
			UnlockReleaseBuffer(metabuf);

		if (curbufset)
		{
			if (XLogReadBufferForRedo(record, 2, &curbuf) == BLK_NEEDS_REDO)
			{
				curpage = BufferGetPage(curbuf);
				opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(curpage);
				opaque->next = nextblkno;
				MarkBufferDirty(curbuf);
				PageSetLSN(curpage, lsn);
			}

			if (BufferIsValid(curbuf))
				UnlockReleaseBuffer(curbuf);
		}
	}
	else
	{
		Assert(curbufset);

		if (XLogReadBufferForRedo(record, 1, &curbuf) == BLK_NEEDS_REDO)
		{
			curpage = BufferGetPage(curbuf);
			allocated = (BlockNumber *)PageGetContents(curpage);
			/* Mark the new block as allocated */
			allocated[idx] = newalloc;
			MarkBufferDirty(curbuf);
			PageSetLSN(curpage, lsn);
		}

		if (BufferIsValid(curbuf))
			UnlockReleaseBuffer(curbuf);
	}
}

static void
sortheap_xlog_merge_done(XLogReaderState *record)
{
	Buffer metabuf;

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page metapage;
		XLogRecPtr lsn = record->EndRecPtr;
		TapeSetsMeta *metad;
		TransactionId lastMergeXid;

		lastMergeXid = *(TransactionId *)XLogRecGetData(record);

		metapage = BufferGetPage(metabuf);
		metad = (TapeSetsMeta *)PageGetContents(metapage);

		metad->lastMergeXid = lastMergeXid;
		metad->status = TS_MERGE_SORTED;
		metad->tapes[metad->resultTape].runs = 1;
		MarkBufferDirty(metabuf);
		PageSetLSN(metapage, lsn);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

static void
sortheap_xlog_vacuum_done(XLogReaderState *record)
{
	Buffer metabuf;

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page metapage;
		XLogRecPtr lsn = record->EndRecPtr;
		TapeSetsMeta *metad;
		TransactionId lastVacuumXid;

		lastVacuumXid = *(TransactionId *)XLogRecGetData(record);

		metapage = BufferGetPage(metabuf);
		metad = (TapeSetsMeta *)PageGetContents(metapage);

		metad->lastVacuumXid = lastVacuumXid;
		metad->status = TS_MERGE_VACUUMED;
		MarkBufferDirty(metabuf);
		PageSetLSN(metapage, lsn);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

static void
sortheap_xlog_release_auxblk(XLogReaderState *record)
{
	Buffer headerbuf;
	Page headerpage;
	BlockNumber nextnext;
	HeapTapeMeta *tapemetad;
	XLogRecPtr lsn = record->EndRecPtr;

	nextnext = *(BlockNumber *)XLogRecGetData(record);

	if (XLogReadBufferForRedo(record, 0, &headerbuf) == BLK_NEEDS_REDO)
	{
		headerpage = BufferGetPage(headerbuf);

		tapemetad = (HeapTapeMeta *)PageGetContents(headerpage);
		tapemetad->firstauxblock = nextnext;
		PageSetLSN(headerpage, lsn);
		MarkBufferDirty(headerbuf);
	}

	if (BufferIsValid(headerbuf))
		UnlockReleaseBuffer(headerbuf);
}

static void
sortheap_xlog_vacuum_tape(XLogReaderState *record)
{
	int tapenum;
	Buffer tapesortbuf;

	tapenum = *(int *)XLogRecGetData(record);

	if (XLogReadBufferForRedo(record, 0, &tapesortbuf) == BLK_NEEDS_REDO)
	{
		Page tapesortpage;
		XLogRecPtr lsn = record->EndRecPtr;
		TapeSetsMeta *tapesortmetad;

		tapesortpage = BufferGetPage(tapesortbuf);
		tapesortmetad = (TapeSetsMeta *)PageGetContents(tapesortpage);

		tapesortmetad->tapes[tapenum].header = InvalidBlockNumber;

		PageSetLSN(tapesortpage, lsn);
		MarkBufferDirty(tapesortbuf);
	}

	if (BufferIsValid(tapesortbuf))
		UnlockReleaseBuffer(tapesortbuf);
}

static void
sortheap_xlog_tape_extend(XLogReaderState *record)
{
	int level;
	Page page;
	bool curbufset;
	Buffer buffer;
	XLogRecPtr lsn;
	BlockNumber newblkno;
	SortHeapPageType pagetype;

	lsn = record->EndRecPtr;
	curbufset = *(bool *)XLogRecGetData(record);
	pagetype = *(SortHeapPageType *)(XLogRecGetData(record) + sizeof(bool));
	level = *(int *)(XLogRecGetData(record) + sizeof(bool) + sizeof(SortHeapPageType));

	/* Re-init the new buf */
	buffer = XLogInitBufferForRedo(record, 0);
	newblkno = BufferGetBlockNumber(buffer);
	page = BufferGetPage(buffer);

	/*
	 * DATA/BRIN/RUNMETA page has the same layout
	 */
	if (pagetype == SORTHEAP_DATAPAGE ||
		pagetype == SORTHEAP_BRINPAGE ||
		pagetype == SORTHEAP_RUNMETAPAGE)
	{
		SortHeapPageOpaqueData *opaque;

		/* Initialized the new data page */
		PageInit(page, BLCKSZ, sizeof(SortHeapPageOpaqueData));
		opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(page);
		opaque->type = pagetype;
		opaque->next = InvalidBlockNumber;

		if (pagetype == SORTHEAP_RUNMETAPAGE)
		{
			TapeRunsMeta *runsmeta;

			runsmeta = (TapeRunsMeta *)PageGetContents(page);

			/*
			 * Set pd_lower just past the end of the metadata.  This is
			 * essential, because without doing so, metadata will be lost if
			 * xlog.c compresses the page.
			 */
			((PageHeader)page)->pd_lower =
				((char *)runsmeta + NUMRUNMETA_PERPAGE * sizeof(TapeRunsMetaEntry)) - (char *)page;
		}
	}
	else if (pagetype == SORTHEAP_BTPAGE)
	{
		BTPageOpaque opaque;

		/* Zero the page and set up standard page header info */
		_bt_pageinit(page, BLCKSZ);

		/* Initialize BT opaque state */
		opaque = (BTPageOpaque)PageGetSpecialPointer(page);
		opaque->btpo_prev = opaque->btpo_next = P_NONE;
		opaque->btpo_cycleid = 0;
		opaque->btpo.level = level;
		opaque->btpo_flags = (level > 0) ? 0 : BTP_LEAF;

		/* Make the P_HIKEY line pointer appear allocated */
		((PageHeader)page)->pd_lower += sizeof(ItemIdData);
	}

	PageSetLSN(page, lsn);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);

	/* Update the lastdatablock */
	if (XLogReadBufferForRedo(record, 1, &buffer) == BLK_NEEDS_REDO &&
		pagetype == SORTHEAP_DATAPAGE)
	{
		HeapTapeMeta *metad;

		page = BufferGetPage(buffer);
		metad = (HeapTapeMeta *)PageGetContents(page);
		metad->lastdatablock = newblkno;

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);

	/* update the next link of cur page */
	if (curbufset)
	{
		if (XLogReadBufferForRedo(record, 2, &buffer) == BLK_NEEDS_REDO)
		{
			SortHeapPageOpaqueData *opaque;

			page = BufferGetPage(buffer);
			opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(page);
			opaque->next = newblkno;

			PageSetLSN(page, lsn);
			MarkBufferDirty(buffer);
		}

		if (BufferIsValid(buffer))
			UnlockReleaseBuffer(buffer);
	}
}

static void
sortheap_xlog_fullpage_update(XLogReaderState *record)
{
	Buffer buffer;

	XLogReadBufferForRedo(record, 0, &buffer);
	MarkBufferDirty(buffer);
	UnlockReleaseBuffer(buffer);
}

static void
sortheap_xlog_runsmetablk(XLogReaderState *record)
{
	Buffer buffer;
	int runno;
	BlockNumber runsmetablk;

	runno = *(int *)XLogRecGetData(record);
	runsmetablk = *(BlockNumber *)(XLogRecGetData(record) + sizeof(int));

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		HeapTapeMeta *metad;
		Page page;
		XLogRecPtr lsn = record->EndRecPtr;

		page = BufferGetPage(buffer);
		metad = (HeapTapeMeta *)PageGetContents(page);
		metad->runsmetad[runno / NUMRUNMETA_PERPAGE] = runsmetablk;
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
sortheap_xlog_extend(XLogReaderState *record)
{
	Buffer buffer;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		SortHeapMeta *metad;
		Page page;
		XLogRecPtr lsn = record->EndRecPtr;

		page = BufferGetPage(buffer);
		metad = (SortHeapMeta *)PageGetContents(page);
		metad->nBlocksWritten = *(long *)XLogRecGetData(record);
		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
sortheap_xlog_newtapesort(XLogReaderState *record)
{
	Page tapesortpage;
	Index tapesort;
	Buffer metabuf;
	Buffer tapesortbuf;
	BlockNumber tapesortblk;
	TapeSetsMeta *tapesortmetad;
	XLogRecPtr lsn = record->EndRecPtr;

	tapesort = *(Index *)XLogRecGetData(record);

	/* re-init the tapesort page */
	tapesortbuf = XLogInitBufferForRedo(record, 1);
	tapesortpage = BufferGetPage(tapesortbuf);
	tapesortblk = BufferGetBlockNumber(tapesortbuf);

	PageInit(tapesortpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
	tapesortmetad = (TapeSetsMeta *)PageGetContents(tapesortpage);
	tapesortmetad->currentRun = 0;
	tapesortmetad->level = 0;
	tapesortmetad->destTape = 0;
	tapesortmetad->maxTapes = 0;
	tapesortmetad->tapeRange = 0;
	tapesortmetad->lastMergeXid = InvalidTransactionId;
	tapesortmetad->lastInsertXid = InvalidTransactionId;
	tapesortmetad->status = TS_INITIAL;

	/*
	 * Set pd_lower just past the end of the metadata.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader)tapesortpage)->pd_lower =
		((char *)tapesortmetad + sizeof(HeapTapeMeta)) - (char *)tapesortpage;

	PageSetLSN(tapesortpage, lsn);
	MarkBufferDirty(tapesortbuf);
	UnlockReleaseBuffer(tapesortbuf);

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		SortHeapMeta *sortheapmetad;
		Page metapage;

		metapage = BufferGetPage(metabuf);
		sortheapmetad = (SortHeapMeta *)PageGetContents(metapage);

		sortheapmetad->tapesets[tapesort] = tapesortblk;
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

static void
sortheap_xlog_freepage_alloc(XLogReaderState *record)
{
	Buffer metabuf;
	Buffer leafbuf;
	Index mapidx;
	uint64 bit;
	long nBlocksAllocated;
	XLogRecPtr lsn = record->EndRecPtr;

	mapidx = *(Index *)XLogRecGetData(record);
	bit = *(uint64 *)(XLogRecGetData(record) + sizeof(Index));
	nBlocksAllocated = *(long *)(XLogRecGetData(record) +
								 sizeof(Index) +
								 sizeof(uint64));

	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page metapage;
		SortHeapMeta *metad;

		metapage = BufferGetPage(metabuf);
		metad = (SortHeapMeta *)PageGetContents(metapage);

		metad->nBlocksAllocated = nBlocksAllocated;

		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);

	if (XLogReadBufferForRedo(record, 1, &leafbuf) == BLK_NEEDS_REDO)
	{
		FreePageLeaf *leaf;
		Page leafpage;

		leafpage = BufferGetPage(leafbuf);
		leaf = (FreePageLeaf *)PageGetContents(leafpage);

		leaf->maps[mapidx] = (~((uint64)1 << (bit - 1))) & leaf->maps[mapidx];

		/* Mark the fastBitmap if necessary */
		if (leaf->maps[mapidx] == 0 &&
			mapidx % NUMMAPS_PERFASTBIT == NUMMAPS_PERFASTBIT - 1)
			leaf->fastBitmap =
				(~(((uint64)1) << mapidx / NUMMAPS_PERFASTBIT)) & leaf->fastBitmap;

		PageSetLSN(leafpage, lsn);
		MarkBufferDirty(leafbuf);
	}

	if (BufferIsValid(leafbuf))
		UnlockReleaseBuffer(leafbuf);
}

static void
sortheap_xlog_freepage_recycle(XLogReaderState *record)
{
	Buffer buffer;

	if (XLogReadBufferForRedo(record, 0, &buffer) == BLK_NEEDS_REDO)
	{
		Page page;
		Index mapidx;
		uint64 bit;
		XLogRecPtr lsn = record->EndRecPtr;
		FreePageLeaf *leaf;

		page = BufferGetPage(buffer);
		leaf = (FreePageLeaf *)PageGetContents(page);

		mapidx = *(Index *)XLogRecGetData(record);
		bit = *(uint64 *)(XLogRecGetData(record) + sizeof(Index));

		leaf->maps[mapidx] |= ((uint64)1 << bit);
		leaf->fastBitmap |= ((uint64)1 << mapidx);

		PageSetLSN(page, lsn);
		MarkBufferDirty(buffer);
	}

	if (BufferIsValid(buffer))
		UnlockReleaseBuffer(buffer);
}

static void
sortheap_xlog_freepage_newroot(XLogReaderState *record)
{
	Buffer metabuf;
	Buffer rootbuf;
	Page rootpage;
	int rootindex;
	long nBlocksAllocated;
	BlockNumber rootblk;
	FreePageRoot *root;
	XLogRecPtr lsn = record->EndRecPtr;
	SortHeapPageOpaqueData *rootopaque;

	rootindex = *(int *)XLogRecGetData(record);
	nBlocksAllocated = *(long *)(XLogRecGetData(record) + sizeof(int));

	/* re-init the new leaf page */
	rootbuf = XLogInitBufferForRedo(record, 1);
	rootpage = BufferGetPage(rootbuf);
	rootblk = BufferGetBlockNumber(rootbuf);
	PageInit(rootpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
	root = (FreePageRoot *)PageGetContents(rootpage);
	root->hasFreePages = true;
	root->numLeaves = 0;
	root->rootIndex = rootindex;
	rootopaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(rootpage);
	rootopaque->next = InvalidBlockNumber;
	rootopaque->type = SORTHEAP_FREESPACEPAGE;

	/*
	 * Set pd_lower just past the end of the metadata.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader)rootpage)->pd_lower =
		((char *)root + sizeof(FreePageRoot) +
		 sizeof(FreePageLeafRef) * (NUMLEAVES_PERROOT - 1)) -
		(char *)rootpage;

	PageSetLSN(rootpage, lsn);
	MarkBufferDirty(rootbuf);
	UnlockReleaseBuffer(rootbuf);

	/* update the nBlocksAllocated and freePageRoot */
	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page metapage;
		SortHeapMeta *metad;

		metapage = BufferGetPage(metabuf);
		metad = (SortHeapMeta *)PageGetContents(metapage);

		metad->nBlocksAllocated = nBlocksAllocated;
		if (metad->firstFreePagesRoot == InvalidBlockNumber)
			metad->firstFreePagesRoot = rootblk;

		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);
}

static void
sortheap_xlog_freepage_newleaf(XLogReaderState *record)
{
	int i;
	int rootIndex;
	int numLeaves;
	long nBlocksAllocated;
	Buffer metabuf;
	Buffer rootbuf;
	Buffer leafbuf;
	Page leafpage;
	BlockNumber leafblk;
	FreePageLeaf *leaf;
	XLogRecPtr lsn = record->EndRecPtr;

	rootIndex = *(int *)XLogRecGetData(record);
	numLeaves = *(int *)(XLogRecGetData(record) + sizeof(int));
	nBlocksAllocated = *(long *)(XLogRecGetData(record) +
								 sizeof(int) +
								 sizeof(int));

	/* re-init the new leaf */
	leafbuf = XLogInitBufferForRedo(record, 2);
	leafpage = BufferGetPage(leafbuf);
	leafblk = BufferGetBlockNumber(leafbuf);
	PageInit(leafpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
	leaf = (FreePageLeaf *)PageGetContents(leafpage);
	leaf->fastBitmap = ~(uint64)0;
	leaf->leafIndex = rootIndex * NUMLEAVES_PERROOT + numLeaves;
	for (i = 0; i < NUMMAPS_PERLEAF; i++)
		leaf->maps[i] = ~(uint64)0;

	/*
	 * For the first leaf, we need to mark the block 0 and block 1 and block 2
	 * as busy, block 0 is the metapage of sort heap, block 1 is always the
	 * first freepage root, block 2 is the first freepage leaf.
	 *
	 * For other leafs, mark the block 0 as valid because it is the leaf
	 * block.
	 */
	if (numLeaves == 0 && leaf->leafIndex == 0)
		leaf->maps[0] = leaf->maps[0] & (~(uint64)0b0111);
	else if (numLeaves == 0)
		leaf->maps[0] = leaf->maps[0] & (~(uint64)0b0011);
	else
		leaf->maps[0] = leaf->maps[0] & (~(uint64)0b0001);

	/*
	 * Set pd_lower just past the end of the metadata.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader)leafpage)->pd_lower =
		((char *)leaf + sizeof(FreePageLeaf) +
		 sizeof(uint64) * (NUMMAPS_PERLEAF - 1)) -
		(char *)leafpage;

	PageSetLSN(leafpage, lsn);
	MarkBufferDirty(leafbuf);
	UnlockReleaseBuffer(leafbuf);

	/* update the nBlocksAllocated */
	if (XLogReadBufferForRedo(record, 0, &metabuf) == BLK_NEEDS_REDO)
	{
		Page metapage;
		SortHeapMeta *metad;

		metapage = BufferGetPage(metabuf);
		metad = (SortHeapMeta *)PageGetContents(metapage);

		metad->nBlocksAllocated = nBlocksAllocated;
		PageSetLSN(metapage, lsn);
		MarkBufferDirty(metabuf);
	}

	if (BufferIsValid(metabuf))
		UnlockReleaseBuffer(metabuf);

	/* set the ref to the leaf page */
	if (XLogReadBufferForRedo(record, 1, &rootbuf) == BLK_NEEDS_REDO)
	{
		Page rootpage;
		FreePageRoot *root;

		rootpage = BufferGetPage(rootbuf);
		root = (FreePageRoot *)PageGetContents(rootpage);

		root->leaves[numLeaves].hasFreePages = true;
		root->leaves[numLeaves].blkno = leafblk;
		root->numLeaves = numLeaves + 1;

		PageSetLSN(rootpage, lsn);
		MarkBufferDirty(rootbuf);
	}

	if (BufferIsValid(rootbuf))
		UnlockReleaseBuffer(rootbuf);
}

void sortheap_redo(XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
	case XLOG_SORTHEAP_DUMP:
		sortheap_xlog_dump(record);
		break;
	case XLOG_SORTHEAP_TAPEEXTEND:
		sortheap_xlog_tape_extend(record);
		break;
	case XLOG_SORTHEAP_FULLPAGEUPDATE:
		sortheap_xlog_fullpage_update(record);
		break;
	case XLOG_SORTHEAP_EXTEND:
		sortheap_xlog_extend(record);
		break;
	case XLOG_SORTHEAP_RUNSMETABLK:
		sortheap_xlog_runsmetablk(record);
		break;
	case XLOG_SORTHEAP_NEWTAPESORT:
		sortheap_xlog_newtapesort(record);
		break;
	case XLOG_SORTHEAP_NEWHEADER:
		sortheap_xlog_newheader(record);
		break;
	case XLOG_SORTHEAP_RECORD_ALLOCATED:
		sortheap_xlog_record_allocated(record);
		break;
	case XLOG_SORTHEAP_MERGE_DONE:
		sortheap_xlog_merge_done(record);
		break;
	case XLOG_SORTHEAP_VACUUM_DONE:
		sortheap_xlog_vacuum_done(record);
		break;
	case XLOG_SORTHEAP_VACUUMTAPE:
		sortheap_xlog_vacuum_tape(record);
		break;
	case XLOG_SORTHEAP_RELEASE_AUXBLK:
		sortheap_xlog_release_auxblk(record);
		break;
	default:
		elog(PANIC, "sortheap_redo: unknown op code %u", info);
	}
}

void sortheap_redo2(XLogReaderState *record)
{
	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	switch (info)
	{
	case XLOG_SORTHEAP_FREEPAGE_ALLOC:
		sortheap_xlog_freepage_alloc(record);
		break;
	case XLOG_SORTHEAP_FREEPAGE_RECYC:
		sortheap_xlog_freepage_recycle(record);
		break;
	case XLOG_SORTHEAP_FREEPAGE_NEWROOT:
		sortheap_xlog_freepage_newroot(record);
		break;
	case XLOG_SORTHEAP_FREEPAGE_NEWLEAF:
		sortheap_xlog_freepage_newleaf(record);
		break;
	default:
		elog(PANIC, "sortheap_redo: unknown op code %u", info);
	}
}

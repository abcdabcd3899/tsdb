/*
 * sortheap_tape.c
 *		Generalized codes for logical tape.
 *
 * This file contains all functions to manipulate the logical tape includind
 * writing/reading tuples, extending the logical tape, building internal btree
 * index.
 *
 * To implement Knuth's Algorithm 5.4.2D, a logical tape is consist of multiple
 * RUNS (sorted sequence) and tuples are sorted within a RUN.
 *
 * From the physical view, a logical tape is consist of heap pages which are
 * linked by pointer in the special area of page, all tuples are spread among
 * heap pages with certain order, we use a special tuple to represent the end
 * of RUN so the logical tape are break into RUNs physically.
 *
 * To accelerate the query, an customized internal btree is build for each RUN.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_tape.c
 */
#include "sortheap.h"
#include "sortheap_tapesets.h"
#include "sortheap_tape.h"
#include "sortheap_btree.h"
#include "sortheap_brin.h"
#include "sortheap_columnstore.h"

/* Routines for logical tapes */
static void tape_record_newblock(TapeInsertState *tapeinsertstate,
								 int nprealloc,
								 BlockNumber newblock);
static Buffer tape_get_buffer_for_insert(TapeInsertState *tapeinsertstate,
										 Size len);
static BlockNumber tape_getpreallocated(TapeInsertState *tapeinsertstate);

/* --------------------------------
 * XLOG related functions
 * --------------------------------
 */

static XLogRecPtr
log_tape_runsmetablk(Buffer headerbuf, int runno, BlockNumber runsmetablk)
{
	XLogBeginInsert();

	XLogRegisterData((char *)&runno, sizeof(int));
	XLogRegisterData((char *)&runsmetablk, sizeof(BlockNumber));

	XLogRegisterBuffer(0, headerbuf, REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_RUNSMETABLK);
}

static XLogRecPtr
log_tape_extend(Buffer headerbuf, Buffer curBuf, Buffer newBuf,
				SortHeapPageType pagetype, int level)
{
	bool curbufset = (curBuf != InvalidBuffer);

	XLogBeginInsert();

	XLogRegisterData((char *)&curbufset, sizeof(bool));
	XLogRegisterData((char *)&pagetype, sizeof(SortHeapPageType));
	XLogRegisterData((char *)&level, sizeof(int));

	XLogRegisterBuffer(0, newBuf, REGBUF_WILL_INIT);
	XLogRegisterBuffer(1, headerbuf, REGBUF_STANDARD);

	if (curbufset)
		XLogRegisterBuffer(2, curBuf, REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_TAPEEXTEND);
}

static XLogRecPtr
log_new_tapeheader(Buffer metabuf, int tapenum, Buffer headerbuf)
{
	XLogBeginInsert();
	XLogRegisterData((char *)&tapenum, sizeof(int));
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
	XLogRegisterBuffer(1, headerbuf, REGBUF_WILL_INIT);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_NEWHEADER);
}

static XLogRecPtr
log_record_allocated(Buffer metabuf,
					 Buffer curbuf,
					 Buffer nextbuf,
					 int nprealloc,
					 long nallocated,
					 BlockNumber newalloc)
{
	bool curbufset = (curbuf != InvalidBuffer);
	bool nextbufset = (nextbuf != InvalidBuffer);

	XLogBeginInsert();
	XLogRegisterData((char *)&curbufset, sizeof(bool));
	XLogRegisterData((char *)&nextbufset, sizeof(bool));
	XLogRegisterData((char *)&nprealloc, sizeof(int));
	XLogRegisterData((char *)&nallocated, sizeof(long));
	XLogRegisterData((char *)&newalloc, sizeof(BlockNumber));

	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

	if (nextbufset)
	{
		XLogRegisterBuffer(1, nextbuf, REGBUF_WILL_INIT);

		if (curbufset)
			XLogRegisterBuffer(2, curbuf, REGBUF_STANDARD);
	}
	else
		XLogRegisterBuffer(1, curbuf, REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_RECORD_ALLOCATED);
}

/*
 * ------------------------------------------------------------
 * Routines for logical tapes
 * ------------------------------------------------------------
 */

/*
 * Get a buffer so we can insert the len data into.
 */
Buffer
tape_get_new_bt_buffer(TapeInsertState *tapeinsertstate, int level)
{
	Buffer newbuf;
	BlockNumber newblk;

	/* Lock the whole file to allocated a new block */
	newblk = tape_extend(tapeinsertstate, InvalidBuffer,
						 SORTHEAP_BTPAGE, level);

	newbuf = ReadBuffer(tapeinsertstate->relation, newblk);
	LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);
	return newbuf;
}

/*
 * Get a buffer so we can insert the data with len.
 *
 * Bulk insert mode is used, then a buffer is pinned to avoid frequent lookup
 * of buffers.
 */
static Buffer
tape_get_buffer_for_insert(TapeInsertState *tapeinsertstate, Size len)
{
	BulkInsertState bistate;
	Page curPage;
	Buffer curBuf;
	Page headerpage = BufferGetPage(tapeinsertstate->headerbuf);
	HeapTapeMeta *tapemetad = (HeapTapeMeta *)PageGetContents(headerpage);

	len = MAXALIGN(len);

	if (len > MaxHeapTupleSize)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("row is too big: size %zu, maximum size %zu",
						len, MaxHeapTupleSize)));
	bistate = tapeinsertstate->bistate;

retry:

	/*
	 * If we have the desired block already pinned and the block has enough
	 * space, re-pin and return it
	 */
	if (bistate->current_buf != InvalidBuffer)
	{
		curBuf = bistate->current_buf;
		curPage = BufferGetPage(curBuf);

		/* The caller should Unlock the buffer after insertion */
		LockBuffer(curBuf, BUFFER_LOCK_EXCLUSIVE);

		if (len <= PageGetFreeSpace(curPage))
		{
			/*
			 * Currently the LOCK variants are only used for extending
			 * relation, which should never reach this branch.
			 */
			IncrBufferRefCount(curBuf);
			return curBuf;
		}
		else
		{
			XLogRecPtr recptr;

			/* Request a new block for insert */
			tape_extend(tapeinsertstate, curBuf, SORTHEAP_DATAPAGE, -1);

			/*
			 * XLOG stuff.
			 *
			 * sortheap didn't write a xlog record for each insertion, instead
			 * it only write xlog record when swithing to new insertion page
			 * and when a dump is done, this can reduce the xlog overhead.
			 *
			 * It is acceptable if a crash happens before the xlog is inserted
			 * because there is only one transaction are performing on current
			 * tape which means there are no tuples from other transactions,
			 * so the crash will not affect other transactions which need a
			 * xlog.
			 */
			recptr = log_full_page_update(curBuf);

			PageSetLSN(curPage, recptr);
			/* Release current block */
			UnlockReleaseBuffer(curBuf);

			bistate->current_buf = InvalidBuffer;
		}
	}
	else
	{
		if (tapemetad->lastdatablock == InvalidBlockNumber)
		{
			tape_extend(tapeinsertstate,
						InvalidBuffer,
						SORTHEAP_DATAPAGE,
						-1);
		}
	}

	/*
	 * We reach here to get a new buffer for insert, the difference between
	 * the heap is that heap will always extend one block ahead the EOF, but
	 * sortheap may extend more blocks because we preallocated sequential
	 * blocks for each tape, so if we switch to another tape for insert, there
	 * will be "holes" in the file.
	 */

	/* Perform a read using the buffer strategy */
	curBuf = ReadBufferExtended(tapeinsertstate->relation,
								MAIN_FORKNUM,
								tapemetad->lastdatablock,
								RBM_NORMAL,
								bistate->strategy);

	LockBuffer(curBuf, BUFFER_LOCK_EXCLUSIVE);
	curPage = BufferGetPage(curBuf);

	/* Save the selected block as target for future inserts */
	IncrBufferRefCount(curBuf);
	bistate->current_buf = curBuf;

	/* Double check the freespace */
	if (len > PageGetFreeSpace(curPage))
	{
		UnlockReleaseBuffer(curBuf);
		goto retry;
	}

	return curBuf;
}

static void
tape_get_runmetaentry(TapeInsertState *insertstate, int curRun)
{
	Page tapemetapage;
	Page runsmetapage;
	HeapTapeMeta *tapemetad;
	TapeRunsMeta *runsmetad;

	/* Get the RunMeta for this run */
	tapemetapage = BufferGetPage(insertstate->headerbuf);
	tapemetad = (HeapTapeMeta *)PageGetContents(tapemetapage);

	if (tapemetad->runsmetad[curRun / NUMRUNMETA_PERPAGE] == InvalidBlockNumber)
	{
		tapemetad->runsmetad[curRun / NUMRUNMETA_PERPAGE] =
			tape_extend(insertstate, InvalidBuffer, SORTHEAP_RUNMETAPAGE, -1);

		/* MarkBufferDirty must be called under lock */
		LockBuffer(insertstate->headerbuf, BUFFER_LOCK_EXCLUSIVE);

		MarkBufferDirty(insertstate->headerbuf);

		/* XLOG stuff */
		{
			XLogRecPtr recptr;

			recptr = log_tape_runsmetablk(insertstate->headerbuf, curRun,
										  tapemetad->runsmetad[curRun / NUMRUNMETA_PERPAGE]);
			PageSetLSN(tapemetapage, recptr);
		}

		LockBuffer(insertstate->headerbuf, BUFFER_LOCK_UNLOCK);
	}

	insertstate->runsmetabuf =
		ReadBuffer(insertstate->relation, tapemetad->runsmetad[curRun / NUMRUNMETA_PERPAGE]);

	LockBuffer(insertstate->runsmetabuf, BUFFER_LOCK_SHARE);
	runsmetapage = BufferGetPage(insertstate->runsmetabuf);
	runsmetad = (TapeRunsMeta *)PageGetContents(runsmetapage);
	insertstate->runmetad = &runsmetad->entries[curRun % NUMRUNMETA_PERPAGE];
	LockBuffer(insertstate->runsmetabuf, BUFFER_LOCK_UNLOCK);
}

static BlockNumber
tape_create_header(TapeSetsState *tsstate, int tapenum)
{
	Buffer headerbuf;
	Page headerpage;
	Page tapesortpage;
	Relation relation = tsstate->relation;
	XLogRecPtr recptr;
	BlockNumber result;
	TapeSetsMeta *tsmetad;
	HeapTapeMeta *headermetad;
	SortHeapState *shstate = tsstate->parent;
	SortHeapPageOpaqueData *opaque;

	LockBuffer(shstate->metabuf, BUFFER_LOCK_EXCLUSIVE);
	LockBuffer(tsstate->metabuf, BUFFER_LOCK_EXCLUSIVE);

	tsmetad = tapesets_getmeta_disk(tsstate);

	if (tsmetad->tapes[tapenum].header != InvalidBlockNumber)
	{
		result = tsmetad->tapes[tapenum].header;
		LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);
		LockBuffer(shstate->metabuf, BUFFER_LOCK_UNLOCK);
		return result;
	}

	/* Assign a new block */
	result = sortheap_get_freeblock(shstate);
	tsmetad->tapes[tapenum].header = result;

	/* Extend the file if needed */
	sortheap_extend(shstate, result);
	MarkBufferDirty(tsstate->metabuf);

	SIMPLE_FAULT_INJECTOR("new_tapeheader_before_xlog");

	/* safe to release the lock on file metabuf */
	LockBuffer(shstate->metabuf, BUFFER_LOCK_UNLOCK);

	/* Initialize the new tape header meta page */
	headerbuf = ReadBuffer(relation, tsmetad->tapes[tapenum].header);
	LockBuffer(headerbuf, BUFFER_LOCK_EXCLUSIVE);
	headerpage = BufferGetPage(headerbuf);
	PageInit(headerpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
	headermetad = (HeapTapeMeta *)PageGetContents(headerpage);
	headermetad->nprealloc = 0;
	headermetad->prealloc_size = 0;
	headermetad->lastdatablock = InvalidBlockNumber;
	headermetad->firstauxblock = InvalidBlockNumber;
	headermetad->currentauxblock = InvalidBlockNumber;
	headermetad->numblocks = 0;
	headermetad->nruns = 0;
	memset(headermetad->runsmetad, InvalidBlockNumber, sizeof(BlockNumber) * NUMPAGES_RUNSMETA);

	opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(headerpage);
	opaque->type = SORTHEAP_TAPEMETAPAGE;
	opaque->next = InvalidBlockNumber;
	MarkBufferDirty(headerbuf);

	/*
	 * Set pd_lower just past the end of the metadata.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader)headerpage)->pd_lower =
		((char *)headermetad + sizeof(HeapTapeMeta)) - (char *)headerpage;

	/* XLOG stuff */
	recptr = log_new_tapeheader(tsstate->metabuf, tapenum, headerbuf);

	/* Relase the new header buf */
	PageSetLSN(headerpage, recptr);
	UnlockReleaseBuffer(headerbuf);

	/* Mark tapesort meta dirty and release lock */
	tapesortpage = BufferGetPage(tsstate->metabuf);
	PageSetLSN(tapesortpage, recptr);

	SIMPLE_FAULT_INJECTOR("new_tapeheader_after_xlog");

	LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);

	return result;
}

/*
 * Get the header blocknum of specified tapenum from specified tapesort. When
 * merging is true, allocate a new header blocknum if it doesn't exist.
 */
BlockNumber
tape_getheader(TapeSetsState *tsstate, int tapenum, bool physical, bool createit)
{
	BlockNumber result;
	TapeSetsMeta *tsmetad;

	if (physical)
	{
		LockBuffer(tsstate->metabuf, BUFFER_LOCK_SHARE);

		tsmetad = tapesets_getmeta_disk(tsstate);
		result = tsmetad->tapes[tapenum].header;

		if (result == InvalidBlockNumber && createit)
		{
			LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);
			result = tape_create_header(tsstate, tapenum);
		}
		else
			LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);
	}
	else
	{
		tsmetad = tapesets_getmeta_snapshot(tsstate);
		result = tsmetad->tapes[tapenum].header;

		if (result == InvalidBlockNumber && createit)
		{
			result = tape_create_header(tsstate, tapenum);
			/* Also update the metad snap */
			tsmetad->tapes[tapenum].header = result;
		}
	}

	return result;
}

/*
 * Return the lowest free block number from the tape's preallocation list.
 * Refill the preallocation list if necessary.
 *
 * The preallocation list is stored in the meta page of the tape.
 */
static BlockNumber
tape_getpreallocated(TapeInsertState *tapeinsertstate)
{
	Page metapage;
	Buffer headerbuf = tapeinsertstate->headerbuf;
	HeapTapeMeta *tapemetad;
	BlockNumber result;

	metapage = BufferGetPage(headerbuf);
	tapemetad = (HeapTapeMeta *)PageGetContents(metapage);

	/* sorted in descending order, so return the last element */
	if (tapemetad->nprealloc > 0)
		result = tapemetad->prealloc[--tapemetad->nprealloc];
	else
	{
		if (tapemetad->prealloc_size == 0)
			tapemetad->prealloc_size = MIN_PREALLOC_SIZE;
		else if (tapemetad->prealloc_size < MAX_PREALLOC_SIZE)
		{
			/* when the preallocation list runs out, double the size */
			tapemetad->prealloc_size *= 2;
			if (tapemetad->prealloc_size > MAX_PREALLOC_SIZE)
				tapemetad->prealloc_size = MAX_PREALLOC_SIZE;
		}

		/* refill preallocation list */
		tapemetad->nprealloc = tapemetad->prealloc_size;
		for (int i = tapemetad->nprealloc; i > 0; i--)
		{
			tapemetad->prealloc[i - 1] =
				sortheap_get_freeblock(tapeinsertstate->parent->parent);
		}
		MarkBufferDirty(headerbuf);

		result = tapemetad->prealloc[--tapemetad->nprealloc];

		/* XLOG stuff */
		{
			XLogRecPtr recptr;

			recptr = log_full_page_update(headerbuf);
			PageSetLSN(metapage, recptr);
		}
	}

	/* Extend the physical block if needed */
	sortheap_extend(tapeinsertstate->parent->parent, result);

	/* record the allocated blocks in addition blocks */
	tape_record_newblock(tapeinsertstate, tapemetad->nprealloc, result);

	return result;
}

/*
 * Record a new block just allocated in this tape.
 */
static void
tape_record_newblock(TapeInsertState *tapeinsertstate, int nprealloc, BlockNumber newblock)
{
	Index idx;
	Page curpage;
	Page nextpage;
	Page metapage;
	BlockNumber *allocated;
	Relation relation = tapeinsertstate->relation;
	HeapTapeMeta *tapemetad;
	SortHeapState *shstate = tapeinsertstate->parent->parent;

	metapage = BufferGetPage(tapeinsertstate->headerbuf);
	tapemetad = (HeapTapeMeta *)PageGetContents(metapage);

	tapemetad->numblocks++;

	if (tapemetad->firstauxblock == InvalidBlockNumber)
	{
		SortHeapPageOpaqueData *opaque;
		Buffer buffer;

		/* Allocate a free block for recording allocated blocks */
		tapemetad->firstauxblock = sortheap_get_freeblock(shstate);
		tapemetad->currentauxblock = tapemetad->firstauxblock;
		sortheap_extend(shstate, tapemetad->firstauxblock);
		MarkBufferDirty(tapeinsertstate->headerbuf);

		/* Initialized the block */
		buffer = ReadBuffer(relation, tapemetad->firstauxblock);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		curpage = BufferGetPage(buffer);
		PageInit(curpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
		allocated = (BlockNumber *)PageGetContents(curpage);

		memset(allocated, InvalidBlockNumber, ALLOCATEDBLOCKPERAUXBLOCK * sizeof(BlockNumber));
		opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(curpage);
		opaque->next = InvalidBlockNumber;
		opaque->type = SORTHEAP_ALLOCATEDPAGE;

		/* Mark the block as allocated */
		allocated[0] = newblock;

		/*
		 * Set pd_lower just past the end of the metadata.  This is essential,
		 * because without doing so, metadata will be lost if xlog.c
		 * compresses the page.
		 */
		((PageHeader)curpage)->pd_lower =
			((char *)allocated + ALLOCATEDBLOCKPERAUXBLOCK * sizeof(BlockNumber)) - (char *)curpage;

		MarkBufferDirty(buffer);

		/* XLOG stuff */
		{
			XLogRecPtr recptr;

			recptr = log_record_allocated(tapeinsertstate->headerbuf,
										  InvalidBuffer,
										  buffer,
										  nprealloc,
										  tapemetad->numblocks,
										  newblock);
			PageSetLSN(curpage, recptr);
		}

		UnlockReleaseBuffer(buffer);

		return;
	}

	if (tapeinsertstate->currentauxbuf == InvalidBuffer)
		tapeinsertstate->currentauxbuf = ReadBuffer(relation, tapemetad->currentauxblock);

	/* Check the number of blocks */
	idx = (tapemetad->numblocks - 1) % ALLOCATEDBLOCKPERAUXBLOCK;

	/* we need to switch to another aux block */
	if (idx == 0 && tapemetad->numblocks > ALLOCATEDBLOCKPERAUXBLOCK)
	{
		BlockNumber nextAuxBlock;
		Buffer nextbuffer;
		SortHeapPageOpaqueData *curopaque;
		SortHeapPageOpaqueData *nextopaque;

		/* allocate a new free block */
		nextAuxBlock = sortheap_get_freeblock(shstate);
		sortheap_extend(shstate, nextAuxBlock);

		/* Update the current auxiliary block */
		tapemetad->currentauxblock = nextAuxBlock;
		MarkBufferDirty(tapeinsertstate->headerbuf);

		LockBuffer(tapeinsertstate->currentauxbuf, BUFFER_LOCK_EXCLUSIVE);
		/* Add the link to the next auxiliary block */
		curpage = BufferGetPage(tapeinsertstate->currentauxbuf);
		curopaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(curpage);
		curopaque->next = nextAuxBlock;
		MarkBufferDirty(tapeinsertstate->currentauxbuf);

		/* Read and initialize the new auxiliary block */
		nextbuffer = ReadBuffer(relation, nextAuxBlock);
		LockBuffer(nextbuffer, BUFFER_LOCK_EXCLUSIVE);
		nextpage = BufferGetPage(nextbuffer);
		PageInit(nextpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
		allocated = (BlockNumber *)PageGetContents(nextpage);
		memset(allocated, InvalidBlockNumber, ALLOCATEDBLOCKPERAUXBLOCK * sizeof(BlockNumber));
		nextopaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(nextpage);
		nextopaque->next = InvalidBlockNumber;

		/* Mark the block as allocated */
		allocated[0] = newblock;

		/*
		 * Set pd_lower just past the end of the metadata.  This is essential,
		 * because without doing so, metadata will be lost if xlog.c
		 * compresses the page.
		 */
		((PageHeader)nextpage)->pd_lower =
			((char *)allocated + ALLOCATEDBLOCKPERAUXBLOCK * sizeof(BlockNumber)) - (char *)nextpage;

		MarkBufferDirty(nextbuffer);

		/* XLOG stuff */
		{
			XLogRecPtr recptr;

			recptr = log_record_allocated(tapeinsertstate->headerbuf,
										  tapeinsertstate->currentauxbuf,
										  nextbuffer,
										  nprealloc,
										  tapemetad->numblocks,
										  newblock);
			PageSetLSN(curpage, recptr);
			PageSetLSN(nextpage, recptr);
		}

		/* Pin the buffer, only release the lock */
		LockBuffer(nextbuffer, BUFFER_LOCK_UNLOCK);
		UnlockReleaseBuffer(tapeinsertstate->currentauxbuf);
		tapeinsertstate->currentauxbuf = nextbuffer;
	}
	else
	{
		/* Record block in the current auxiliary block */
		LockBuffer(tapeinsertstate->currentauxbuf, BUFFER_LOCK_EXCLUSIVE);

		curpage = BufferGetPage(tapeinsertstate->currentauxbuf);
		allocated = (BlockNumber *)PageGetContents(curpage);

		/* Mark the block as allocated */
		allocated[idx] = newblock;
		MarkBufferDirty(tapeinsertstate->currentauxbuf);

		/* XLOG stuff */
		{
			XLogRecPtr recptr;

			recptr = log_record_allocated(tapeinsertstate->headerbuf,
										  tapeinsertstate->currentauxbuf,
										  InvalidBuffer,
										  nprealloc,
										  tapemetad->numblocks,
										  newblock);
			PageSetLSN(curpage, recptr);
		}

		LockBuffer(tapeinsertstate->currentauxbuf, BUFFER_LOCK_UNLOCK);
	}
}

/*
 * Extend the logical tape.
 *
 * If curbuf is set, the curbuf->next will be set to the new block.
 *
 * CAUTION: The caller should have acquired the exclusive lock on the file and
 * tapesets.
 *
 * CAUTION: level is only valid for BTPAGE
 *
 * RETURN: the new block number is returned.
 */
BlockNumber
tape_extend(TapeInsertState *tapeinsertstate,
			Buffer curbuf,
			SortHeapPageType pagetype,
			int level)
{
	Buffer newbuf;
	Page newpage;
	Page curpage;
	Page headerpage = NULL;
	BlockNumber newblk;
	XLogRecPtr recptr;
	Relation relation = tapeinsertstate->relation;
	SortHeapState *shstate = tapeinsertstate->parent->parent;

	LockBuffer(shstate->metabuf, BUFFER_LOCK_EXCLUSIVE);
	LockBuffer(tapeinsertstate->headerbuf, BUFFER_LOCK_EXCLUSIVE);

	Assert(pagetype == SORTHEAP_DATAPAGE ||
		   pagetype == SORTHEAP_BTPAGE ||
		   pagetype == SORTHEAP_BRINPAGE ||
		   pagetype == SORTHEAP_RUNMETAPAGE);

	newblk = tape_getpreallocated(tapeinsertstate);

	/* If the curbuf is set, link its next to newblk */
	if (curbuf != InvalidBuffer)
	{
		SortHeapPageOpaqueData *opaque;

		/* Link it as the next block of current block */
		curpage = BufferGetPage(curbuf);
		opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(curpage);
		opaque->next = newblk;
		Assert(opaque->type == pagetype);
		MarkBufferDirty(curbuf);
	}

	/*
	 * Page Initialize.
	 */
	newbuf = ReadBuffer(relation, newblk);
	newpage = BufferGetPage(newbuf);
	LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * DATA/BRIN/RUNMETA page has the same layout
	 */
	if (pagetype == SORTHEAP_DATAPAGE ||
		pagetype == SORTHEAP_BRINPAGE ||
		pagetype == SORTHEAP_RUNMETAPAGE)
	{
		SortHeapPageOpaqueData *opaque;

		/* Initialized the new data page */
		PageInit(newpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
		opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(newpage);
		opaque->type = pagetype;
		opaque->next = InvalidBlockNumber;

		/* Update the last data block and set the range number */
		if (pagetype == SORTHEAP_DATAPAGE)
		{
			HeapTapeMeta *metad;

			headerpage = BufferGetPage(tapeinsertstate->headerbuf);

			metad = (HeapTapeMeta *)PageGetContents(headerpage);
			/* Request a new block for insert */
			metad->lastdatablock = newblk;
			MarkBufferDirty(tapeinsertstate->headerbuf);

			/* update the range */
			opaque->range = tapeinsertstate->nextrange;
		}
		else if (pagetype == SORTHEAP_RUNMETAPAGE)
		{
			TapeRunsMeta *runsmeta;

			runsmeta = (TapeRunsMeta *)PageGetContents(newpage);

			/*
			 * Set pd_lower just past the end of the metadata.  This is
			 * essential, because without doing so, metadata will be lost if
			 * xlog.c compresses the page.
			 */
			((PageHeader)newpage)->pd_lower =
				((char *)runsmeta + NUMRUNMETA_PERPAGE * sizeof(TapeRunsMetaEntry)) - (char *)newpage;
		}
	}
	else
	{
		BTPageOpaque opaque;

		Assert(pagetype == SORTHEAP_BTPAGE);

		/* Zero the page and set up standard page header info */
		_bt_pageinit(newpage, BLCKSZ);

		/* Initialize BT opaque state */
		opaque = (BTPageOpaque)PageGetSpecialPointer(newpage);
		opaque->btpo_prev = opaque->btpo_next = P_NONE;
		opaque->btpo_cycleid = 0;
		opaque->btpo.level = level;
		opaque->btpo_flags = (level > 0) ? 0 : BTP_LEAF;

		/* Make the P_HIKEY line pointer appear allocated */
		((PageHeader)newpage)->pd_lower += sizeof(ItemIdData);
	}

	MarkBufferDirty(newbuf);

	/*
	 * XLOG stuff, record we have a new data page.
	 */
	recptr = log_tape_extend(tapeinsertstate->headerbuf,
							 curbuf,
							 newbuf,
							 pagetype,
							 level);

	/* Update the lsn and mark dirty */
	PageSetLSN(newpage, recptr);

	if (pagetype == SORTHEAP_DATAPAGE)
		PageSetLSN(headerpage, recptr);

	if (curbuf != InvalidBuffer)
		PageSetLSN(curpage, recptr);

	UnlockReleaseBuffer(newbuf);

	LockBuffer(tapeinsertstate->headerbuf, BUFFER_LOCK_UNLOCK);
	LockBuffer(shstate->metabuf, BUFFER_LOCK_UNLOCK);

	return newblk;
}

static void
tape_free_scandesc(TapeScanDesc scandesc)
{
	if (!scandesc)
		return;

	if (scandesc->empty)
		return;

	if (scandesc->brinbuf)
		ReleaseBuffer(scandesc->brinbuf);
	if (scandesc->runsmetabuf)
		ReleaseBuffer(scandesc->runsmetabuf);
	if (scandesc->hdesc)
		heap_endscan((TableScanDesc)scandesc->hdesc);
	if (scandesc->slab_tuple)
		pfree(scandesc->slab_tuple);
	if (scandesc->idesc)
		index_endscan(scandesc->idesc);
	if (scandesc->scanslot)
		ExecDropSingleTupleTableSlot(scandesc->scanslot);

	scandesc->empty = true;
}

bool tape_initialize_scandesc(TapeReadState *tapereadstate, int runno,
							  TapeRunsMetaEntry *runmetad,
							  SortHeapScanDesc tablescandesc)
{
	int runmetadidx;
	int nattrs;
	Page tapemetapage;
	Page runsmetapage;
	BlockNumber runsmetablk;
	HeapTapeMeta *tapemetad;
	TapeRunsMeta *runsmetad;
	TapeScanDesc tapescandesc = tapereadstate->runs_scandescs[runno];
	TapeSetsState *tsstate = tapereadstate->parent;
	SortHeapState *shstate = tsstate->parent;

	runmetadidx = runno / NUMRUNMETA_PERPAGE;
	Assert(runmetadidx < NUMPAGES_RUNSMETA);

	/* Initialize basic info to scan a run or all run */

	if (shstate->op == SHOP_MERGE)
	{
		/*
		 * Must use Current Transaction snapshot for merge, so the new merged
		 * tuple will be visible by the snapshot. Also disable page mode to
		 * be able to read newly inserted tuples in the page.
		 */
		tapescandesc->hdesc =
			(HeapScanDesc)heap_beginscan(shstate->relation,
										 GetTransactionSnapshot(),
										 0, NULL,
										 NULL,
										 SO_TYPE_SEQSCAN | SO_ALLOW_STRAT);

		/*
		 * Also don't restrict the number of blocks that scandesc can see
		 * because the newly merged tuples may exceed the nblocks when the
		 * scandesc is created
		 */
		tapescandesc->hdesc->rs_nblocks = MaxBlockNumber;
	}
	else
		tapescandesc->hdesc =
			(HeapScanDesc)heap_beginscan(shstate->relation,
										 tapereadstate->snapshot,
										 0, NULL,
										 NULL,
										 SO_TYPE_SEQSCAN | SO_ALLOW_STRAT | SO_ALLOW_PAGEMODE);
	tapescandesc->tablescandesc = tablescandesc;
	tapescandesc->runno = runno;
	tapescandesc->slab_size = 1024;
	tapescandesc->slab_tuple = (HeapTuple)palloc(tapescandesc->slab_size);
	tapescandesc->parent = tapereadstate;
	tapescandesc->range = -1;

	/* Initialize a scan slot */
	tapescandesc->scanslot =
		MakeSingleTupleTableSlot(RelationGetDescr(shstate->relation),
								 &TTSOpsHeapTuple);

	/* Initialize the compressed tuple descr */
	tapescandesc->columnstore_desc =
		build_columnstore_tupledesc(shstate->relation,
									shstate->indexrel,
									&tapescandesc->columnchunk_descs,
									NULL);
	tapescandesc->columnchunkcontext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "columnchunk context",
							  ALLOCSET_DEFAULT_SIZES);

	tapescandesc->cached = false;
	nattrs = RelationGetDescr(shstate->relation)->natts;
	tapescandesc->datum_cache = palloc0(sizeof(Datum *) * nattrs);
	tapescandesc->isnull_cache = palloc0(sizeof(bool *) * nattrs);

	/* Initialize the metadata about this run */
	if (!runmetad)
	{
		tapemetapage = BufferGetPage(tapereadstate->headerbuf);
		tapemetad = (HeapTapeMeta *)PageGetContents(tapemetapage);
		runsmetablk = tapemetad->runsmetad[runmetadidx];

		if (runsmetablk == InvalidBlockNumber)
		{
			tape_free_scandesc(tapescandesc);
			return NULL;
		}

		tapescandesc->runsmetabuf = ReadBuffer(shstate->relation, runsmetablk);

		runsmetapage = BufferGetPage(tapescandesc->runsmetabuf);
		runsmetad = (TapeRunsMeta *)PageGetContents(runsmetapage);

		tapescandesc->runmetad = runsmetad->entries[runno % NUMRUNMETA_PERPAGE];
		tapescandesc->firsttuple = tapescandesc->runmetad.firsttuple;
		tapescandesc->lasttuple = tapescandesc->runmetad.lasttuple;

		if (!ItemPointerIsValid(&tapescandesc->firsttuple) ||
			!ItemPointerIsValid(&tapescandesc->lasttuple))
		{
			tape_free_scandesc(tapescandesc);
			return NULL;
		}
	}
	else
	{
		memcpy(&tapescandesc->runmetad, runmetad, sizeof(TapeRunsMetaEntry));
		tapescandesc->firsttuple = tapescandesc->runmetad.firsttuple;
		tapescandesc->lasttuple = tapescandesc->runmetad.lasttuple;
	}

	/*
	 * If we have index key predicate quals, apply it to find the first tuple
	 * in the run.
	 */
	if (tablescandesc && tablescandesc->iss)
	{
		ItemPointerData rangestart;
		IndexScanDesc idesc;
		IndexScanState *iss;
		BTScanOpaque so;
		int i;
		int range;
		bool use_bt_first = false;

		iss = tablescandesc->iss;
		idesc = index_beginscan(iss->ss.ss_currentRelation,
								iss->iss_RelationDesc,
								tapereadstate->snapshot,
								iss->iss_NumScanKeys,
								iss->iss_NumOrderByKeys);
		if (iss->iss_NumRuntimeKeys == 0 || iss->iss_RuntimeKeysReady)
			index_rescan(idesc,
						 iss->iss_ScanKeys, iss->iss_NumScanKeys,
						 iss->iss_OrderByKeys, iss->iss_NumOrderByKeys);

		_bt_preprocess_keys(idesc);

		tapescandesc->idesc = idesc;

		/*
		 * Quick locate the first tuple to scan. Firstly, use brin to identify
		 * the first tuple
		 */
		range = sortheap_brin_get_next_range(shstate->relation, tapescandesc, &rangestart);

		/* No tuples satisfy the scan keys */
		if (range == -1)
		{
			tape_free_scandesc(tapescandesc);
			return NULL;
		}

		/* Update the firsttuple to the first item found by brin index */
		tapescandesc->firsttuple = rangestart;

		so = (BTScanOpaque)idesc->opaque;

		for (i = 0; i < so->numberOfKeys; i++)
		{
			ScanKeyData *scankey = &so->keyData[i];

			/*
			 * If there are scan key is required to continue Forward scan, we
			 * must use btree to find exactly the first tuple, otherwise, the
			 * scan key check will stop the scan earlier unexpectedly.
			 *
			 * FIXME: handle BACKWARD direction
			 */
			if (scankey->sk_flags & SK_BT_REQFWD)
			{
				use_bt_first = true;
				break;
			}
		}

		if (range == 0 || use_bt_first)
		{
			/* FIXME: ungly here */
			shstate->cur_readtapesets = tsstate->tapesetsno;
			tapereadstate->parent->cur_readtape = tapereadstate->tapeno;
			tapereadstate->parent->cur_readrun = runno;

			/* No matching tuples in this run, retrun NULL */
			if (sortheap_bt_first(idesc, ForwardScanDirection) == false)
			{
				tape_free_scandesc(tapescandesc);
				return NULL;
			}

			/* Update the firsttuple to the first item found by index */
			tapescandesc->firsttuple = idesc->xs_heaptid;
		}
	}

	return tapescandesc;
}

TapeScanDesc
tape_get_scandesc(TapeSetsState *tsstate, int tapeno, int runno,
				  SortHeapScanDesc tablescandesc)
{
	TapeReadState *tapereadstate =
		tsstate->readstates ? tsstate->readstates[tapeno] : NULL;

	/* If the tape has no data, return NULL */
	if (!tapereadstate)
		return NULL;

	Assert(runno >= 0);

	if (!tapereadstate->runs_scandescs)
	{
		Assert(runno < tapereadstate->nruns);

		tapereadstate->runs_scandescs = (TapeScanDesc *)
			palloc0(sizeof(TapeScanDesc) * tapereadstate->nruns);
	}

	if (!tapereadstate->runs_scandescs[runno])
	{
		tapereadstate->runs_scandescs[runno] =
			(TapeScanDesc)palloc0(sizeof(struct TapeScanDescData));

		if (!tape_initialize_scandesc(tapereadstate, runno,
									  NULL, tablescandesc))
		{
			tapereadstate->runs_scandescs[runno]->empty = true;
			return NULL;
		}
		else
			return tapereadstate->runs_scandescs[runno];
	}
	else if (tapereadstate->runs_scandescs[runno]->empty)
		return NULL;

	return tapereadstate->runs_scandescs[runno];
}

/*
 * Create an insert state.
 *
 * Init a bulk insert state and lock the tape for insert.
 */
TapeInsertState *
tape_create_insertstate(TapeSetsState *tsstate, TransactionId xid, CommandId cid,
						BlockNumber tapeheader, int curRun, bool buildIndex)
{
	Relation relation;
	SortHeapState *shstate;
	TapeInsertState *insert;

	relation = tsstate->relation;
	shstate = tsstate->parent;
	insert = (TapeInsertState *)palloc0(sizeof(TapeInsertState));

	insert->bistate = GetBulkInsertState();
	insert->header = tapeheader;
	insert->curRun = curRun;
	insert->buildindex = buildIndex;
	insert->xid = xid;
	insert->cid = cid;
	insert->parent = tsstate;
	insert->relation = tsstate->relation;

	/*
	 * Lock the tape for write which means we disallow concurrently insertion
	 * into the same physical tape
	 */
	LockPage(insert->relation, insert->header, ExclusiveLock);

	/*
	 * Record the header buffer, we have pinned it so it will always valid
	 * until we destroy the insert state
	 */
	insert->headerbuf = ReadBuffer(insert->relation, insert->header);

	/* Initialize the structure for internal btree/brin index */
	if (buildIndex)
	{
		/* For btree */
		insert->btstate = sortheap_bt_pagestate(insert->relation, insert, 0);
		insert->indexrel = shstate->indexrel;
		insert->inskey = _bt_mkscankey(insert->indexrel, NULL);

		/* For range brin-like */
		insert->range_firstblock = InvalidBlockNumber;
		insert->range_lastblock = InvalidBlockNumber;
		insert->range_nblocks = 0;
		insert->range_minmax = (Datum *)
			palloc0(sizeof(Datum) * (2 * shstate->nkeys + 1));
		insert->range_minmax_nulls = (bool *)
			palloc0(sizeof(Datum) * (2 * shstate->nkeys + 1));

		/* For run brin-like */
		insert->run_minmax = (Datum *)
			palloc0(sizeof(Datum) * (2 * shstate->nkeys + 1));
		insert->run_minmax_nulls = (bool *)
			palloc0(sizeof(Datum) * (2 * shstate->nkeys + 1));

		/*
		 * Initialize the brin tuple descriptor, |min1|min2|...|max1|max2|
		 */
		insert->brindesc =
			sortheap_brin_builddesc(RelationGetDescr(shstate->indexrel));
	}

	/*
	 * Initialize the runsmeta entry.
	 *
	 * For Merge (except the final turn), we don't actually update the physical
	 * metadata in case the Merge is aborted.
	 */
	if (tsstate->op == SHOP_MERGE && !buildIndex)
		insert->runmetad = (TapeRunsMetaEntry *)palloc0(sizeof(TapeRunsMetaEntry));
	else
		tape_get_runmetaentry(insert, curRun);

	/* Initialize the compressed tuple descr */
	insert->columnstore_desc =
		build_columnstore_tupledesc(relation,
									shstate->indexrel,
									&insert->columnchunk_descs,
									NULL);

	if (tsstate->op == SHOP_MERGE)
	{
		insert->mergecached = (SortTuple *)
			palloc0(sizeof(SortTuple) * COMPRESS_MAX_NTUPLES_THRESHOLD);
		insert->mergecache_cur = 0;
		insert->mergecache_size = 0;

		insert->mergecachecontext =
			AllocSetContextCreate(CurrentMemoryContext,
								  "mergecache context",
								  ALLOCSET_DEFAULT_SIZES);
	}

	return insert;
}

/*
 * Create an read state.
 */
TapeReadState *
tape_create_readstate(TapeSetsState *tsstate, Snapshot snap,
					  BlockNumber header,
					  int tapenum,
					  int nruns,
					  ScanDirection dir)
{
	TapeReadState *readstate = palloc0(sizeof(TapeReadState));

	Assert(snap);

	readstate->parent = tsstate;
	readstate->header = header;
	readstate->headerbuf = ReadBuffer(tsstate->relation, readstate->header);
	readstate->dir = dir;
	readstate->tapeno = tapenum;
	readstate->nruns = nruns;
	readstate->snapshot = snap;

	return readstate;
}

void tape_destroy_insertstate(TapeInsertState *insertstate)
{
	Page page;
	XLogRecPtr recptr;

	SIMPLE_FAULT_INJECTOR("tape_insert_flush");

	/* XLOG stuff. */
	{
		/*
		 * sortheap didn't write a xlog record for each insertion, instead it
		 * only write xlog record when swithing to new insertion page and when
		 * a dump is done, this can reduce the xlog overhead.
		 *
		 * It is acceptable if a crash happens before the xlog is inserted
		 * because there is only one transaction are performing on current
		 * tape which means there are no tuples from other transactions, so
		 * the crash will not affect other transactions which need a xlog.
		 */
		if (insertstate->bistate->current_buf != InvalidBuffer)
		{
			LockBuffer(insertstate->bistate->current_buf, BUFFER_LOCK_EXCLUSIVE);
			recptr = log_full_page_update(insertstate->bistate->current_buf);
			page = BufferGetPage(insertstate->bistate->current_buf);
			PageSetLSN(page, recptr);
			LockBuffer(insertstate->bistate->current_buf, BUFFER_LOCK_UNLOCK);
		}
	}

	FreeBulkInsertState(insertstate->bistate);

	/*
	 * Finalize the run including write down the start of the run,
	 * btree root of the run and the min/max of columns in the run.
	 */
	if (insertstate->buildindex)
	{
		bool initminmax;
		SortHeapState *shstate;

		shstate = insertstate->parent->parent;
		initminmax = (insertstate->nranges == 0);

		insertstate->runmetad->btroot =
			sortheap_bt_uppershutdown(insertstate->relation,
									  insertstate,
									  insertstate->curRun);

		/* Update the min in the run level */
		if (insertstate->range_firstblock != InvalidBlockNumber)
		{
			sortheap_brin_update_minmax(RelationGetDescr(shstate->indexrel),
										shstate->nkeys, shstate->sortkeys,
										insertstate->run_minmax,
										insertstate->run_minmax_nulls,
										insertstate->range_minmax,
										insertstate->range_minmax_nulls,
										initminmax);

			/* Update the min in the run level */
			sortheap_brin_update_minmax(RelationGetDescr(shstate->indexrel),
										shstate->nkeys, shstate->sortkeys,
										insertstate->run_minmax,
										insertstate->run_minmax_nulls,
										insertstate->range_minmax +
											shstate->nkeys,
										insertstate->range_minmax_nulls +
											shstate->nkeys,
										false);

			sortheap_brin_flush_minmax(insertstate,
									   insertstate->run_minmax,
									   insertstate->run_minmax_nulls,
									   true);
		}

		if (BufferIsValid(insertstate->brinbuf))
			UnlockReleaseBuffer(insertstate->brinbuf);
	}

	if (BufferIsValid(insertstate->runsmetabuf))
	{
		LockBuffer(insertstate->runsmetabuf, BUFFER_LOCK_EXCLUSIVE);
		/* FIXME: full page update is two heavy, XLOG stuff */
		{
			recptr = log_full_page_update(insertstate->runsmetabuf);
			page = BufferGetPage(insertstate->runsmetabuf);
			PageSetLSN(page, recptr);
		}
		UnlockReleaseBuffer(insertstate->runsmetabuf);
	}

	if (insertstate->headerbuf != InvalidBuffer)
	{
		ReleaseBuffer(insertstate->headerbuf);
		insertstate->headerbuf = InvalidBuffer;
	}
	if (insertstate->currentauxbuf != InvalidBuffer)
	{
		ReleaseBuffer(insertstate->currentauxbuf);
		insertstate->currentauxbuf = InvalidBuffer;
	}
	/* Unlock the insertion in this tape */
	UnlockPage(insertstate->relation, insertstate->header, ExclusiveLock);
	pfree(insertstate);
}

void tape_destroy_readstate(TapeReadState *readstate)
{
	if (readstate->headerbuf != InvalidBuffer)
	{
		ReleaseBuffer(readstate->headerbuf);
		readstate->headerbuf = InvalidBuffer;
	}

	if (readstate->runs_scandescs)
	{
		for (int i = 0; i < readstate->nruns; i++)
			tape_free_scandesc(readstate->runs_scandescs[i]);

		pfree(readstate->runs_scandescs);
		readstate->runs_scandescs = NULL;
	}

	pfree(readstate);
}

/* ---------------------------------------------
 * Tape data management functions
 *
 * WRITE / READ
 * ---------------------------------------------
 */
ItemPointerData
tape_writetup(TapeInsertState *tapeinsertstate, HeapTuple htup)
{
	Buffer buffer;
	Relation relation = tapeinsertstate->relation;
	ItemPointerData ctid;

	buffer = tape_get_buffer_for_insert(tapeinsertstate, htup->t_len);

	START_CRIT_SECTION();

	RelationPutHeapTuple(relation, buffer, htup, false);
	ctid = htup->t_self;
	MarkBufferDirty(buffer);

	END_CRIT_SECTION();

	UnlockReleaseBuffer(buffer);

	return ctid;
}

HeapTuple
tape_readtup_pagemode(TapeScanDesc tapescandesc)
{
	Relation relation = tapescandesc->parent->parent->relation;
	ScanDirection dir = tapescandesc->parent->dir;
	HeapScanDesc scan = (HeapScanDesc)tapescandesc->hdesc;
	HeapTuple tuple = &(scan->rs_ctup);
	Snapshot snapshot = scan->rs_base.rs_snapshot;
	bool backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	bool finished;
	Page dp;
	int lines;
	int lineindex;
	OffsetNumber lineoff;
	int linesleft;
	ItemId lpp;

	Assert(scan->rs_base.rs_flags & SO_ALLOW_PAGEMODE);

	/*
	 * calculate next starting lineoff, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/* get first block number to read */
			page = ItemPointerGetBlockNumber(&tapescandesc->firsttuple);
			heapgetpage((TableScanDesc)scan, page);

			for (lineindex = 0; lineindex < scan->rs_ntuples; lineindex++)
			{
				if (scan->rs_vistuples[lineindex] ==
					ItemPointerGetOffsetNumber(&tapescandesc->firsttuple))
					break;
			}

			if (lineindex == scan->rs_ntuples)
			{
				if (BufferIsValid(scan->rs_cbuf))
					ReleaseBuffer(scan->rs_cbuf);
				scan->rs_cbuf = InvalidBuffer;
				scan->rs_cblock = InvalidBlockNumber;
				tuple->t_data = NULL;
				scan->rs_inited = false;
				return NULL;
			}

			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
			lineindex = scan->rs_cindex + 1;
		}

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, relation, dp);
		lines = scan->rs_ntuples;
		/* page and lineindex now reference the next visible tid */

		linesleft = lines - lineindex;
	}
	else
	{
		elog(ERROR, "Backward scan on sortheap is not supported");
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		while (linesleft > 0)
		{
			while (linesleft > 0)
			{
				lineoff = scan->rs_vistuples[lineindex];
				lpp = PageGetItemId(dp, lineoff);
				Assert(ItemIdIsNormal(lpp));

				tuple->t_data = (HeapTupleHeader)PageGetItem((Page)dp, lpp);
				tuple->t_len = ItemIdGetLength(lpp);
				ItemPointerSet(&(tuple->t_self), page, lineoff);

				scan->rs_cindex = lineindex;
				return tuple;
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			if (backward)
			{
				/* not implement yet */
			}
			else
				++lineindex;
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		if (backward)
		{
		}
		else
		{
			SortHeapPageOpaqueData *opaque =
				(SortHeapPageOpaqueData *)PageGetSpecialPointer(dp);

			page = opaque->next;

			finished = (page == InvalidBlockNumber || page >= scan->rs_nblocks);
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return NULL;
		}

	retry:
		heapgetpage((TableScanDesc)scan, page);
		dp = BufferGetPage(scan->rs_cbuf);

		/* Check whether the block still in the current range */
		if (tapescandesc->idesc)
		{
			SortHeapPageOpaqueData *opaque;

			opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(dp);

			Assert(opaque->type == SORTHEAP_DATAPAGE);

			if (opaque->range != tapescandesc->range)
			{
				int desired_range;
				ItemPointerData rangestart;

				/* Check whether the new range matches the scan keys */
				desired_range =
					sortheap_brin_get_next_range(relation,
												 tapescandesc,
												 &rangestart);

				if (desired_range == -1)
				{
					tuple->t_data = NULL;
					return NULL;
				}
				else if (opaque->range != desired_range)
				{
					page = ItemPointerGetBlockNumber(&rangestart);
					goto retry;
				}
			}
		}

		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lines = scan->rs_ntuples;
		linesleft = lines;
		if (backward)
			lineindex = lines - 1;
		else
			lineindex = 0;
	}
}

HeapTuple
tape_readtup(TapeScanDesc tapescandesc)
{
	Relation relation = tapescandesc->parent->parent->relation;
	ScanDirection dir = tapescandesc->parent->dir;
	HeapScanDesc scan = (HeapScanDesc)tapescandesc->hdesc;
	HeapTuple tuple = &(scan->rs_ctup);
	Snapshot snapshot = scan->rs_base.rs_snapshot;
	bool backward = ScanDirectionIsBackward(dir);
	BlockNumber page;
	bool finished;
	Page dp;
	int lines;
	OffsetNumber lineoff;
	int linesleft;
	ItemId lpp;

	/*
	 * calculate next starting lineoff, given scan direction
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (!scan->rs_inited)
		{
			/* get first block number to read */
			page = ItemPointerGetBlockNumber(&tapescandesc->firsttuple);
			heapgetpage((TableScanDesc)scan, page);
			lineoff = ItemPointerGetOffsetNumber(&tapescandesc->firsttuple);
			scan->rs_inited = true;
		}
		else
		{
			/* continue from previously returned page/tuple */
			page = scan->rs_cblock; /* current page */
			lineoff =				/* next offnum */
				OffsetNumberNext(ItemPointerGetOffsetNumber(&(tuple->t_self)));
		}

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = BufferGetPage(scan->rs_cbuf);
		TestForOldSnapshot(snapshot, relation, dp);
		lines = PageGetMaxOffsetNumber(dp);
		/* page and lineindex now reference the next visible tid */

		linesleft = lines - lineoff + 1;
	}
	else
	{
		elog(ERROR, "Backward scan on sortheap is not supported");
	}

	/*
	 * advance the scan until we find a qualifying tuple or run out of stuff
	 * to scan
	 */
	lpp = PageGetItemId(dp, lineoff);
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		while (linesleft > 0)
		{
			if (ItemIdIsNormal(lpp))
			{
				bool valid;

				tuple->t_data = (HeapTupleHeader)PageGetItem((Page)dp, lpp);
				tuple->t_len = ItemIdGetLength(lpp);
				ItemPointerSet(&(tuple->t_self), page, lineoff);

				/*
				 * if current tuple qualifies, return it.
				 */
				valid = HeapTupleSatisfiesVisibility(scan->rs_base.rs_rd,
													 tuple,
													 snapshot,
													 scan->rs_cbuf);

				CheckForSerializableConflictOut(valid, scan->rs_base.rs_rd,
												tuple, scan->rs_cbuf,
												snapshot);

				if (valid)
				{
					LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);
					return tuple;
				}
			}

			/*
			 * otherwise move to the next item on the page
			 */
			--linesleft;
			if (backward)
			{
				/* not implement yet */
			}
			else
			{
				++lpp; /* move forward in this page's ItemId array */
				++lineoff;
			}

			if (ItemPointerIsValid(&tapescandesc->lasttuple) &&
				ItemPointerGetBlockNumber(&tapescandesc->lasttuple) == scan->rs_cblock &&
				ItemPointerGetOffsetNumber(&tapescandesc->lasttuple) == lineoff)
			{
				if (BufferIsValid(scan->rs_cbuf))
					UnlockReleaseBuffer(scan->rs_cbuf);
				scan->rs_cbuf = InvalidBuffer;
				scan->rs_cblock = InvalidBlockNumber;
				tuple->t_data = NULL;
				scan->rs_inited = false;
				return NULL;
			}
		}

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

		/*
		 * if we get here, it means we've exhausted the items on this page and
		 * it's time to move to the next.
		 */
		if (backward)
		{
		}
		else
		{
			SortHeapPageOpaqueData *opaque =
				(SortHeapPageOpaqueData *)PageGetSpecialPointer(dp);

			page = opaque->next;

			finished = (page == InvalidBlockNumber || page >= scan->rs_nblocks);
		}

		/*
		 * return NULL if we've exhausted all the pages
		 */
		if (finished)
		{
			if (BufferIsValid(scan->rs_cbuf))
				ReleaseBuffer(scan->rs_cbuf);
			scan->rs_cbuf = InvalidBuffer;
			scan->rs_cblock = InvalidBlockNumber;
			tuple->t_data = NULL;
			scan->rs_inited = false;
			return NULL;
		}

	retry:
		heapgetpage((TableScanDesc)scan, page);

		LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

		dp = BufferGetPage(scan->rs_cbuf);

		/* Check whether the block still in the current range */
		if (tapescandesc->idesc)
		{
			SortHeapPageOpaqueData *opaque;

			opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(dp);

			Assert(opaque->type == SORTHEAP_DATAPAGE);

			if (opaque->range != tapescandesc->range)
			{
				int desired_range;
				ItemPointerData rangestart;

				/* Check whether the new range matches the scan keys */
				desired_range =
					sortheap_brin_get_next_range(relation,
												 tapescandesc,
												 &rangestart);

				if (desired_range == -1)
				{
					tuple->t_data = NULL;
					return NULL;
				}
				else if (opaque->range != desired_range)
				{
					page = ItemPointerGetBlockNumber(&rangestart);
					goto retry;
				}
			}
		}

		TestForOldSnapshot(snapshot, scan->rs_base.rs_rd, dp);
		lines = PageGetMaxOffsetNumber((Page)dp);
		linesleft = lines;
		if (backward)
		{
			/* not implement yet */
		}
		else
		{
			lineoff = FirstOffsetNumber;
			lpp = PageGetItemId(dp, FirstOffsetNumber);
		}
	}
}

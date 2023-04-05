/*----------------------------------------------------------------------------
 *
 * sortheap_freepages.c
 * 		Generic routines to manage freepages in sort heap
 *
 * We use one bit to identify whether a BLOCK is free or not.
 *
 * There are two level:
 *
 * FreePageLeaf level: record the real bitmap for blocks
 * FreePageRoot level: record the block number of the leaf level
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_freepages.c
 *
 *----------------------------------------------------------------------------
 */
#include "sortheap.h"
#include "sortheap_common.h"
#include "sortheap_freepages.h"
#include "sortheap_tapesets.h"

/*
 * -------------------------------------------------
 *  XLOG related functions
 *  -----------------------------------------------
 */

static XLogRecPtr
log_freepages_allocate(Buffer metabuf, long nBlocksAllocated,
					   Buffer leafbuf, Index mapidx, uint64 bit)
{
	XLogBeginInsert();

	XLogRegisterData((char *)&mapidx, sizeof(Index));
	XLogRegisterData((char *)&bit, sizeof(uint64));
	XLogRegisterData((char *)&nBlocksAllocated, sizeof(long));
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
	XLogRegisterBuffer(1, leafbuf, REGBUF_STANDARD);
	return XLogInsert(RM_SORTHEAP2_ID, XLOG_SORTHEAP_FREEPAGE_ALLOC);
}

static XLogRecPtr
log_freepages_recycle(Buffer leafbuf, Index mapidx, uint64 bit)
{
	XLogBeginInsert();

	XLogRegisterData((char *)&mapidx, sizeof(Index));
	XLogRegisterData((char *)&bit, sizeof(uint64));
	XLogRegisterBuffer(0, leafbuf, REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP2_ID, XLOG_SORTHEAP_FREEPAGE_RECYC);
}

static XLogRecPtr
log_freepages_newroot(Buffer metabuf, long nBlocksAllocated,
					  Buffer rootbuf, int rootindex)
{
	XLogBeginInsert();
	XLogRegisterData((char *)&rootindex, sizeof(int));
	XLogRegisterData((char *)&nBlocksAllocated, sizeof(long));
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
	XLogRegisterBuffer(1, rootbuf, REGBUF_WILL_INIT);

	return XLogInsert(RM_SORTHEAP2_ID, XLOG_SORTHEAP_FREEPAGE_NEWROOT);
}

static XLogRecPtr
log_freespace_newleaf(Buffer metabuf, long nBlocksAllocated,
					  Buffer rootbuf, int rootIndex,
					  int numLeaves, Buffer leafbuf)
{
	XLogBeginInsert();
	XLogRegisterData((char *)&rootIndex, sizeof(int));
	XLogRegisterData((char *)&numLeaves, sizeof(int));
	XLogRegisterData((char *)&nBlocksAllocated, sizeof(long));

	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
	XLogRegisterBuffer(1, rootbuf, REGBUF_STANDARD);
	XLogRegisterBuffer(2, leafbuf, REGBUF_WILL_INIT);

	return XLogInsert(RM_SORTHEAP2_ID, XLOG_SORTHEAP_FREEPAGE_NEWLEAF);
}

/*
 * -------------------------------------------------
 *  functions to manage free pages
 *  -----------------------------------------------
 */

/*
 * The free BlockNumber is returned, the caller need to read the according buffer.
 *
 * the caller should have acquired proper Exclusive lock, so there is only one
 * request is acting.
 */
BlockNumber
sortheap_get_freeblock(SortHeapState *shstate)
{
	SortHeapMeta *metad;
	Page metapage;
	Buffer rootbuffer;
	Page rootpage;
	BlockNumber rootblock;
	Buffer prev_rootbuffer = InvalidBuffer;
	int prev_rootindex = -1;
	Buffer leafbuffer;
	Page leafpage;
	BlockNumber leafblock;
	FreePageRoot *root;
	FreePageLeaf *leaf;
	FreePageLeafRef *leafref;
	SortHeapPageOpaqueData *rootopaque;
	int i,
		j,
		start,
		last;

	metapage = BufferGetPage(shstate->metabuf);
	metad = (SortHeapMeta *)PageGetContents(metapage);

	rootblock = metad->firstFreePagesRoot;

	while (true)
	{
		if (rootblock == InvalidBlockNumber)
		{
			/* sanity check */
			if (metad->nBlocksAllocated != 1 &&
				metad->nBlocksAllocated % NUMBLOCKS_PERROOT != 0)
				elog(PANIC, "root blkno %ld is invalid", metad->nBlocksAllocated);

			sortheap_extend(shstate, metad->nBlocksAllocated);
			rootbuffer = ReadBuffer(shstate->relation, metad->nBlocksAllocated++);
			rootblock = BufferGetBlockNumber(rootbuffer);

			if (prev_rootbuffer != InvalidBuffer)
			{
				rootpage = BufferGetPage(prev_rootbuffer);
				rootopaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(rootpage);
				rootopaque->next = rootblock;
				MarkBufferDirty(prev_rootbuffer);
				UnlockReleaseBuffer(prev_rootbuffer);
			}
			else
				metad->firstFreePagesRoot = rootblock;

			MarkBufferDirty(shstate->metabuf);

			LockBuffer(rootbuffer, BUFFER_LOCK_EXCLUSIVE);
			rootpage = BufferGetPage(rootbuffer);
			PageInit(rootpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
			root = (FreePageRoot *)PageGetContents(rootpage);
			root->hasFreePages = true;
			root->numLeaves = 0;
			root->rootIndex = prev_rootindex + 1;
			rootopaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(rootpage);
			rootopaque->next = InvalidBlockNumber;
			rootopaque->type = SORTHEAP_FREESPACEPAGE;

			/*
			 * Set pd_lower just past the end of the metadata.  This is
			 * essential, because without doing so, metadata will be lost if
			 * xlog.c compresses the page.
			 */
			((PageHeader)rootpage)->pd_lower =
				((char *)root + sizeof(FreePageRoot) +
				 sizeof(FreePageLeafRef) * (NUMLEAVES_PERROOT - 1)) -
				(char *)rootpage;
			MarkBufferDirty(rootbuffer);

			SIMPLE_FAULT_INJECTOR("newroot_before_xlog");

			/* XLOG stuff */
			{
				XLogRecPtr recptr;

				recptr = log_freepages_newroot(shstate->metabuf,
											   metad->nBlocksAllocated,
											   rootbuffer,
											   prev_rootindex + 1);
				PageSetLSN(metapage, recptr);
				PageSetLSN(rootpage, recptr);
			}

			UnlockReleaseBuffer(rootbuffer);

			SIMPLE_FAULT_INJECTOR("newroot_after_xlog");
		}

		rootbuffer = ReadBuffer(shstate->relation, rootblock);
		LockBuffer(rootbuffer, BUFFER_LOCK_EXCLUSIVE);
		rootpage = BufferGetPage(rootbuffer);
		root = (FreePageRoot *)PageGetContents(rootpage);
		rootopaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(rootpage);

		last = 0;
		while (true)
		{
			/* find any free pages in existing leaves */
			for (i = last; i < root->numLeaves && root->hasFreePages; i++)
			{
				BlockNumber result = InvalidBlockNumber;

				leafref = &root->leaves[i];
				Assert(leafref->blkno != 0);
				if (!leafref->hasFreePages)
					continue;

				/* search maps in the leaf page for free pages */
				leafbuffer = ReadBuffer(shstate->relation, leafref->blkno);
				LockBuffer(leafbuffer, BUFFER_LOCK_EXCLUSIVE);
				leafpage = BufferGetPage(leafbuffer);
				leaf = (FreePageLeaf *)PageGetContents(leafpage);

				/* first identify the range through fastBitmap */
				if ((start = ffsll(leaf->fastBitmap)) == 0)
				{
					UnlockReleaseBuffer(leafbuffer);
					continue;
				}

				for (j = (start - 1) * NUMMAPS_PERFASTBIT; j < NUMMAPS_PERLEAF; j++)
				{
					uint64 index;

					/*
					 * find a free page in this map, calculate the block
					 * number
					 */
					if ((index = ffsll(leaf->maps[j])) == 0)
						continue;

					result = leaf->leafIndex * NUMBLOCKS_PERLEAF +
							 j * NUMBLOCKS_PERMAP +
							 index - 1;

					/* Mark the page as busy */
					leaf->maps[j] = (~((uint64)1 << (index - 1))) & leaf->maps[j];

					/* Mark the fastBitmap if necessary */
					if (leaf->maps[j] == 0 &&
						j % NUMMAPS_PERFASTBIT == NUMMAPS_PERFASTBIT - 1)
						leaf->fastBitmap =
							(~(((uint64)1) << j / NUMMAPS_PERFASTBIT)) & leaf->fastBitmap;
					MarkBufferDirty(leafbuffer);

					if (result >= metad->nBlocksAllocated)
						metad->nBlocksAllocated = result + 1;
					MarkBufferDirty(shstate->metabuf);

					SIMPLE_FAULT_INJECTOR("freepage_alloc_before_xlog");

					/* XLOG stuff */
					{
						XLogRecPtr recptr;

						recptr = log_freepages_allocate(shstate->metabuf,
														metad->nBlocksAllocated,
														leafbuffer,
														j,
														index);
						PageSetLSN(leafpage, recptr);
					}

					UnlockReleaseBuffer(rootbuffer);
					UnlockReleaseBuffer(leafbuffer);

					SIMPLE_FAULT_INJECTOR("freepage_alloc_after_xlog");

					/* sanity check */
					if (result - metad->nBlocksWritten >
						MAX_PREALLOC_SIZE * MAX_TAPESETS * MAX_TAPES)
						elog(PANIC, "unexpected blocknum: target %d, nBlocksWritten %ld", result, metad->nBlocksWritten);

					if (result == SORTHEAP_METABLOCK)
						elog(PANIC, "unexpected blocknum: %d is not expected", result);

					return result;
				}

				leafref->hasFreePages = false;
				MarkBufferDirty(rootbuffer);
				UnlockReleaseBuffer(leafbuffer);
			}

			last = i;

			/* check whether current root can contain one more leaf */
			if (root->numLeaves < NUMLEAVES_PERROOT)
			{
				sortheap_extend(shstate, metad->nBlocksAllocated);
				leafbuffer = ReadBuffer(shstate->relation, metad->nBlocksAllocated++);
				MarkBufferDirty(shstate->metabuf);

				/* initialize new leaf buffer */
				LockBuffer(leafbuffer, BUFFER_LOCK_EXCLUSIVE);
				leafpage = BufferGetPage(leafbuffer);
				leafblock = BufferGetBlockNumber(leafbuffer);
				PageInit(leafpage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
				leaf = (FreePageLeaf *)PageGetContents(leafpage);
				leaf->fastBitmap = ~(uint64)0;
				leaf->leafIndex = root->rootIndex * NUMLEAVES_PERROOT + root->numLeaves;
				for (i = 0; i < NUMMAPS_PERLEAF; i++)
					leaf->maps[i] = ~(uint64)0;

				/*
				 * For the first leaf, we need to mark the block 0 and block 1
				 * and block 2 as busy, block 0 is the metapage of sort heap,
				 * block 1 is always the first freepage root, block 2 is the
				 * first freepage leaf.
				 *
				 * For other leafs, mark the block 0 as valid because it is
				 * the leaf block.
				 */
				if (root->numLeaves == 0 && leaf->leafIndex == 0)
					leaf->maps[0] = leaf->maps[0] & (~(uint64)0b0111);
				else if (root->numLeaves == 0)
					leaf->maps[0] = leaf->maps[0] & (~(uint64)0b0011);
				else
					leaf->maps[0] = leaf->maps[0] & (~(uint64)0b0001);

				/*
				 * Set pd_lower just past the end of the metadata.  This is
				 * essential, because without doing so, metadata will be lost
				 * if xlog.c compresses the page.
				 */
				((PageHeader)leafpage)->pd_lower =
					((char *)leaf + sizeof(FreePageLeaf) +
					 sizeof(uint64) * (NUMMAPS_PERLEAF - 1)) -
					(char *)leafpage;
				MarkBufferDirty(leafbuffer);

				root->leaves[root->numLeaves].hasFreePages = true;
				root->leaves[root->numLeaves].blkno = leafblock;
				root->numLeaves++;
				MarkBufferDirty(rootbuffer);

				SIMPLE_FAULT_INJECTOR("newleaf_before_xlog");

				/* XLOG stuff */
				{
					XLogRecPtr recptr;

					recptr = log_freespace_newleaf(shstate->metabuf,
												   metad->nBlocksAllocated,
												   rootbuffer,
												   root->rootIndex,
												   root->numLeaves - 1,
												   leafbuffer);
					PageSetLSN(leafpage, recptr);
					PageSetLSN(rootpage, recptr);
				}

				SIMPLE_FAULT_INJECTOR("newleaf_after_xlog");

				UnlockReleaseBuffer(leafbuffer);
			}
			else
			{
				/* Advance to next root page */
				prev_rootbuffer = rootbuffer;
				prev_rootindex = root->rootIndex;
				rootblock = rootopaque->next;
				root->hasFreePages = false;
				MarkBufferDirty(rootbuffer);

				/*
				 * Unlock and release current root buffer if next root page
				 * is valid.
				 */
				if (rootblock != InvalidBlockNumber)
					UnlockReleaseBuffer(rootbuffer);

				break;
			}
		}
	}
}

void sortheap_recycle_block(Relation relation, BlockNumber blkno)
{
	SortHeapState *state = (SortHeapState *)relation->rd_amcache;
	SortHeapMeta *metad;
	Buffer rootbuffer = InvalidBuffer;
	Buffer leafbuffer;
	Page metapage;
	Page rootpage;
	Page leafpage;
	BlockNumber rootblock;
	BlockNumber leafblock;
	FreePageRoot *root = NULL;
	FreePageLeaf *leaf;
	FreePageLeafRef *leafref;
	Index rootidx;
	Index leafidx;
	Index mapidx;
	Index left;
	uint64 bit;

	Assert(blkno != 0);

	rootidx = blkno / NUMBLOCKS_PERROOT;
	left = (blkno % NUMBLOCKS_PERROOT);
	leafidx = left / NUMBLOCKS_PERLEAF;
	left = left % NUMBLOCKS_PERLEAF;
	mapidx = left / NUMBLOCKS_PERMAP;
	bit = left % NUMBLOCKS_PERMAP;

	metapage = BufferGetPage(state->metabuf);
	metad = (SortHeapMeta *)PageGetContents(metapage);

	rootblock = metad->firstFreePagesRoot;
	/* Read the page contain rootidx */
	while (true)
	{
		if (rootblock == InvalidBlockNumber)
			break;

		rootbuffer = ReadBuffer(relation, rootblock);
		LockBuffer(rootbuffer, BUFFER_LOCK_EXCLUSIVE);
		rootpage = BufferGetPage(rootbuffer);
		root = (FreePageRoot *)PageGetContents(rootpage);
		if (root->rootIndex != rootidx)
		{
			SortHeapPageOpaqueData *rootopaque;

			rootopaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(rootpage);

			rootblock = rootopaque->next;

			UnlockReleaseBuffer(rootbuffer);
			continue;
		}
		else
			break;
	}

	Assert(rootblock);

	leafref = &root->leaves[leafidx];
	leafblock = leafref->blkno;
	leafbuffer = ReadBuffer(relation, leafblock);
	LockBuffer(leafbuffer, BUFFER_LOCK_EXCLUSIVE);
	leafpage = BufferGetPage(leafbuffer);
	leaf = (FreePageLeaf *)PageGetContents(leafpage);
	leaf->maps[mapidx] |= ((uint64)1 << bit);
	leaf->fastBitmap |= ((uint64)1 << mapidx);

	if (leafref->hasFreePages == false)
	{
		leafref->hasFreePages = true;
		root->hasFreePages = true;
		MarkBufferDirty(rootbuffer);
	}

	SIMPLE_FAULT_INJECTOR("freepage_recycle");

	/* XLOG stuff */
	log_freepages_recycle(leafbuffer, mapidx, bit);

	MarkBufferDirty(leafbuffer);
	if (leafbuffer != InvalidBuffer)
		UnlockReleaseBuffer(leafbuffer);
	if (rootbuffer != InvalidBuffer)
		UnlockReleaseBuffer(rootbuffer);
}

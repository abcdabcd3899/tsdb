/*----------------------------------------------------------------------------
 *
 * sortheap_tapesets.c
 *		Generalized codes to keep heap sorting and continuously merged.
 *
 * TapeSets is designed to implement Knuth's Algorithm 5.4.2D, it is a set of
 * logical tapes which is composed of heap pages, heap pages are linked in the
 * order.
 *
 * This is another version of LogicalTapeSet (in logtape.c) except that:
 * 1) It is a persistent storage.
 * 2) It supports MVCC.
 * 3) It support compression/column-oriented store.
 * 4) It builds internal btree index to accelerate queries.
 * 5) It builds internal brin index to accelerate queries.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_tapesets.c
 *
 *----------------------------------------------------------------------------
 */
#include "sortheap_common.h"
#include "sortheap.h"
#include "sortheap_tape.h"
#include "sortheap_tapesets.h"

/*
 * -------------------------------------------------
 *  XLOG related functions
 *  -----------------------------------------------
 */

static XLogRecPtr
log_new_tapesets(Buffer metabuf, Index tsno, Buffer tapesetsbuf)
{
	XLogBeginInsert();

	XLogRegisterData((char *)&tsno, sizeof(Index));
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);
	XLogRegisterBuffer(1, tapesetsbuf, REGBUF_WILL_INIT);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_NEWTAPESORT);
}

/*
 * --------------------------------------------------------------
 * Helper functions
 * --------------------------------------------------------------
 */

/*
 * Grow the memtuples[] array, if possible within our memory constraint.  We
 * must not exceed INT_MAX tuples in memory or the caller-provided memory
 * limit.  Return true if we were able to enlarge the array, false if not.
 *
 * Normally, at each increment we double the size of the array.  When doing
 * that would exceed a limit, we attempt one last, smaller increase (and then
 * clear the growmemtuples flag so we don't try any more).  That allows us to
 * use memory as fully as permitted; sticking to the pure doubling rule could
 * result in almost half going unused.  Because availMem moves around with
 * tuple addition/removal, we need some rule to prevent making repeated small
 * increases in memtupsize, which would just be useless thrashing.  The
 * growmemtuples flag accomplishes that and also prevents useless
 * recalculations in this function.
 */
static bool
grow_memtuples(TapeSetsState *tsstate)
{
	int newmemtupsize;
	int memtupsize = tsstate->memtupsize;
	int64 memNowUsed = tsstate->allowedMem - tsstate->availMem;

	/* Forget it if we've already maxed out memtuples, per comment above */
	if (!tsstate->growmemtuples)
		return false;

	/* Select new value of memtupsize */
	if (memNowUsed <= tsstate->availMem)
	{
		/*
		 * We've used no more than half of allowedMem; double our usage,
		 * clamping at INT_MAX tuples.
		 */
		if (memtupsize < INT_MAX / 2)
			newmemtupsize = memtupsize * 2;
		else
		{
			newmemtupsize = INT_MAX;
			tsstate->growmemtuples = false;
		}
	}
	else
	{
		/*
		 * This will be the last increment of memtupsize.  Abandon doubling
		 * strategy and instead increase as much as we safely can.
		 *
		 * To stay within allowedMem, we can't increase memtupsize by more
		 * than availMem / sizeof(SortTuple) elements.  In practice, we want
		 * to increase it by considerably less, because we need to leave some
		 * space for the tuples to which the new array slots will refer.  We
		 * assume the new tuples will be about the same size as the tuples
		 * we've already seen, and thus we can extrapolate from the space
		 * consumption so far to estimate an appropriate new size for the
		 * memtuples array.  The optimal value might be higher or lower than
		 * this estimate, but it's hard to know that in advance.  We again
		 * clamp at INT_MAX tuples.
		 *
		 * This calculation is safe against enlarging the array so much that
		 * LACKMEM becomes true, because the memory currently used includes
		 * the present array; thus, there would be enough allowedMem for the
		 * new array elements even if no other memory were currently used.
		 *
		 * We do the arithmetic in float8, because otherwise the product of
		 * memtupsize and allowedMem could overflow.  Any inaccuracy in the
		 * result should be insignificant; but even if we computed a
		 * completely insane result, the checks below will prevent anything
		 * really bad from happening.
		 */
		double grow_ratio;

		grow_ratio = (double)tsstate->allowedMem / (double)memNowUsed;
		if (memtupsize * grow_ratio < INT_MAX)
			newmemtupsize = (int)(memtupsize * grow_ratio);
		else
			newmemtupsize = INT_MAX;

		/* We won't make any further enlargement attempts */
		tsstate->growmemtuples = false;
	}

	/* Must enlarge array by at least one element, else report failure */
	if (newmemtupsize <= memtupsize)
		goto noalloc;

	/*
	 * On a 32-bit machine, allowedMem could exceed MaxAllocHugeSize.  Clamp
	 * to ensure our request won't be rejected.  Note that we can easily
	 * exhaust address space before facing this outcome.  (This is presently
	 * impossible due to guc.c's MAX_KILOBYTES limitation on work_mem, but
	 * don't rely on that at this distance.)
	 */
	if ((Size)newmemtupsize >= MaxAllocHugeSize / sizeof(SortTuple))
	{
		newmemtupsize = (int)(MaxAllocHugeSize / sizeof(SortTuple));
		tsstate->growmemtuples = false; /* can't grow any more */
	}

	/*
	 * We need to be sure that we do not cause LACKMEM to become true, else
	 * the space management algorithm will go nuts.  The code above should
	 * never generate a dangerous request, but to be safe, check explicitly
	 * that the array growth fits within availMem.  (We could still cause
	 * LACKMEM if the memory chunk overhead associated with the memtuples
	 * array were to increase.  That shouldn't happen because we chose the
	 * initial array size large enough to ensure that palloc will be treating
	 * both old and new arrays as separate chunks.  But we'll check LACKMEM
	 * explicitly below just in case.)
	 */
	if (tsstate->availMem < (int64)((newmemtupsize - memtupsize) * sizeof(SortTuple)))
		goto noalloc;

	/* OK, do it */
	tsstate->availMem += GetMemoryChunkSpace(tsstate->memtuples);
	tsstate->memtupsize = newmemtupsize;
	tsstate->memtuples = (SortTuple *)
		repalloc_huge(tsstate->memtuples,
					  tsstate->memtupsize * sizeof(SortTuple));

	tsstate->availMem -= GetMemoryChunkSpace(tsstate->memtuples);
	if (tsstate->availMem < 0)
		elog(ERROR, "unexpected out-of-memory situation in sort heap");
	return true;

noalloc:
	/* If for any reason we didn't realloc, shut off future attempts */
	tsstate->growmemtuples = false;
	return false;
}

/*
 * --------------------------------------------------------------
 * Routines for the create/destroy TapeSetsState
 * --------------------------------------------------------------
 */

/*
 * Create a tape sort state, it captures a snapshot of the meta page of
 * tapesets and initialize readstates for the SELECT & MERGE
 */
void tapesets_initialize_state(TapeSetsState *tsstate)
{
	SortHeapState *shstate;
	SortHeapMeta *shmetad;
	TapeSetsMeta *tsmetad;
	BlockNumber tsblkno;
	Page shpage;
	Page tspage;
	Index tsno = tsstate->tapesetsno;
	Relation relation = tsstate->relation;

	/* initialize the tapesets state */
	tsstate->status_forread = TS_UNKNOWN;
	tsstate->status_forwrite = TS_UNKNOWN;
	tsstate->cur_readrun = 0;

	/*
	 * workMem is forced to be at least 64KB, the current minimum valid value
	 * for the work_mem GUC.  This is a defense against parallel sort callers
	 * that divide out memory among many workers in a way that leaves each
	 * with very little memory.
	 */
	tsstate->availMem = tsstate->allowedMem;
	tsstate->memtupcount = 0;

	/*
	 * Only preallocate memory for INSERT. For SELECT & MERGE, memtuples are
	 * use to serve a min heap
	 */
	if (tsstate->op == SHOP_INSERT)
	{
		/*
		 * Initial size of array must be more than
		 * ALLOCSET_SEPARATE_THRESHOLD; see comments in grow_memtuples().
		 */
		tsstate->memtupsize = Max(1024,
								  ALLOCSET_SEPARATE_THRESHOLD / sizeof(SortTuple) + 1);

		tsstate->growmemtuples = true;
		tsstate->memtuples = (SortTuple *)palloc(tsstate->memtupsize * sizeof(SortTuple));

		tsstate->availMem -= GetMemoryChunkSpace(tsstate->memtuples);
	}

	/* workMem must be large enough for the minimal memtuples array */
	if (tsstate->availMem < 0)
		elog(ERROR, "insufficient memory allowed for sort");

	/* Initialize contents from the disk */
	shstate = tsstate->parent;

	/* Get the block number that store the metadata of the tapesets */
	LockBuffer(shstate->metabuf, BUFFER_LOCK_SHARE);
	shpage = BufferGetPage(shstate->metabuf);
	shmetad = (SortHeapMeta *)PageGetContents(shpage);

	/* Allocate a block if it is not assigned */
	if (shmetad->tapesets[tsno] == InvalidBlockNumber)
	{
		LockBuffer(shstate->metabuf, BUFFER_LOCK_UNLOCK);
		/* upgrade the lock level to allocate a new tapesets */
		LockBuffer(shstate->metabuf, BUFFER_LOCK_EXCLUSIVE);

		/* Race cond check, in case someone already allocated one */
		if (shmetad->tapesets[tsno] != InvalidBlockNumber)
		{
			/* do nothing */
		}
		else
		{
			Buffer tsbuf;

			/* Allocate a new block */
			tsblkno = sortheap_get_freeblock(shstate);
			shmetad->tapesets[tsno] = tsblkno;
			sortheap_extend(shstate, tsblkno);
			MarkBufferDirty(shstate->metabuf);

			/* Read the block and initialize it */
			tsbuf = ReadBuffer(relation, tsblkno);
			LockBuffer(tsbuf, BUFFER_LOCK_EXCLUSIVE);
			tspage = BufferGetPage(tsbuf);

			PageInit(tspage, BLCKSZ, sizeof(SortHeapPageOpaqueData));
			tsmetad = (TapeSetsMeta *)PageGetContents(tspage);
			tsmetad->currentRun = 0;
			tsmetad->level = 0;
			tsmetad->destTape = 0;
			tsmetad->maxTapes = 0;
			tsmetad->tapeRange = 0;
			tsmetad->lastMergeXid = InvalidTransactionId;
			tsmetad->lastInsertXid = InvalidTransactionId;
			tsmetad->status = TS_INITIAL;

			SIMPLE_FAULT_INJECTOR("new_tapesets_before_xlog");

			/*
			 * Set pd_lower just past the end of the metadata.  This is
			 * essential, because without doing so, metadata will be lost if
			 * xlog.c compresses the page.
			 */
			((PageHeader)tspage)->pd_lower =
				((char *)tsmetad + sizeof(TapeSetsMeta)) - (char *)tspage;
			MarkBufferDirty(tsbuf);

			/* XLOG stuff */
			{
				XLogRecPtr recptr;

				recptr = log_new_tapesets(shstate->metabuf, tsno, tsbuf);
				PageSetLSN(tspage, recptr);
				PageSetLSN(shpage, recptr);
			}

			UnlockReleaseBuffer(tsbuf);

			SIMPLE_FAULT_INJECTOR("new_tapesets_after_xlog");
		}
	}
	tsblkno = shmetad->tapesets[tsno];
	LockBuffer(shstate->metabuf, BUFFER_LOCK_UNLOCK);

	tsstate->metablk = tsblkno;
	tsstate->metabuf = ReadBuffer(relation, tsblkno);

	/* Lock the buffer and capture a snapshot of the meta data */
	LockBuffer(tsstate->metabuf, BUFFER_LOCK_SHARE);
	tspage = BufferGetPage(tsstate->metabuf);
	tsmetad = (TapeSetsMeta *)PageGetContents(tspage);
	memcpy(&tsstate->metad_snap, tsmetad, sizeof(TapeSetsMeta));
	/* Unlock the buffer but don't release the buffer */
	LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);

	tsmetad = tapesets_getmeta_snapshot(tsstate);

	/*
	 * If the tuples are already sorted, set the first tape to the resultTape.
	 *
	 * CAUTION: use tapesets_getstatus_forread to get the real logical status
	 */
	if (tapesets_getstatus_forread(tsstate) >= TS_MERGE_SORTED)
	{
		tsstate->cur_readtape = tsstate->metad_snap.resultTape;
		tsstate->end_readtape = tsstate->metad_snap.resultTape + 1;
	}
	else
	{
		tsstate->cur_readtape = 0;
		tsstate->end_readtape = tsmetad->tapeRange;
	}

	/* Initialize the readstates for SELECT */
	if (tapesets_getstatus_forread(tsstate) != TS_INITIAL &&
		tsstate->op != SHOP_INSERT)
	{

		tsstate->readstates = (TapeReadState **)
			palloc0(tsmetad->maxTapes * sizeof(TapeReadState *));

		for (int tapenum = 0; tapenum < tsmetad->maxTapes; tapenum++)
		{
			if (tsmetad->tapes[tapenum].header == InvalidBlockNumber)
				continue;

			tsstate->readstates[tapenum] =
				tape_create_readstate(tsstate, tsstate->snapshot,
									  tsmetad->tapes[tapenum].header,
									  tapenum,
									  tsmetad->tapes[tapenum].runs,
									  ForwardScanDirection);
		}
	}
}

void tapesets_destroy_state(TapeSetsState *tsstate)
{
	if (!tsstate)
		return;

	if (tsstate->readstates)
	{
		TapeSetsMeta *tsmetad = tapesets_getmeta_snapshot(tsstate);
		int i;

		for (i = 0; i < tsmetad->maxTapes; i++)
		{
			if (tsstate->readstates[i])
			{
				tape_destroy_readstate(tsstate->readstates[i]);
				tsstate->readstates[i] = NULL;
			}
		}
		tsstate->readstates = NULL;
	}

	if (tsstate->metabuf != InvalidBuffer)
	{
		ReleaseBuffer(tsstate->metabuf);
		tsstate->metabuf = InvalidBuffer;
	}
}

/*
 * -------------------------------------
 * Help functions to access TapeSets
 * -------------------------------------
 */
char *
tapesets_statusstr(TapeSetsStatus status)
{
	char *ret;

	switch (status)
	{
	case TS_INITIAL:
		ret = "initial";
		break;
	case TS_BUILDRUNS:
		ret = "building runs";
		break;
	case TS_MERGE_SORTED:
		ret = "merged but not vacuumed";
		break;
	case TS_MERGE_VACUUMED:
		ret = "merged and vacuumed";
		break;
	default:
		ret = "unknown";
		break;
	}

	return ret;
}

/*
 * Get the status of from tapesets snapshot for read.
 *
 * The special case is TS_MERGE_SORTED, we need to check whether the merge
 * transaction is visible to my snapshot.
 */
TapeSetsStatus
tapesets_getstatus_forread(TapeSetsState *tsstate)
{
	if (tsstate->status_forread == TS_UNKNOWN)
	{
		if (tsstate->metad_snap.status == TS_MERGE_SORTED)
		{
			XidInMVCCSnapshotCheckResult result;
			bool setDistributedSnapshotIgnore = false;

			if (!TransactionIdDidCommit(tsstate->metad_snap.lastMergeXid))
			{
				tsstate->status_forread = TS_BUILDRUNS;
			}
			else
			{
				result = XidInMVCCSnapshot(tsstate->metad_snap.lastMergeXid,
										   GetActiveSnapshot(),
										   true,
										   &setDistributedSnapshotIgnore);

				if (result == XID_SURELY_COMMITTED || result == XID_NOT_IN_SNAPSHOT)
					tsstate->status_forread = TS_MERGE_SORTED;
				else
					tsstate->status_forread = TS_BUILDRUNS;
			}
		}
		else
			tsstate->status_forread = tsstate->metad_snap.status;
	}

	return tsstate->status_forread;
}

/*
 * Get the status of from tapesets disk metadata for write.
 */
TapeSetsStatus
tapesets_getstatus_forwrite(TapeSetsState *tsstate, bool refresh)
{
	TapeSetsMeta *tsmetad;

	if (tsstate->status_forwrite == TS_UNKNOWN || refresh)
	{
		LockBuffer(tsstate->metabuf, BUFFER_LOCK_SHARE);
		tsmetad = tapesets_getmeta_disk(tsstate);

		if (tsmetad->status == TS_MERGE_SORTED &&
			!TransactionIdDidCommit(tsmetad->lastMergeXid) &&
			!TransactionIdIsCurrentTransactionId(tsmetad->lastMergeXid))
			tsstate->status_forwrite = TS_BUILDRUNS;
		else if (tsmetad->status == TS_MERGE_VACUUMED &&
				 !TransactionIdIsCurrentTransactionId(tsmetad->lastVacuumXid) &&
				 !TransactionIdDidCommit(tsmetad->lastVacuumXid))
			tsstate->status_forwrite = TS_MERGE_SORTED;
		else
			tsstate->status_forwrite = tsmetad->status;
		LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);
	}

	return tsstate->status_forwrite;
}

/*
 * Used by merge to check whether the last INSERT transaction is visable to
 * current transaction snapshot.
 *
 * This must be called when we hold the exclusive merge lock on the tapsort.
 */
bool tapesets_merge_validate_xid(TransactionId xid)
{
	XidInMVCCSnapshotCheckResult result;
	bool setDistributedSnapshotIgnore = false;
	Snapshot snapshot = GetTransactionSnapshot();

	if (TransactionIdFollows(xid, snapshot->xmin))
		return false;

	result = XidInMVCCSnapshot(xid,
							   snapshot,
							   true,
							   &setDistributedSnapshotIgnore);

	if (result == XID_SURELY_COMMITTED || result == XID_NOT_IN_SNAPSHOT)
		return true;
	else
		return false;
}

/*
 * Used by vacuum to check whether the last merge transaction is visable to
 * the oldestXmin. If that is true, it means the SELECT will always fetch
 * tuples from resultTapes and other tapes is invisible to all queries, so
 * it can be recycled.
 *
 * This must be called when we hold the exclusive merge lock on the tapsort.
 *
 * Notice: we use local oldest xmin here because vacuum and merge of sort
 * heap are use local transaction only.
 */
bool tapesets_vacuum_validate_mergexid(TapeSetsState *tsstate)
{
	TransactionId OldestXmin;

	OldestXmin = GetLocalOldestXmin(NULL, PROCARRAY_FLAGS_VACUUM);
	TapeSetsMeta *tsmetad = tapesets_getmeta_disk(tsstate);

	if (TransactionIdPrecedesOrEquals(tsmetad->lastMergeXid, OldestXmin))
		return true;

	return false;
}

bool tapesets_try_lock(TapeSetsState *tsstate,
					   LOCKMODE mode)
{
	LOCKTAG tag;
	Relation relation = tsstate->relation;
	LockAcquireResult result;

	SET_LOCKTAG_PAGE(tag,
					 relation->rd_lockInfo.lockRelId.dbId,
					 relation->rd_lockInfo.lockRelId.relId,
					 tsstate->metablk);

	result = LockAcquire(&tag, mode, false, true);

	if (result == LOCKACQUIRE_NOT_AVAIL)
		return false;

	return true;
}

int tapesets_num_insert(TapeSetsState *tsstate)
{
	LOCKTAG tag;
	Relation relation = tsstate->relation;

	SET_LOCKTAG_PAGE(tag,
					 relation->rd_lockInfo.lockRelId.dbId,
					 relation->rd_lockInfo.lockRelId.relId,
					 tsstate->metablk);

	return LockWaiterCount(&tag);
}

void tapesets_unlock(TapeSetsState *tsstate,
					 LOCKMODE mode)
{
	LOCKTAG tag;
	Relation relation = tsstate->relation;

	SET_LOCKTAG_PAGE(tag,
					 relation->rd_lockInfo.lockRelId.dbId,
					 relation->rd_lockInfo.lockRelId.relId,
					 tsstate->metablk);

	LockRelease(&tag, mode, false);
}

/*
 * -------------------------------------------------------------
 *  Generic Data Management functions to the tapesets.
 *
 *  INSERT/SELECT/MERGE/VACUUM
 *  ------------------------------------------------------------
 */

bool tapesets_insert_finish(TapeSetsState *tsstate)
{
	return dump_tuples(tsstate,
					   GetCurrentTransactionId(),
					   GetCurrentCommandId(true),
					   true);
}

void tapesets_insert(TapeSetsState *tsstate,
					 TupleTableSlot *slot,
					 CommandId cid,
					 int options,
					 TransactionId xid)
{
	SortTuple stup;
	TapeSetsStatus status;

	/* Make a SortTuple with the input tuple */
	if (!COPYTUP(tsstate->parent, &stup, slot, &tsstate->availMem))
		return;

	status = tapesets_getstatus_forwrite(tsstate, false);

	switch (status)
	{
	case TS_INITIAL:
	case TS_MERGE_VACUUMED:
		/* change to TS_BUILDRUNS mode */
		init_tapes(tsstate);
		/* fall through */
	case TS_BUILDRUNS:

		/*
		 * Save the tuple into the unsorted array (there must be space)
		 */
		if (tsstate->memtupcount >= tsstate->memtupsize - 1)
			grow_memtuples(tsstate);

		tsstate->memtuples[tsstate->memtupcount++] = stup;

		/*
		 * Nothing to do if we still fit in available memory and have
		 * array slots, unless this is the final call during initial run
		 * generation.
		 */
		if (tsstate->memtupcount >= tsstate->memtupsize ||
			tsstate->availMem < 0)
			dump_tuples(tsstate, xid, cid, false);

		break;
	default:
		elog(ERROR, "insert on a tapesets that is not vacuumed");
	}
}

/*
 * Fetch the next tuple in required tapesets.
 *
 * if caller specified a slottuple, store the tuple in that slot and return,
 * otherwise, this function will provide a slot contains the tuple.
 */
TupleTableSlot *
tapesets_getnexttuple(TapeSetsState *tsstate, ScanDirection direction,
					  SortTuple *sorttuple, TupleTableSlot *slottuple,
					  SortHeapScanDesc tablescandesc)
{
	TapeSetsMeta *tsmetad;
	TapeScanDesc tapescandesc;
	TapeSetsStatus status;
	SortHeapState *shstate = tsstate->parent;

	tsmetad = tapesets_getmeta_snapshot(tsstate);

	status = tapesets_getstatus_forread(tsstate);

	if (status == TS_INITIAL)
		return NULL;
	else if (status >= TS_MERGE_SORTED)
	{
		tapescandesc = tape_get_scandesc(tsstate, tsmetad->resultTape,
										 0, tablescandesc);
		if (!tapescandesc)
			return NULL;

		if (slottuple == NULL)
			slottuple = tapescandesc->scanslot;

		/* the single result tape is always zero */
		if (FETCHTUP(shstate, tapescandesc, sorttuple, slottuple, false))
			return slottuple;
		else
			return NULL;
	}

	if (tablescandesc->keeporder)
	{
		SortTuple stup;

		/* use a minheap to keep the sort */
		if (!tsstate->sortdone)
		{
			int tapeno;

			tsstate->memtuples =
				(SortTuple *)palloc0(sizeof(SortTuple) * tsmetad->currentRun);
			tsstate->memtupcount = 0;

			/* build and initialize the minheap */
			for (tapeno = 0; tapeno < tsmetad->tapeRange; tapeno++)
			{
				int runno;

				if (tsmetad->tapes[tapeno].runs == 0)
					break;

				for (runno = 0; runno < tsmetad->tapes[tapeno].runs; runno++)
				{
					tapescandesc =
						tape_get_scandesc(tsstate, tapeno, runno, tablescandesc);
					if (!tapescandesc)
						continue;

					if (FETCHTUP(shstate, tapescandesc, &stup,
								 tapescandesc->scanslot, false))
					{
						stup.tapeno = tapeno;
						stup.runno = runno;
						minheap_insert(shstate,
									   tsstate->memtuples,
									   &tsstate->memtupcount, &stup);
					}
				}
			}

			tsstate->sortdone = true;
		}

		if (tsstate->lastReturnedTuple != NULL)
		{
			int srctapeno;
			int srcrunno;

			srctapeno = tsstate->lastReturnedTuple->tapeno;
			srcrunno = tsstate->lastReturnedTuple->runno;
			tapescandesc =
				tape_get_scandesc(tsstate, srctapeno, srcrunno, tablescandesc);

			/* refill the minheap */
			if (FETCHTUP(shstate, tapescandesc, &stup, tapescandesc->scanslot, false))
			{
				stup.tapeno = srctapeno;
				stup.runno = srcrunno;
				minheap_replace_top(shstate,
									tsstate->memtuples,
									&tsstate->memtupcount, &stup);
			}
			else
				minheap_delete_top(shstate,
								   tsstate->memtuples,
								   &tsstate->memtupcount);

			tsstate->lastReturnedTuple = NULL;
		}

		if (tsstate->memtupcount > 0)
		{
			tsstate->lastReturnedTuple = &tsstate->memtuples[0];

			memcpy(sorttuple, tsstate->lastReturnedTuple, sizeof(SortTuple));

			tapescandesc = tape_get_scandesc(tsstate,
											 tsstate->lastReturnedTuple->tapeno,
											 tsstate->lastReturnedTuple->runno,
											 tablescandesc);

			if (slottuple)
				return ExecCopySlot(slottuple, tapescandesc->scanslot);
			else
				return tapescandesc->scanslot;
		}

		return NULL;
	}
	else
	{
		/* Read tuples from each tape one by one */
		for (;;)
		{
			tapescandesc = tape_get_scandesc(tsstate,
											 tsstate->cur_readtape,
											 tsstate->cur_readrun,
											 tablescandesc);

			if (FETCHTUP(shstate, tapescandesc, sorttuple, slottuple, false))
				return slottuple;
			else
			{
				tsstate->cur_readrun++;

				if (tsstate->cur_readrun >= tsmetad->tapes[tsstate->cur_readtape].runs)
				{
					tsstate->cur_readrun = 0;
					tsstate->cur_readtape++;
				}

				if (tsstate->cur_readtape >= tsmetad->tapeRange)
					return NULL;
			}
		}
	}
}

/*
 * Perform vacuum on specified tapesets.
 *
 * 1. Recycle the tapes except the result Tape.
 * 2. Change the status to TS_BUILDRUNS.
 *
 * The current status must be TS_MERGE_SORTED.
 */
void tapesets_performvacuum(TapeSetsState *tsstate)
{
	vacuum_runs(tsstate);
}

/*
 * Perform merge on specified tapesets.
 *
 * Return true if a merge is actually performed.
 */
bool tapesets_performmerge(TapeSetsState *tsstate)
{
	return merge_runs(tsstate);
}

/*
 * -------------------------------------------------
 *  functions to get metadata of tapesets.
 *  -----------------------------------------------
 */
TapeSetsMeta *
tapesets_getmeta_disk(TapeSetsState *tsstate)
{
	Page page = BufferGetPage(tsstate->metabuf);

	return (TapeSetsMeta *)PageGetContents(page);
}

TapeSetsMeta *
tapesets_getmeta_snapshot(TapeSetsState *tsstate)
{
	return &tsstate->metad_snap;
}

/*-----------------------------------------------------------------------------
 *
 * sortheap_external_sort.c
 *		Generalized codes to to do external sort and continuous merging.
 *
 * This is the framework of external sorting logical in Knuth's Algorithm 5.4.2D.
 *
 * Mainly including:
 * 1) how to select a logical tape to dump tuples
 * 2) how to dump tuples
 * 3) how to merge logical tapes
 * 4) how to vacuum tapes
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_tapesets.c
 *
 *-----------------------------------------------------------------------------
 */
#include "sortheap.h"
#include "sortheap_tapesets.h"
#include "sortheap_external_sort.h"

typedef int (*SortTupleComparator)(const SortTuple *a, const SortTuple *b,
								   SortHeapState *shstate);

/*
 * -------------------------------------------------
 *  XLOG related functions
 *  -----------------------------------------------
 */

static XLogRecPtr
log_release_auxblk(Buffer headerbuf, BlockNumber nextnext)
{
	XLogBeginInsert();
	XLogRegisterData((char *)&nextnext, sizeof(int));
	XLogRegisterBuffer(0, headerbuf, REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_RELEASE_AUXBLK);
}

static XLogRecPtr
log_dump_tuples(Buffer metabuf, int tapenum,
				TapeSetsMeta *metad, bool full_page_image)
{
	XLogBeginInsert();

	/*
	 * If the level is changed in select_newtape() which means all fibs in the
	 * tapes are changed, we prefer to do a full page image instead of
	 * recording each changes.
	 */
	if (full_page_image)
		XLogRegisterBuffer(0, metabuf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);
	else
	{
		xl_sortheap_dump xlrec;

		XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

		xlrec.lastInsertXid = metad->lastInsertXid;
		xlrec.destTape = tapenum;
		xlrec.newDestTape = metad->destTape;
		xlrec.newCurrentRun = metad->currentRun;
		xlrec.newRuns = metad->tapes[tapenum].runs;
		xlrec.newDummy = metad->tapes[tapenum].dummy;
		XLogRegisterData((char *)&xlrec, SizeOfSortHeapDump);
	}

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_DUMP);
}

static XLogRecPtr
log_merge_done(Buffer metabuf, TransactionId lastmergexid)
{
	XLogBeginInsert();

	XLogRegisterData((char *)&lastmergexid, sizeof(TransactionId));
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_MERGE_DONE);
}

static XLogRecPtr
log_vacuum_done(Buffer metabuf, TransactionId lastvacuumxid)
{
	XLogBeginInsert();

	XLogRegisterData((char *)&lastvacuumxid, sizeof(TransactionId));
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_VACUUM_DONE);
}

static XLogRecPtr
log_vacuum_tape(Buffer metabuf, int tapenum)
{
	XLogBeginInsert();
	XLogRegisterData((char *)&tapenum, sizeof(int));
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_VACUUMTAPE);
}

/*
 * -------------------------------------------------
 *  Knuth's Algorithm 5.4.2D. related functions
 *  -----------------------------------------------
 */

/*
 * Special versions of qsort just for SortTuple objects.  qsort_tuple() sorts
 * any variant of SortTuples, using the appropriate comparetup function.
 * qsort_ssup() is specialized for the case where the default_comparetup function
 * reduces to ApplySortComparator(), that is single-key MinimalTuple sorts
 * and Datum sorts.
 */
#include "qsort_tuple.c"

/*
 * Sort all memtuples using specialized qsort() routines.
 *
 * Quicksort is used for small in-memory sorts, and external sort runs.
 */
static void
qsort_tuples(TapeSetsState *tsstate)
{
	if (tsstate->memtupcount > 1)
	{
		qsort_tuple(tsstate->memtuples,
					tsstate->memtupcount,
					(SortTupleComparator)tsstate->extsort_methods->comparetup,
					tsstate->parent);
	}
}

/*
 * Change current status to TS_BUILDRUNS stage, we might changing the status
 * from TS_INITIAL, TS_MERGE_VACUUMED.
 *
 * If changing from TS_INITIAL, we initial a brand new set of fibonacci arrays,
 * runs array, dummy array that needed by Knuth's Algorithm 5.4.2D.
 *
 * If changing from TS_MERGE_VACUUMED, all tuples are already sorted on
 * resultTape, switch the resultTape and the tape 0.
 */
void init_tapes(TapeSetsState *tsstate)
{
	int maxTapes;
	int i;
	TapeSetsMeta *tsmetad;
	TapeSetsMeta *tsmetadsnap;

	maxTapes = (BLCKSZ - sizeof(TapeSetsMeta) -
				MAXALIGN(sizeof(SortHeapPageOpaqueData)) -
				MAXALIGN(SizeOfPageHeaderData)) /
			   sizeof(SortHeapLogicalTape);

	if (maxTapes <= MIN_TAPES)
		elog(ERROR, "unexpected error, the metadata block cannot hold %d tapes", MIN_TAPES);
	maxTapes = Min(maxTapes, MAX_TAPES);

	/*
	 * one or more backends may reach here in the same time, only one is
	 * allowed to init_tapes
	 */
	LockBuffer(tsstate->metabuf, BUFFER_LOCK_EXCLUSIVE);
	tsmetad = tapesets_getmeta_disk(tsstate);

	/* Recheck the status in case someone already initialized the tapes */
	if (tsmetad->status == TS_BUILDRUNS)
	{
		tsstate->status_forwrite = TS_BUILDRUNS;
		LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);
		return;
	}

	/* Record # of tapes allocated (for duration of sort) */
	tsmetad->maxTapes = maxTapes;
	/* The last tape is always used to store the final merged tuples */
	tsmetad->resultTape = maxTapes - 1;
	/* Record maximum # of tapes usable as inputs when merging */
	tsmetad->tapeRange = maxTapes - 2;
	tsmetad->currentRun = 0;
	tsmetad->destTape = 0;

	for (i = 0; i < maxTapes; i++)
	{
		tsmetad->tapes[i].fib = 1;
		tsmetad->tapes[i].dummy = 1;
		tsmetad->tapes[i].runs = 0;

		if (tsmetad->status == TS_INITIAL)
			tsmetad->tapes[i].header = InvalidBlockNumber;
	}

	tsmetad->tapes[tsmetad->tapeRange].fib = 0;
	tsmetad->tapes[tsmetad->tapeRange].dummy = 0;
	tsmetad->tapes[tsmetad->resultTape].fib = 0;
	tsmetad->tapes[tsmetad->resultTape].dummy = 0;
	tsmetad->level = 1;

	/* All tuples are sorted on the tape 0, adjust the runs/dummy */
	Assert(tsmetad->status != TS_MERGE_SORTED);
	if (tsmetad->status == TS_MERGE_VACUUMED)
	{
		tsmetad->tapes[0].dummy = 0;
		tsmetad->tapes[0].runs = 1;
		tsmetad->currentRun = 1;
		tsmetad->destTape = 1;
		/* swap the physical header */
		tsmetad->tapes[0].header =
			tsmetad->tapes[tsmetad->resultTape].header;
		tsmetad->tapes[tsmetad->resultTape].header =
			InvalidBlockNumber;
	}

	tsmetad->status = TS_BUILDRUNS;
	tsstate->status_forwrite = TS_BUILDRUNS;

	/* sync the tapesets meta snapshot */
	tsmetadsnap = tapesets_getmeta_snapshot(tsstate);
	memcpy(tsmetadsnap, tsmetad, sizeof(TapeSetsMeta));
	MarkBufferDirty(tsstate->metabuf);

	SIMPLE_FAULT_INJECTOR("inittape_before_xlog");

	/* XLog stuff: do a full page image log */
	{
		XLogRecPtr recptr;
		Page page;

		recptr = log_full_page_update(tsstate->metabuf);
		page = BufferGetPage(tsstate->metabuf);
		PageSetLSN(page, recptr);
	}

	SIMPLE_FAULT_INJECTOR("inittape_after_xlog");

	LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);
}

/*
 * selectnewtape -- select new tape for new initial run.
 *
 * This is called after finishing a run when we know another run must be
 * started.  This implements steps D3, D4 of Algorithm D.
 */
void select_newtape(TapeSetsMeta *metad)
{
	int j;
	int a;

	/* Step D3: advance j (destTape) */
	if (metad->tapes[metad->destTape].dummy < metad->tapes[metad->destTape + 1].dummy)
	{
		metad->destTape++;
		return;
	}
	if (metad->tapes[metad->destTape].dummy != 0)
	{
		metad->destTape = 0;
		return;
	}

	/* Step D4: increase level */
	metad->level++;
	a = metad->tapes[0].fib;
	for (j = 0; j < metad->tapeRange; j++)
	{
		metad->tapes[j].dummy = a + metad->tapes[j + 1].fib - metad->tapes[j].fib;
		metad->tapes[j].fib = a + metad->tapes[j + 1].fib;
	}
	metad->destTape = 0;
}

/*
 * -------------------------------------------------
 *  INSERT related functions
 *  -----------------------------------------------
 */

/*
 * dumptuples - remove tuples from memtuples and write initial run to tape
 *
 * This function returns the signal whether the caller should trigger a
 * merge or not.
 */
bool dump_tuples(TapeSetsState *tsstate, TransactionId xid,
				 CommandId cid, bool lastdump)
{
	int destTape;
	int destRun;
	int level;
	BlockNumber tapeheader;
	TransactionId lastInsertXid;
	TapeSetsMeta *tsmetad;
	SortHeapState *shstate = tsstate->parent;

	if (tsstate->memtupcount <= 0)
	{
		/* Dump the tuples to the dest tape */
		if (lastdump)
			DUMPTUPS(shstate, NULL, 0, NULL, true);
		return false;
	}

	/*
	 * Sort all tuples accumulated within the allowed amount of memory for
	 * this run using quicksort.
	 */
	qsort_tuples(tsstate);

	LockBuffer(tsstate->metabuf, BUFFER_LOCK_EXCLUSIVE);
	tsmetad = tapesets_getmeta_disk(tsstate);

	/* Allocate a destTape */
	level = tsmetad->level;
	destTape = tsmetad->destTape;
	tsmetad->currentRun++;
	destRun = tsmetad->tapes[destTape].runs++;
	tsmetad->tapes[destTape].dummy--; /* per Alg D step D2 */

	/* update the last insertion xid */
	lastInsertXid = GetCurrentTransactionId();
	if (TransactionIdPrecedes(tsmetad->lastInsertXid, lastInsertXid))
		tsmetad->lastInsertXid = lastInsertXid;

	/* Advance the destTape to a new tape */
	select_newtape(tsmetad);

	MarkBufferDirty(tsstate->metabuf);

	SIMPLE_FAULT_INJECTOR("dump_tuples_before_xlog");

	/* XLOG stuff */
	{
		XLogRecPtr recptr;
		Page metapage;

		metapage = BufferGetPage(tsstate->metabuf);
		recptr = log_dump_tuples(tsstate->metabuf, destTape, tsmetad,
								 level != tsmetad->level);
		PageSetLSN(metapage, recptr);
	}

	LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);

	SIMPLE_FAULT_INJECTOR("dump_tuples_after_xlog");

	/* Allocate a TapeInsertState and lock the tape for insert */
	tapeheader = tape_getheader(tsstate, destTape, true, true);
	tsstate->insertstate = tape_create_insertstate(tsstate,
												   xid,
												   cid,
												   tapeheader,
												   destRun,
												   true);

	/* Dump the tuples to the dest tape */
	DUMPTUPS(shstate,
			 tsstate->memtuples,
			 tsstate->memtupcount,
			 &tsstate->availMem,
			 lastdump);

	/* Destroy the insert state */
	tape_destroy_insertstate(tsstate->insertstate);
	tsstate->insertstate = NULL;

	/*
	 * Reset tuple memory.  We've freed all of the tuples that we previously
	 * allocated.  It's important to avoid fragmentation when there is a stark
	 * change in the sizes of incoming tuples.  Fragmentation due to
	 * AllocSetFree's bucketing by size class might be particularly bad if
	 * this step wasn't taken.
	 */
	MemoryContextReset(shstate->tuplecontext);
	tsstate->memtupcount = 0;

	if (level > 1)
	{
		Page shmetapage;
		SortHeapMeta *shmetad;

		LockBuffer(shstate->metabuf, BUFFER_LOCK_EXCLUSIVE);
		shmetapage = BufferGetPage(shstate->metabuf);
		shmetad = (SortHeapMeta *)PageGetContents(shmetapage);

		if (shmetad->mergets == -1)
		{
			shmetad->mergets = tsstate->tapesetsno;
			shmetad->insertts = (tsstate->tapesetsno + 1) % MAX_TAPESETS;
		}

		MarkBufferDirty(shstate->metabuf);
		LockBuffer(shstate->metabuf, BUFFER_LOCK_UNLOCK);
		return true;
	}
	else
		return false;
}

/*
 * -------------------------------------------------
 *  MERGE related functions
 *  -----------------------------------------------
 */

static void
merge_runs_done(TapeSetsState *tsstate)
{
	SortHeapMeta *shmetad;
	TapeSetsMeta *tsmetad;
	SortHeapState *shstate;
	Page page;

	shstate = tsstate->parent;

	/*
	 * Set the status to TS_MERGE_SORTED, meanwhile, record the transaction id
	 * performed this merge. TapeSetsMeta has no MVCC, so tapesets_getstatus()
	 * rely on the lastMergeXid to identify whether this merge is visible to
	 * other transactions.
	 */
	LockBuffer(shstate->metabuf, BUFFER_LOCK_EXCLUSIVE);
	LockBuffer(tsstate->metabuf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(shstate->metabuf);
	shmetad = (SortHeapMeta *)PageGetContents(page);
	/* FIXME: xlog */
	shmetad->mergets = -1;

	page = BufferGetPage(tsstate->metabuf);
	tsmetad = (TapeSetsMeta *)PageGetContents(page);
	tsmetad->status = TS_MERGE_SORTED;
	tsmetad->lastMergeXid = GetCurrentTransactionId();
	tsmetad->tapes[tsmetad->resultTape].runs = 1;
	MarkBufferDirty(tsstate->metabuf);

	/* XLOG stuff */
	{
		XLogRecPtr recptr;

		recptr = log_merge_done(tsstate->metabuf,
								tsmetad->lastMergeXid);
		PageSetLSN(page, recptr);
	}

	LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);
	LockBuffer(shstate->metabuf, BUFFER_LOCK_UNLOCK);

	SIMPLE_FAULT_INJECTOR("merge_done");
}

/*
 * beginmerge - initialize for a merge pass
 *
 * We decrease the counts of real and dummy runs for each tape, and mark
 * which tapes contain active input runs in mergeactive[].  Then, fill the
 * merge heap with the first tuple from each active tape.
 */
static void
begin_merge(TapeSetsState *tsstate, TapeSetsMeta *tsmetadsnap)
{
	int i;
	int activeTapes;
	int tapenum;
	int srcTape;

	/* Heap should be empty here */
	Assert(tsstate->memtupcount == 0);

	/* Adjust run counts and mark the active tapes */
	for (i = 0; i < tsmetadsnap->maxTapes; i++)
		tsmetadsnap->tapes[i].mergeactive = false;

	activeTapes = 0;
	for (tapenum = 0; tapenum < tsmetadsnap->tapeRange; tapenum++)
	{
		if (tsmetadsnap->tapes[tapenum].dummy > 0)
			tsmetadsnap->tapes[tapenum].dummy--;
		else if (tsmetadsnap->tapes[tapenum].runs == 0)
			continue;
		else
		{
			Assert(tsmetadsnap->tapes[tapenum].runs > 0);
			tsmetadsnap->tapes[tapenum].runs--;
			tsmetadsnap->tapes[tapenum].mergeactive = true;
			activeTapes++;
		}
	}
	Assert(activeTapes > 0);

	/* Load the merge heap with the first tuple from each input tape */
	for (srcTape = 0; srcTape < tsmetadsnap->resultTape; srcTape++)
	{
		SortTuple stup;
		TapeScanDesc tapescandesc;

		if (!tsmetadsnap->tapes[srcTape].mergeactive)
			continue;

		tsmetadsnap->tapes[srcTape].currun =
			tsmetadsnap->tapes[srcTape].nextrun++;

		tapescandesc = tape_get_scandesc(tsstate,
										 srcTape,
										 tsmetadsnap->tapes[srcTape].currun,
										 NULL);

		if (!tapescandesc)
		{
			tsmetadsnap->tapes[srcTape].mergeactive = false;
			continue;
		}

		if (FETCHTUP(tsstate->parent, tapescandesc, &stup, NULL, true))
		{
			stup.tapeno = srcTape;
			minheap_insert(tsstate->parent,
						   tsstate->memtuples,
						   &tsstate->memtupcount,
						   &stup);
		}
		else
			tsmetadsnap->tapes[srcTape].mergeactive = false;
	}
}

/*
 * Merge one run from each input tape, except ones with dummy runs.
 *
 * This is the inner loop of Algorithm D step D5.  We know that the
 * output tape is TAPE[T].
 */
static void
merge_onerun(TapeSetsState *tsstate, TapeSetsMeta *tsmetadsnap)
{
	int srctape;
	int inserttape;
	bool buildindex;
	BlockNumber tapeheader;
	SortHeapState *shstate = tsstate->parent;
	TapeReadState *tapereadstate;

	/*
	 * Start the merge by loading one tuple from each active source tape into
	 * the heap.  We can also decrease the input run/dummy run counts.
	 */
	begin_merge(tsstate, tsmetadsnap);

	/* If this is the last run, insert all tuples to the resultTape */
	if (tsmetadsnap->level == 1)
	{
		inserttape = tsmetadsnap->resultTape;
		buildindex = true;
	}
	else
	{
		inserttape = tsmetadsnap->tapeRange;
		buildindex = false;
	}

	tapeheader = tape_getheader(tsstate, inserttape, false, true);
	tsstate->insertstate = tape_create_insertstate(tsstate,
												   GetCurrentTransactionId(),
												   GetCurrentCommandId(true),
												   tapeheader,
												   0,
												   buildindex);

	/*
	 * Execute merge by repeatedly extracting lowest tuple in heap, writing it
	 * out, and replacing it with next tuple from same tape (if there is
	 * another one).
	 */
	while (tsstate->memtupcount > 0)
	{
		SortTuple stup;
		TapeScanDesc tapescandesc;

		/* write the tuple to destTape */
		srctape = tsstate->memtuples[0].tapeno;

		MERGETUP(shstate, &tsstate->memtuples[0]);

		tapescandesc = tape_get_scandesc(tsstate,
										 srctape,
										 tsmetadsnap->tapes[srctape].currun,
										 NULL);
		Assert(tapescandesc);

		/*
		 * pull next tuple from the tape, and replace the written-out tuple in
		 * the heap with it.
		 */
		if (FETCHTUP(shstate, tapescandesc, &stup, NULL, true))
		{
			stup.tapeno = srctape;
			minheap_replace_top(shstate,
								tsstate->memtuples,
								&tsstate->memtupcount, &stup);
		}
		else
			minheap_delete_top(shstate,
							   tsstate->memtuples,
							   &tsstate->memtupcount);
	}

	/*
	 * When the heap empties, we're done.  Write an end-of-run marker on the
	 * output tape, and increment its count of real runs.
	 */
	MERGETUP(tsstate->parent, NULL);

	/* rewind output tape T to use as new input */
	if (!tsstate->readstates[tsmetadsnap->tapeRange])
	{
		/*
		 * Current Transaction snapshot is used, so the new merged tuple
		 * will be visible by the snapshot
		 */
		tsstate->readstates[tsmetadsnap->tapeRange] =
			tape_create_readstate(tsstate, GetTransactionSnapshot(),
								  tsstate->insertstate->header,
								  0, 1, ForwardScanDirection);

		tapereadstate = tsstate->readstates[tsmetadsnap->tapeRange];
		tapereadstate->runs_scandescs = (TapeScanDesc *)palloc0(sizeof(TapeScanDesc));
		tapereadstate->runs_scandescs[0] =
			(TapeScanDesc)palloc0(sizeof(struct TapeScanDescData));

		/*
		 * We keep a snapshot of the tapesets in merge stage which means we do
		 * not change the physical (metadata) of runs, so we need to explictly
		 * tell where the newly insert RUN starts and ends.
		 */
		tape_initialize_scandesc(tapereadstate, 0,
								 tsstate->insertstate->runmetad,
								 NULL);
	}
	else
	{
		tapereadstate = tsstate->readstates[tsmetadsnap->tapeRange];

		tapereadstate->nruns++;

		if (tapereadstate->runs_scandescs)
			tapereadstate->runs_scandescs =
				(TapeScanDesc *)repalloc(tapereadstate->runs_scandescs,
										 sizeof(TapeScanDesc) * tapereadstate->nruns);
		else
			tapereadstate->runs_scandescs = (TapeScanDesc *)palloc0(sizeof(TapeScanDesc));

		tapereadstate->runs_scandescs[tapereadstate->nruns - 1] =
			(TapeScanDesc)palloc0(sizeof(struct TapeScanDescData));

		/*
		 * We keep a snapshot of the tapesets in merge stage which means we do
		 * not change the physical (metadata) of runs, so we need to explictly
		 * tell where the newly insert RUN starts and ends.
		 */
		tape_initialize_scandesc(tapereadstate, tapereadstate->nruns - 1,
								 tsstate->insertstate->runmetad,
								 NULL);
	}

	tsmetadsnap->tapes[tsmetadsnap->tapeRange].runs++;
	tape_destroy_insertstate(tsstate->insertstate);
	tsstate->insertstate = NULL;

	/*
	 * Increase the command ID so merged tuples can be seen by following
	 * merging
	 */
	CommandCounterIncrement();
}

/*
 * mergeruns -- merge all the completed initial runs.
 *
 * This implements steps D5, D6 of Algorithm D.  All input data has
 * already been written to initial runs on tape (see dumptuples).
 *
 * CAUTION: we use a snapshot of the TapeSetsMeta to do the merge, MERGE only
 * change the status & lastMergeXid which means the old content in the real
 * TapeSetsMeta is almost not modified. We hope next INSERT operation reset
 * the tapes infos.
 */
bool merge_runs(TapeSetsState *tsstate)
{
	int tapenum;
	int svTapeHeader;
	int svRuns;
	int svDummy;
	int svCurRun;
	int svNextRun;
	int numInputTapes;
	TapeReadState *svReadState;
	TapeSetsMeta *tsmetadsnap;
	SortHeapState *shstate;

	shstate = tsstate->parent;
	/*
	 * Get the latest meta from disk and copy it, we don't want to change the
	 * runs/dummys in the metadata page.
	 */
	memcpy(&tsstate->metad_snap, tapesets_getmeta_disk(tsstate), sizeof(TapeSetsMeta));
	tsmetadsnap = tapesets_getmeta_snapshot(tsstate);

	if (tsmetadsnap->status != TS_BUILDRUNS)
		return false;

	Assert(tsstate->memtupcount == 0);

	if (shstate->sortkeys != NULL && shstate->sortkeys->abbrev_converter != NULL)
	{
		/*
		 * If there are multiple runs to be merged, when we go to read back
		 * tuples from disk, abbreviated keys will not have been stored, and
		 * we don't care to regenerate them.  Disable abbreviation from this
		 * point on.
		 */
		shstate->sortkeys->abbrev_converter = NULL;
		shstate->sortkeys->comparator = shstate->sortkeys->abbrev_full_comparator;

		/* Not strictly necessary, but be tidy */
		shstate->sortkeys->abbrev_abort = NULL;
		shstate->sortkeys->abbrev_full_comparator = NULL;
	}

	/*
	 * If we had fewer runs than tapes, refund the memory that we imagined we
	 * would need for the tape buffers of the unused tapes.
	 *
	 * numTapes and numInputTapes reflect the actual number of tapes we will
	 * use.  Note that the output tape's tape number is tapeRange, so the tape
	 * numbers of the used tapes are not consecutive, and you cannot just loop
	 * from 0 to numTapes to visit all used tapes!
	 */
	if (tsmetadsnap->level == 1)
		numInputTapes = tsmetadsnap->currentRun;
	else
		numInputTapes = tsmetadsnap->tapeRange;

	/* Keep the status, nothing should be done */
	if (tsmetadsnap->level == 1 && tsmetadsnap->currentRun == 1)
		return false;

	/*
	 * Allocate a new 'memtuples' array, for the heap.  It will hold one tuple
	 * from each input tape.
	 */
	tsstate->memtupsize = numInputTapes;
	tsstate->memtuples = (SortTuple *)palloc(numInputTapes * sizeof(SortTuple));

	for (;;)
	{
		/* Step D5: merge runs onto tape[T] until tape[P] is empty */
		while (tsmetadsnap->tapes[tsmetadsnap->tapeRange - 1].runs ||
			   tsmetadsnap->tapes[tsmetadsnap->tapeRange - 1].dummy)
		{
			bool allDummy = true;

			for (tapenum = 0; tapenum < tsmetadsnap->tapeRange; tapenum++)
			{
				if (tsmetadsnap->tapes[tapenum].dummy == 0)
				{
					allDummy = false;
					break;
				}
			}

			if (allDummy)
			{
				tsmetadsnap->tapes[tsmetadsnap->tapeRange].dummy++;
				for (tapenum = 0; tapenum < tsmetadsnap->tapeRange; tapenum++)
					tsmetadsnap->tapes[tapenum].dummy--;
			}
			else
				merge_onerun(tsstate, tsmetadsnap);
		}

		/* Step D6: decrease level */
		if (--tsmetadsnap->level == 0)
			break;

		tsmetadsnap->tapes[tsmetadsnap->tapeRange - 1].runs = 0;

		/*
		 * reassign tape units per step D6; note we no longer care about A[]
		 */
		svTapeHeader = tsmetadsnap->tapes[tsmetadsnap->tapeRange].header;
		svDummy = tsmetadsnap->tapes[tsmetadsnap->tapeRange].dummy;
		svRuns = tsmetadsnap->tapes[tsmetadsnap->tapeRange].runs;
		svCurRun = tsmetadsnap->tapes[tsmetadsnap->tapeRange].currun;
		svNextRun = tsmetadsnap->tapes[tsmetadsnap->tapeRange].nextrun;
		svReadState = tsstate->readstates[tsmetadsnap->tapeRange];
		for (tapenum = tsmetadsnap->tapeRange; tapenum > 0; tapenum--)
		{
			tsmetadsnap->tapes[tapenum].header = tsmetadsnap->tapes[tapenum - 1].header;
			tsmetadsnap->tapes[tapenum].dummy = tsmetadsnap->tapes[tapenum - 1].dummy;
			tsmetadsnap->tapes[tapenum].runs = tsmetadsnap->tapes[tapenum - 1].runs;
			tsmetadsnap->tapes[tapenum].currun = tsmetadsnap->tapes[tapenum - 1].currun;
			tsmetadsnap->tapes[tapenum].nextrun = tsmetadsnap->tapes[tapenum - 1].nextrun;
			tsstate->readstates[tapenum] = tsstate->readstates[tapenum - 1];
		}
		tsmetadsnap->tapes[0].header = svTapeHeader;
		tsmetadsnap->tapes[0].dummy = svDummy;
		tsmetadsnap->tapes[0].runs = svRuns;
		tsmetadsnap->tapes[0].currun = svCurRun;
		tsmetadsnap->tapes[0].nextrun = svNextRun;
		tsstate->readstates[0] = svReadState;
	}

	merge_runs_done(tsstate);

	return true;
}

/*
 * Vacuum pages in the tapesets.
 *
 * The tapesets must be in the status of TS_MERGE_SORTED, all tuples are sorted
 * in the resultTape and only resultTape is visible to all queries, so it's safe
 * to vacuum pages other logical tapes in batches.
 *
 * This is not the same with normal vacuum operations, it doesn't touch the data
 * pages that need vacuum, instead, it only reads the auxiliary blocks and mark
 * the allocated blocks as free pages.
 */
void vacuum_runs(TapeSetsState *tsstate)
{
	TapeSetsMeta *tsmetad;
	int tapenum;

	tsmetad = tapesets_getmeta_disk(tsstate);

	for (tapenum = 0; tapenum < tsmetad->resultTape; tapenum++)
	{
		int i;
		Buffer tapeheadbuf;
		Page tapeheadpage;
		Relation relation;
		BlockNumber tapeheader;
		BlockNumber next;
		HeapTapeMeta *tapemetad;

		relation = tsstate->relation;
		tapeheader = tsmetad->tapes[tapenum].header;
		if (tapeheader == InvalidBlockNumber)
			continue;

		/* Read the tapeheader page and start vacuuming */
		tapeheadbuf = ReadBuffer(relation, tapeheader);
		LockBuffer(tapeheadbuf, BUFFER_LOCK_EXCLUSIVE);
		tapeheadpage = BufferGetPage(tapeheadbuf);

		tapemetad = (HeapTapeMeta *)PageGetContents(tapeheadpage);

		next = tapemetad->firstauxblock;
		while (next != InvalidBlockNumber)
		{
			Buffer allocauxbuf;
			Page allocauxpage;
			BlockNumber *allocated;
			BlockNumber nextnext;
			SortHeapPageOpaqueData *opaque;

			allocauxbuf = ReadBuffer(relation, next);
			allocauxpage = BufferGetPage(allocauxbuf);
			allocated = (BlockNumber *)PageGetContents(allocauxpage);
			opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(allocauxpage);
			nextnext = opaque->next;

			for (i = 0; i < ALLOCATEDBLOCKPERAUXBLOCK; i++)
			{
				if (allocated[i] == InvalidBlockNumber)
					continue;
				sortheap_recycle_block(relation, allocated[i]);
			}

			/* recycle the auxiliary block too */
			sortheap_recycle_block(relation, next);

			/* ref to the next auxiliary block in case some error happens */
			next = nextnext;
			tapemetad->firstauxblock = nextnext;
			MarkBufferDirty(tapeheadbuf);

			/* XLOG stuff */
			{
				XLogRecPtr recptr = log_release_auxblk(tapeheadbuf, nextnext);

				PageSetLSN(tapeheadpage, recptr);
			}

			ReleaseBuffer(allocauxbuf);
		}

		/* recycle all prealloc blocks */
		for (i = 0; i < tapemetad->nprealloc; i++)
			sortheap_recycle_block(relation, tapemetad->prealloc[i]);

		UnlockReleaseBuffer(tapeheadbuf);

		/* Recycle the tapeheader too */
		LockBuffer(tsstate->metabuf, BUFFER_LOCK_EXCLUSIVE);

		tsmetad->tapes[tapenum].header = InvalidBlockNumber;
		sortheap_recycle_block(relation, tapeheader);
		MarkBufferDirty(tsstate->metabuf);

		/* XLOG stuff */
		log_vacuum_tape(tsstate->metabuf, tapenum);

		LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);
	}

	tsmetad->lastVacuumXid = GetCurrentTransactionId();
	tsmetad->status = TS_MERGE_VACUUMED;

	LockBuffer(tsstate->metabuf, BUFFER_LOCK_EXCLUSIVE);
	MarkBufferDirty(tsstate->metabuf);
	/* XLOG stuff */
	log_vacuum_done(tsstate->metabuf, tsmetad->lastVacuumXid);

	LockBuffer(tsstate->metabuf, BUFFER_LOCK_UNLOCK);

	SIMPLE_FAULT_INJECTOR("vacuum_done");
}

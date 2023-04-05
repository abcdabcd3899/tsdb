/*-------------------------------------------------------------------------
 *
 * sortheapam.c
 *	  Generalized sorting heap routines.
 *
 * Sort Heap storage can store large amounts of data. The inserted tuples are
 * sorted in batch in memory and dumped into disks using an effective external
 * sorting algorithm which guarantees these partially sorted results can be
 * resorted and continuously merged.
 *
 * The first block is always Meta Data Block, containing overall meta
 * information needed in the Knuth's algorithm. Tapes are composed with a
 * series of linked heap blocks. Each tape has a header block which stores
 * physical information of the whole tape.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/access/sortheap/sortheapam.c
 *
 *-------------------------------------------------------------------------
 */
#include "sortheap.h"
#include "sortheap_tapesets.h"

List *sortheapstate_cachelist = NIL;

/*
 * -------------------------------------------------
 *  XLOG related functions
 *  -----------------------------------------------
 */

static XLogRecPtr
log_extend(Buffer metabuf, long nwritten)
{
	XLogBeginInsert();

	XLogRegisterData((char *)&nwritten, sizeof(long));
	XLogRegisterBuffer(0, metabuf, REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_EXTEND);
}

XLogRecPtr
log_full_page_update(Buffer curbuf)
{
	XLogBeginInsert();

	/* force a full-page image, so we can restore the page entirely */
	XLogRegisterBuffer(0, curbuf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

	return XLogInsert(RM_SORTHEAP_ID, XLOG_SORTHEAP_FULLPAGEUPDATE);
}

/* Resource owner callback to cleanup a sort state */
static void
cleanup_sortheap_callback(ResourceReleasePhase phase,
						  bool isCommit,
						  bool isTopLevel,
						  void *arg)
{
	SortHeapState *shstate = (SortHeapState *)arg;

	if (!shstate || CurrentResourceOwner != shstate->cleanupresowner ||
		phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	if (!isCommit)
	{
		UnregisterResourceReleaseCallback(cleanup_sortheap_callback, arg);
		return;
	}

	sortheap_endsort(shstate);
}

/*
 * Extend the length of the physical file, the caller should have acquired
 * the Exclusive Lock on the metabuf
 */
void sortheap_extend(SortHeapState *shstate, BlockNumber target)
{
	SortHeapMeta *shmetad;
	XLogRecPtr recptr;
	Page metapage;

	metapage = BufferGetPage(shstate->metabuf);
	shmetad = (SortHeapMeta *)PageGetContents(metapage);

	/* Sanity check */
	if (target - shmetad->nBlocksWritten >
		MAX_PREALLOC_SIZE * MAX_TAPESETS * MAX_TAPES)
		elog(PANIC, "unexpected blocknum: target %d, nBlocksWritten %ld",
			 target, shmetad->nBlocksWritten);

	while (target >= shmetad->nBlocksWritten)
	{
		char zero[BLCKSZ];

		/* new buffers are zero-filled */
		MemSet(zero, 0, BLCKSZ);

		/* don't set checksum for all-zero page */
		smgrextend(shstate->relation->rd_smgr, MAIN_FORKNUM,
				   shmetad->nBlocksWritten++, (char *)zero, false);
	}
	MarkBufferDirty(shstate->metabuf);

	/* XLOG stuff */
	recptr = log_extend(shstate->metabuf, shmetad->nBlocksWritten);
	PageSetLSN(metapage, recptr);
}

/* -------------------------------------------------------
 * Helper functions on the sort heap
 * ------------------------------------------------------
 */

/*
 * Advance to the next tapesets
 */
bool sortheap_advance_next_tapesets(SortHeapState *shstate)
{
	if (shstate->cur_readtapesets == MAX_TAPESETS - 1)
		return false;

	shstate->cur_readtapesets++;
	return true;
}

/*
 * Try to lock the MERGE operation, we use InvalidBlockNumber to identify
 * the MERGER operation.
 */
bool sortheap_try_lock_merge(Relation relation, LOCKMODE mode)
{
	LOCKTAG tag;
	LockAcquireResult result;

	SET_LOCKTAG_PAGE(tag,
					 relation->rd_lockInfo.lockRelId.dbId,
					 relation->rd_lockInfo.lockRelId.relId,
					 InvalidBlockNumber);

	result = LockAcquire(&tag, mode, false, true);

	if (result == LOCKACQUIRE_NOT_AVAIL)
		return false;

	return true;
}

void sortheap_unlock_merge(Relation relation, LOCKMODE mode)
{
	LOCKTAG tag;

	SET_LOCKTAG_PAGE(tag,
					 relation->rd_lockInfo.lockRelId.dbId,
					 relation->rd_lockInfo.lockRelId.relId,
					 InvalidBlockNumber);

	LockRelease(&tag, mode, false);
}

/* -----------------------------------------------------------------------------
 * Sort Heap State routines.
 *
 * A SortHeapState must be created before accessing a sort heap, memory content,
 * sort keys and sort methods are initialized.
 * ----------------------------------------------------------------------------
 */

SortHeapState *
sortheap_beginsort(Relation relation, Snapshot snap, Relation indexrel, SortHeapOP op, void *args)
{
	int i;
	List *indexes;
	ListCell *lc;
	SortHeapState *state;
	MemoryContext sortcontext;
	MemoryContext tuplecontext;
	MemoryContext oldcontext;
	BTScanInsert indexScanKey;
	ExternalSortMethods *methods;

	/* Find the shstate in the cache list */
	foreach (lc, sortheapstate_cachelist)
	{
		state = (SortHeapState *)lfirst(lc);

		if (state->relation == relation &&
			state->relation->rd_id == relation->rd_id)
			return state;
	}

	methods = (ExternalSortMethods *)relation->rd_tableam->priv;

	state = (SortHeapState *)MemoryContextAllocZero(TopTransactionContext,
													sizeof(SortHeapState));

	/*
	 * Create a working memory context for this sort operation. All data
	 * needed by the sort will live inside this context.
	 */
	sortcontext = AllocSetContextCreate(TopTransactionContext,
										"sortagg main",
										ALLOCSET_DEFAULT_SIZES);
	MemoryContextDeclareAccountingRoot(sortcontext);

	/*
	 * Caller tuple (e.g. IndexTuple) memory context.
	 *
	 * A dedicated child context used exclusively for caller passed tuples
	 * eases memory management.  Resetting at key points reduces
	 * fragmentation. Note that the memtuples array of SortTuples is allocated
	 * in the parent context, not this context, because there is no need to
	 * free memtuples early.
	 */
	tuplecontext = AllocSetContextCreate(sortcontext,
										 "Caller tuples",
										 ALLOCSET_DEFAULT_SIZES);

	/*
	 * Make the AggHeapSortState within the per-sort context.  This way, we
	 * don't need a separate pfree() operation for it at shutdown.
	 */
	oldcontext = MemoryContextSwitchTo(sortcontext);

	state->sortcontext = sortcontext;
	state->tuplecontext = tuplecontext;
	state->op = op;
	state->args = args;
	state->extsort_methods = methods;
	state->resowner = CurrentResourceOwner;

	RelationIncrementReferenceCount(relation);
	state->relation = relation;

	/* Initialize the internal btree structure if we have one */
	if (indexrel == NULL)
	{
		Oid indexoid;

		indexes = RelationGetIndexList(relation);

		if (indexes)
		{
			Assert(list_length(indexes) == 1);
			indexoid = linitial_oid(indexes);

			state->indexrel = relation_open(indexoid, RowExclusiveLock);
			state->closeindexrel = true;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("You must create a sortheap_btree index "
							"on sortheap table before using it")));
		}
	}
	else
	{
		state->indexrel = indexrel;
		state->closeindexrel = false;
	}

	RelationOpenSmgr(state->indexrel);

	/*
	 * Initialize the sort keys, sortheap relies on the index to tell the sort
	 * keys, so it acts like "cluster" which means sortheap can also support
	 * expression in the sort keys.
	 */
	state->nkeys = IndexRelationGetNumberOfKeyAttributes(state->indexrel);
	state->indexinfo = BuildIndexInfo(state->indexrel);
	indexScanKey = _bt_mkscankey(state->indexrel, NULL);

	if (state->indexinfo->ii_Expressions != NULL)
	{
		TupleTableSlot *slot;
		ExprContext *econtext;

		/*
		 * We will need to use FormIndexDatum to evaluate the index
		 * expressions. To do that, we need an EState, as well as a
		 * TupleTableSlot to put the table tuples into. The econtext's
		 * scantuple has to point to that slot, too.
		 */
		state->estate = CreateExecutorState();
		slot = MakeSingleTupleTableSlot(RelationGetDescr(relation), &TTSOpsHeapTuple);
		econtext = GetPerTupleExprContext(state->estate);
		econtext->ecxt_scantuple = slot;
	}

	/* Prepare SortSupport data for each column */
	state->sortkeys =
		(SortSupport)palloc0(state->nkeys * sizeof(SortSupportData));

	for (i = 0; i < state->nkeys; i++)
	{
		SortSupport sortkey = state->sortkeys + i;
		ScanKey scankey = indexScanKey->scankeys + i;
		int16 strategy;
		Oid savedindexam;

		sortkey->ssup_cxt = CurrentMemoryContext;
		sortkey->ssup_collation = scankey->sk_collation;
		sortkey->ssup_nulls_first =
			(scankey->sk_flags & SK_BT_NULLS_FIRST) != 0;
		sortkey->ssup_attno = scankey->sk_attno;
		/* Convey if abbreviation optimization is applicable in principle */
		sortkey->abbreviate = (i == 0);

		AssertState(sortkey->ssup_attno != 0);

		strategy = (scankey->sk_flags & SK_BT_DESC) != 0 ? BTGreaterStrategyNumber : BTLessStrategyNumber;

		/* Hack the sanity check of indexam in PrepareSortSupportFromIndexRel */
		savedindexam = state->indexrel->rd_rel->relam;
		state->indexrel->rd_rel->relam = BTREE_AM_OID;
		PrepareSortSupportFromIndexRel(state->indexrel, strategy, sortkey);
		state->indexrel->rd_rel->relam = savedindexam;
	}

	pfree(indexScanKey);

	/* Pin the metabuf until the sort state is destroyed */
	state->metabuf = ReadBuffer(relation, SORTHEAP_METABLOCK);
	state->cur_readtapesets = 0;
	state->cur_inserttapesets = -1;

	/* We have two tapesets now, initialize their sort states */
	for (i = 0; i < MAX_TAPESETS; i++)
	{
		TapeSetsState *tsstate =
			(TapeSetsState *)palloc0(sizeof(TapeSetsState));

		state->tapesetsstates[i] = tsstate;

		tsstate->tapesetsno = i;
		tsstate->relation = relation;
		tsstate->allowedMem = Max(sortheap_sort_mem, 64) * (int64)1024;
		tsstate->op = op;
		tsstate->snapshot = snap;
		tsstate->extsort_methods = methods;
		tsstate->parent = state;

		tapesets_initialize_state(tsstate);
	}

	MemoryContextSwitchTo(oldcontext);

	/*
	 * Use the resource owner mechanism to manage the sort states in case we
	 * cannot clean it up in the error cases
	 */
	state->cleanupresowner =
		ResourceOwnerCreate(CurrentResourceOwner, "SortHeapStateResOwner");
	RegisterResourceReleaseCallback(cleanup_sortheap_callback, (void *)state);

	/* Record the state in cache list */
	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	sortheapstate_cachelist = lappend(sortheapstate_cachelist, state);
	MemoryContextSwitchTo(oldcontext);

	return state;
}

bool sortheap_endsort(SortHeapState *shstate)
{
	int i;
	bool trigger_merge = false;
	ResourceOwner oldowner;

	if (!shstate)
		return false;

	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = shstate->resowner;

	/* Flush out the tuples in current inserting tapesets */
	if (shstate->cur_inserttapesets != -1)
		trigger_merge =
			tapesets_insert_finish(TAPESETSSTATE(shstate,
												 shstate->cur_inserttapesets));

	for (i = 0; i < MAX_TAPESETS; i++)
	{
		tapesets_destroy_state(TAPESETSSTATE(shstate, i));
		shstate->tapesetsstates[i] = NULL;
	}

	if (shstate->metabuf != InvalidBuffer)
	{
		ReleaseBuffer(shstate->metabuf);
		shstate->metabuf = InvalidBuffer;
	}

	if (shstate->estate != NULL)
	{
		ExprContext *econtext = GetPerTupleExprContext(shstate->estate);

		ExecDropSingleTupleTableSlot(econtext->ecxt_scantuple);
		FreeExecutorState(shstate->estate);
	}

	if (shstate->indexrel && shstate->closeindexrel)
	{
		relation_close(shstate->indexrel, RowExclusiveLock);
		shstate->indexrel = NULL;
	}

	if (shstate->relation)
	{
		RelationDecrementReferenceCount(shstate->relation);
		shstate->relation->rd_amcache = NULL;
		shstate->relation = NULL;
	}

	sortheapstate_cachelist = list_delete(sortheapstate_cachelist, (void *)shstate);

	UnregisterResourceReleaseCallback(cleanup_sortheap_callback, (void *)shstate);

	CurrentResourceOwner = oldowner;

	return trigger_merge;
}

/* ---------------------------------------------
 *
 * Data Manipulation access methods
 *
 * --------------------------------------------
 */

/*
 * Fetch the next slot from the sort heap.
 */
bool sortheap_getnextslot(TableScanDesc sscan, ScanDirection direction,
						  TupleTableSlot *slot)
{
	int i;
	Relation relation = sscan->rs_rd;
	SortTuple stup;
	TapeSetsState *tsstate;
	SortHeapState *shstate;
	SortHeapScanDesc shscan;

	shscan = (SortHeapScanDesc)sscan;

	if (shscan->sortstate)
		shstate = shscan->sortstate;
	else
	{
		shstate = sortheap_beginsort(relation, sscan->rs_snapshot, NULL, SHOP_SEQSCAN, NULL);
		shscan->sortstate = shstate;
	}

	if (shscan->activedsets == -1)
	{
		TapeSetsStatus s0 = tapesets_getstatus_forread(TAPESETSSTATE(shstate, 0));
		TapeSetsStatus s1 = tapesets_getstatus_forread(TAPESETSSTATE(shstate, 1));

		if (s0 != TS_INITIAL && s1 != TS_INITIAL)
			shscan->activedsets = 2;
		else if (s0 != TS_INITIAL)
			shscan->activedsets = 0;
		else if (s1 != TS_INITIAL)
			shscan->activedsets = 1;
		else
		{
			sortheap_endsort(shstate);
			shscan->sortstate = NULL;
			ExecClearTuple(slot);
			return false;
		}
	}

	if (shscan->activedsets == 0 || shscan->activedsets == 1)
	{
		ExecClearTuple(slot);

		/* Fetch one tuple from target tapesets */
		tsstate = TAPESETSSTATE(shstate, shscan->activedsets);
		if (tapesets_getnexttuple(tsstate,
								  direction,
								  &stup,
								  slot,
								  shscan))
			return true;
	}
	else
	{
		/*
		 * We have two tapesets now, if planner require to keep order, we use
		 * a two-elements minheap to keep the order.
		 */
		if (shscan->keeporder)
		{
			/* allocate a two-elements minheap */
			if (shstate->minheapcount == 0)
			{
				shstate->minheapcount = 0;
				shstate->minheap =
					(SortTuple *)palloc(
						MAX_TAPESETS * sizeof(SortTuple));

				/* read the first tuple from each tapesets */
				for (i = 0; i < MAX_TAPESETS; i++)
				{
					tsstate = TAPESETSSTATE(shstate, i);
					tsstate->lastReturnedSlot =
						tapesets_getnexttuple(tsstate,
											  direction,
											  &stup,
											  NULL,
											  shscan);

					if (tsstate->lastReturnedSlot)
					{
						stup.tapeno = i;
						minheap_insert(shstate, shstate->minheap,
									   &shstate->minheapcount, &stup);
					}
				}
			}

			if (shstate->lastReturnedTuple != NULL)
			{
				int tapesetsno = shstate->minheap[0].tapeno;

				Assert(shstate->lastReturnedTuple == &shstate->minheap[0]);

				tsstate = TAPESETSSTATE(shstate, tapesetsno);
				tsstate->lastReturnedSlot =
					tapesets_getnexttuple(tsstate,
										  direction,
										  &stup,
										  NULL,
										  shscan);
				if (tsstate->lastReturnedSlot)
				{
					stup.tapeno = tapesetsno;
					minheap_replace_top(shstate, shstate->minheap,
										&shstate->minheapcount, &stup);
				}
				else
					minheap_delete_top(shstate, shstate->minheap,
									   &shstate->minheapcount);

				shstate->lastReturnedTuple = NULL;
			}

			/* fetch tuples from the minheap */
			if (shstate->minheapcount > 0)
			{
				shstate->lastReturnedTuple = &shstate->minheap[0];

				tsstate =
					TAPESETSSTATE(shstate, shstate->lastReturnedTuple->tapeno);

				ExecCopySlot(slot, tsstate->lastReturnedSlot);

				return true;
			}
		}
		else
		{
			/* Unnecessary to keep order, scan tuples from tapesets one by one */
			while (true)
			{
				/* Fetch one tuple from current tapesets */
				tsstate = TAPESETSSTATE(shstate, shstate->cur_readtapesets);
				if (tapesets_getnexttuple(tsstate,
										  direction,
										  &stup,
										  slot,
										  shscan))
					return true;

				/* Switch to next tapesets */
				if (!sortheap_advance_next_tapesets(shstate))
					break;
			}
		}
	}

	sortheap_endsort(shstate);
	shscan->sortstate = NULL;
	ExecClearTuple(slot);
	return false;
}

/*
 * Called by the resource owner cleanup callback to flush any tuples lefts in
 * the sorting array.
 */
void sortheap_insert_finish(Relation relation)
{
	bool trigger_merge = false;
	SortHeapState *state = NULL;

	if (relation->rd_amcache)
		state = (SortHeapState *)relation->rd_amcache;
	else
	{
		ListCell *lc;

		/* Find the shstate in the cache list */
		foreach (lc, sortheapstate_cachelist)
		{
			state = (SortHeapState *)lfirst(lc);

			if (state->relation == relation &&
				state->relation->rd_id == relation->rd_id)
				break;

			state = NULL;
		}

		if (state == NULL)
			return;
	}

	trigger_merge = sortheap_endsort(state);
	relation->rd_amcache = NULL;

	/* We might fail because exceeding the max_parallel_worker */
	if (trigger_merge)
	{
		PG_TRY();
		{
			LaunchSortHeapMergeWorker(RelationGetRelid(relation),
									  ShareUpdateExclusiveLock,
									  false, /* on qd */
									  false, /* vacuum full */
									  false, /* force merge */
									  false /* wait */);
		}
		PG_CATCH();
		{
			/* ignore the error */
		}
		PG_END_TRY();
	}
}

/*
 * select one tapesets to insert, we need to make sure no tuples are inserted
 * into one tapesets that is performing merging.
 */
void sortheap_insert(Relation relation, TupleTableSlot *slot,
					 CommandId cid, int options,
					 TransactionId xid)
{
	SortHeapState *shstate;

	/* Make sure we have a sort state */
	if (!relation->rd_amcache)
		relation->rd_amcache = (void *)
			sortheap_beginsort(relation, NULL, NULL, SHOP_INSERT, NULL);

	shstate = (SortHeapState *)relation->rd_amcache;

	/* Select one tapesets to insert */
	if (shstate->cur_inserttapesets == -1)
	{
		int tsno;
		Page shmetapage;
		SortHeapMeta *shmetad;
		TapeSetsState *tsstate = NULL;

		/* Get the preferred tapesets */
		LockBuffer(shstate->metabuf, BUFFER_LOCK_SHARE);
		shmetapage = BufferGetPage(shstate->metabuf);
		shmetad = (SortHeapMeta *)PageGetContents(shmetapage);
		tsno = shmetad->insertts;
		LockBuffer(shstate->metabuf, BUFFER_LOCK_UNLOCK);

		while (true)
		{
			bool found = true;

			tsno = tsno % MAX_TAPESETS;
			tsstate = TAPESETSSTATE(shstate, tsno);

			/*
			 * MERGE tries to lock the tapesets in ExclusiveLock mode and
			 * INSERT will lock the tapesets in RowExclusiveLock mode, so on
			 * the same tapesets, MERGE and INSERT cannot run concurrently.
			 */
			while (!tapesets_try_lock(tsstate, RowExclusiveLock))
			{
				/*
				 * The lock cannot be acquired because: 1) someone has started
				 * merging and will release lock in the unknown future. 2)
				 * someone is holding the lock to do check and will release
				 * lock very soon.
				 *
				 * For the first case, we break out to try next tapesets and
				 * we retry the same tapesets immediately for the second case.
				 */
				if (!sortheap_try_lock_merge(relation, ExclusiveLock))
				{
					/* Someone is merging, try next tapesets */
					tsno++;
					found = false;
					break;
				}
				else
				{
					sortheap_unlock_merge(relation, ExclusiveLock);
					/* retry locking current tapesets */
					continue;
				}
			}

			if (!found)
				continue;

			/*
			 * If the tapesets is merged but not vacuumed, we prefer not
			 * inserting to it. If we do that, it will make the vacuuming much
			 * harder because one tape may contain pages can be vacuumed and
			 * cannot be vacuumed.
			 */
			if (tapesets_getstatus_forwrite(tsstate, true) == TS_MERGE_SORTED)
			{
				tsno++;
				tapesets_unlock(tsstate, RowExclusiveLock);
				continue;
			}

			shstate->cur_inserttapesets = tsno;
			break;
		}

		if (shstate->cur_inserttapesets == -1)
			elog(FATAL, "unexpected error: cannot find tapesets to insert");
	}

	tapesets_insert(TAPESETSSTATE(shstate, shstate->cur_inserttapesets),
					slot, cid, options, xid);
	/* keep the tapesets INSERT lock until the end of transaction */
}

/*
 * Merge and vacuum a sort heap file.
 *
 * We first check whether there are tapesets that is merged sorted but not
 * vacuumed. If found one, we vacuum it and return;
 *
 * For Merge, All tuples have been provided; finish the sort.
 * We need to make sure:
 * 1. there is only one process doing merge at the same time.
 * 2. INSERT and MERGE are not performed on the same tuplesets at the same time.
 * 3. The last insert transaction is visible to my snapshot.
 *
 * We didn't perform vacuum on the tapesets which just done the merge sorting,
 * it's unsaft to vacuum unless the merge transaction is committed.
 *
 * Return true if we want the caller to commit current transactions and retry
 * with a new transaction and new snapshot.
 */
bool sortheap_performmerge(Relation relation, bool force)
{
	int i;
	int candidate = 0;
	bool worth_retry = false;
	Page shmetapage;
	SortHeapMeta *shmetad;
	SortHeapState *shstate;

	/* Make sure we have a sort state */
	if (!relation->rd_amcache)
		relation->rd_amcache =
			sortheap_beginsort(relation, GetActiveSnapshot(), NULL, SHOP_MERGE, NULL);

	shstate = (SortHeapState *)relation->rd_amcache;

	/* Step1: vacuum any tapesets which just finished the merge sort */
	for (i = 0; i < MAX_TAPESETS; i++)
	{
		TapeSetsStatus status;
		TapeSetsState *tsstate = TAPESETSSTATE(shstate, i);

		/*
		 * Try to lock the merge operation on this file, if somesone is doing
		 * merge/insert, quit directly and tell the caller not retry.
		 */
		if (!sortheap_try_lock_merge(relation, ExclusiveLock))
		{
			sortheap_endsort(shstate);
			relation->rd_amcache = NULL;
			return false;
		}

		status = tapesets_getstatus_forwrite(tsstate, true);
		if (status != TS_MERGE_SORTED)
			continue;

		/*
		 * Check whether the last merge xid is visible to all queries.
		 *
		 * If not, quit immediately because we only want to keep at most one
		 * tapesets to be merging.
		 */
		if (!tapesets_vacuum_validate_mergexid(tsstate))
		{
			sortheap_unlock_merge(relation, ExclusiveLock);
			sortheap_endsort(shstate);
			relation->rd_amcache = NULL;

			/*
			 * Hint the caller to retry again with a new transaction and a new
			 * snapshot.
			 */
			return true;
		}

		/*
		 * Do the vacuum. 1. Recycle the tapes except the result Tape. 2.
		 * Change the status to TS_BUILDRUNS.
		 */
		tapesets_performvacuum(TAPESETSSTATE(shstate, i));

		/*
		 * Vacuum a tapesets once at a time and return immediately.
		 *
		 * It is only possible that one tapesets is in TS_MERGE_SORTED status
		 * and do not eager to merge another tapesets because current vacuum
		 * transaction is not committed, if we merge another tapesets, there
		 * will be no tapesets for other backends to insert
		 */
		sortheap_unlock_merge(relation, ExclusiveLock);
		sortheap_endsort(shstate);
		relation->rd_amcache = NULL;

		/*
		 * Hint the caller to commit current transaction and retry again with
		 * a new transaction and a new snapshot.
		 */
		return true;
	}

	if (!force)
	{
		LockBuffer(shstate->metabuf, BUFFER_LOCK_SHARE);
		shmetapage = BufferGetPage(shstate->metabuf);
		shmetad = (SortHeapMeta *)PageGetContents(shmetapage);
		candidate = shmetad->mergets == -1 ? 0 : shmetad->mergets;
		LockBuffer(shstate->metabuf, BUFFER_LOCK_UNLOCK);
	}

	/* Step2: Try to do merge sorting on tapesets */
	for (i = candidate; i < MAX_TAPESETS; i++)
	{
		bool merged = false;
		TapeSetsStatus status;
		TapeSetsState *tsstate;
		TapeSetsMeta *tsmetad;

		tsstate = TAPESETSSTATE(shstate, i);

		/*
		 * MERGE tries to lock the tapesets in ExclusiveLock mode and INSERT
		 * will lock the tapesets in RowExclusiveLock mode, so on the same
		 * tapesets, MERGE and INSERT cannot run concurrently.
		 */
		if (!tapesets_try_lock(tsstate, ExclusiveLock))
		{
			/*
			 * the lock cannot be acquired because: 1) someone is INSERT in
			 * this tape. 2) someone is MERGE in this tape. 3) someone
			 * acquired the lock and checking.
			 */
			if (sortheap_try_lock_merge(relation, ExclusiveLock))
			{
				/* No one is merging, tell the caller try again. */
				sortheap_unlock_merge(relation, ExclusiveLock);
				worth_retry = true;
				continue;
			}
			else
			{
				/*
				 * If someone is merging, quit immediately and tell caller not
				 * retry
				 */
				sortheap_endsort(shstate);
				relation->rd_amcache = NULL;
				return false;
			}
		}

		/* Hint other backends that there is already one process merging */
		sortheap_try_lock_merge(relation, ExclusiveLock);

		/* Check the latest status of tapesets */
		status = tapesets_getstatus_forwrite(tsstate, true);
		if (status != TS_BUILDRUNS)
		{
			sortheap_unlock_merge(relation, ExclusiveLock);
			tapesets_unlock(tsstate, ExclusiveLock);
			continue;
		}

		/* Caution: get the newest metadata of tapesets */
		tsmetad = tapesets_getmeta_disk(tsstate);

		/*
		 * check whether the last INSERT transaction of this tapesets is
		 * visible to my snapshot
		 */
		if (!tapesets_merge_validate_xid(tsmetad->lastInsertXid))
		{
			sortheap_unlock_merge(relation, ExclusiveLock);
			tapesets_unlock(tsstate, ExclusiveLock);
			worth_retry = true;
			continue;
		}

		/*
		 * check whether the last VACUUM transaction of this tapsets is
		 * visible to my snapshot, so we can use resultTape for merging.
		 */
		if (!tapesets_merge_validate_xid(tsmetad->lastVacuumXid))
		{
			sortheap_unlock_merge(relation, ExclusiveLock);
			tapesets_unlock(tsstate, ExclusiveLock);
			worth_retry = true;
			continue;
		}

		shstate->cur_inserttapesets = i;
		merged = tapesets_performmerge(tsstate);

		/*
		 * merged is false when there is only one run in current tapesets, we
		 * can try next tapesets in this case
		 */
		if (!merged)
		{
			sortheap_unlock_merge(relation, ExclusiveLock);
			tapesets_unlock(tsstate, ExclusiveLock);
			continue;
		}

		/*
		 * We want to release the lock in the end of transaction, so there is
		 * no in-progress transaction existing within the tapesets. That is
		 * why the lock is not release here immediately.
		 *
		 * This is also why we quit earlier without merging next tapesets to
		 * avoid the merge transaction span two tapesets.
		 */

		/* We want the lock to be released in the end of transaction */
		sortheap_endsort(shstate);
		relation->rd_amcache = NULL;

		/*
		 * Hint the caller to commit current transaction and retry again with
		 * a new transaction and a new snapshot, so we can vacuum the tape
		 * sort which just be merged.
		 */
		return true;
	}

	sortheap_endsort(shstate);
	relation->rd_amcache = NULL;

	return worth_retry;
}

void sortheap_fetch_details(Relation relation, StringInfoData *info)
{
	int i;
	SortHeapState *state;

	state = sortheap_beginsort(relation, GetActiveSnapshot(), NULL, SHOP_SEQSCAN, NULL);

	appendStringInfo(info, "gp_segment_id: %d, %s,",
					 GpIdentity.segindex,
					 get_rel_name(relation->rd_id));

	for (i = 0; i < MAX_TAPESETS; i++)
	{
		TapeSetsState *tsstate = state->tapesetsstates[i];
		TapeSetsMeta *tsmetad = tapesets_getmeta_snapshot(tsstate);

		appendStringInfo(info,
						 "tapesets[%d]: status: %s, totalRuns %d, level %d\n",
						 i,
						 tapesets_statusstr(tapesets_getstatus_forread(tsstate)),
						 tsmetad->currentRun,
						 tsmetad->level);
	}

	sortheap_endsort(state);
}

/*-------------------------------------------------------------------------
 *
 * sortheapam_handler.c
 *	  sort heap access method code
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/sortheap/sortheapam_handler.c
 *
 *
 * NOTES
 *	  This files wires up the lower level sortheapam.c et al routines with the
 *	  tableam abstraction.
 *
 *	  Sort heap access method is an advanced access method based on heap, it
 *	  implements a powerful sorting algorithm internal to guarantee the data is
 *	  (continuously merge) sorted. It also provides an internal index on the
 *	  sorting keys with very fast build/query speed, meanwhile, a column store
 *	  and compression are also implemented based on TOAST. Sort heap also
 *	  inherits most of great properties of heap including buffer, mvcc etc.
 *
 * 	  This file is the micro-modified version of heapam_handler.c
 *-------------------------------------------------------------------------
 */

#include "sortheap.h"
#include "sortheap_columnstore.h"
#include "sortheap_tapesets.h"
#include "sortheap_tape.h"

/* ------------------------------------------------------------------------
 * Slot related callbacks for sort heap AM
 * ------------------------------------------------------------------------
 */
static const TupleTableSlotOps *
sortheapam_slot_callbacks(Relation relation)
{
	return &TTSOpsHeapTuple;
}

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for sort heap AM
 * ------------------------------------------------------------------------
 */
static IndexFetchTableData *
sortheapam_index_fetch_begin(Relation rel)
{
	IndexFetchSortHeapData *shscan;
	int nattrs;

	shscan = palloc0(sizeof(IndexFetchSortHeapData));
	shscan->xs_base.rel = rel;
	shscan->xs_cbuf = InvalidBuffer;

	shscan->sortstate = sortheap_beginsort(rel, GetActiveSnapshot(),
										   NULL, SHOP_INDEXSCAN, NULL);

	/* Initialize the compressed tuple descr */
	nattrs = RelationGetDescr(rel)->natts;

	shscan->columnstore_desc =
		build_columnstore_tupledesc(rel,
									shscan->sortstate->indexrel,
									&shscan->columnchunk_descs,
									NULL);
	shscan->columnchunkcontext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "columnchunk context",
							  ALLOCSET_DEFAULT_SIZES);
	shscan->cached = false;
	shscan->datum_cache = palloc0(sizeof(Datum *) * nattrs);
	shscan->isnull_cache = palloc0(sizeof(bool *) * nattrs);

	return &shscan->xs_base;
}

static void
sortheapam_index_fetch_reset(IndexFetchTableData *scan)
{
	IndexFetchSortHeapData *shscan = (IndexFetchSortHeapData *)scan;
	int i;

	if (BufferIsValid(shscan->xs_cbuf))
	{
		ReleaseBuffer(shscan->xs_cbuf);
		shscan->xs_cbuf = InvalidBuffer;
	}

	for (i = 0; i < MAX_TAPESETS; i++)
	{
		TapeSetsState *tsstate = TAPESETSSTATE(shscan->sortstate, i);

		TapeSetsMeta *tsmetad = tapesets_getmeta_snapshot(tsstate);

		/* Reset current reading tape to start pos */
		tsstate->cur_readtape = 0;
		tsstate->cur_readrun = 0;
		if (tapesets_getstatus_forread(tsstate) >= TS_MERGE_SORTED)
			tsstate->cur_readtape = tsmetad->resultTape;
	}
	shscan->sortstate->cur_readtapesets = 0;
}

static void
sortheapam_index_fetch_end(IndexFetchTableData *scan)
{
	IndexFetchSortHeapData *shscan = (IndexFetchSortHeapData *)scan;

	if (BufferIsValid(shscan->xs_cbuf))
	{
		ReleaseBuffer(shscan->xs_cbuf);
		shscan->xs_cbuf = InvalidBuffer;
	}

	if (shscan->sortstate && shscan->sortstate->op == SHOP_INDEXSCAN)
	{
		sortheap_endsort(shscan->sortstate);
		shscan->sortstate = NULL;
	}

	pfree(shscan->datum_cache);
	pfree(shscan->isnull_cache);

	pfree(shscan);
}

static void
sortheapam_index_fetch_extractcolumns(IndexFetchTableData *scan,
									  List *targetlist,
									  List *qual)
{
	IndexFetchSortHeapData *shscan = (IndexFetchSortHeapData *)scan;
	AttrNumber natts = RelationGetNumberOfAttributes(scan->rel);
	bool found = false;
	bool *proj;
	int i;

	proj = palloc0(natts * sizeof(bool));

	found |= extractcolumns_from_node(scan->rel, (Node *)targetlist, proj, natts);
	found |= extractcolumns_from_node(scan->rel, (Node *)qual, proj, natts);

	/*
	 * In some cases (for example, count(*)), no columns are specified. We
	 * always scan the first column.
	 */
	if (!found)
		proj[0] = true;

	shscan->proj_atts = palloc0(sizeof(AttrNumber) * natts);

	for (i = 0; i < natts; i++)
	{
		if (proj[i])
			shscan->proj_atts[shscan->n_proj_atts++] = i;
	}
}

static bool
sortheapam_index_fetch_tuple(struct IndexFetchTableData *scan,
							 ItemPointer tid,
							 Snapshot snapshot,
							 TupleTableSlot *slot,
							 bool *call_again, bool *all_dead)
{
	IndexFetchSortHeapData *shscan = (IndexFetchSortHeapData *)scan;
	bool got_heap_tuple;
	int i;
	int attno;
	int natts;
	int prev_chunkno = -1;
	Relation relation = shscan->xs_base.rel;
	ColumnChunk *prev_chunk = NULL;

	natts = RelationGetNumberOfAttributes(relation);

	/* We can skip the buffer-switching logic if we're in mid-HOT chain. */
	if (!*call_again)
	{
		/* Switch to correct buffer if we don't have it already */

		shscan->xs_cbuf = ReleaseAndReadBuffer(shscan->xs_cbuf,
											   shscan->xs_base.rel,
											   ItemPointerGetBlockNumber(tid));

		/* Obtain share-lock on the buffer so we can examine visibility */
		LockBuffer(shscan->xs_cbuf, BUFFER_LOCK_SHARE);
		got_heap_tuple = heap_hot_search_buffer(tid,
												shscan->xs_base.rel,
												shscan->xs_cbuf,
												snapshot,
												&shscan->columnstore_tuple,
												all_dead,
												!*call_again);
		shscan->columnstore_tuple.t_self = *tid;
		LockBuffer(shscan->xs_cbuf, BUFFER_LOCK_UNLOCK);

		if (got_heap_tuple)
		{
			if (!IsColumnStoreTuple(&shscan->columnstore_tuple))
			{
				ExecClearTuple(slot);
				slot->tts_tableOid = RelationGetRelid(scan->rel);
				ExecStoreHeapTuple(&shscan->columnstore_tuple, slot, false);
				*call_again = false;
				return true;
			}
			else
				*call_again = true;
		}
		else
		{
			*call_again = false;
			return false;
		}
	}

	/* Start to handle compressed column stored tuple */
	if (!shscan->cached)
	{
		int chunksize;
		Datum d;
		bool isnull;
		SortHeapState *shstate;
		MemoryContext oldcontext;

		shstate = shscan->sortstate;
		chunksize = ColumnChunkSize(relation);

		MemoryContextReset(shscan->columnchunkcontext);
		oldcontext = MemoryContextSwitchTo(shscan->columnchunkcontext);

		d = heap_getattr(&shscan->columnstore_tuple, 1,
						 shscan->columnstore_desc,
						 &isnull);
		shscan->size_cache = DatumGetInt32(d);

		/* decompress or detoast tuples from the chunks */
		for (i = 0; i < shscan->n_proj_atts; i++)
		{
			int attnoinchunk;
			int chunkno;
			TupleDesc chunkdesc;
			ColumnChunk *chunk;

			attno = shscan->proj_atts[i];
			attnoinchunk = attno % chunksize;
			chunkno = attno / chunksize;
			chunkdesc = shscan->columnchunk_descs[chunkno];

			/* If it is the index key, don't need to deform the chunk */
			if (attno == (shstate->indexinfo->ii_IndexAttrNumbers[0] - 1))
			{
				int j;

				d = heap_getattr(&shscan->columnstore_tuple, 2,
								 shscan->columnstore_desc,
								 &isnull);

				shscan->datum_cache[attno] =
					palloc(sizeof(Datum) * shscan->size_cache);
				shscan->isnull_cache[attno] =
					palloc(sizeof(bool) * shscan->size_cache);

				for (j = 0; j < shscan->size_cache; j++)
				{
					shscan->datum_cache[attno][j] = d;
					shscan->isnull_cache[attno][j] = isnull;
				}

				continue;
			}

			/* Get the column chunk */
			if (chunkno != prev_chunkno)
			{
				d = heap_getattr(&shscan->columnstore_tuple,
								 shstate->nkeys + chunkno + 2,
								 shscan->columnstore_desc,
								 &isnull);
				chunk = (ColumnChunk *)DatumGetByteaP(d);

				prev_chunkno = chunkno;
				prev_chunk = chunk;
			}
			else
				chunk = prev_chunk;

			deformColumnChunk(chunk,
							  chunkdesc,
							  attnoinchunk,
							  shscan->size_cache,
							  &shscan->datum_cache[attno],
							  &shscan->isnull_cache[attno]);
		}

		MemoryContextSwitchTo(oldcontext);

		shscan->cached = true;
		shscan->cur_cache = 0;
	}

	ExecClearTuple(slot);

	if (natts > shscan->n_proj_atts)
		MemSet(slot->tts_isnull, true, natts * sizeof(bool));

	for (i = 0; i < shscan->n_proj_atts; i++)
	{
		attno = shscan->proj_atts[i];

		slot->tts_values[attno] =
			shscan->datum_cache[attno][shscan->cur_cache];
		slot->tts_isnull[attno] =
			shscan->isnull_cache[attno][shscan->cur_cache];
	}

	shscan->cur_cache++;

	if (shscan->cur_cache == shscan->size_cache)
	{
		*call_again = false;
		shscan->cached = false;
		shscan->cur_cache = 0;
		prev_chunk = NULL;
		prev_chunkno = -1;
	}

	slot->tts_tableOid = RelationGetRelid(scan->rel);
	ExecStoreVirtualTuple(slot);
	return true;
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for sort heap AM
 * ------------------------------------------------------------------------
 */

static void
sortheapam_get_latest_tid(TableScanDesc sscan,
						  ItemPointer tid)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static bool
sortheapam_fetch_row_version(Relation relation,
							 ItemPointer tid,
							 Snapshot snapshot,
							 TupleTableSlot *slot)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static bool
sortheapam_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static bool
sortheapam_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
									Snapshot snapshot)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static TransactionId
sortheapam_compute_xid_horizon_for_tuples(Relation rel,
										  ItemPointerData *tids,
										  int nitems)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for sort heap AM.
 * ----------------------------------------------------------------------------
 */

static void
sortheapam_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid,
						int options, BulkInsertState bistate)
{
	TransactionId xid = GetCurrentTransactionId();

	/* If it's an empty slot, it's the end of insert */
	if (TTS_EMPTY(slot))
	{
		sortheap_insert_finish(relation);
		return;
	}

	sortheap_insert(relation, slot, cid, options, xid);

	/*
	 * Generate a fake tid for slot, sortheap_insert doesn't put the tuple to
	 * the block immediately, so the tid cannot be assigned. We return a fake
	 * tid to the caller to avoid the assertion.
	 */
	slot->tts_tid.ip_posid = 1;
}

static void
sortheapam_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples,
						CommandId cid, int options, BulkInsertState bistate)
{
	int i;

	for (i = 0; i < ntuples; i++)
		sortheapam_tuple_insert(relation, slots[i], cid, options, bistate);
}

static void
sortheapam_tuple_insert_speculative(Relation relation, TupleTableSlot *slot,
									CommandId cid, int options,
									BulkInsertState bistate, uint32 specToken)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static void
sortheapam_tuple_complete_speculative(Relation relation, TupleTableSlot *slot,
									  uint32 specToken, bool succeeded)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static TM_Result
sortheapam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid,
						Snapshot snapshot, Snapshot crosscheck, bool wait,
						TM_FailureData *tmfd, bool changingPart)
{
	elog(ERROR, "Tuples cannot be deleted in this version");
}

static TM_Result
sortheapam_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot,
						CommandId cid, Snapshot snapshot, Snapshot crosscheck,
						bool wait, TM_FailureData *tmfd,
						LockTupleMode *lockmode, bool *update_indexes)
{
	elog(ERROR, "Tuples cannot be updated in this version");
}

static TM_Result
sortheapam_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
					  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
					  LockWaitPolicy wait_policy, uint8 flags,
					  TM_FailureData *tmfd)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static void
sortheapam_finish_bulk_insert(Relation relation, int options)
{
	/*
	 * If we skipped writing WAL, then we need to sync the heap (but not
	 * indexes since those use WAL anyway / don't go through tableam)
	 */
	if (options & HEAP_INSERT_SKIP_WAL)
		heap_sync(relation);
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for sort heap AM.
 * ------------------------------------------------------------------------
 */

static void
sortheapam_relation_set_new_filenode(Relation rel,
									 const RelFileNode *newrnode,
									 char persistence,
									 TransactionId *freezeXid,
									 MultiXactId *minmulti)
{
	SMgrRelation srel;
	SortHeapPageOpaqueData *special;
	SortHeapMeta *metad;
	Page metapg;

	/*
	 * Initialize to the minimum XID that could put tuples in the table. We
	 * know that no xacts older than RecentXmin are still running, so that
	 * will do.
	 */
	*freezeXid = RecentXmin;

	/*
	 * Similarly, initialize the minimum Multixact to the first value that
	 * could possibly be stored in tuples in the table.  Running transactions
	 * could reuse values from their local cache, so we are careful to
	 * consider all currently running multis.
	 *
	 * XXX this could be refined further, but is it worth the hassle?
	 */
	*minmulti = GetOldestMultiXactId();

	srel = RelationCreateStorage(*newrnode, persistence, SMGR_MD);

	/*
	 * If required, set up an init fork for an unlogged table so that it can
	 * be correctly reinitialized on restart.  An immediate sync is required
	 * even if the page has been logged, because the write did not go through
	 * shared_buffers and therefore a concurrent checkpoint may have moved the
	 * redo pointer past our xlog record.  Recovery may as well remove it
	 * while replaying, for example, XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE
	 * record. Therefore, logging is necessary even if wal_level=minimal.
	 */
	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		Assert(rel->rd_rel->relkind == RELKIND_RELATION ||
			   rel->rd_rel->relkind == RELKIND_MATVIEW ||
			   rel->rd_rel->relkind == RELKIND_TOASTVALUE ||
			   rel->rd_rel->relkind == RELKIND_AOSEGMENTS ||
			   rel->rd_rel->relkind == RELKIND_AOVISIMAP ||
			   rel->rd_rel->relkind == RELKIND_AOBLOCKDIR);
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrnode, INIT_FORKNUM);
		smgrimmedsync(srel, INIT_FORKNUM);
	}

	/* Construct metapage. */
	metapg = (Page)palloc(BLCKSZ);
	PageInit(metapg, BLCKSZ, sizeof(SortHeapPageOpaqueData));
	metad = (SortHeapMeta *)PageGetContents(metapg);

	metad->meta_magic = SORTHEAP_MAGIC;
	metad->firstFreePagesRoot = InvalidBlockNumber;
	metad->nBlocksAllocated = 1;
	metad->nBlocksWritten = 1;
	metad->mergets = -1;
	metad->tapesets[0] = InvalidBlockNumber;
	metad->tapesets[1] = InvalidBlockNumber;

	special = (SortHeapPageOpaqueData *)PageGetSpecialPointer(metapg);
	special->type = SORTHEAP_METAPAGE;

	/*
	 * Set pd_lower just past the end of the metadata.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader)metapg)->pd_lower =
		((char *)metad + sizeof(SortHeapMeta)) - (char *)metapg;

	PageSetChecksumInplace(metapg, SORTHEAP_METABLOCK);

	smgrwrite(srel, MAIN_FORKNUM, SORTHEAP_METABLOCK, (char *)metapg, true);

	/*
	 * An immediate sync is required even if we xlog'd the page, because the
	 * write did not go through shared_buffers and therefore a concurrent
	 * checkpoint may have moved the redo pointer past our xlog record.
	 */
	smgrimmedsync(srel, MAIN_FORKNUM);

	smgrclose(srel);
}

static void
sortheapam_relation_nontransactional_truncate(Relation rel)
{
	SortHeapMeta *metad;
	Page metapg;
	SortHeapPageOpaqueData *special;

	/* First truncate the relation */
	RelationTruncate(rel, 0);

	/* Reconstruct metapage. */
	metapg = (Page)palloc(BLCKSZ);
	PageInit(metapg, BLCKSZ, sizeof(SortHeapPageOpaqueData));
	metad = (SortHeapMeta *)PageGetContents(metapg);

	metad->meta_magic = SORTHEAP_MAGIC;
	metad->firstFreePagesRoot = InvalidBlockNumber;
	metad->nBlocksAllocated = 1;
	metad->nBlocksWritten = 1;
	metad->mergets = -1;
	metad->tapesets[0] = InvalidBlockNumber;
	metad->tapesets[1] = InvalidBlockNumber;

	special = (SortHeapPageOpaqueData *)PageGetSpecialPointer(metapg);
	special->type = SORTHEAP_METAPAGE;

	/*
	 * Set pd_lower just past the end of the metadata.  This is essential,
	 * because without doing so, metadata will be lost if xlog.c compresses
	 * the page.
	 */
	((PageHeader)metapg)->pd_lower =
		((char *)metad + sizeof(SortHeapMeta)) - (char *)metapg;

	PageSetChecksumInplace(metapg, SORTHEAP_METABLOCK);

	smgrwrite(rel->rd_smgr, MAIN_FORKNUM, SORTHEAP_METABLOCK, (char *)metapg, true);

	/*
	 * An immediate sync is required even if we xlog'd the page, because the
	 * write did not go through shared_buffers and therefore a concurrent
	 * checkpoint may have moved the redo pointer past our xlog record.
	 */
	smgrimmedsync(rel->rd_smgr, MAIN_FORKNUM);
}

static void
sortheapam_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static void
sortheapam_relation_copy_for_cluster(Relation OldHeap, Relation NewHeap,
									 Relation OldIndex, bool use_sort,
									 TransactionId OldestXmin,
									 TransactionId *xid_cutoff,
									 MultiXactId *multi_cutoff,
									 double *num_tuples,
									 double *tups_vacuumed,
									 double *tups_recently_dead)
{
	int i;
	int minheapcount = 0;
	SortTuple stup;
	SortTuple *minheap;
	BlockNumber tapeheader;
	SortHeapState *oldstate;
	SortHeapState *newstate;
	TapeScanDesc tapescandesc;
	TapeSetsState *newtsstate;
	TapeSetsState *oldtsstate;
	TapeSetsMeta *newtsmetad;
	MemoryContext context_ts0;
	MemoryContext context_ts1;
	int resulttapes[MAX_TAPESETS];

	if (Gp_role == GP_ROLE_DISPATCH)
		return;

retry:
	/* Force a merge on the old table */
	sortheap_performmerge(OldHeap, true);

	/* Initialize the read states from the old table */
	oldstate = sortheap_beginsort(OldHeap, GetTransactionSnapshot(), NULL, SHOP_SEQSCAN, NULL);

	for (i = 0; i < MAX_TAPESETS; i++)
	{
		TapeSetsMeta *oldtsmetad;
		TapeSetsStatus status;

		oldtsstate = TAPESETSSTATE(oldstate, i);
		oldtsmetad = tapesets_getmeta_snapshot(oldtsstate);
		status = tapesets_getstatus_forread(oldtsstate);

		if (status >= TS_MERGE_SORTED)
			resulttapes[i] = oldtsmetad->resultTape;
		else if (status == TS_BUILDRUNS &&
				 oldtsmetad->currentRun == 1)
			resulttapes[i] = 0;
		else if (status == TS_INITIAL)
			resulttapes[i] = 0;
		else
		{
			/* Sleep 10 ms */
			pg_usleep(10 * 1000);
			sortheap_endsort(oldstate);
			goto retry;
		}
	}

	/* Select tuples from the result Tape to the new table */
	newstate = sortheap_beginsort(NewHeap, GetTransactionSnapshot(), oldstate->indexrel, SHOP_INSERT, (void *)OldHeap);
	newtsstate = TAPESETSSTATE(newstate, 0);

	init_tapes(newtsstate);

	LockBuffer(newtsstate->metabuf, BUFFER_LOCK_EXCLUSIVE);
	newtsmetad = tapesets_getmeta_disk(newtsstate);
	newtsmetad->currentRun++;
	newtsmetad->tapes[0].runs++;
	newtsmetad->tapes[0].dummy--;
	select_newtape(newtsmetad);
	MarkBufferDirty(newtsstate->metabuf);

	/* XLOG stuff */
	{
		XLogRecPtr recptr;
		Page page;

		recptr = log_full_page_update(newtsstate->metabuf);
		page = BufferGetPage(newtsstate->metabuf);
		PageSetLSN(page, recptr);
	}

	LockBuffer(newtsstate->metabuf, BUFFER_LOCK_UNLOCK);

	tapeheader = tape_getheader(newtsstate, 0, true, true);
	newtsstate->insertstate =
		tape_create_insertstate(newtsstate,
								GetCurrentTransactionId(),
								GetCurrentCommandId(true),
								tapeheader,
								0,
								true);

	newstate->cur_inserttapesets = 0;

	minheap = (SortTuple *)palloc(MAX_TAPESETS * sizeof(SortTuple));

	/* read the first tuple from each tapesets */
	for (i = 0; i < MAX_TAPESETS; i++)
	{
		oldtsstate = TAPESETSSTATE(oldstate, i);
		tapescandesc = tape_get_scandesc(oldtsstate, resulttapes[i], 0, NULL);

		if (FETCHTUP(oldstate, tapescandesc, &stup, NULL, true))
		{
			stup.tapeno = i;
			minheap_insert(oldstate, minheap, &minheapcount, &stup);
		}
	}

	context_ts0 = AllocSetContextCreate(CurrentMemoryContext,
										"sortheap vacuum full ts0",
										ALLOCSET_DEFAULT_SIZES);
	context_ts1 = AllocSetContextCreate(CurrentMemoryContext,
										"sortheap vacuum full ts1",
										ALLOCSET_DEFAULT_SIZES);

	while (minheapcount > 0)
	{
		int srctapesets;
		Datum datums[MaxHeapAttributeNumber + 3];
		bool isnulls[MaxHeapAttributeNumber + 3];
		HeapTuple htup;
		MemoryContext tmpcontext;

		CHECK_FOR_INTERRUPTS();

		/* write the tuple to destTape */
		srctapesets = minheap[0].tapeno;
		htup = minheap[0].tuple;

		if (srctapesets == 0)
			tmpcontext = context_ts0;
		else
			tmpcontext = context_ts1;

		if (!HeapTupleHasExternal(htup))
			WRITETUP(newstate, &minheap[0]);
		else
		{
			int natts;
			TupleDesc tupdesc;
			MemoryContext oldcontext;

			if (IsColumnStoreTuple(htup))
				tupdesc = newtsstate->insertstate->columnstore_desc;
			else
				tupdesc = RelationGetDescr(newtsstate->insertstate->relation);

			natts = tupdesc->natts;
			heap_deform_tuple(htup, tupdesc, datums, isnulls);

			MemoryContextReset(tmpcontext);
			oldcontext = MemoryContextSwitchTo(tmpcontext);

			for (i = 0; i < natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupdesc, i);
				struct varlena *new_value;

				new_value = (struct varlena *)DatumGetPointer(datums[i]);

				if (isnulls[i] || att->attlen != -1)
					continue;
				if (!VARATT_IS_EXTERNAL(new_value))
					continue;

				new_value = heap_tuple_fetch_attr(new_value);

				/* retoast it to the new toast relation */
				datums[i] =
					sortheap_toast_save_datum(NewHeap, PointerGetDatum(new_value));
			}

			minheap[0].tuple = heap_form_tuple(tupdesc, datums, isnulls);

			MemoryContextSwitchTo(oldcontext);

			/*
			 * HEAP_HASOID_OLD flag is eliminated, we use it to signal this is
			 * a compressed sortheap tuple
			 */
			minheap[0].tuple->t_data->t_infomask |= HEAP_HASOID_OLD;

			WRITETUP(newstate, &minheap[0]);
		}

		oldtsstate = TAPESETSSTATE(oldstate, srctapesets);
		tapescandesc =
			tape_get_scandesc(oldtsstate, resulttapes[srctapesets], 0, NULL);

		if (FETCHTUP(oldstate, tapescandesc, &stup, NULL, true))
		{
			stup.tapeno = srctapesets;
			minheap_replace_top(oldstate,
								minheap,
								&minheapcount,
								&stup);
		}
		else
		{
			minheap_delete_top(oldstate,
							   minheap,
							   &minheapcount);
		}

		*num_tuples += 1;
	}

	WRITETUP(newstate, NULL);

	tape_destroy_insertstate(newtsstate->insertstate);

	sortheap_endsort(oldstate);
	sortheap_endsort(newstate);
}

static bool
sortheapam_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								   BufferAccessStrategy bstrategy)
{
	HeapScanDesc hscan = (HeapScanDesc)scan;
	SortHeapPageOpaqueData *opaque;
	Page page;

	/*
	 * We must maintain a pin on the target page's buffer to ensure that
	 * concurrent activity - e.g. HOT pruning - doesn't delete tuples out from
	 * under us.  Hence, pin the page until we are done looking at it.  We
	 * also choose to hold sharelock on the buffer throughout --- we could
	 * release and re-acquire sharelock for each tuple, but since we aren't
	 * doing much work per tuple, the extra lock traffic is probably better
	 * avoided.
	 */
	hscan->rs_cblock = blockno;
	hscan->rs_cindex = FirstOffsetNumber;
	hscan->rs_cbuf = ReadBufferExtended(scan->rs_rd, MAIN_FORKNUM,
										blockno, RBM_NORMAL, bstrategy);
	LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(hscan->rs_cbuf);

	if (PageIsNew(page))
	{
		UnlockReleaseBuffer(hscan->rs_cbuf);
		hscan->rs_cbuf = InvalidBuffer;
		return false;
	}

	if (PageGetSpecialSize(page) !=
		MAXALIGN(sizeof(SortHeapPageOpaqueData)))
	{
		/* Is an internal BT page, skip */
		return false;
	}

	opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(page);

	if (opaque->type != SORTHEAP_DATAPAGE)
	{
		UnlockReleaseBuffer(hscan->rs_cbuf);
		hscan->rs_cbuf = InvalidBuffer;
		return false;
	}

	/* in heap all blocks can contain tuples, so always return true */
	return true;
}

static bool
sortheapam_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
								   double *liverows, double *deadrows,
								   TupleTableSlot *slot)
{
	Page targpage;
	HeapScanDesc hscan = (HeapScanDesc)scan;
	OffsetNumber maxoffset;
	HeapTupleTableSlot *hslot = (HeapTupleTableSlot *)slot;

	Assert(TTS_IS_HEAPTUPLE(slot));

	targpage = BufferGetPage(hscan->rs_cbuf);
	maxoffset = PageGetMaxOffsetNumber(targpage);

	/* Inner loop over all tuples on the selected page */
	for (; hscan->rs_cindex <= maxoffset; hscan->rs_cindex++)
	{
		ItemId itemid;
		HeapTuple targtuple = &hslot->tupdata;
		bool sample_it = false;

		itemid = PageGetItemId(targpage, hscan->rs_cindex);

		/*
		 * We ignore unused and redirect line pointers.  DEAD line pointers
		 * should be counted as dead, because we need vacuum to run to get rid
		 * of them.  Note that this rule agrees with the way that
		 * heap_page_prune() counts things.
		 */
		if (!ItemIdIsNormal(itemid))
		{
			if (ItemIdIsDead(itemid))
				*deadrows += 1;
			continue;
		}

		ItemPointerSet(&targtuple->t_self, hscan->rs_cblock, hscan->rs_cindex);

		targtuple->t_tableOid = RelationGetRelid(scan->rs_rd);
		targtuple->t_data = (HeapTupleHeader)PageGetItem(targpage, itemid);
		targtuple->t_len = ItemIdGetLength(itemid);

		/* FIXME */
		if (IsColumnStoreTuple(targtuple))
			continue;

		switch (HeapTupleSatisfiesVacuum(hscan->rs_base.rs_rd,
										 targtuple, OldestXmin,
										 hscan->rs_cbuf))
		{
		case HEAPTUPLE_LIVE:
			sample_it = true;
			*liverows += 1;
			break;

		case HEAPTUPLE_DEAD:
		case HEAPTUPLE_RECENTLY_DEAD:
			/* Count dead and recently-dead rows */
			*deadrows += 1;
			break;

		case HEAPTUPLE_INSERT_IN_PROGRESS:

			/*
			 * Insert-in-progress rows are not counted.  We assume that
			 * when the inserting transaction commits or aborts, it will
			 * send a stats message to increment the proper count.  This
			 * works right only if that transaction ends after we finish
			 * analyzing the table; if things happen in the other order,
			 * its stats update will be overwritten by ours.  However, the
			 * error will be large only if the other transaction runs long
			 * enough to insert many tuples, so assuming it will finish
			 * after us is the safer option.
			 *
			 * A special case is that the inserting transaction might be
			 * our own.  In this case we should count and sample the row,
			 * to accommodate users who load a table and analyze it in one
			 * transaction.  (pgstat_report_analyze has to adjust the
			 * numbers we send to the stats collector to make this come
			 * out right.)
			 */
			if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(targtuple->t_data)))
			{
				sample_it = true;
				*liverows += 1;
			}
			break;

		case HEAPTUPLE_DELETE_IN_PROGRESS:

			/*
			 * We count and sample delete-in-progress rows the same as
			 * live ones, so that the stats counters come out right if the
			 * deleting transaction commits after us, per the same
			 * reasoning given above.
			 *
			 * If the delete was done by our own transaction, however, we
			 * must count the row as dead to make pgstat_report_analyze's
			 * stats adjustments come out right.  (Note: this works out
			 * properly when the row was both inserted and deleted in our
			 * xact.)
			 *
			 * The net effect of these choices is that we act as though an
			 * IN_PROGRESS transaction hasn't happened yet, except if it
			 * is our own transaction, which we assume has happened.
			 *
			 * This approach ensures that we behave sanely if we see both
			 * the pre-image and post-image rows for a row being updated
			 * by a concurrent transaction: we will sample the pre-image
			 * but not the post-image.  We also get sane results if the
			 * concurrent transaction never commits.
			 */
			if (TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetUpdateXid(targtuple->t_data)))
				*deadrows += 1;
			else
			{
				sample_it = true;
				*liverows += 1;
			}
			break;

		default:
			elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
			break;
		}

		if (sample_it)
		{
			ExecForceStoreHeapTuple(targtuple, slot, false);
			hscan->rs_cindex++;

			/* note that we leave the buffer locked here! */
			return true;
		}
	}

	/* Now release the lock and pin on the page */
	UnlockReleaseBuffer(hscan->rs_cbuf);
	hscan->rs_cbuf = InvalidBuffer;

	/* also prevent old slot contents from having pin on page */
	ExecClearTuple(slot);

	return false;
}

/*
 * table_index_build_range_scan doesn't take effect for sortheap, however, it is
 * used to do a sortheap_btree sanity check
 */
static double
sortheapam_index_build_range_scan(Relation heapRelation,
								  Relation indexRelation,
								  IndexInfo *indexInfo,
								  bool allow_sync,
								  bool anyvisible,
								  bool progress,
								  BlockNumber start_blockno,
								  BlockNumber numblocks,
								  IndexBuildCallback callback,
								  void *callback_state,
								  TableScanDesc scan)
{
	/* Only support sortheap_btree */
	if (!external_amcheck(indexRelation->rd_rel->relam, SORTHEAPBTREEAMNAME))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sortheap table only support %s index",
						SORTHEAPBTREEAMNAME)));

	/* Only one index is allowed */
	if (list_length(RelationGetIndexList(heapRelation)) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sortheap table only support one index")));

	return 0;
}

static void
sortheapam_index_validate_scan(Relation heapRelation,
							   Relation indexRelation,
							   IndexInfo *indexInfo,
							   Snapshot snapshot,
							   ValidateIndexState *state)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the sort heap AM
 * ------------------------------------------------------------------------
 */

static uint64
sortheapam_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64 nblocks = 0;

	/* Open it at the smgr level if not already done */
	RelationOpenSmgr(rel);

	/* InvalidForkNumber indicates returning the size for all forks */
	if (forkNumber == InvalidForkNumber)
	{
		for (int i = 0; i < MAX_FORKNUM; i++)
			nblocks += smgrnblocks(rel->rd_smgr, i);
	}
	else
		nblocks = smgrnblocks(rel->rd_smgr, forkNumber);

	return nblocks * BLCKSZ;
}

static bool
sortheapam_relation_needs_toast_table(Relation rel)
{
	/*
	 * Unlike the heap, we always create a toast table for sort heap, a toast
	 * table will be used to implement compression of sort heap
	 */
	return true;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the sort heap AM
 * ------------------------------------------------------------------------
 */

/*
 * Keep the logic the same with heap, sort heap has no much difference with
 * the heap in the page level
 */
static void
sortheapam_estimate_rel_size(Relation rel, int32 *attr_widths,
							 BlockNumber *pages, double *tuples,
							 double *allvisfrac)
{
	BlockNumber curpages;
	BlockNumber relpages;
	double reltuples;
	BlockNumber relallvisible;
	double density;

	/* it has storage, ok to call the smgr */
	curpages = RelationGetNumberOfBlocks(rel);

	/* coerce values in pg_class to more desirable types */
	relpages = (BlockNumber)rel->rd_rel->relpages;
	reltuples = (double)rel->rd_rel->reltuples;
	relallvisible = (BlockNumber)rel->rd_rel->relallvisible;

	/*
	 * HACK: if the relation has never yet been vacuumed, use a minimum size
	 * estimate of 10 pages.  The idea here is to avoid assuming a
	 * newly-created table is really small, even if it currently is, because
	 * that may not be true once some data gets loaded into it.  Once a vacuum
	 * or analyze cycle has been done on it, it's more reasonable to believe
	 * the size is somewhat stable.
	 *
	 * (Note that this is only an issue if the plan gets cached and used again
	 * after the table has been filled.  What we're trying to avoid is using a
	 * nestloop-type plan on a table that has grown substantially since the
	 * plan was made.  Normally, autovacuum/autoanalyze will occur once enough
	 * inserts have happened and cause cached-plan invalidation; but that
	 * doesn't happen instantaneously, and it won't happen at all for cases
	 * such as temporary tables.)
	 *
	 * We approximate "never vacuumed" by "has relpages = 0", which means this
	 * will also fire on genuinely empty relations.  Not great, but
	 * fortunately that's a seldom-seen case in the real world, and it
	 * shouldn't degrade the quality of the plan too much anyway to err in
	 * this direction.
	 *
	 * If the table has inheritance children, we don't apply this heuristic.
	 * Totally empty parent tables are quite common, so we should be willing
	 * to believe that they are empty.
	 */
	if (curpages < 10 &&
		relpages == 0 &&
		!rel->rd_rel->relhassubclass)
		curpages = 10;

	/* report estimated # pages */
	*pages = curpages;
	/* quick exit if rel is clearly empty */
	if (curpages == 0)
	{
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	/* estimate number of tuples from previous tuple density */
	if (relpages > 0)
		density = reltuples / (double)relpages;
	else
	{
		/*
		 * When we have no data because the relation was truncated, estimate
		 * tuple width from attribute datatypes.  We assume here that the
		 * pages are completely full, which is OK for tables (since they've
		 * presumably not been VACUUMed yet) but is probably an overestimate
		 * for indexes.  Fortunately get_relation_info() can clamp the
		 * overestimate to the parent table's size.
		 *
		 * Note: this code intentionally disregards alignment considerations,
		 * because (a) that would be gilding the lily considering how crude
		 * the estimate is, and (b) it creates platform dependencies in the
		 * default plans which are kind of a headache for regression testing.
		 */
		int32 tuple_width;

		tuple_width = get_rel_data_width(rel, attr_widths);
		tuple_width += MAXALIGN(SizeofHeapTupleHeader);
		tuple_width += sizeof(ItemIdData);
		/* note: integer division is intentional here */
		density = (BLCKSZ - SizeOfPageHeaderData) / tuple_width;
	}
	*tuples = rint(density * (double)curpages);

	/*
	 * We use relallvisible as-is, rather than scaling it up like we do for
	 * the pages and tuples counts, on the theory that any pages added since
	 * the last VACUUM are most likely not marked all-visible.  But costsize.c
	 * wants it converted to a fraction.
	 */
	if (relallvisible == 0 || curpages <= 0)
		*allvisfrac = 0;
	else if ((double)relallvisible >= curpages)
		*allvisfrac = 1;
	else
		*allvisfrac = (double)relallvisible / curpages;
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the sort heap AM
 * ------------------------------------------------------------------------
 */

static bool
sortheapam_scan_bitmap_next_block(TableScanDesc scan,
								  TBMIterateResult *tbmres)
{
	HeapScanDesc hscan = (HeapScanDesc)scan;
	BlockNumber page = tbmres->blockno;
	Buffer buffer;
	Snapshot snapshot;
	int ntup;

	hscan->rs_cindex = 0;
	hscan->rs_ntuples = 0;

	/*
	 * Ignore any claimed entries past what we think is the end of the
	 * relation. It may have been extended after the start of our scan (we
	 * only hold an AccessShareLock, and it could be inserts from this
	 * backend).
	 */
	if (page >= hscan->rs_nblocks)
		return false;

	/*
	 * Acquire pin on the target heap page, trading in any pin we held before.
	 */
	hscan->rs_cbuf = ReleaseAndReadBuffer(hscan->rs_cbuf,
										  scan->rs_rd,
										  page);
	hscan->rs_cblock = page;
	buffer = hscan->rs_cbuf;
	snapshot = scan->rs_snapshot;

	ntup = 0;

	/*
	 * We must hold share lock on the buffer content while examining tuple
	 * visibility.  Afterwards, however, the tuples we have found to be
	 * visible are guaranteed good as long as we hold the buffer pin.
	 */
	LockBuffer(buffer, BUFFER_LOCK_SHARE);

	/*
	 * We need two separate strategies for lossy and non-lossy cases.
	 */
	if (tbmres->ntuples >= 0)
	{
		/*
		 * Bitmap is non-lossy, so we just look through the offsets listed in
		 * tbmres; but we have to follow any HOT chain starting at each such
		 * offset.
		 */
		int curslot;

		for (curslot = 0; curslot < tbmres->ntuples; curslot++)
		{
			OffsetNumber offnum = tbmres->offsets[curslot];
			ItemPointerData tid;
			HeapTupleData heapTuple;

			ItemPointerSet(&tid, page, offnum);
			if (heap_hot_search_buffer(&tid, scan->rs_rd, buffer, snapshot,
									   &heapTuple, NULL, true))
				hscan->rs_vistuples[ntup++] = ItemPointerGetOffsetNumber(&tid);
		}
	}
	else
	{
		/*
		 * Bitmap is lossy, so we must examine each line pointer on the page.
		 * But we can ignore HOT chains, since we'll check each tuple anyway.
		 */
		Page dp = (Page)BufferGetPage(buffer);
		OffsetNumber maxoff = PageGetMaxOffsetNumber(dp);
		OffsetNumber offnum;

		for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
		{
			ItemId lp;
			HeapTupleData loctup;
			bool valid;

			lp = PageGetItemId(dp, offnum);
			if (!ItemIdIsNormal(lp))
				continue;
			loctup.t_data = (HeapTupleHeader)PageGetItem((Page)dp, lp);
			loctup.t_len = ItemIdGetLength(lp);
			loctup.t_tableOid = scan->rs_rd->rd_id;
			ItemPointerSet(&loctup.t_self, page, offnum);
			valid = HeapTupleSatisfiesVisibility(scan->rs_rd, &loctup, snapshot, buffer);
			if (valid)
			{
				hscan->rs_vistuples[ntup++] = offnum;
				PredicateLockTuple(scan->rs_rd, &loctup, snapshot);
			}
			CheckForSerializableConflictOut(valid, scan->rs_rd, &loctup,
											buffer, snapshot);
		}
	}

	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

	Assert(ntup <= MaxHeapTuplesPerPage);
	hscan->rs_ntuples = ntup;

	return ntup > 0;
}

static bool
sortheapam_scan_bitmap_next_tuple(TableScanDesc scan,
								  TBMIterateResult *tbmres,
								  TupleTableSlot *slot)
{
	SortHeapBitmapScanDesc bitscan = (SortHeapBitmapScanDesc)scan;
	SortHeapScanDesc shscan = (SortHeapScanDesc)scan;
	HeapScanDesc hscan = (HeapScanDesc)scan;
	OffsetNumber targoffset;
	Relation relation = hscan->rs_base.rs_rd;
	Page dp;
	ItemId lp;
	int attno;
	int natts;
	int i;
	int prev_chunkno = -1;
	ColumnChunk *prev_chunk = NULL;

	/*
	 * Out of range?  If so, nothing more to look at on this page
	 */
	if (hscan->rs_cindex < 0 || hscan->rs_cindex >= hscan->rs_ntuples)
		return false;

	natts = RelationGetNumberOfAttributes(relation);

	if (!bitscan->cached)
	{
		targoffset = hscan->rs_vistuples[hscan->rs_cindex];
		dp = (Page)BufferGetPage(hscan->rs_cbuf);
		lp = PageGetItemId(dp, targoffset);
		Assert(ItemIdIsNormal(lp));

		hscan->rs_ctup.t_data = (HeapTupleHeader)PageGetItem((Page)dp, lp);
		hscan->rs_ctup.t_len = ItemIdGetLength(lp);
		hscan->rs_ctup.t_tableOid = scan->rs_rd->rd_id;
		ItemPointerSet(&hscan->rs_ctup.t_self, hscan->rs_cblock, targoffset);

		if (!IsColumnStoreTuple(&hscan->rs_ctup))
		{
			pgstat_count_heap_fetch(scan->rs_rd);

			/*
			 * Set up the result slot to point to this tuple.  Note that the
			 * slot acquires a pin on the buffer.
			 */
			ExecStoreHeapTuple(&hscan->rs_ctup, slot, false);

			hscan->rs_cindex++;

			return true;
		}
		else
		{
			int chunksize;
			int nindexkeys;
			Datum d;
			bool isnull;
			MemoryContext oldcontext;

			nindexkeys = bitscan->nindexkeys;
			chunksize = ColumnChunkSize(relation);

			MemoryContextReset(bitscan->columnchunkcontext);
			oldcontext = MemoryContextSwitchTo(bitscan->columnchunkcontext);

			d = heap_getattr(&hscan->rs_ctup, 1,
							 bitscan->columnstore_desc,
							 &isnull);
			bitscan->size_cache = DatumGetInt32(d);

			/* decompress or detoast tuples from the chunks */
			for (i = 0; i < shscan->n_proj_atts; i++)
			{
				int attnoinchunk;
				int chunkno;
				TupleDesc chunkdesc;
				ColumnChunk *chunk;

				attno = shscan->proj_atts[i];
				attnoinchunk = attno % chunksize;
				chunkno = attno / chunksize;
				chunkdesc = bitscan->columnchunk_descs[chunkno];

				/* Get the column chunk */
				if (chunkno != prev_chunkno)
				{
					d = heap_getattr(&hscan->rs_ctup,
									 nindexkeys + chunkno + 2,
									 bitscan->columnstore_desc,
									 &isnull);
					chunk = (ColumnChunk *)DatumGetByteaP(d);

					prev_chunkno = chunkno;
					prev_chunk = chunk;
				}
				else
					chunk = prev_chunk;

				deformColumnChunk(chunk,
								  chunkdesc,
								  attnoinchunk,
								  bitscan->size_cache,
								  &bitscan->datum_cache[attno],
								  &bitscan->isnull_cache[attno]);
			}

			MemoryContextSwitchTo(oldcontext);

			bitscan->cached = true;
			bitscan->cur_cache = 0;
		}
	}

	ExecClearTuple(slot);

	if (natts > shscan->n_proj_atts)
		MemSet(slot->tts_isnull, true, natts * sizeof(bool));

	for (i = 0; i < shscan->n_proj_atts; i++)
	{
		attno = shscan->proj_atts[i];

		slot->tts_values[attno] =
			bitscan->datum_cache[attno][bitscan->cur_cache];
		slot->tts_isnull[attno] =
			bitscan->isnull_cache[attno][bitscan->cur_cache];
	}

	bitscan->cur_cache++;

	if (bitscan->cur_cache == bitscan->size_cache)
	{
		bitscan->cached = false;
		bitscan->cur_cache = 0;
		prev_chunk = NULL;
		prev_chunkno = -1;
		hscan->rs_cindex++;
	}

	pgstat_count_heap_fetch(scan->rs_rd);
	ExecStoreVirtualTuple(slot);
	return true;
}

static bool
sortheapam_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static bool
sortheapam_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
								  TupleTableSlot *slot)
{
	elog(ERROR, "%s is not implemented yet", __func__);
}

static void
sortheapam_lazy_vacuum_rel(Relation onerel, VacuumParams *params,
						   BufferAccessStrategy bstrategy)
{
	if (Gp_role == GP_ROLE_EXECUTE)
		LaunchSortHeapMergeWorker(onerel->rd_id, AccessShareLock,
								  false, /* on_qd */
								  false, /* vacuum full */
								  true,	 /* force merge */
								  true /* wait */);
}

/* Same with the heap except we have extra field to signal whether keep order */
static TableScanDesc
sortheapam_beginscan_internal(Relation relation,
							  Snapshot snapshot,
							  int nkeys,
							  ScanKey key,
							  ParallelTableScanDesc parallel_scan,
							  uint32 flags,
							  bool *projs)
{
	int i;
	int natts;
	SortHeapScanDesc scan;
	TableScanDesc hscan;

	hscan = heap_beginscan(relation, snapshot, nkeys, key, parallel_scan, flags);

	/* allocate and initialize scan descriptor */
	scan = (SortHeapScanDesc)palloc0(sizeof(SortHeapScanDescData));
	scan->keeporder = false;
	scan->activedsets = -1;

	natts = RelationGetNumberOfAttributes(relation);
	scan->proj_atts = palloc0(natts * sizeof(AttrNumber));

	if (projs)
	{
		for (i = 0; i < natts; i++)
		{
			if (projs[i])
				scan->proj_atts[scan->n_proj_atts++] = i;
		}
	}
	else
	{
		for (i = 0; i < natts; i++)
			scan->proj_atts[scan->n_proj_atts++] = i;
	}

	memcpy(&scan->hdesc, hscan, sizeof(HeapScanDescData));

	pfree(hscan);
	return (TableScanDesc)scan;
}

static TableScanDesc
sortheapam_beginscan(Relation relation, Snapshot snapshot,
					 int nkeys, ScanKey key,
					 ParallelTableScanDesc parallel_scan,
					 uint32 flags)
{
	return sortheapam_beginscan_internal(relation, snapshot,
										 nkeys, key,
										 parallel_scan, flags, NULL);
}

static TableScanDesc
sortheapam_beginscan_extractcolumns(Relation relation, Snapshot snapshot,
									List *targetlist, List *qual,
									ParallelTableScanDesc pscan,
									uint32 flags)
{
	bool found = false;
	bool *projs;
	AttrNumber natts = RelationGetNumberOfAttributes(relation);

	projs = palloc0(natts * sizeof(*projs));

	found |= extractcolumns_from_node(relation, (Node *)targetlist, projs, natts);
	found |= extractcolumns_from_node(relation, (Node *)qual, projs, natts);

	/*
	 * In some cases (for example, count(*)), no columns are specified. We
	 * always scan the first column.
	 *
	 * Analyze may also specify no columns which means analyze all columns.
	 */
	if (!found)
	{
		if ((flags & SO_TYPE_ANALYZE) != 0)
		{
			pfree(projs);
			projs = NULL;
		}
		else
			projs[0] = true;
	}

	SortHeapScanDesc scandesc = (SortHeapScanDesc)
		sortheapam_beginscan_internal(relation, snapshot,
									  0, NULL, NULL,
									  flags, projs);

	return (TableScanDesc)scandesc;
}

static TableScanDesc
sortheap_beginscan_extractcolumns_bm(Relation rel, Snapshot snapshot,
									 List *targetlist, List *qual,
									 List *bitmapqualorig,
									 uint32 flags)
{
	Oid indexoid;
	bool found = false;
	bool *projs;
	List *indexes;
	Relation indexrel;
	AttrNumber natts = RelationGetNumberOfAttributes(rel);
	SortHeapScanDesc shscan;
	SortHeapBitmapScanDesc scan;

	projs = palloc0(natts * sizeof(*projs));

	found |= extractcolumns_from_node(rel, (Node *)targetlist, projs, natts);
	found |= extractcolumns_from_node(rel, (Node *)qual, projs, natts);
	found |= extractcolumns_from_node(rel, (Node *)bitmapqualorig, projs, natts);

	if (!found)
		projs[0] = true;

	/* allocate and initialize scan descriptor */
	scan = (SortHeapBitmapScanDesc)palloc0(sizeof(SortHeapBitmapScanDescData));

	natts = RelationGetNumberOfAttributes(rel);

	indexes = RelationGetIndexList(rel);
	indexoid = linitial_oid(indexes);
	indexrel = relation_open(indexoid, AccessShareLock);

	/* Initialize column store desc */
	scan->columnstore_desc =
		build_columnstore_tupledesc(rel,
									indexrel,
									&scan->columnchunk_descs,
									&scan->nindexkeys);

	relation_close(indexrel, AccessShareLock);

	scan->columnchunkcontext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "columnchunk context",
							  ALLOCSET_DEFAULT_SIZES);
	scan->cached = false;
	scan->datum_cache = palloc0(sizeof(Datum *) * natts);
	scan->isnull_cache = palloc0(sizeof(bool *) * natts);

	/* Initialize the basic heap scan desc */
	shscan = (SortHeapScanDesc)
		sortheapam_beginscan_internal(rel, snapshot, 0, NULL, NULL, flags, projs);
	memcpy(&scan->shdesc, shscan, sizeof(SortHeapScanDescData));

	pfree(shscan);
	return (TableScanDesc)scan;
}

static void
sortheapam_endscan(TableScanDesc sscan)
{
	SortHeapScanDesc scan = (SortHeapScanDesc)sscan;

	sortheap_endsort(scan->sortstate);
	scan->sortstate = NULL;

	heap_endscan(sscan);
}

static void
sortheapam_rescan(TableScanDesc sscan, ScanKey key, bool set_params,
				  bool allow_strat, bool allow_sync, bool allow_pagemode)
{
	heap_rescan(sscan, key, set_params, allow_strat, allow_sync, allow_pagemode);
}

static bool
sortheapam_getnextslot(TableScanDesc sscan, ScanDirection direction,
					   TupleTableSlot *slot)
{
	return sortheap_getnextslot(sscan, direction, slot);
}

static void
sortheapam_event_notify(Relation relation, CmdType operation, void *param)
{

	if (operation == CMD_INSERT)
		sortheap_insert_finish(relation);
}

/* ------------------------------------------------------
 * Helper function to do an external access method check
 * ------------------------------------------------------
 */
bool external_amcheck(Oid amhandler, char *amname)
{
	Oid expect;

	expect = get_am_oid(amname, true);

	if (!OidIsValid(expect))
		return false;

	return amhandler == expect;
}

/* ------------------------------------------------------------------------
 * Definition of the sort heap table access method.
 * ------------------------------------------------------------------------
 */

TableAmRoutine sortheapam_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = sortheapam_slot_callbacks,

	.scan_begin = sortheapam_beginscan,
	.scan_begin_extractcolumns = sortheapam_beginscan_extractcolumns,
	.scan_begin_extractcolumns_bm = sortheap_beginscan_extractcolumns_bm,
	.scan_end = sortheapam_endscan,
	.scan_rescan = sortheapam_rescan,
	.scan_getnextslot = sortheapam_getnextslot,

	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	.index_fetch_begin = sortheapam_index_fetch_begin,
	.index_fetch_reset = sortheapam_index_fetch_reset,
	.index_fetch_end = sortheapam_index_fetch_end,
	.index_fetch_tuple = sortheapam_index_fetch_tuple,

	.tuple_insert = sortheapam_tuple_insert,
	.tuple_insert_speculative = sortheapam_tuple_insert_speculative,
	.tuple_complete_speculative = sortheapam_tuple_complete_speculative,
	.multi_insert = sortheapam_multi_insert,
	.tuple_delete = sortheapam_tuple_delete,
	.tuple_update = sortheapam_tuple_update,
	.tuple_lock = sortheapam_tuple_lock,
	.finish_bulk_insert = sortheapam_finish_bulk_insert,

	.tuple_fetch_row_version = sortheapam_fetch_row_version,
	.tuple_get_latest_tid = sortheapam_get_latest_tid,
	.tuple_tid_valid = sortheapam_tuple_tid_valid,
	.tuple_satisfies_snapshot = sortheapam_tuple_satisfies_snapshot,
	.compute_xid_horizon_for_tuples = sortheapam_compute_xid_horizon_for_tuples,

	.relation_set_new_filenode = sortheapam_relation_set_new_filenode,
	.relation_nontransactional_truncate = sortheapam_relation_nontransactional_truncate,
	.relation_copy_data = sortheapam_relation_copy_data,
	.relation_copy_for_cluster = sortheapam_relation_copy_for_cluster,
	.relation_vacuum = sortheapam_lazy_vacuum_rel,
	.scan_analyze_next_block = sortheapam_scan_analyze_next_block,
	.scan_analyze_next_tuple = sortheapam_scan_analyze_next_tuple,
	.index_build_range_scan = sortheapam_index_build_range_scan,
	.index_validate_scan = sortheapam_index_validate_scan,

	.relation_size = sortheapam_relation_size,
	.relation_needs_toast_table = sortheapam_relation_needs_toast_table,

	.relation_estimate_size = sortheapam_estimate_rel_size,

	.scan_bitmap_next_block = sortheapam_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = sortheapam_scan_bitmap_next_tuple,
	.scan_sample_next_block = sortheapam_scan_sample_next_block,
	.scan_sample_next_tuple = sortheapam_scan_sample_next_tuple,

	.index_fetch_extractcolumns = sortheapam_index_fetch_extractcolumns,
	/* Set the callback when the insert finish */
	.event_notify = sortheapam_event_notify,
	/* Set the private data for the access method */
	.priv = (void *)&externalsort_compress_methods,
};

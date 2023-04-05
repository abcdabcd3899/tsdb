/*----------------------------------------------------------------------------
 *
 * sortheap_sort_handler.c
 *		Customized callbacks for external sort framework.
 *
 * Provide customized external sorting methods for sortheap_external_sort.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_sort_handler.c
 *
 *----------------------------------------------------------------------------
 */

#include "sortheap.h"
#include "sortheap_tape.h"
#include "sortheap_tapesets.h"
#include "sortheap_btree.h"
#include "sortheap_brin.h"

static HeapTuple
sortheap_prepare_insert(Relation relation, HeapTuple tup, TransactionId xid,
						CommandId cid, int options, bool isFrozen)
{
	tup->t_data->t_infomask &= ~(HEAP_XACT_MASK);

	if (isFrozen)
		tup->t_data->t_infomask |= HEAP_XMIN_COMMITTED;

	tup->t_data->t_infomask2 &= ~(HEAP2_XACT_MASK);
	tup->t_data->t_infomask |= HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tup->t_data, xid);

	if (options & HEAP_INSERT_FROZEN)
		HeapTupleHeaderSetXminFrozen(tup->t_data);

	HeapTupleHeaderSetCmin(tup->t_data, cid);
	HeapTupleHeaderSetXmax(tup->t_data, 0); /* for cleanliness */
	tup->t_tableOid = RelationGetRelid(relation);

	/*
	 * If the new tuple is too big for storage or contains already toasted
	 * out-of-line attributes from some other relation, invoke the toaster.
	 */
	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_MATVIEW)
	{
		/* toast table entries should never be recursively toasted */
		Assert(!HeapTupleHasExternal(tup));
		return tup;
	}
	else if (!IsColumnStoreTuple(tup))
	{
		/* If it is not a compressed tuple, check whether need toast */
		if (HeapTupleHasExternal(tup) || tup->t_len > TOAST_TUPLE_THRESHOLD)
			return toast_insert_or_update(relation, tup, NULL,
										  TOAST_TUPLE_TARGET, isFrozen, options);
		return tup;
	}
	else
		return tup;
}

int default_comparetup(SortTuple *a, SortTuple *b, SortHeapState *shstate)
{
	SortSupport sortKey = shstate->sortkeys;
	int nkey;
	int32 compare;
	Datum ldatum;
	Datum rdatum;
	bool lnull;
	bool rnull;

	/* Be prepared to compare additional sort keys */

	for (nkey = 0; nkey < shstate->nkeys; nkey++, sortKey++)
	{
		ldatum = get_sorttuple_cache(shstate, a, nkey, &lnull);
		rdatum = get_sorttuple_cache(shstate, b, nkey, &rnull);

		compare = ApplySortComparator(ldatum, lnull,
									  rdatum, rnull,
									  sortKey);
		if (compare != 0)
			return compare;
	}

	return 0;
}

bool default_copytup(SortHeapState *shstate, SortTuple *stup, TupleTableSlot *slot, int64 *availmem)
{
	Datum original;
	MemoryContext oldcontext = MemoryContextSwitchTo(shstate->tuplecontext);

	/* copy the tuple into sort storage */
	stup->tuple = ExecCopySlotHeapTuple(slot);
	MemSet(stup->cached, false, sizeof(bool) * MAXKEYCACHE);

	if (availmem)
		*availmem -= GetMemoryChunkSpace(stup->tuple);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * set up first-column key value if it's a simple column
	 */
	if (shstate->indexinfo->ii_IndexAttrNumbers[0] == 0)
		return true;

	/* set up first-column key value */
	original = heap_getattr(stup->tuple,
							shstate->indexinfo->ii_IndexAttrNumbers[0],
							RelationGetDescr(shstate->relation),
							&stup->isnulls[0]);

	stup->datums[0] = original;
	stup->cached[0] = true;

	return true;
}

void default_writetup(SortHeapState *shstate, SortTuple *stup)
{
	int i;
	bool initminmax = false;
	HeapTuple htup;
	IndexTuple itup;
	BlockNumber blkno;
	ItemPointerData ctid;
	TapeInsertState *tapeinsertstate;

	tapeinsertstate = TAPEINSERTSTATE(shstate);

	/* We use a tuple with only header to represent the end of run */
	if (stup == NULL)
	{
		TupleDesc desc = (TupleDesc)palloc0(sizeof(TupleDescData));

		htup = heap_form_tuple(desc, NULL, NULL);
		/*
		 * HEAP_HASOID_OLD flag is eliminated, we use it to signal this is a
		 * run end marker
		 */
		htup->t_data->t_infomask |= HEAP_HASOID_OLD;
	}
	else
		htup = stup->tuple;

	htup = sortheap_prepare_insert(shstate->relation,
								   htup,
								   tapeinsertstate->xid,
								   tapeinsertstate->cid,
								   0,
								   false);

	ctid = tape_writetup(tapeinsertstate, htup);

	/* Update the start ctid of the run */
	if (!ItemPointerIsValid(&tapeinsertstate->runmetad->firsttuple))
		tapeinsertstate->runmetad->firsttuple = ctid;

	/* Update the end ctid of the run */
	if (stup == NULL)
		tapeinsertstate->runmetad->lasttuple = ctid;

	/* If building index is not required, retrun immediately */
	if (!tapeinsertstate->buildindex)
		return;

	/*
	 * Input is NULL means it's the end of run and don't need a index tuple
	 * for the end marker tuple of the run.
	 */
	if (stup == NULL)
		return;

	/* build a index tuple */
	for (i = 0; i < shstate->nkeys; i++)
	{
		tapeinsertstate->indexdatums[i] =
			get_sorttuple_cache(shstate, stup, i,
								&tapeinsertstate->indexnulls[i]);
	}

	itup = index_form_tuple(RelationGetDescr(shstate->indexrel),
							tapeinsertstate->indexdatums,
							tapeinsertstate->indexnulls);

	itup->t_tid = ctid;

	sortheap_bt_buildadd(shstate->relation, tapeinsertstate, tapeinsertstate->btstate, itup);

	/* Update the Brin-like min/max index info */
	blkno = ItemPointerGetBlockNumber(&ctid);

	if (tapeinsertstate->range_firstblock == InvalidBlockNumber)
	{
		initminmax = true;
		tapeinsertstate->range_firstblock = blkno;
		tapeinsertstate->range_lastblock = blkno;
		tapeinsertstate->range_nblocks = 1;
	}
	else if (tapeinsertstate->range_lastblock != blkno)
	{
		tapeinsertstate->range_nblocks++;

		/*
		 * This is the last block in the current range, update the next range,
		 * so any new create data block should use new range
		 */
		if (tapeinsertstate->range_nblocks == SORTHEAP_BRIN_RANGE)
			tapeinsertstate->nextrange = tapeinsertstate->nranges + 1;

		/*
		 * If a range is full, switch to the next range and flush the min/max
		 * of columns in the last range
		 */
		if (tapeinsertstate->range_nblocks > SORTHEAP_BRIN_RANGE)
		{
			initminmax = (tapeinsertstate->nranges == 0);
			/* Update the min in the run level */
			sortheap_brin_update_minmax(RelationGetDescr(shstate->indexrel),
										shstate->nkeys, shstate->sortkeys,
										tapeinsertstate->run_minmax,
										tapeinsertstate->run_minmax_nulls,
										tapeinsertstate->range_minmax,
										tapeinsertstate->range_minmax_nulls,
										initminmax);

			/* Update the min in the run level */
			sortheap_brin_update_minmax(RelationGetDescr(shstate->indexrel),
										shstate->nkeys, shstate->sortkeys,
										tapeinsertstate->run_minmax,
										tapeinsertstate->run_minmax_nulls,
										tapeinsertstate->range_minmax +
											shstate->nkeys,
										tapeinsertstate->range_minmax_nulls +
											shstate->nkeys,
										false);

			/* Flush out the range level min/max */
			tapeinsertstate->range_minmax[shstate->nkeys * 2] =
				UInt32GetDatum(tapeinsertstate->range_firstblock);
			tapeinsertstate->range_minmax_nulls[shstate->nkeys * 2] = false;
			sortheap_brin_flush_minmax(tapeinsertstate,
									   tapeinsertstate->range_minmax,
									   tapeinsertstate->range_minmax_nulls,
									   false);

			/* Switch to the next range */
			tapeinsertstate->range_firstblock = blkno;
			tapeinsertstate->range_lastblock = blkno;
			tapeinsertstate->range_nblocks = 1;
			initminmax = true;
		}
		else
		{
			initminmax = false;
			tapeinsertstate->range_lastblock = blkno;
		}
	}

	/* Update the min/max in the range */
	sortheap_brin_update_minmax(RelationGetDescr(shstate->indexrel),
								shstate->nkeys, shstate->sortkeys,
								tapeinsertstate->range_minmax,
								tapeinsertstate->range_minmax_nulls,
								tapeinsertstate->indexdatums,
								tapeinsertstate->indexnulls,
								initminmax);
	pfree(itup);
}

void default_mergetup(SortHeapState *shstate, SortTuple *stup)
{
	default_writetup(shstate, stup);
}

void default_dumptups(SortHeapState *shstate, SortTuple *memtuples, int memtupcount,
					  int64 *availmem, bool lastdump)
{
	int i;

	if (!memtuples)
		return;

	for (i = 0; i < memtupcount; i++)
	{
		SortTuple *stup = &memtuples[i];

		default_writetup(shstate, stup);

		*availmem += GetMemoryChunkSpace(stup->tuple);
		heap_freetuple(stup->tuple);
	}

	/* Write up a end of run marker */
	default_writetup(shstate, NULL);
}

bool default_readtup(SortHeapState *shstate, TapeScanDesc tapescandesc,
					 SortTuple *stup, bool ismerge)
{
	HeapTuple htup;
	HeapScanDesc hscan = (HeapScanDesc)tapescandesc->hdesc;

	if (hscan->rs_base.rs_flags & SO_ALLOW_PAGEMODE)
		htup = tape_readtup_pagemode(tapescandesc);
	else
		htup = tape_readtup(tapescandesc);

	/* EOF or end of run */
	if (htup == NULL)
		return false;

	if (htup->t_len == ENDRUNMARKER_LEN && IsRunEndMarker(htup))
		return false;

	/*
	 * In merge stage, to avoid frequent palloc and pfree in heap_copytuple,
	 * we preallocate a space to store the tmp tuple in memory and use
	 * heap_copy_to instead.
	 */
	if (ismerge)
	{
		if (htup->t_len + HEAPTUPLESIZE > tapescandesc->slab_size)
		{
			tapescandesc->slab_size = htup->t_len + HEAPTUPLESIZE;
			tapescandesc->slab_tuple = repalloc(tapescandesc->slab_tuple,
												tapescandesc->slab_size);
		}

		stup->tuple = heaptuple_copy_to(htup,
										tapescandesc->slab_tuple,
										&tapescandesc->slab_size);
	}
	else
		stup->tuple = htup;

	MemSet(stup->cached, false, sizeof(bool) * MAXKEYCACHE);

	return true;
}

bool default_fetchtup(SortHeapState *shstate, TapeScanDesc tapescandesc, SortTuple *stup,
					  TupleTableSlot *slot, bool ismerge)
{
	bool continuescan = false;

	if (!tapescandesc)
		return false;

retry:
	if (!READTUP(shstate, tapescandesc, stup, ismerge))
		return false;

	if (!ismerge)
	{
		Assert(slot);
		ExecStoreHeapTuple(stup->tuple, slot, false);

		if (!sortheap_match_scankey_quals(shstate->relation, tapescandesc,
										  stup,
										  slot,
										  &continuescan,
										  true,
										  true))
		{
			if (continuescan)
				goto retry;
			else
				return false;
		}
	}

	return true;
}

const ExternalSortMethods externalsort_default_methods =
	{
		.comparetup = default_comparetup,
		.copytup = default_copytup,
		.dumptups = default_dumptups,
		.mergetup = default_mergetup,
		.writetup = default_writetup,
		.readtup = default_readtup,
		.fetchtup = default_fetchtup};

/*-------------------------------------------------------------------------
 *
 * toin_brin.c
 *	  TODO file description
 *
 *
 * Copyright (c) 2020-Present TODO
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/aocssegfiles.h"
#include "access/aosegfiles.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_appendonly_fn.h"
#include "nodes/makefuncs.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "toin.h"


List *
_toin_make_tlist_from_scankeys(IndexScanDesc scan)
{
	List	   *tlist = NIL;
	Form_pg_index rd_index = scan->indexRelation->rd_index;
	/*
	 * This function is called by _toin_get_heap_relation(), so we cannot call
	 * it recursively.
	 */
	Relation	heapRel = scan->heapRelation;
	TupleDesc	tupdesc = RelationGetDescr(heapRel);

	for (int i = 0; i < scan->numberOfKeys; i++)
	{
		ScanKey		key = &scan->keyData[i];
		AttrNumber	sk_attno = key->sk_attno;
		AttrNumber	attnum = rd_index->indkey.values[sk_attno - 1];
		Form_pg_attribute attr = &tupdesc->attrs[attnum - 1];
		TargetEntry *tle;
		Var		   *var;

		/*
		 * XXX: the var is only passed to extractcolumns_walker(), which only
		 * cares about var->varattno as long as var->varno is non-special.
		 */
		var = makeVar(1							/* varno */,
					  attnum					/* varattno */,
					  attr->atttypid			/* vartype */,
					  attr->atttypmod			/* vartypmod */,
					  attr->attcollation		/* varcollid */,
					  0							/* varlevelsup */);

		tle = makeTargetEntry((Expr *) var,
							  attnum,
							  NameStr(attr->attname),
							  false /* resjunk */);
		tlist = lappend(tlist, tle);
	}

	return tlist;
}

Relation
_toin_get_heap_relation(IndexScanDesc scan)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;

	if (!scan->heapRelation)
	{
		Relation	idxRel = scan->indexRelation;
		Oid			heapOid;

		heapOid = IndexGetRelation(RelationGetRelid(idxRel), false);
		/* FIXME: could scan->heapRelation be set by us? */
		scan->heapRelation = table_open(heapOid, AccessShareLock);
	}

	/*
	 * We must fetch with our own heapfetch instead of scan->xs_heapfetch due
	 * to that:
	 *
	 * - the targetlist of scan->xs_heapfetch might not contain the scankeys;
	 * - the targetlist of scan->xs_heapfetch might contain too many columns,
	 *   while we only care about the scankeys;
	 */
	if (!so->heapfetch)
	{
		List	   *targetlist = _toin_make_tlist_from_scankeys(scan);

		so->heapfetch = table_index_fetch_begin(scan->heapRelation);
		table_index_fetch_extractcolumns(so->heapfetch,
										 targetlist, NIL /* qual */);
	}

	if (!so->slot)
		so->slot = table_slot_create(scan->heapRelation, NULL);

	return scan->heapRelation;
}

/*
 * Get the number of blocks of the heap/ao.
 *
 * For AO/AOCO the result is the total number of all the segfiles.
 */
void
_toin_get_heap_nblocks(IndexScanDesc scan)
{
	Toin	   *toin = (Toin *) scan;

	if (!toin->counted)
	{
		Relation	heapRel = _toin_get_heap_relation(scan);

		toin->counted = true;

		/*
		 * Derived from bringetbitmap(), the difference is that we will raise
		 * an exception on holes.
		 */
		if (RelationIsAppendOptimized(heapRel))
		{
			Snapshot	snapshot;
			int			nsegs;
			int			minseg = INT_MAX;
			int			maxseg = INT_MIN;
			BlockNumber	minblk = MaxBlockNumber;
			BlockNumber maxblk = 0;

			snapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));

			if (RelationIsAoRows(heapRel))
			{
				FileSegInfo **segs;

				segs = GetAllFileSegInfo(heapRel, snapshot, &nsegs);
				for (int i = 0; i < nsegs; i++)
				{
					FileSegInfo *seg = segs[i];
					BlockNumber	nblocks = (seg->total_tupcount + 32767) / 32768;
					BlockNumber	firstblk = seg->segno << 25;
					BlockNumber lastblk = firstblk + nblocks - 1;

					minblk = Min(minblk, firstblk);
					maxblk = Max(maxblk, lastblk);

					minseg = Min(minseg, seg->segno);
					maxseg = Max(maxseg, seg->segno);

					pfree(seg);
				}
				pfree(segs);
			}
			else
			{
				AOCSFileSegInfo **segs;

				segs = GetAllAOCSFileSegInfo(heapRel, snapshot, &nsegs);
				for (int i = 0; i < nsegs; i++)
				{
					AOCSFileSegInfo *seg = segs[i];
					BlockNumber	nblocks = (seg->total_tupcount + 32767) / 32768;
					BlockNumber	firstblk = seg->segno << 25;
					BlockNumber lastblk = firstblk + nblocks - 1;

					minblk = Min(minblk, firstblk);
					maxblk = Max(maxblk, lastblk);

					minseg = Min(minseg, seg->segno);
					maxseg = Max(maxseg, seg->segno);

					pfree(seg);
				}
				pfree(segs);
			}

			UnregisterSnapshot(snapshot);

			if (nsegs > 0)
			{
				if (maxseg - minseg + 1 != nsegs)
					elog(ERROR, "found holes in aosegs");

				toin->firstBlock = minblk;
				toin->lastBlock = maxblk;
			}
			else
			{
				toin->firstBlock = InvalidBlockNumber;
				toin->lastBlock = InvalidBlockNumber;
			}

		}
		else
		{
			BlockNumber	nblocks = RelationGetNumberOfBlocks(heapRel);

			if (nblocks == 0 || nblocks == InvalidBlockNumber)
			{
				toin->firstBlock = InvalidBlockNumber;
				toin->lastBlock = InvalidBlockNumber;
			}
			else
			{
				toin->firstBlock = 0;
				toin->lastBlock = nblocks - 1;
			}
		}
	}
}

/*
 * Get the count of tuples of the data block.
 *
 * For AO "blkno" is the brin record id, not varblock.
 *
 * Return InvalidOffsetNumber if the block is invalid or empty.
 */
OffsetNumber
_toin_get_heap_block_ntuples(IndexScanDesc scan, BlockNumber blkno,
							 OffsetNumber *firstOff, OffsetNumber *lastOff)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	Relation	heapRelation;
	OffsetNumber minoff;
	OffsetNumber maxoff;

	if (blkno == InvalidBlockNumber)
		return InvalidOffsetNumber;

	heapRelation = _toin_get_heap_relation(scan);

	/*
	 * There is no common AM API to get the buffer level information, we have
	 * to do the job with type specific API.
	 */
	switch (heapRelation->rd_rel->relam)
	{
		case HEAP_TABLE_AM_OID:
			{
				IndexFetchHeapData *hscan = (IndexFetchHeapData *) so->heapfetch;
				Buffer		buf;
				Page		page;

				/* Check for empty block */
				if (!_toin_get_heap_tuple(scan, blkno, FirstOffsetNumber))
					return InvalidOffsetNumber;

				buf = hscan->xs_cbuf;
				Assert(BufferIsValid(buf));
				page = BufferGetPage(buf);
				maxoff = PageGetMaxOffsetNumber(page);
				minoff = FirstOffsetNumber;
				break;
			}
		case AO_ROW_TABLE_AM_OID:
		case AO_COLUMN_TABLE_AM_OID:
			{
				int			segno = blkno >> 25;
				int64		tupcount;
				Snapshot	snapshot;

				_toin_get_heap_nblocks(scan);

				if (blkno == segnoGetCurrentAosegStart(segno))
					minoff = 2; /* TODO: a macro instead of 2 */
				else
					minoff = FirstOffsetNumber;

				if (blkno < toin->lastBlock)
				{
					maxoff = 32768;
					break;
				}

				snapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));

				/*
				 * FIXME: total_tupcount includes invisible tuple, how to get
				 * the accurate value?
				 */
				if (RelationIsAoRows(heapRelation))
				{
					FileSegInfo *seginfo;

					seginfo = GetFileSegInfo(heapRelation, snapshot, segno,
											 false /* locked */);
					tupcount = seginfo->total_tupcount;
				}
				else
				{
					AOCSFileSegInfo *seginfo;

					seginfo = GetAOCSFileSegInfo(heapRelation, snapshot, segno,
												 false /* locked */);
					tupcount = seginfo->total_tupcount;
				}

				UnregisterSnapshot(snapshot);

				maxoff = tupcount % 32768 + FirstOffsetNumber;
				break;
			}
		default:
			elog(WARNING, "unsupported AM OID: %d",
				 heapRelation->rd_rel->relam);
			return InvalidOffsetNumber;
	}

	if (firstOff)
		*firstOff = minoff;
	if (lastOff)
		*lastOff = maxoff;

	return maxoff - minoff + 1;
}

/*
 * Derived from _bt_compare().
 *
 *		This routine returns:
 *			<0 if scankey < tuple at offnum;
 *			 0 if scankey == tuple at offnum;
 *			>0 if scankey > tuple at offnum.
 *		NULLs in the keys are treated as sortable values.  Therefore
 *		"equality" does not necessarily mean that the item should be
 *		returned to the caller as a matching key!
 */
int32
_toin_compare_brin(IndexScanDesc scan, BTScanInsert key, BrinMemTuple *dtup)
{
	Relation	rel = scan->indexRelation;
	TupleDesc	itupdesc = RelationGetDescr(rel);
	ScanKey		scankey;
	int			ncmpkey;
	int			ntupatts;

	Assert(key->keysz <= IndexRelationGetNumberOfKeyAttributes(rel));

	ntupatts = itupdesc->natts;

	/*
	 * The scan key is set up with the attribute number associated with each
	 * term in the key.  It is important that, if the index is multi-key, the
	 * scan contain the first k key attributes, and that they be in order.  If
	 * you think about how multi-key ordering works, you'll understand why
	 * this is.
	 *
	 * We don't test for violation of this condition here, however.  The
	 * initial setup for the index scan had better have gotten it right (see
	 * _bt_first).
	 */

	ncmpkey = Min(ntupatts, key->keysz);
	Assert(ncmpkey == key->keysz);
	scankey = key->scankeys;
	for (int i = 1; i <= ncmpkey; i++)
	{
		BrinValues *bval;
		Datum		datum;
		bool		isNull;
		int32		result;
		int			edge;

		switch (scankey->sk_strategy)
		{
			case BTLessStrategyNumber:
			case BTLessEqualStrategyNumber:
				edge = 0; /* min */
				break;
			case BTGreaterStrategyNumber:
			case BTGreaterEqualStrategyNumber:
				edge = 1; /* max */
				break;
			case BTEqualStrategyNumber:
				/* FIXME: check for scan direction */
				edge = 1; /* max */
				break;
			default:
				/* FIXME: should this happen?  Anyway, treat as EQ */
				edge = 1; /* max */
				break;
		}

		bval = &dtup->bt_columns[scankey->sk_attno - 1];
		datum = bval->bv_values[edge];
		isNull = bval->bv_allnulls;

		/* see comments about NULLs handling in btbuild */
		if (scankey->sk_flags & SK_ISNULL)	/* key is NULL */
		{
			if (isNull)
				result = 0;		/* NULL "=" NULL */
			else if (scankey->sk_flags & SK_BT_NULLS_FIRST)
				result = -1;	/* NULL "<" NOT_NULL */
			else
				result = 1;		/* NULL ">" NOT_NULL */
		}
		else if (isNull)		/* key is NOT_NULL and item is NULL */
		{
			if (scankey->sk_flags & SK_BT_NULLS_FIRST)
				result = 1;		/* NOT_NULL ">" NULL */
			else
				result = -1;	/* NOT_NULL "<" NULL */
		}
		else
		{
			/*
			 * The sk_func needs to be passed the index value as left arg and
			 * the sk_argument as right arg (they might be of different
			 * types).  Since it is convenient for callers to think of
			 * _bt_compare as comparing the scankey to the index item, we have
			 * to flip the sign of the comparison result.  (Unless it's a DESC
			 * column, in which case we *don't* flip the sign.)
			 */
			result = DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
													 scankey->sk_collation,
													 datum,
													 scankey->sk_argument));

			if (!(scankey->sk_flags & SK_BT_DESC))
				INVERT_COMPARE_RESULT(result);
		}

		/* if the keys are unequal, return the difference */
		if (result != 0)
			return result;

		scankey++;
	}

	/*
	 * All non-truncated attributes (other than heap TID) were found to be
	 * equal.  Treat truncated attributes as minus infinity when scankey has a
	 * key attribute value that would otherwise be compared directly.
	 *
	 * Note: it doesn't matter if ntupatts includes non-key attributes;
	 * scankey won't, so explicitly excluding non-key attributes isn't
	 * necessary.
	 */
	if (key->keysz > ntupatts)
		return 1;

	/* All provided scankey arguments found to be equal */
	return 0;
}

static bool
_toin_get_block_brin(IndexScanDesc scan, BlockNumber block,
					 Buffer *buf, BrinMemTuple *dtup)
{
	BrinOpaque *brin = scan->opaque;
	BrinTuple  *itup;
	OffsetNumber off;
	Size		size;

	itup = brinGetTupleForHeapBlock(brin->bo_rmAccess, block, buf,
									&off, &size, BUFFER_LOCK_SHARE,
									scan->xs_snapshot);
	if (!itup)
		/* buf is untouched in such a case */
		return false;

	brin_deform_tuple(brin->bo_bdesc, itup, dtup);
	LockBuffer(*buf, BUFFER_LOCK_UNLOCK);

	return true;
}

/*
 * Search for the first logical block who matches the scankey.
 *
 * Returns the BlockNumber of the first key >= given scankey, or > scankey if
 * nextkey is true.  (NOTE: in particular, this means it is possible to return
 * a value 1 greater than the number of keys on the page, if the scankey is >
 * all keys on the page.)
 */
BlockNumber
_toin_search_brin(IndexScanDesc scan, BTScanInsert key)
{
	Toin	   *toin = (Toin *) scan;
	BrinOpaque *brin = scan->opaque;
	BrinMemTuple *dtup = NULL;
	Buffer		buf = InvalidBuffer;
	BlockNumber	L;
	BlockNumber	R;
	StrategyNumber strategy;

	/*
	 * TODO: move this check to the creation of the index, or even better,
	 * force it to be 1.
	 */
	if (brin->bo_pagesPerRange != 1)
		elog(ERROR, "toin: pages_per_range must be 1");

	if (key->nextkey)
		strategy = BTGreaterStrategyNumber;
	else
		strategy = BTGreaterEqualStrategyNumber;

	/*
	 * FIXME: this only works when non-empty aosegs are continuous, if there
	 * are holes we could give empty result.
	 */
	_toin_get_heap_nblocks(scan);
	L = toin->firstBlock;
	R = toin->lastBlock;
	if (L == InvalidBlockNumber || R == InvalidBlockNumber)
		return InvalidBlockNumber;

	/* allocate an initial in-memory tuple, out of the per-range memcxt */
	dtup = brin_new_memtuple(brin->bo_bdesc);

	/* TODO: short circuit by checking L.min and R.max */

	while (L < R)
	{
		BlockNumber	M = L + (R - L) / 2;

		/*
		 * TODO: is it possible to fetch the attrs one by one?  so we do not
		 * need to deform the entire index tuple.
		 */
		if (!_toin_get_block_brin(scan, M, &buf, dtup))
		{
			/*
			 * TODO: why would this happen, should we move to M-1 or M+1
			 * instead?  I guess this only happens if the toin is not reindexed
			 * after the reorder, to be verified.
			 */
			L = R = InvalidBlockNumber;
			break;
		}

		switch (strategy)
		{
			case BTEqualStrategyNumber:
				/*
				 * for EQ we still want to find the the first page >= key,
				 * so fall through.
				 *
				 * TODO: this is only true for forward search.
				 */
			case BTGreaterEqualStrategyNumber:
				if (_toin_compare_brin(scan, key, dtup) > 0) /* sk > max */
					L = M + 1;
				else
					R = M;
				break;
			case BTGreaterStrategyNumber:
				if (_toin_compare_brin(scan, key, dtup) >= 0) /* sk >= max */
					L = M + 1;
				else
					R = M;
				break;
			case BTLessEqualStrategyNumber:
				if (_toin_compare_brin(scan, key, dtup) < 0) /* sk < max */
					R = M - 1;
				else
					L = M;
				break;
			case BTLessStrategyNumber:
				if (_toin_compare_brin(scan, key, dtup) <= 0) /* sk <= max */
					R = M - 1;
				else
					L = M;
				break;
		}
	}

	if (BufferIsValid(buf))
		ReleaseBuffer(buf);

	/* free the brin memtuple */
	MemoryContextDelete(dtup->bt_context);
	pfree(dtup->bt_hasnulls);
	pfree(dtup->bt_allnulls);
	pfree(dtup->bt_values);
	pfree(dtup);

	return L;
}

/*
 * Get the tuple at off of the page buffer.
 *
 * Return true if the tuple is successfully fetched; false otherwise, and the
 * tupler buffer is untouched.
 */
bool
_toin_get_heap_tuple(IndexScanDesc scan, BlockNumber blkno, OffsetNumber off)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	ItemPointerData tid;
	bool		all_dead = false;
	bool		found;

	ItemPointerSet(&tid, blkno, off);

	/*
	 * Below logic is similar to index_fetch_heap(), except that the tid is
	 * (blkno, off) instead of scan->xs_heaptid .
	 */

	found = table_index_fetch_tuple(so->heapfetch, &tid,
									scan->xs_snapshot, so->slot,
									&scan->xs_heap_continue, &all_dead);

#if 0
	if (found)
		pgstat_count_heap_fetch(scan->indexRelation);
#endif

	/*
	 * If we scanned a whole HOT chain and found only dead tuples, tell index
	 * AM to kill its entry for that TID (this will take effect in the next
	 * amgettuple call, in index_getnext_tid).  We do not do this when in
	 * recovery because it may violate MVCC to do so.  See comments in
	 * RelationGetIndexScan().
	 */
	if (!scan->xactStartedInRecovery)
		scan->kill_prior_tuple = all_dead;

	return found;
}

/*
 * Derived from _bt_compare(), "tuple" refers to toin->slot.
 *
 *		This routine returns:
 *			<0 if scankey < tuple at offnum;
 *			 0 if scankey == tuple at offnum;
 *			>0 if scankey > tuple at offnum.
 *		NULLs in the keys are treated as sortable values.  Therefore
 *		"equality" does not necessarily mean that the item should be
 *		returned to the caller as a matching key!
 */
int32
_toin_compare_heap(IndexScanDesc scan, BTScanInsert key)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	Relation	heapRel = _toin_get_heap_relation(scan);
	TupleDesc	tupdesc = RelationGetDescr(heapRel);
	ScanKey		scankey;
	int			ncmpkey;
	int			ntupatts;

	ntupatts = tupdesc->natts;

	/*
	 * The scan key is set up with the attribute number associated with each
	 * term in the key.  It is important that, if the index is multi-key, the
	 * scan contain the first k key attributes, and that they be in order.  If
	 * you think about how multi-key ordering works, you'll understand why
	 * this is.
	 *
	 * We don't test for violation of this condition here, however.  The
	 * initial setup for the index scan had better have gotten it right (see
	 * _bt_first).
	 */

	ncmpkey = Min(ntupatts, key->keysz);
	Assert(ncmpkey == key->keysz);
	scankey = key->scankeys;
	for (int i = 1; i <= ncmpkey; i++)
	{
		Datum		datum;
		bool		isNull;
		int32		result;

		datum = slot_getattr(so->slot,
							 scan->indexRelation->rd_index->indkey.values[scankey->sk_attno - 1],
							 &isNull);

		/* see comments about NULLs handling in btbuild */
		if (scankey->sk_flags & SK_ISNULL)	/* key is NULL */
		{
			if (isNull)
				result = 0;		/* NULL "=" NULL */
			else if (scankey->sk_flags & SK_BT_NULLS_FIRST)
				result = -1;	/* NULL "<" NOT_NULL */
			else
				result = 1;		/* NULL ">" NOT_NULL */
		}
		else if (isNull)		/* key is NOT_NULL and item is NULL */
		{
			if (scankey->sk_flags & SK_BT_NULLS_FIRST)
				result = 1;		/* NOT_NULL ">" NULL */
			else
				result = -1;	/* NOT_NULL "<" NULL */
		}
		else
		{
			/*
			 * The sk_func needs to be passed the index value as left arg and
			 * the sk_argument as right arg (they might be of different
			 * types).  Since it is convenient for callers to think of
			 * _bt_compare as comparing the scankey to the index item, we have
			 * to flip the sign of the comparison result.  (Unless it's a DESC
			 * column, in which case we *don't* flip the sign.)
			 */
			result = DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
													 scankey->sk_collation,
													 datum,
													 scankey->sk_argument));

			if (!(scankey->sk_flags & SK_BT_DESC))
				INVERT_COMPARE_RESULT(result);
		}

		/* if the keys are unequal, return the difference */
		if (result != 0)
			return result;

		scankey++;
	}

	/*
	 * All non-truncated attributes (other than heap TID) were found to be
	 * equal.  Treat truncated attributes as minus infinity when scankey has a
	 * key attribute value that would otherwise be compared directly.
	 *
	 * Note: it doesn't matter if ntupatts includes non-key attributes;
	 * scankey won't, so explicitly excluding non-key attributes isn't
	 * necessary.
	 */
	if (key->keysz > ntupatts)
		return 1;

	/* All provided scankey arguments found to be equal */
	return 0;
}

/*
 * Search for the first tuple who matches scankey within the block.
 *
 * Returns the OffsetNumber of the first key >= given scankey, or > scankey if
 * nextkey is true.  (NOTE: in particular, this means it is possible to return
 * a value 1 greater than the number of keys on the page, if the scankey is >
 * all keys on the page.)
 */
OffsetNumber
_toin_search_heap(IndexScanDesc scan, BTScanInsert key, BlockNumber blkno)
{
	OffsetNumber ntups;
	OffsetNumber L;
	OffsetNumber R;
	OffsetNumber M;
	StrategyNumber strategy;

	ntups = _toin_get_heap_block_ntuples(scan, blkno, &L, &R);
	if (ntups == InvalidOffsetNumber)
		return InvalidOffsetNumber;

	if (key->nextkey)
		strategy = BTGreaterStrategyNumber;
	else
		strategy = BTGreaterEqualStrategyNumber;

	while (L < R)
	{
		M = L + (R - L) / 2;

		if (!_toin_get_heap_tuple(scan, blkno, M))
			pg_unreachable();

		switch (strategy)
		{
			case BTEqualStrategyNumber:
				/*
				 * for EQ we still want to find the the first page >= key,
				 * so fall through.
				 *
				 * TODO: this is only true for forward search.
				 */
			case BTGreaterEqualStrategyNumber:
				if (_toin_compare_heap(scan, key) > 0) /* sk > tup */
					L = M + 1;
				else
					R = M;
				break;
			case BTGreaterStrategyNumber:
				if (_toin_compare_heap(scan, key) >= 0) /* sk >= tup */
					L = M + 1;
				else
					R = M;
				break;
			case BTLessEqualStrategyNumber:
				if (_toin_compare_heap(scan, key) < 0) /* sk < tup */
					R = M - 1;
				else
					L = M;
				break;
			case BTLessStrategyNumber:
				if (_toin_compare_heap(scan, key) <= 0) /* sk <= tup */
					R = M - 1;
				else
					L = M;
				break;
		}
	}

	return L;
}

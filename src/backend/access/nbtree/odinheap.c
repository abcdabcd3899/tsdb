/*-------------------------------------------------------------------------
 *
 * odinheap.c
 *	  TODO file description
 *
 *
 * Copyright (c) 2020-Present TODO
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/aosegfiles.h"
#include "access/aocssegfiles.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/pg_appendonly_fn.h"
#include "nodes/makefuncs.h"


List *
odin_make_tlist_from_scankeys(IndexScanDesc scan)
{
	List	   *tlist = NIL;
	Form_pg_index rd_index = scan->indexRelation->rd_index;
	Relation	heapRel = odin_get_heap_relation(scan);
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
odin_get_heap_relation(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	if (!scan->heapRelation)
	{
		Relation	idxRel = scan->indexRelation;
		Oid			heapOid;

		heapOid = IndexGetRelation(RelationGetRelid(idxRel), false);
		/* FIXME: could scan->heapRelation be set by us? */
		scan->heapRelation = table_open(heapOid, AccessShareLock);
	}

	if (!so->odin_slot)
		so->odin_slot = table_slot_create(scan->heapRelation, NULL);

	return scan->heapRelation;
}

/*
 * Get the tuple at off of the page buffer.
 *
 * Return true if the tuple is successfully fetched; false otherwise, and the
 * tupler buffer is untouched.
 */
bool
odin_get_heap_tuple(IndexScanDesc scan, BlockNumber blkno, OffsetNumber off)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	ItemPointerData tid;
	bool		all_dead = false;
	bool		found;

	ItemPointerSet(&tid, blkno, off);

	/*
	 * Below logic is similar to index_fetch_heap(), except that the tid is
	 * (blkno, off) instead of scan->xs_heaptid .
	 */

	found = table_index_fetch_tuple(so->odin_heapfetch, &tid,
									scan->xs_snapshot, so->odin_slot,
									&scan->xs_heap_continue, &all_dead);

#if 0
	if (found)
		pgstat_count_heap_fetch(scan->indexRelation);
#endif

#if 0
	/*
	 * If we scanned a whole HOT chain and found only dead tuples, tell index
	 * AM to kill its entry for that TID (this will take effect in the next
	 * amgettuple call, in index_getnext_tid).  We do not do this when in
	 * recovery because it may violate MVCC to do so.  See comments in
	 * RelationGetIndexScan().
	 */
	if (!scan->xactStartedInRecovery)
		scan->kill_prior_tuple = all_dead;
#endif

	return found;
}

/*
 * Get the number of blocks of the heap/ao.
 *
 * For AO/AOCO the result is the total number of all the segfiles.
 */
void
odin_get_heap_nblocks(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	if (!so->odin_counted)
	{
		Relation	heapRel = odin_get_heap_relation(scan);

		so->odin_counted = true;
		so->odin_currentBlock = InvalidBlockNumber;

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

				so->odin_firstBlock = minblk;
				so->odin_lastBlock = maxblk;
			}
			else
			{
				so->odin_firstBlock = InvalidBlockNumber;
				so->odin_lastBlock = InvalidBlockNumber;
			}

		}
		else
		{
			BlockNumber	nblocks = RelationGetNumberOfBlocks(heapRel);

			if (nblocks == 0 || nblocks == InvalidBlockNumber)
			{
				so->odin_firstBlock = InvalidBlockNumber;
				so->odin_lastBlock = InvalidBlockNumber;
			}
			else
			{
				so->odin_firstBlock = 0;
				so->odin_lastBlock = nblocks - 1;
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
odin_get_heap_block_ntuples(IndexScanDesc scan, BlockNumber blkno,
							OffsetNumber *firstOff, OffsetNumber *lastOff)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation	heapRelation;

	if (blkno == InvalidBlockNumber)
		return InvalidOffsetNumber;

	if (blkno == so->odin_currentBlock)
		goto end;

	heapRelation = odin_get_heap_relation(scan);

	/*
	 * There is no common AM API to get the buffer level information, we have
	 * to do the job with type specific API.
	 */
	switch (heapRelation->rd_rel->relam)
	{
		case HEAP_TABLE_AM_OID:
			{
				IndexFetchHeapData *hscan = (IndexFetchHeapData *) so->odin_heapfetch;
				Buffer		buf;
				Page		page;

				/* Check for empty block */
				if (!odin_get_heap_tuple(scan, blkno, FirstOffsetNumber))
					return InvalidOffsetNumber;

				buf = hscan->xs_cbuf;
				Assert(BufferIsValid(buf));
				page = BufferGetPage(buf);
				so->odin_maxoff = PageGetMaxOffsetNumber(page);
				so->odin_minoff = FirstOffsetNumber;
				break;
			}
		case AO_ROW_TABLE_AM_OID:
		case AO_COLUMN_TABLE_AM_OID:
			{
				int			segno = blkno >> 25;
				int64		tupcount;
				Snapshot	snapshot;

				odin_get_heap_nblocks(scan);

				if (blkno == (segno << 25))
					so->odin_minoff = 2; /* TODO: a macro instead of 2 */
				else
					so->odin_minoff = FirstOffsetNumber;

				if (blkno < so->odin_lastBlock)
				{
					so->odin_maxoff = 32768;
					break;
				}

				snapshot = RegisterSnapshot(GetCatalogSnapshot(InvalidOid));

				/*
				 * FIXME: total_tupcount includes invisible tuple, how to get
				 * the accurate value?
				 *
				 * TODO: cache per-seg total_tupcount, or minoff & maxoff.
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

				so->odin_maxoff = tupcount % 32768 + FirstOffsetNumber;
				break;
			}
		default:
			elog(ERROR, "unsupported AM OID: %d", heapRelation->rd_rel->relam);
			return InvalidOffsetNumber;
	}

	so->odin_currentBlock = blkno;

end:
	if (firstOff)
		*firstOff = so->odin_minoff;
	if (lastOff)
		*lastOff = so->odin_maxoff;

	return so->odin_maxoff - so->odin_minoff + 1;
}

/*
 * Derived from _bt_compare(), "tuple" refers to so->odin_slot.
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
odin_compare_heap(IndexScanDesc scan, BTScanInsert key)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation	heapRel = odin_get_heap_relation(scan);
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
		int			attnum;

		attnum = scan->indexRelation->rd_index->indkey.values[scankey->sk_attno - 1];

		if (TTS_IS_VIRTUAL(so->odin_slot))
		{
			datum = so->odin_slot->tts_values[attnum - 1];
			isNull = so->odin_slot->tts_isnull[attnum - 1];
		}
		else
			datum = slot_getattr(so->odin_slot, attnum, &isNull);

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
odin_binsearch_heap(IndexScanDesc scan, ItemPointer first_tid)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BTScanInsert key = &so->odin_inskey;
	BlockNumber	blkno = ItemPointerGetBlockNumber(first_tid);
	OffsetNumber ntups;
	OffsetNumber L;
	OffsetNumber R;
	OffsetNumber M;
	StrategyNumber strategy;

	ntups = odin_get_heap_block_ntuples(scan, blkno, &L, &R);
	if (ntups == InvalidOffsetNumber)
		return InvalidOffsetNumber;

	if (RelationIsAppendOptimized(odin_get_heap_relation(scan)) &&
		OdinGetTuplesPerRange(scan->indexRelation) > 0)
	{
		/*
		 * The odin record contains the first heap tid of the range, but not
		 * the last, so we have to guess.
		 */
		OffsetNumber count = OdinGetTuplesPerRange(scan->indexRelation);
		OffsetNumber first = ItemPointerGetOffsetNumber(first_tid);
		OffsetNumber last = first + count - 1;

		L = Max(L, first);

		if (last > first)
			R = Min(R, last);
	}

	if (key->nextkey)
		strategy = BTGreaterStrategyNumber;
	else
		strategy = BTGreaterEqualStrategyNumber;

	while (L < R)
	{
		M = L + (R - L) / 2;

		if (!odin_get_heap_tuple(scan, blkno, M))
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
				if (odin_compare_heap(scan, key) > 0) /* sk > tup */
					L = M + 1;
				else
					R = M;
				break;
			case BTGreaterStrategyNumber:
				if (odin_compare_heap(scan, key) >= 0) /* sk >= tup */
					L = M + 1;
				else
					R = M;
				break;
			case BTLessEqualStrategyNumber:
				if (odin_compare_heap(scan, key) < 0) /* sk < tup */
					R = M - 1;
				else
					L = M;
				break;
			case BTLessStrategyNumber:
				if (odin_compare_heap(scan, key) <= 0) /* sk <= tup */
					R = M - 1;
				else
					L = M;
				break;
		}
	}

	return L;
}

/*
 * Find the first matching tid from the position (h_blkno, h_off).
 *
 * It will move to the next/previous offset(s) or even next block(s) when
 * necessary.  The scan->xs_heaptid is updated accordingly.
 *
 * Return true if found a matching one, or false if not found.
 */
bool
odin_first_match(IndexScanDesc scan, ScanDirection dir,
				 BlockNumber h_blkno, OffsetNumber h_off)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	if (h_off != InvalidOffsetNumber)
	{
		bool		found;
		bool		continuescan;
		int			indnatts;

		/* Must (re-)fetch the heap tuple */
		odin_get_heap_tuple(scan, h_blkno, h_off);

		/* Must check whether the tuple matches */
		indnatts = RelationGetNumberOfAttributes(scan->heapRelation);
		found = _bt_checkkeys(scan, NULL /* tuple */,
							  indnatts, dir, &continuescan);

		/* Remember the heap tid anyway, clear it when necessary */
		ItemPointerSet(&scan->xs_heaptid, h_blkno, h_off);
		if (found)
		{
			if (scan->parallel_scan)
				/* Set the next page for other parallel workers */
				odin_parallel_advance_block(scan, dir, h_blkno);

			return true;
		}
		else if (continuescan)
			return _bt_next(scan, dir);
	}

	_bt_parallel_done(scan);
	if (BufferIsValid(so->currPos.buf))
		_bt_relbuf(scan->indexRelation, so->currPos.buf);
	BTScanPosInvalidate(so->currPos);
	ItemPointerSetInvalid(&scan->xs_heaptid);
	return false;
}

/*
 * Record the next block for parallel scan.
 *
 * This should only be called in parallel scan.
 *
 * Return the advanced blkno, or InvalidBlockNumber if no more.
 */
BlockNumber
odin_parallel_advance_block(IndexScanDesc scan,
							ScanDirection dir, BlockNumber h_blkno)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	BlockNumber	next_blkno = InvalidBlockNumber;

	if (BlockNumberIsValid(h_blkno))
	{
		odin_get_heap_nblocks(scan);

		if (ScanDirectionIsForward(dir))
		{
			if (h_blkno < so->odin_lastBlock)
				next_blkno = h_blkno + 1;
		}
		else
		{
			if (h_blkno > so->odin_firstBlock)
				next_blkno = h_blkno - 1;
		}
	}

	if (BlockNumberIsValid(next_blkno))
		_bt_parallel_release(scan, next_blkno);
	else
		_bt_parallel_done(scan);

	return next_blkno;
}

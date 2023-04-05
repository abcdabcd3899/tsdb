/*-------------------------------------------------------------------------
 *
 * nbtsearch.c
 *	  Search code for postgres btrees.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtsearch.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relscan.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/predicate.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "toin.h"

static bool _toin_readpage(IndexScanDesc scan, ScanDirection dir,
						   OffsetNumber offnum);
static void _toin_saveitem(ToinScanOpaque so, int itemIndex);
static bool _toin_steppage(IndexScanDesc scan, ScanDirection dir);
static bool _toin_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir);
static bool _toin_endpoint(IndexScanDesc scan, ScanDirection dir);
static inline void _toin_initialize_more_data(ToinScanOpaque so, ScanDirection dir);


/*
 * Derived from _bt_first().
 *
 *	_bt_first() -- Find the first item in a scan.
 *
 *		We need to be clever about the direction of scan, the search
 *		conditions, and the tree ordering.  We find the first item (or,
 *		if backwards scan, the last item) in the tree that satisfies the
 *		qualifications in the scan key.  On success exit, the page containing
 *		the current index tuple is pinned but not locked, and data about
 *		the matching tuple(s) on the page has been loaded into so->currPos.
 *		scan->xs_ctup.t_self is set to the heap TID of the current tuple,
 *		and if requested, scan->xs_itup points to a copy of the index tuple.
 *
 * If there are no matching items in the index, we return false, with no
 * pins or locks held.
 *
 * Note that scan->keyData[], and the so->keyData[] scankey built from it,
 * are both search-type scankeys (see nbtree/README for more about this).
 * Within this routine, we build a temporary insertion-type scankey to use
 * in locating the scan start position.
 */
bool
_toin_first(IndexScanDesc scan, ScanDirection dir)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	Relation	rel = scan->indexRelation;
	BlockNumber	blkno;
	OffsetNumber offnum;
	StrategyNumber strat;
	bool		nextkey;
	bool		goback;
	BTScanInsertData inskey;
	ScanKey		startKeys[INDEX_MAX_KEYS];
	ScanKeyData notnullkeys[INDEX_MAX_KEYS];
	int			keysCount = 0;
	int			i;
	StrategyNumber strat_total;
	ToinScanPosItem *currItem;

	Assert(!ToinScanPosIsValid(so->currPos));

	pgstat_count_index_scan(rel);

	/*
	 * Examine the scan keys and eliminate any redundant keys; also mark the
	 * keys that must be matched to continue the scan.
	 */
	_toin_preprocess_keys(scan);

	/*
	 * Quit now if _bt_preprocess_keys() discovered that the scan keys can
	 * never be satisfied (eg, x == 1 AND x > 2).
	 */
	if (!so->qual_ok)
		return false;

	/*----------
	 * Examine the scan keys to discover where we need to start the scan.
	 *
	 * We want to identify the keys that can be used as starting boundaries;
	 * these are =, >, or >= keys for a forward scan or =, <, <= keys for
	 * a backwards scan.  We can use keys for multiple attributes so long as
	 * the prior attributes had only =, >= (resp. =, <=) keys.  Once we accept
	 * a > or < boundary or find an attribute with no boundary (which can be
	 * thought of as the same as "> -infinity"), we can't use keys for any
	 * attributes to its right, because it would break our simplistic notion
	 * of what initial positioning strategy to use.
	 *
	 * When the scan keys include cross-type operators, _bt_preprocess_keys
	 * may not be able to eliminate redundant keys; in such cases we will
	 * arbitrarily pick a usable one for each attribute.  This is correct
	 * but possibly not optimal behavior.  (For example, with keys like
	 * "x >= 4 AND x >= 5" we would elect to scan starting at x=4 when
	 * x=5 would be more efficient.)  Since the situation only arises given
	 * a poorly-worded query plus an incomplete opfamily, live with it.
	 *
	 * When both equality and inequality keys appear for a single attribute
	 * (again, only possible when cross-type operators appear), we *must*
	 * select one of the equality keys for the starting point, because
	 * _bt_checkkeys() will stop the scan as soon as an equality qual fails.
	 * For example, if we have keys like "x >= 4 AND x = 10" and we elect to
	 * start at x=4, we will fail and stop before reaching x=10.  If multiple
	 * equality quals survive preprocessing, however, it doesn't matter which
	 * one we use --- by definition, they are either redundant or
	 * contradictory.
	 *
	 * Any regular (not SK_SEARCHNULL) key implies a NOT NULL qualifier.
	 * If the index stores nulls at the end of the index we'll be starting
	 * from, and we have no boundary key for the column (which means the key
	 * we deduced NOT NULL from is an inequality key that constrains the other
	 * end of the index), then we cons up an explicit SK_SEARCHNOTNULL key to
	 * use as a boundary key.  If we didn't do this, we might find ourselves
	 * traversing a lot of null entries at the start of the scan.
	 *
	 * In this loop, row-comparison keys are treated the same as keys on their
	 * first (leftmost) columns.  We'll add on lower-order columns of the row
	 * comparison below, if possible.
	 *
	 * The selected scan keys (at most one per index column) are remembered by
	 * storing their addresses into the local startKeys[] array.
	 *----------
	 */
	strat_total = BTEqualStrategyNumber;
	if (so->numberOfKeys > 0)
	{
		AttrNumber	curattr;
		ScanKey		chosen;
		ScanKey		impliesNN;
		ScanKey		cur;

		/*
		 * chosen is the so-far-chosen key for the current attribute, if any.
		 * We don't cast the decision in stone until we reach keys for the
		 * next attribute.
		 */
		curattr = 1;
		chosen = NULL;
		/* Also remember any scankey that implies a NOT NULL constraint */
		impliesNN = NULL;

		/*
		 * Loop iterates from 0 to numberOfKeys inclusive; we use the last
		 * pass to handle after-last-key processing.  Actual exit from the
		 * loop is at one of the "break" statements below.
		 */
		for (cur = so->keyData, i = 0;; cur++, i++)
		{
			if (i >= so->numberOfKeys || cur->sk_attno != curattr)
			{
				/*
				 * Done looking at keys for curattr.  If we didn't find a
				 * usable boundary key, see if we can deduce a NOT NULL key.
				 */
				if (chosen == NULL && impliesNN != NULL &&
					((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
					 ScanDirectionIsForward(dir) :
					 ScanDirectionIsBackward(dir)))
				{
					/* Yes, so build the key in notnullkeys[keysCount] */
					chosen = &notnullkeys[keysCount];
					ScanKeyEntryInitialize(chosen,
										   (SK_SEARCHNOTNULL | SK_ISNULL |
											(impliesNN->sk_flags &
											 (SK_BT_DESC | SK_BT_NULLS_FIRST))),
										   curattr,
										   ((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ?
											BTGreaterStrategyNumber :
											BTLessStrategyNumber),
										   InvalidOid,
										   InvalidOid,
										   InvalidOid,
										   (Datum) 0);
				}

				/*
				 * If we still didn't find a usable boundary key, quit; else
				 * save the boundary key pointer in startKeys.
				 */
				if (chosen == NULL)
					break;
				startKeys[keysCount++] = chosen;

				/*
				 * Adjust strat_total, and quit if we have stored a > or <
				 * key.
				 */
				strat = chosen->sk_strategy;
				if (strat != BTEqualStrategyNumber)
				{
					strat_total = strat;
					if (strat == BTGreaterStrategyNumber ||
						strat == BTLessStrategyNumber)
						break;
				}

				/*
				 * Done if that was the last attribute, or if next key is not
				 * in sequence (implying no boundary key is available for the
				 * next attribute).
				 */
				if (i >= so->numberOfKeys ||
					cur->sk_attno != curattr + 1)
					break;

				/*
				 * Reset for next attr.
				 */
				curattr = cur->sk_attno;
				chosen = NULL;
				impliesNN = NULL;
			}

			/*
			 * Can we use this key as a starting boundary for this attr?
			 *
			 * If not, does it imply a NOT NULL constraint?  (Because
			 * SK_SEARCHNULL keys are always assigned BTEqualStrategyNumber,
			 * *any* inequality key works for that; we need not test.)
			 */
			switch (cur->sk_strategy)
			{
				case BTLessStrategyNumber:
				case BTLessEqualStrategyNumber:
					if (chosen == NULL)
					{
						if (ScanDirectionIsBackward(dir))
							chosen = cur;
						else
							impliesNN = cur;
					}
					break;
				case BTEqualStrategyNumber:
					/* override any non-equality choice */
					chosen = cur;
					break;
				case BTGreaterEqualStrategyNumber:
				case BTGreaterStrategyNumber:
					if (chosen == NULL)
					{
						if (ScanDirectionIsForward(dir))
							chosen = cur;
						else
							impliesNN = cur;
					}
					break;
			}
		}
	}

	/*
	 * If we found no usable boundary keys, we have to start from one end of
	 * the tree.  Walk down that edge to the first or last key, and scan from
	 * there.
	 */
	if (keysCount == 0)
	{
		bool		match;

		match = _toin_endpoint(scan, dir);

		return match;
	}

	/*
	 * We want to start the scan somewhere within the index.  Set up an
	 * insertion scankey we can use to search for the boundary point we
	 * identified above.  The insertion scankey is built using the keys
	 * identified by startKeys[].  (Remaining insertion scankey fields are
	 * initialized after initial-positioning strategy is finalized.)
	 */
	Assert(keysCount <= INDEX_MAX_KEYS);
	for (i = 0; i < keysCount; i++)
	{
		ScanKey		cur = startKeys[i];

		Assert(cur->sk_attno == i + 1);

		if (cur->sk_flags & SK_ROW_HEADER)
		{
			/*
			 * Row comparison header: look to the first row member instead.
			 *
			 * The member scankeys are already in insertion format (ie, they
			 * have sk_func = 3-way-comparison function), but we have to watch
			 * out for nulls, which _bt_preprocess_keys didn't check. A null
			 * in the first row member makes the condition unmatchable, just
			 * like qual_ok = false.
			 */
			ScanKey		subkey = (ScanKey) DatumGetPointer(cur->sk_argument);

			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			if (subkey->sk_flags & SK_ISNULL)
			{
				return false;
			}
			memcpy(inskey.scankeys + i, subkey, sizeof(ScanKeyData));

			/*
			 * If the row comparison is the last positioning key we accepted,
			 * try to add additional keys from the lower-order row members.
			 * (If we accepted independent conditions on additional index
			 * columns, we use those instead --- doesn't seem worth trying to
			 * determine which is more restrictive.)  Note that this is OK
			 * even if the row comparison is of ">" or "<" type, because the
			 * condition applied to all but the last row member is effectively
			 * ">=" or "<=", and so the extra keys don't break the positioning
			 * scheme.  But, by the same token, if we aren't able to use all
			 * the row members, then the part of the row comparison that we
			 * did use has to be treated as just a ">=" or "<=" condition, and
			 * so we'd better adjust strat_total accordingly.
			 */
			if (i == keysCount - 1)
			{
				bool		used_all_subkeys = false;

				Assert(!(subkey->sk_flags & SK_ROW_END));
				for (;;)
				{
					subkey++;
					Assert(subkey->sk_flags & SK_ROW_MEMBER);
					if (subkey->sk_attno != keysCount + 1)
						break;	/* out-of-sequence, can't use it */
					if (subkey->sk_strategy != cur->sk_strategy)
						break;	/* wrong direction, can't use it */
					if (subkey->sk_flags & SK_ISNULL)
						break;	/* can't use null keys */
					Assert(keysCount < INDEX_MAX_KEYS);
					memcpy(inskey.scankeys + keysCount, subkey,
						   sizeof(ScanKeyData));
					keysCount++;
					if (subkey->sk_flags & SK_ROW_END)
					{
						used_all_subkeys = true;
						break;
					}
				}
				if (!used_all_subkeys)
				{
					switch (strat_total)
					{
						case BTLessStrategyNumber:
							strat_total = BTLessEqualStrategyNumber;
							break;
						case BTGreaterStrategyNumber:
							strat_total = BTGreaterEqualStrategyNumber;
							break;
					}
				}
				break;			/* done with outer loop */
			}
		}
		else
		{
			/*
			 * Ordinary comparison key.  Transform the search-style scan key
			 * to an insertion scan key by replacing the sk_func with the
			 * appropriate btree comparison function.
			 *
			 * If scankey operator is not a cross-type comparison, we can use
			 * the cached comparison function; otherwise gotta look it up in
			 * the catalogs.  (That can't lead to infinite recursion, since no
			 * indexscan initiated by syscache lookup will use cross-data-type
			 * operators.)
			 *
			 * We support the convention that sk_subtype == InvalidOid means
			 * the opclass input type; this is a hack to simplify life for
			 * ScanKeyInit().
			 */
			if (cur->sk_subtype == rel->rd_opcintype[i] ||
				cur->sk_subtype == InvalidOid)
			{
				FmgrInfo   *procinfo;

				procinfo = index_getprocinfo(rel, cur->sk_attno, TOIN_ORDER_PROC);
				ScanKeyEntryInitializeWithInfo(inskey.scankeys + i,
											   cur->sk_flags,
											   cur->sk_attno,
											   InvalidStrategy,
											   cur->sk_subtype,
											   cur->sk_collation,
											   procinfo,
											   cur->sk_argument);
			}
			else
			{
				RegProcedure cmp_proc;

				cmp_proc = get_opfamily_proc(rel->rd_opfamily[i],
											 rel->rd_opcintype[i],
											 cur->sk_subtype,
											 TOIN_ORDER_PROC);
				if (!RegProcedureIsValid(cmp_proc))
					elog(ERROR, "missing support function %d(%u,%u) for attribute %d of index \"%s\"",
						 TOIN_ORDER_PROC, rel->rd_opcintype[i], cur->sk_subtype,
						 cur->sk_attno, RelationGetRelationName(rel));
				ScanKeyEntryInitialize(inskey.scankeys + i,
									   cur->sk_flags,
									   cur->sk_attno,
									   InvalidStrategy,
									   cur->sk_subtype,
									   cur->sk_collation,
									   cmp_proc,
									   cur->sk_argument);
			}
		}
	}

	/*----------
	 * Examine the selected initial-positioning strategy to determine exactly
	 * where we need to start the scan, and set flag variables to control the
	 * code below.
	 *
	 * If nextkey = false, _bt_search and _bt_binsrch will locate the first
	 * item >= scan key.  If nextkey = true, they will locate the first
	 * item > scan key.
	 *
	 * If goback = true, we will then step back one item, while if
	 * goback = false, we will start the scan on the located item.
	 *----------
	 */
	switch (strat_total)
	{
		case BTLessStrategyNumber:

			/*
			 * Find first item >= scankey, then back up one to arrive at last
			 * item < scankey.  (Note: this positioning strategy is only used
			 * for a backward scan, so that is always the correct starting
			 * position.)
			 */
			nextkey = false;
			goback = true;
			break;

		case BTLessEqualStrategyNumber:

			/*
			 * Find first item > scankey, then back up one to arrive at last
			 * item <= scankey.  (Note: this positioning strategy is only used
			 * for a backward scan, so that is always the correct starting
			 * position.)
			 */
			nextkey = true;
			goback = true;
			break;

		case BTEqualStrategyNumber:

			/*
			 * If a backward scan was specified, need to start with last equal
			 * item not first one.
			 */
			if (ScanDirectionIsBackward(dir))
			{
				/*
				 * This is the same as the <= strategy.  We will check at the
				 * end whether the found item is actually =.
				 */
				nextkey = true;
				goback = true;
			}
			else
			{
				/*
				 * This is the same as the >= strategy.  We will check at the
				 * end whether the found item is actually =.
				 */
				nextkey = false;
				goback = false;
			}
			break;

		case BTGreaterEqualStrategyNumber:

			/*
			 * Find first item >= scankey.  (This is only used for forward
			 * scans.)
			 */
			nextkey = false;
			goback = false;
			break;

		case BTGreaterStrategyNumber:

			/*
			 * Find first item > scankey.  (This is only used for forward
			 * scans.)
			 */
			nextkey = true;
			goback = false;
			break;

		default:
			/* can't get here, but keep compiler quiet */
			elog(ERROR, "unrecognized strat_total: %d", (int) strat_total);
			return false;
	}

	/* Initialize remaining insertion scan key fields */
	inskey.anynullkeys = false; /* unused */
	inskey.nextkey = nextkey;
	inskey.pivotsearch = false;
	inskey.scantid = NULL;
	inskey.keysz = keysCount;

	/* toin: position to the precise heap block */
	blkno = _toin_search_brin(scan, &inskey);

	/* toin: position to the precise item on the heap page */
	offnum = _toin_search_heap(scan, &inskey, blkno);

	if (offnum == InvalidOffsetNumber)
	{

		ToinScanPosInvalidate(so->currPos);

		return false;
	}

	_toin_initialize_more_data(so, dir);

	/*
	 * If nextkey = false, we are positioned at the first item >= scan key, or
	 * possibly at the end of a page on which all the existing items are less
	 * than the scan key and we know that everything on later pages is greater
	 * than or equal to scan key.
	 *
	 * If nextkey = true, we are positioned at the first item > scan key, or
	 * possibly at the end of a page on which all the existing items are less
	 * than or equal to the scan key and we know that everything on later
	 * pages is greater than scan key.
	 *
	 * The actually desired starting point is either this item or the prior
	 * one, or in the end-of-page case it's the first item on the next page or
	 * the last item on this page.  Adjust the starting offset if needed. (If
	 * this results in an offset before the first item or after the last one,
	 * _bt_readpage will report no items found, and then we'll step to the
	 * next page as needed.)
	 */
	if (goback)
		offnum = OffsetNumberPrev(offnum);

	/* remember which buffer we have pinned, if any */
	Assert(!ToinScanPosIsValid(so->currPos));
	so->currPos.currPage = blkno;

	/*
	 * Now load data from the first page of the scan.
	 */
	if (!_toin_readpage(scan, dir, offnum))
	{
		/*
		 * There's no actually-matching data on this page.  Try to advance to
		 * the next page.  Return false if there's no matching data at all.
		 */
		if (!_toin_steppage(scan, dir))
			return false;
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;

	return true;
}

/*
 * Derived from _bt_next().
 *
 *	_bt_next() -- Get the next item in a scan.
 *
 *		On entry, so->currPos describes the current page, which may be pinned
 *		but is not locked, and so->currPos.itemIndex identifies which item was
 *		previously returned.
 *
 *		On successful exit, scan->xs_ctup.t_self is set to the TID of the
 *		next heap tuple, and if requested, scan->xs_itup points to a copy of
 *		the index tuple.  so->currPos is updated as needed.
 *
 *		On failure exit (no more tuples), we release pin and set
 *		so->currPos.buf to InvalidBuffer.
 */
bool
_toin_next(IndexScanDesc scan, ScanDirection dir)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	ToinScanPosItem *currItem;

	/*
	 * Advance to next tuple on current page; or if there's no more, try to
	 * step to the next page with data.
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (++so->currPos.itemIndex > so->currPos.lastItem)
		{
			if (!_toin_steppage(scan, dir))
				return false;
		}
	}
	else
	{
		if (--so->currPos.itemIndex < so->currPos.firstItem)
		{
			if (!_toin_steppage(scan, dir))
				return false;
		}
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;

	return true;
}

/*
 * Derived from _bt_readpage().
 *
 *	_bt_readpage() -- Load data from current index page into so->currPos
 *
 * Caller must have pinned and read-locked so->currPos.buf; the buffer's state
 * is not changed here.  Also, currPos.moreLeft and moreRight must be valid;
 * they are updated as appropriate.  All other fields of so->currPos are
 * initialized from scratch here.
 *
 * We scan the current page starting at offnum and moving in the indicated
 * direction.  All items matching the scan keys are loaded into currPos.items.
 * moreLeft or moreRight (as appropriate) is cleared if _bt_checkkeys reports
 * that there can be no more matching tuples in the current scan direction.
 *
 * In the case of a parallel scan, caller must have called _bt_parallel_seize
 * prior to calling this function; this function will invoke
 * _bt_parallel_release before returning.
 *
 * Returns true if any matching items found on the page, false if none.
 */
static bool
_toin_readpage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	BlockNumber	blkno;
	OffsetNumber minoff;
	OffsetNumber maxoff;
	int			itemIndex;
	bool		continuescan;
	int			indnatts;

	blkno = so->currPos.currPage;

	continuescan = true;		/* default assumption */
	indnatts = IndexRelationGetNumberOfAttributes(scan->indexRelation);
	_toin_get_heap_block_ntuples(scan, blkno, &minoff, &maxoff);

	if (ScanDirectionIsForward(dir))
	{
		/* load items[] in ascending order */
		itemIndex = 0;

		offnum = Max(offnum, minoff);

		while (offnum <= maxoff)
		{
			_toin_get_heap_tuple(scan, blkno, offnum);

			if (_toin_checkkeys(scan, indnatts, dir, &continuescan))
			{
				/* tuple passes all scan key conditions, so remember it */
				_toin_saveitem(so, itemIndex);
				itemIndex++;
			}
			/* When !continuescan, there can't be any more matches, so stop */
			if (!continuescan)
				break;

			offnum = OffsetNumberNext(offnum);
		}

		/* toin: FIXME: how to do this? */
#if 0
		/*
		 * We don't need to visit page to the right when the high key
		 * indicates that no more matches will be found there.
		 *
		 * Checking the high key like this works out more often than you might
		 * think.  Leaf page splits pick a split point between the two most
		 * dissimilar tuples (this is weighed against the need to evenly share
		 * free space).  Leaf pages with high key attribute values that can
		 * only appear on non-pivot tuples on the right sibling page are
		 * common.
		 */
		if (continuescan && !P_RIGHTMOST(opaque))
		{
			ItemId		iid = PageGetItemId(page, P_HIKEY);
			IndexTuple	itup = (IndexTuple) PageGetItem(page, iid);
			int			truncatt;

			truncatt = BTreeTupleGetNAtts(itup, scan->indexRelation);
			_bt_checkkeys(scan, itup, truncatt, dir, &continuescan);
		}
#endif

		if (!continuescan)
			so->currPos.moreRight = false;

		/* brin: FIXME: is this true for a heap page? */
		Assert(itemIndex <= MaxIndexTuplesPerPage);
		so->currPos.firstItem = 0;
		so->currPos.lastItem = itemIndex - 1;
		so->currPos.itemIndex = 0;
	}
	else
	{
		/* load items[] in descending order */
		/* brin: FIXME: is this correct for a heap page? */
		itemIndex = MaxIndexTuplesPerPage;

		offnum = Min(offnum, maxoff);

		while (offnum >= minoff)
		{
			bool		tuple_alive;
			bool		passes_quals;

			tuple_alive = true;

			_toin_get_heap_tuple(scan, blkno, offnum);

			passes_quals = _toin_checkkeys(scan, indnatts, dir, &continuescan);
			if (passes_quals && tuple_alive)
			{
				/* tuple passes all scan key conditions, so remember it */
				itemIndex--;
				_toin_saveitem(so, itemIndex);
			}
			if (!continuescan)
			{
				/* there can't be any more matches, so stop */
				so->currPos.moreLeft = false;
				break;
			}

			offnum = OffsetNumberPrev(offnum);
		}

		Assert(itemIndex >= 0);
		so->currPos.firstItem = itemIndex;
		so->currPos.lastItem = MaxIndexTuplesPerPage - 1;
		so->currPos.itemIndex = MaxIndexTuplesPerPage - 1;
	}

	return (so->currPos.firstItem <= so->currPos.lastItem);
}

/*
 * Derived from _bt_saveitem().
 *
 * Save an index item into so->currPos.items[itemIndex]
 */
static void
_toin_saveitem(ToinScanOpaque so, int itemIndex)
{
	ToinScanPosItem *currItem = &so->currPos.items[itemIndex];

	currItem->heapTid = so->slot->tts_tid;
}

/*
 * Derived from _bt_steppage().
 *
 *	_bt_steppage() -- Step to next page containing valid data for scan
 *
 * On entry, if so->currPos.buf is valid the buffer is pinned but not locked;
 * if pinned, we'll drop the pin before moving to next page.  The buffer is
 * not locked on entry.
 *
 * For success on a scan using a non-MVCC snapshot we hold a pin, but not a
 * read lock, on that page.  If we do not hold the pin, we set so->currPos.buf
 * to InvalidBuffer.  We return true to indicate success.
 */
static bool
_toin_steppage(IndexScanDesc scan, ScanDirection dir)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	BlockNumber blkno = InvalidBlockNumber;

	Assert(ToinScanPosIsValid(so->currPos));

	if (ScanDirectionIsForward(dir))
	{
		{
			/* Not parallel, so use the previously-saved nextPage link. */
			blkno = so->currPos.currPage + 1;
		}

		/* Remember we left a page with data */
		so->currPos.moreLeft = true;
	}
	else
	{
		/* Remember we left a page with data */
		so->currPos.moreRight = true;

		{
			/* Not parallel, so just use our own notion of the current page */
			blkno = so->currPos.currPage;
		}
	}

	if (!_toin_readnextpage(scan, blkno, dir))
		return false;

	return true;
}

/*
 * Derived from _bt_readnextpage().
 *
 *	_bt_readnextpage() -- Read next page containing valid data for scan
 *
 * On success exit, so->currPos is updated to contain data from the next
 * interesting page.  Caller is responsible to release lock and pin on
 * buffer on success.  We return true to indicate success.
 *
 * If there are no more matching records in the given direction, we drop all
 * locks and pins, set so->currPos.buf to InvalidBuffer, and return false.
 */
static bool
_toin_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	BlockNumber nblocks;

	/* FIXME: the caller should initialize blkno with toin->firstBlock */
	_toin_get_heap_nblocks(scan);
	if (toin->firstBlock == InvalidBlockNumber ||
		toin->lastBlock == InvalidBlockNumber)
		return false;

	nblocks = toin->lastBlock - toin->firstBlock + 1;

	if (ScanDirectionIsForward(dir))
	{
		for (;;)
		{
			/*
			 * if we're at end of scan, give up and mark parallel scan as
			 * done, so that all the workers can finish their scan
			 */
			if (blkno >= nblocks || !so->currPos.moreRight)
			{
				ToinScanPosInvalidate(so->currPos);
				return false;
			}
			/* check for interrupts while we're not holding any buffer lock */
			CHECK_FOR_INTERRUPTS();
			/* step right one page */
			so->currPos.currPage = blkno;
			{
				OffsetNumber minoff;

				_toin_get_heap_block_ntuples(scan, blkno, &minoff, NULL);

				/* see if there are any matches on this page */
				/* note that this will clear moreRight if we can stop */
				if (_toin_readpage(scan, dir, minoff))
					break;
			}
			blkno++;
		}
	}
	else
	{
		/*
		 * Should only happen in parallel cases, when some other backend
		 * advanced the scan.
		 */
		if (BlockNumberIsValid(blkno) && so->currPos.currPage != blkno)
		{
			so->currPos.currPage = blkno;
		}

		/*
		 * Walk left to the next page with data.  This is much more complex
		 * than the walk-right case because of the possibility that the page
		 * to our left splits while we are in flight to it, plus the
		 * possibility that the page we were on gets deleted after we leave
		 * it.  See nbtree/README for details.
		 *
		 * It might be possible to rearrange this code to have less overhead
		 * in pinning and locking, but that would require capturing the left
		 * pointer when the page is initially read, and using it here, along
		 * with big changes to _bt_walk_left() and the code below.  It is not
		 * clear whether this would be a win, since if the page immediately to
		 * the left splits after we read this page and before we step left, we
		 * would need to visit more pages than with the current code.
		 *
		 * Note that if we change the code so that we drop the pin for a scan
		 * which uses a non-MVCC snapshot, we will need to modify the code for
		 * walking left, to allow for the possibility that a referenced page
		 * has been deleted.  As long as the buffer is pinned or the snapshot
		 * is MVCC the page cannot move past the half-dead state to fully
		 * deleted.
		 */

		for (;;)
		{
			/* Done if we know there are no matching keys to the left */
			if (!so->currPos.moreLeft || blkno == 0 || !BlockNumberIsValid(blkno))
			{
				ToinScanPosInvalidate(so->currPos);
				return false;
			}

			/* Step to next physical page */
			blkno--;
			so->currPos.currPage = blkno;

			{
				OffsetNumber maxoff;

				_toin_get_heap_block_ntuples(scan, blkno, NULL, &maxoff);

				/* see if there are any matches on this page */
				/* note that this will clear moreLeft if we can stop */
				if (_toin_readpage(scan, dir, maxoff))
					break;
			}
		}
	}

	return true;
}

/*
 * Derived from _bt_endpoint().
 *
 *	_bt_endpoint() -- Find the first or last page in the index, and scan
 * from there to the first key satisfying all the quals.
 *
 * This is used by _bt_first() to set up a scan when we've determined
 * that the scan must start at the beginning or end of the index (for
 * a forward or backward scan respectively).  Exit conditions are the
 * same as for _bt_first().
 */
static bool
_toin_endpoint(IndexScanDesc scan, ScanDirection dir)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	BlockNumber	blkno;
	OffsetNumber start;
	ToinScanPosItem *currItem;

	{
		_toin_get_heap_nblocks(scan);

		if (toin->firstBlock == InvalidBlockNumber ||
			toin->lastBlock == InvalidBlockNumber)
		{
			/*
			 * Empty index. Lock the whole relation, as nothing finer to lock
			 * exists.
			 */
			ToinScanPosInvalidate(so->currPos);
			return false;
		}

		if (ScanDirectionIsForward(dir))
			blkno = toin->firstBlock;
		else
			blkno = toin->lastBlock;
	}

	if (ScanDirectionIsForward(dir))
		_toin_get_heap_block_ntuples(scan, blkno, &start, NULL);
	else if (ScanDirectionIsBackward(dir))
		_toin_get_heap_block_ntuples(scan, blkno, NULL, &start);
	else
	{
		elog(ERROR, "invalid scan direction: %d", (int) dir);
		start = 0;				/* keep compiler quiet */
	}

	/* remember which buffer we have pinned */
	so->currPos.currPage = blkno;

	_toin_initialize_more_data(so, dir);

	/*
	 * Now load data from the first page of the scan.
	 */
	if (!_toin_readpage(scan, dir, start))
	{
		/*
		 * There's no actually-matching data on this page.  Try to advance to
		 * the next page.  Return false if there's no matching data at all.
		 */
		if (!_toin_steppage(scan, dir))
			return false;
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;

	return true;
}

/*
 * Derived from _bt_initialize_more_data().
 *
 * _bt_initialize_more_data() -- initialize moreLeft/moreRight appropriately
 * for scan direction
 */
static inline void
_toin_initialize_more_data(ToinScanOpaque so, ScanDirection dir)
{
	/* initialize moreLeft/moreRight appropriately for scan direction */
	if (ScanDirectionIsForward(dir))
	{
		so->currPos.moreLeft = false;
		so->currPos.moreRight = true;
	}
	else
	{
		so->currPos.moreLeft = true;
		so->currPos.moreRight = false;
	}
}

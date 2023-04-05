/*-------------------------------------------------------------------------
 *
 * sortheap_btree.c
 *	  Internal BTREE index for sort heap.
 *
 *
 * Sort Heap build a BTREE like index automatically and internally, a
 * sortheap_btree index must be created on the sortheap table before
 * any operations because the btree index key also indicates:
 *
 * 1) the underlying sorting/merging keys of the whole table.
 * 2) the grouping keys to start the columnstore and compression.
 *
 * We build a brand new btree for each logical tape run which means it
 * is a btree forest in general. An index scan on the table is consist
 * of multiple smaller index scan on logical tape runs internally. We
 * keep merging smaller runs to bigger runs, so the scale of runs is
 * kept in a reasonable number.
 *
 * Another difference is the index tuples are kept in the same file of
 * the data, we only use the same search logical with a normal btree. It
 * is much more convenient to implement like this because the logical
 * tape runs are merged/recycled frequently in the sortheap, if we manage
 * or recycle the index tuples in the old way, it will be a disaster.
 *
 * The third difference is that we always build the btree on a sorted
 * tuple sets which makes the index building much faster.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_btree.c
 *
 *-------------------------------------------------------------------------
 */

#include "sortheap.h"
#include "sortheap_btree.h"
#include "sortheap_tapesets.h"

static OffsetNumber sortheap_bt_binsrch(Relation rel, BTScanInsert key, Buffer buf);
static void sortheap_bt_drop_lock_and_maybe_pin(IndexScanDesc scan, BTScanPos sp);
static bool sortheap_bt_readpage(IndexScanDesc scan, ScanDirection dir,
								 OffsetNumber offnum);
static void sortheap_bt_saveitem(BTScanOpaque so, int itemIndex,
								 OffsetNumber offnum, IndexTuple itup);
static bool sortheap_bt_steppage(IndexScanDesc scan, ScanDirection dir);
static bool sortheap_bt_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir);
static bool sortheap_bt_endpoint(IndexScanDesc scan, ScanDirection dir);
static bool sortheap_bt_advance_runs(IndexScanDesc scan, ScanDirection dir);
static inline void sortheap_bt_initialize_more_data(BTScanOpaque so, ScanDirection dir);
static Buffer sortheap_bt_walk_left(Relation rel, Buffer buf, Snapshot snapshot);

static Buffer
sortheap_bt_getbuf(Relation rel, BlockNumber blkno, int access)
{
	Buffer buf;

	Assert(access == BT_READ);
	Assert(blkno != P_NEW);

	/* Read an existing block of the relation */
	buf = ReadBuffer(rel, blkno);
	LockBuffer(buf, access);
	_bt_checkpage(rel, buf);

	return buf;
}

static void
sortheap_bt_relbuf(Relation rel, Buffer buf)
{
	UnlockReleaseBuffer(buf);
}

static Buffer
sortheap_bt_relandgetbuf(Relation rel, Buffer obuf, BlockNumber blkno, int access)
{
	Buffer buf;

	Assert(blkno != P_NEW);
	if (BufferIsValid(obuf))
		LockBuffer(obuf, BUFFER_LOCK_UNLOCK);
	buf = ReleaseAndReadBuffer(obuf, rel, blkno);
	LockBuffer(buf, access);
	_bt_checkpage(rel, buf);
	return buf;
}

static Buffer
sortheap_bt_getroot(IndexScanDesc scan, int access)
{
	IndexFetchSortHeapData *sortheap_fetch;
	SortHeapState *shstate;
	TapeSetsState *tsstate;
	TapeScanDesc scandesc;
	BlockNumber blkno;
	Buffer buffer;

	sortheap_fetch = (IndexFetchSortHeapData *)scan->xs_heapfetch;
	shstate = sortheap_fetch->sortstate;
	tsstate = TAPESETSSTATE(shstate, shstate->cur_readtapesets);

	if (tapesets_getstatus_forread(tsstate) == TS_INITIAL)
		return InvalidBuffer;

	scandesc = tape_get_scandesc(tsstate,
								 tsstate->cur_readtape,
								 tsstate->cur_readrun,
								 NULL);

	if (!scandesc)
		return InvalidBuffer;

	blkno = scandesc->runmetad.btroot;

	if (blkno == InvalidBlockNumber || blkno == 0)
		return InvalidBuffer;

	buffer = ReadBuffer(scan->heapRelation, blkno);
	LockBuffer(buffer, BT_READ);
	return buffer;
}

/*
 *	sortheap_bt_binsrch() -- Do a binary search for a key on a particular page.
 *
 * On a leaf page, sortheap_bt_binsrch() returns the OffsetNumber of the first
 * key >= given scankey, or > scankey if nextkey is true.  (NOTE: in
 * particular, this means it is possible to return a value 1 greater than the
 * number of keys on the page, if the scankey is > all keys on the page.)
 *
 * On an internal (non-leaf) page, sortheap_bt_binsrch() returns the OffsetNumber
 * of the last key < given scankey, or last key <= given scankey if nextkey
 * is true.  (Since sortheap_bt_compare treats the first data key of such a page as
 * minus infinity, there will be at least one key < scankey, so the result
 * always points at one of the keys on the page.)  This key indicates the
 * right place to descend to be sure we find all leaf keys >= given scankey
 * (or leaf keys > given scankey when nextkey is true).
 *
 * This procedure is not responsible for walking right, it just examines
 * the given page.  sortheap_bt_binsrch() has no lock or refcount side effects
 * on the buffer.
 */
static OffsetNumber
sortheap_bt_binsrch(Relation rel,
					BTScanInsert key,
					Buffer buf)
{
	Page page;
	BTPageOpaque opaque;
	OffsetNumber low,
		high;
	int32 result,
		cmpval;

	/* Requesting nextkey semantics while using scantid seems nonsensical */
	Assert(!key->nextkey || key->scantid == NULL);

	page = BufferGetPage(buf);
	opaque = (BTPageOpaque)PageGetSpecialPointer(page);

	low = P_FIRSTDATAKEY(opaque);
	high = PageGetMaxOffsetNumber(page);

	/*
	 * If there are no keys on the page, return the first available slot. Note
	 * this covers two cases: the page is really empty (no keys), or it
	 * contains only a high key.  The latter case is possible after vacuuming.
	 * This can never happen on an internal page, however, since they are
	 * never empty (an internal page must have children).
	 */
	if (unlikely(high < low))
		return low;

	/*
	 * Binary search to find the first key on the page >= scan key, or first
	 * key > scankey when nextkey is true.
	 *
	 * For nextkey=false (cmpval=1), the loop invariant is: all slots before
	 * 'low' are < scan key, all slots at or after 'high' are >= scan key.
	 *
	 * For nextkey=true (cmpval=0), the loop invariant is: all slots before
	 * 'low' are <= scan key, all slots at or after 'high' are > scan key.
	 *
	 * We can fall out when high == low.
	 */
	high++; /* establish the loop invariant for high */

	cmpval = key->nextkey ? 0 : 1; /* select comparison value */

	while (high > low)
	{
		OffsetNumber mid = low + ((high - low) / 2);

		/* We have low <= mid < high, so mid points at a real slot */

		result = _bt_compare(rel, key, page, mid);

		if (result >= cmpval)
			low = mid + 1;
		else
			high = mid;
	}

	/*
	 * At this point we have high == low, but be careful: they could point
	 * past the last slot on the page.
	 *
	 * On a leaf page, we always return the first key >= scan key (resp. >
	 * scan key), which could be the last slot + 1.
	 */
	if (P_ISLEAF(opaque))
		return low;

	/*
	 * On a non-leaf page, return the last key < scan key (resp. <= scan key).
	 * There must be one if _bt_compare() is playing by the rules.
	 */
	Assert(low > P_FIRSTDATAKEY(opaque));

	return OffsetNumberPrev(low);
}

/*
 *	_bt_drop_lock_and_maybe_pin()
 *
 * Unlock the buffer; and if it is safe to release the pin, do that, too.  It
 * is safe if the scan is using an MVCC snapshot and the index is WAL-logged.
 * This will prevent vacuum from stalling in a blocked state trying to read a
 * page when a cursor is sitting on it -- at least in many important cases.
 *
 * Set the buffer to invalid if the pin is released, since the buffer may be
 * re-used.  If we need to go back to this block (for example, to apply
 * LP_DEAD hints) we must get a fresh reference to the buffer.  Hopefully it
 * will remain in shared memory for as long as it takes to scan the index
 * buffer page.
 */
static void
sortheap_bt_drop_lock_and_maybe_pin(IndexScanDesc scan, BTScanPos sp)
{
	LockBuffer(sp->buf, BUFFER_LOCK_UNLOCK);

	if (IsMVCCSnapshot(scan->xs_snapshot) &&
		RelationNeedsWAL(scan->indexRelation) &&
		!scan->xs_want_itup)
	{
		ReleaseBuffer(sp->buf);
		sp->buf = InvalidBuffer;
	}
}

/*
 *	_bt_moveright() -- move right in the btree if necessary.
 *
 * When we follow a pointer to reach a page, it is possible that
 * the page has changed in the meanwhile.  If this happens, we're
 * guaranteed that the page has "split right" -- that is, that any
 * data that appeared on the page originally is either on the page
 * or strictly to the right of it.
 *
 * This routine decides whether or not we need to move right in the
 * tree by examining the high key entry on the page.  If that entry is
 * strictly less than the scankey, or <= the scankey in the
 * key.nextkey=true case, then we followed the wrong link and we need
 * to move right.
 *
 * The passed insertion-type scankey can omit the rightmost column(s) of the
 * index. (see nbtree/README)
 *
 * When key.nextkey is false (the usual case), we are looking for the first
 * item >= key.  When key.nextkey is true, we are looking for the first item
 * strictly greater than key.
 *
 * If forupdate is true, we will attempt to finish any incomplete splits
 * that we encounter.  This is required when locking a target page for an
 * insertion, because we don't allow inserting on a page before the split
 * is completed.  'stack' is only used if forupdate is true.
 *
 * On entry, we have the buffer pinned and a lock of the type specified by
 * 'access'.  If we move right, we release the buffer and lock and acquire
 * the same on the right sibling.  Return value is the buffer we stop at.
 *
 * If the snapshot parameter is not NULL, "old snapshot" checking will take
 * place during the descent through the tree.  This is not needed when
 * positioning for an insert or delete, so NULL is used for those cases.
 */
static Buffer
sortheap_bt_moveright(IndexScanDesc scan,
					  BTScanInsert key,
					  Buffer buf,
					  bool forupdate,
					  BTStack stack,
					  int access,
					  Snapshot snapshot)
{
	Page page;
	BTPageOpaque opaque;
	int32 cmpval;

	/*
	 * When nextkey = false (normal case): if the scan key that brought us to
	 * this page is > the high key stored on the page, then the page has split
	 * and we need to move right.  (pg_upgrade'd !heapkeyspace indexes could
	 * have some duplicates to the right as well as the left, but that's
	 * something that's only ever dealt with on the leaf level, after
	 * _bt_search has found an initial leaf page.)
	 *
	 * When nextkey = true: move right if the scan key is >= page's high key.
	 * (Note that key.scantid cannot be set in this case.)
	 *
	 * The page could even have split more than once, so scan as far as
	 * needed.
	 *
	 * We also have to move right if we followed a link that brought us to a
	 * dead page.
	 */
	cmpval = key->nextkey ? 0 : 1;

	for (;;)
	{
		page = BufferGetPage(buf);
		TestForOldSnapshot(snapshot, scan->heapRelation, page);
		opaque = (BTPageOpaque)PageGetSpecialPointer(page);

		if (P_RIGHTMOST(opaque))
			break;

		/*
		 * Finish any incomplete splits we encounter along the way.
		 */
		if (forupdate && P_INCOMPLETE_SPLIT(opaque))
		{
			elog(PANIC, "incomplete split is not implemented yet");

			BlockNumber blkno = BufferGetBlockNumber(buf);

			/* upgrade our lock if necessary */
			if (access == BT_READ)
			{
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);
				LockBuffer(buf, BT_WRITE);
			}

			if (P_INCOMPLETE_SPLIT(opaque))
			{
				_bt_finish_split(scan->heapRelation, buf, stack);
			}
			else
				_bt_relbuf(scan->heapRelation, buf);

			/* re-acquire the lock in the right mode, and re-check */
			buf = _bt_getbuf(scan->heapRelation, blkno, access);
			continue;
		}

		if (P_IGNORE(opaque) || _bt_compare(scan->indexRelation, key, page, P_HIKEY) >= cmpval)
		{
			/* step right one page */
			buf = sortheap_bt_relandgetbuf(scan->heapRelation, buf, opaque->btpo_next, access);
			continue;
		}
		else
			break;
	}

	if (P_IGNORE(opaque))
		elog(ERROR, "fell off the end of index \"%s\"",
			 RelationGetRelationName(scan->heapRelation));

	return buf;
}

/*
 *	sortheap_bt_search() -- Search the tree for a particular scankey,
 *		or more precisely for the first leaf page it could be on.
 *
 * The passed scankey is an insertion-type scankey (see nbtree/README),
 * but it can omit the rightmost column(s) of the index.
 *
 * Return value is a stack of parent-page pointers.  *bufP is set to the
 * address of the leaf-page buffer, which is read-locked and pinned.
 * No locks are held on the parent pages, however!
 *
 * If the snapshot parameter is not NULL, "old snapshot" checking will take
 * place during the descent through the tree.  This is not needed when
 * positioning for an insert or delete, so NULL is used for those cases.
 *
 * The returned buffer is locked according to access parameter.  Additionally,
 * access = BT_WRITE will allow an empty root page to be created and returned.
 * When access = BT_READ, an empty index will result in *bufP being set to
 * InvalidBuffer.  Also, in BT_WRITE mode, any incomplete splits encountered
 * during the search will be finished.
 */
static BTStack
sortheap_bt_search(IndexScanDesc scan, BTScanInsert key, Buffer *bufP, int access,
				   Snapshot snapshot)
{
	BTStack stack_in = NULL;
	int page_access = BT_READ;

	/* Get the root page to start with */
	*bufP = sortheap_bt_getroot(scan, access);

	/* If index is empty and access = BT_READ, no root page is created. */
	if (!BufferIsValid(*bufP))
		return (BTStack)NULL;

	/* Loop iterates once per level descended in the tree */
	for (;;)
	{
		Page page;
		BTPageOpaque opaque;
		OffsetNumber offnum;
		ItemId itemid;
		IndexTuple itup;
		BlockNumber blkno;
		BlockNumber par_blkno;
		BTStack new_stack;

		/*
		 * Race -- the page we just grabbed may have split since we read its
		 * pointer in the parent (or metapage).  If it has, we may need to
		 * move right to its new sibling.  Do that.
		 *
		 * In write-mode, allow _bt_moveright to finish any incomplete splits
		 * along the way.  Strictly speaking, we'd only need to finish an
		 * incomplete split on the leaf page we're about to insert to, not on
		 * any of the upper levels (they are taken care of in _bt_getstackbuf,
		 * if the leaf page is split and we insert to the parent page).  But
		 * this is a good opportunity to finish splits of internal pages too.
		 */
		*bufP = sortheap_bt_moveright(scan, key, *bufP, (access == BT_WRITE), stack_in,
									  page_access, snapshot);

		/* if this is a leaf page, we're done */
		page = BufferGetPage(*bufP);
		opaque = (BTPageOpaque)PageGetSpecialPointer(page);
		if (P_ISLEAF(opaque))
			break;

		/*
		 * Find the appropriate item on the internal page, and get the child
		 * page that it points to.
		 */
		offnum = sortheap_bt_binsrch(scan->indexRelation, key, *bufP);
		itemid = PageGetItemId(page, offnum);
		itup = (IndexTuple)PageGetItem(page, itemid);
		blkno = BTreeInnerTupleGetDownLink(itup);
		par_blkno = BufferGetBlockNumber(*bufP);

		/*
		 * We need to save the location of the index entry we chose in the
		 * parent page on a stack. In case we split the tree, we'll use the
		 * stack to work back up to the parent page.  We also save the actual
		 * downlink (block) to uniquely identify the index entry, in case it
		 * moves right while we're working lower in the tree.  See the paper
		 * by Lehman and Yao for how this is detected and handled. (We use the
		 * child link during the second half of a page split -- if caller ends
		 * up splitting the child it usually ends up inserting a new pivot
		 * tuple for child's new right sibling immediately after the original
		 * bts_offset offset recorded here.  The downlink block will be needed
		 * to check if bts_offset remains the position of this same pivot
		 * tuple.)
		 */
		new_stack = (BTStack)palloc(sizeof(BTStackData));
		new_stack->bts_blkno = par_blkno;
		new_stack->bts_offset = offnum;
		new_stack->bts_btentry = blkno;
		new_stack->bts_parent = stack_in;

		/*
		 * Page level 1 is lowest non-leaf page level prior to leaves.  So, if
		 * we're on the level 1 and asked to lock leaf page in write mode,
		 * then lock next page in write mode, because it must be a leaf.
		 */
		if (opaque->btpo.level == 1 && access == BT_WRITE)
			page_access = BT_WRITE;

		/* drop the read lock on the parent page, acquire one on the child */
		*bufP = sortheap_bt_relandgetbuf(scan->heapRelation, *bufP, blkno, page_access);

		/* okay, all set to move down a level */
		stack_in = new_stack;
	}

	/*
	 * If we're asked to lock leaf in write mode, but didn't manage to, then
	 * relock.  This should only happen when the root page is a leaf page (and
	 * the only page in the index other than the metapage).
	 */
	if (access == BT_WRITE && page_access == BT_READ)
	{
		/* trade in our read lock for a write lock */
		LockBuffer(*bufP, BUFFER_LOCK_UNLOCK);
		LockBuffer(*bufP, BT_WRITE);

		/*
		 * If the page was split between the time that we surrendered our read
		 * lock and acquired our write lock, then this page may no longer be
		 * the right place for the key we want to insert.  In this case, we
		 * need to move right in the tree.  See Lehman and Yao for an
		 * excruciatingly precise description.
		 */
		*bufP = sortheap_bt_moveright(scan, key, *bufP, true, stack_in, BT_WRITE,
									  snapshot);
	}

	return stack_in;
}

/*
 *	sortheap_bt_first() -- Find the first item in a scan.
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
bool sortheap_bt_first(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;
	Buffer buf;
	BTStack stack;
	OffsetNumber offnum;
	StrategyNumber strat;
	bool nextkey;
	bool goback;
	BTScanInsertData inskey;
	ScanKey startKeys[INDEX_MAX_KEYS];
	ScanKeyData notnullkeys[INDEX_MAX_KEYS];
	int keysCount = 0;
	int i;
	StrategyNumber strat_total;
	BTScanPosItem *currItem;

	Assert(!BTScanPosIsValid(so->currPos));

	/*
	 * Examine the scan keys and eliminate any redundant keys; also mark the
	 * keys that must be matched to continue the scan.
	 */
	_bt_preprocess_keys(scan);

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
		AttrNumber curattr;
		ScanKey chosen;
		ScanKey impliesNN;
		ScanKey cur;

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
					((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ? ScanDirectionIsForward(dir) : ScanDirectionIsBackward(dir)))
				{
					/* Yes, so build the key in notnullkeys[keysCount] */
					chosen = &notnullkeys[keysCount];
					ScanKeyEntryInitialize(chosen,
										   (SK_SEARCHNOTNULL | SK_ISNULL |
											(impliesNN->sk_flags &
											 (SK_BT_DESC | SK_BT_NULLS_FIRST))),
										   curattr,
										   ((impliesNN->sk_flags & SK_BT_NULLS_FIRST) ? BTGreaterStrategyNumber : BTLessStrategyNumber),
										   InvalidOid,
										   InvalidOid,
										   InvalidOid,
										   (Datum)0);
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
		return sortheap_bt_endpoint(scan, dir);

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
		ScanKey cur = startKeys[i];

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
			ScanKey subkey = (ScanKey)DatumGetPointer(cur->sk_argument);

			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			if (subkey->sk_flags & SK_ISNULL)
				return false;

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
				bool used_all_subkeys = false;

				Assert(!(subkey->sk_flags & SK_ROW_END));
				for (;;)
				{
					subkey++;
					Assert(subkey->sk_flags & SK_ROW_MEMBER);
					if (subkey->sk_attno != keysCount + 1)
						break; /* out-of-sequence, can't use it */
					if (subkey->sk_strategy != cur->sk_strategy)
						break; /* wrong direction, can't use it */
					if (subkey->sk_flags & SK_ISNULL)
						break; /* can't use null keys */
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
				break; /* done with outer loop */
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
			if (cur->sk_subtype == scan->indexRelation->rd_opcintype[i] ||
				cur->sk_subtype == InvalidOid)
			{
				FmgrInfo *procinfo;

				procinfo = index_getprocinfo(scan->indexRelation, cur->sk_attno, BTORDER_PROC);
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

				cmp_proc = get_opfamily_proc(scan->indexRelation->rd_opfamily[i],
											 scan->indexRelation->rd_opcintype[i],
											 cur->sk_subtype,
											 BTORDER_PROC);
				if (!RegProcedureIsValid(cmp_proc))
					elog(ERROR, "missing support function %d(%u,%u) for attribute %d of index \"%s\"",
						 BTORDER_PROC, scan->indexRelation->rd_opcintype[i], cur->sk_subtype,
						 cur->sk_attno, RelationGetRelationName(scan->indexRelation));
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
		elog(ERROR, "unrecognized strat_total: %d", (int)strat_total);
		return false;
	}

	/* Initialize remaining insertion scan key fields */
	inskey.heapkeyspace = true;
	inskey.anynullkeys = false; /* unused */
	inskey.nextkey = nextkey;
	inskey.pivotsearch = false;
	inskey.scantid = NULL;
	inskey.keysz = keysCount;

	/*
	 * Use the manufactured insertion scan key to descend the tree and
	 * position ourselves on the target leaf page.
	 */
	stack = sortheap_bt_search(scan, &inskey, &buf, BT_READ, scan->xs_snapshot);

	/* don't need to keep the stack around... */
	_bt_freestack(stack);

	if (!BufferIsValid(buf))
	{
		/*
		 * We only get here if the index is completely empty. Lock relation
		 * because nothing finer to lock exists.
		 */
		PredicateLockRelation(scan->indexRelation, scan->xs_snapshot);

		BTScanPosInvalidate(so->currPos);

		return false;
	}
	else
		PredicateLockPage(scan->heapRelation, BufferGetBlockNumber(buf),
						  scan->xs_snapshot);

	sortheap_bt_initialize_more_data(so, dir);

	/* position to the precise item on the page */
	offnum = sortheap_bt_binsrch(scan->indexRelation, &inskey, buf);

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
	Assert(!BTScanPosIsValid(so->currPos));
	so->currPos.buf = buf;

	/*
	 * Now load data from the first page of the scan.
	 */
	if (!sortheap_bt_readpage(scan, dir, offnum))
	{
		/*
		 * There's no actually-matching data on this page.  Try to advance to
		 * the next page.  Return false if there's no matching data at all.
		 */
		LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
		if (!sortheap_bt_steppage(scan, dir))
			return false;
	}
	else
	{
		/* Drop the lock, and maybe the pin, on the current page */
		sortheap_bt_drop_lock_and_maybe_pin(scan, &so->currPos);
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple)(so->currTuples + currItem->tupleOffset);

	return true;
}

/*
 *	sortheap_bt_next() -- Get the next item in a scan.
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
static bool
sortheap_bt_next(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;
	BTScanPosItem *currItem;

	/*
	 * Advance to next tuple on current page; or if there's no more, try to
	 * step to the next page with data.
	 */
	if (ScanDirectionIsForward(dir))
	{
		if (++so->currPos.itemIndex > so->currPos.lastItem)
		{
			if (!sortheap_bt_steppage(scan, dir))
				return false;
		}
	}
	else
	{
		if (--so->currPos.itemIndex < so->currPos.firstItem)
		{
			if (!sortheap_bt_steppage(scan, dir))
				return false;
		}
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple)(so->currTuples + currItem->tupleOffset);

	return true;
}

/*
 *	sortheap_bt_readpage() -- Load data from current index page into so->currPos
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
 * Returns true if any matching items found on the page, false if none.
 */
static bool
sortheap_bt_readpage(IndexScanDesc scan, ScanDirection dir, OffsetNumber offnum)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;
	Page page;
	BTPageOpaque opaque;
	OffsetNumber minoff;
	OffsetNumber maxoff;
	int itemIndex;
	bool continuescan;
	int indnatts;

	/*
	 * We must have the buffer pinned and locked, but the usual macro can't be
	 * used here; this function is what makes it good for currPos.
	 */
	Assert(BufferIsValid(so->currPos.buf));

	page = BufferGetPage(so->currPos.buf);
	opaque = (BTPageOpaque)PageGetSpecialPointer(page);

	continuescan = true; /* default assumption */
	indnatts = IndexRelationGetNumberOfAttributes(scan->indexRelation);
	minoff = P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * We note the buffer's block number so that we can release the pin later.
	 * This allows us to re-read the buffer if it is needed again for hinting.
	 */
	so->currPos.currPage = BufferGetBlockNumber(so->currPos.buf);

	/*
	 * We save the LSN of the page as we read it, so that we know whether it
	 * safe to apply LP_DEAD hints to the page later.  This allows us to drop
	 * the pin for MVCC scans, which allows vacuum to avoid blocking.
	 */
	so->currPos.lsn = BufferGetLSNAtomic(so->currPos.buf);

	/*
	 * we must save the page's right-link while scanning it; this tells us
	 * where to step right to after we're done with these items.  There is no
	 * corresponding need for the left-link, since splits always go right.
	 */
	so->currPos.nextPage = opaque->btpo_next;

	/* initialize tuple workspace to empty */
	so->currPos.nextTupleOffset = 0;

	/*
	 * Now that the current page has been made consistent, the macro should be
	 * good.
	 */
	Assert(BTScanPosIsPinned(so->currPos));

	if (ScanDirectionIsForward(dir))
	{
		/* load items[] in ascending order */
		itemIndex = 0;

		offnum = Max(offnum, minoff);

		while (offnum <= maxoff)
		{
			ItemId iid = PageGetItemId(page, offnum);
			IndexTuple itup;

			/*
			 * If the scan specifies not to return killed tuples, then we
			 * treat a killed tuple as not passing the qual
			 */
			if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
			{
				offnum = OffsetNumberNext(offnum);
				continue;
			}

			itup = (IndexTuple)PageGetItem(page, iid);

			if (_bt_checkkeys(scan, itup, indnatts, dir, &continuescan))
			{
				/* tuple passes all scan key conditions, so remember it */
				sortheap_bt_saveitem(so, itemIndex, offnum, itup);
				itemIndex++;
			}
			/* When !continuescan, there can't be any more matches, so stop */
			if (!continuescan)
				break;

			offnum = OffsetNumberNext(offnum);
		}

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
			ItemId iid = PageGetItemId(page, P_HIKEY);
			IndexTuple itup = (IndexTuple)PageGetItem(page, iid);
			int truncatt;

			truncatt = BTreeTupleGetNAtts(itup, scan->indexRelation);
			_bt_checkkeys(scan, itup, truncatt, dir, &continuescan);
		}

		if (!continuescan)
			so->currPos.moreRight = false;

		Assert(itemIndex <= MaxIndexTuplesPerPage);
		so->currPos.firstItem = 0;
		so->currPos.lastItem = itemIndex - 1;
		so->currPos.itemIndex = 0;
	}
	else
	{
		/* load items[] in descending order */
		itemIndex = MaxIndexTuplesPerPage;

		offnum = Min(offnum, maxoff);

		while (offnum >= minoff)
		{
			ItemId iid = PageGetItemId(page, offnum);
			IndexTuple itup;
			bool tuple_alive;
			bool passes_quals;

			/*
			 * If the scan specifies not to return killed tuples, then we
			 * treat a killed tuple as not passing the qual.  Most of the
			 * time, it's a win to not bother examining the tuple's index
			 * keys, but just skip to the next tuple (previous, actually,
			 * since we're scanning backwards).  However, if this is the first
			 * tuple on the page, we do check the index keys, to prevent
			 * uselessly advancing to the page to the left.  This is similar
			 * to the high key optimization used by forward scans.
			 */
			if (scan->ignore_killed_tuples && ItemIdIsDead(iid))
			{
				Assert(offnum >= P_FIRSTDATAKEY(opaque));
				if (offnum > P_FIRSTDATAKEY(opaque))
				{
					offnum = OffsetNumberPrev(offnum);
					continue;
				}

				tuple_alive = false;
			}
			else
				tuple_alive = true;

			itup = (IndexTuple)PageGetItem(page, iid);

			passes_quals = _bt_checkkeys(scan, itup, indnatts, dir,
										 &continuescan);
			if (passes_quals && tuple_alive)
			{
				/* tuple passes all scan key conditions, so remember it */
				itemIndex--;
				sortheap_bt_saveitem(so, itemIndex, offnum, itup);
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

/* Save an index item into so->currPos.items[itemIndex] */
static void
sortheap_bt_saveitem(BTScanOpaque so, int itemIndex,
					 OffsetNumber offnum, IndexTuple itup)
{
	BTScanPosItem *currItem = &so->currPos.items[itemIndex];

	currItem->heapTid = itup->t_tid;
	currItem->indexOffset = offnum;
	if (so->currTuples)
	{
		Size itupsz = IndexTupleSize(itup);

		currItem->tupleOffset = so->currPos.nextTupleOffset;
		memcpy(so->currTuples + so->currPos.nextTupleOffset, itup, itupsz);
		so->currPos.nextTupleOffset += MAXALIGN(itupsz);
	}
}

/*
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
sortheap_bt_steppage(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;
	BlockNumber blkno = InvalidBlockNumber;

	Assert(BTScanPosIsValid(so->currPos));

	/*
	 * Before we modify currPos, make a copy of the page data if there was a
	 * mark position that needs it.
	 */
	if (so->markItemIndex >= 0)
	{
		/* bump pin on current buffer for assignment to mark buffer */
		if (BTScanPosIsPinned(so->currPos))
			IncrBufferRefCount(so->currPos.buf);
		memcpy(&so->markPos, &so->currPos,
			   offsetof(BTScanPosData, items[1]) +
				   so->currPos.lastItem * sizeof(BTScanPosItem));
		if (so->markTuples)
			memcpy(so->markTuples, so->currTuples,
				   so->currPos.nextTupleOffset);
		so->markPos.itemIndex = so->markItemIndex;
		so->markItemIndex = -1;
	}

	if (ScanDirectionIsForward(dir))
	{
		blkno = so->currPos.nextPage;

		/* Remember we left a page with data */
		so->currPos.moreLeft = true;

		/* release the previous buffer, if pinned */
		BTScanPosUnpinIfPinned(so->currPos);
	}
	else
	{
		/* Remember we left a page with data */
		so->currPos.moreRight = true;

		blkno = so->currPos.currPage;
	}

	if (!sortheap_bt_readnextpage(scan, blkno, dir))
		return false;

	/* Drop the lock, and maybe the pin, on the current page */
	sortheap_bt_drop_lock_and_maybe_pin(scan, &so->currPos);

	return true;
}

/*
 *	sortheap_bt_readnextpage() -- Read next page containing valid data for scan
 *
 * On success exit, so->currPos is updated to contain data from the next
 * interesting page.  Caller is responsible to release lock and pin on
 * buffer on success.  We return true to indicate success.
 *
 * If there are no more matching records in the given direction, we drop all
 * locks and pins, set so->currPos.buf to InvalidBuffer, and return false.
 */
static bool
sortheap_bt_readnextpage(IndexScanDesc scan, BlockNumber blkno, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;
	Page page;
	BTPageOpaque opaque;

	if (ScanDirectionIsForward(dir))
	{
		for (;;)
		{
			/*
			 * if we're at end of scan, give up and mark parallel scan as
			 * done, so that all the workers can finish their scan
			 */
			if (blkno == P_NONE || !so->currPos.moreRight)
			{
				BTScanPosInvalidate(so->currPos);
				return false;
			}
			/* check for interrupts while we're not holding any buffer lock */
			CHECK_FOR_INTERRUPTS();
			/* step right one page */
			so->currPos.buf = sortheap_bt_getbuf(scan->heapRelation, blkno, BT_READ);
			page = BufferGetPage(so->currPos.buf);
			TestForOldSnapshot(scan->xs_snapshot, scan->heapRelation, page);
			opaque = (BTPageOpaque)PageGetSpecialPointer(page);
			/* check for deleted page */
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(scan->heapRelation, blkno, scan->xs_snapshot);
				/* see if there are any matches on this page */
				/* note that this will clear moreRight if we can stop */
				if (sortheap_bt_readpage(scan, dir, P_FIRSTDATAKEY(opaque)))
					break;
			}

			blkno = opaque->btpo_next;
			sortheap_bt_relbuf(scan->heapRelation, so->currPos.buf);
		}
	}
	else
	{
		/*
		 * Should only happen in parallel cases, when some other backend
		 * advanced the scan.
		 */
		if (so->currPos.currPage != blkno)
		{
			BTScanPosUnpinIfPinned(so->currPos);
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
		if (BTScanPosIsPinned(so->currPos))
			LockBuffer(so->currPos.buf, BT_READ);
		else
			so->currPos.buf = sortheap_bt_getbuf(scan->heapRelation, so->currPos.currPage, BT_READ);

		for (;;)
		{
			/* Done if we know there are no matching keys to the left */
			if (!so->currPos.moreLeft)
			{
				sortheap_bt_relbuf(scan->heapRelation, so->currPos.buf);
				BTScanPosInvalidate(so->currPos);
				return false;
			}

			/* Step to next physical page */
			so->currPos.buf = sortheap_bt_walk_left(scan->heapRelation, so->currPos.buf,
													scan->xs_snapshot);

			/* if we're physically at end of index, return failure */
			if (so->currPos.buf == InvalidBuffer)
			{
				BTScanPosInvalidate(so->currPos);
				return false;
			}

			/*
			 * Okay, we managed to move left to a non-deleted page. Done if
			 * it's not half-dead and contains matching tuples. Else loop back
			 * and do it all again.
			 */
			page = BufferGetPage(so->currPos.buf);
			TestForOldSnapshot(scan->xs_snapshot, scan->heapRelation, page);
			opaque = (BTPageOpaque)PageGetSpecialPointer(page);
			if (!P_IGNORE(opaque))
			{
				PredicateLockPage(scan->heapRelation, BufferGetBlockNumber(so->currPos.buf), scan->xs_snapshot);
				/* see if there are any matches on this page */
				/* note that this will clear moreLeft if we can stop */
				if (sortheap_bt_readpage(scan, dir, PageGetMaxOffsetNumber(page)))
					break;
			}
		}
	}

	return true;
}

/*
 * sortheap_bt_walk_left() -- step left one page, if possible
 *
 * The given buffer must be pinned and read-locked.  This will be dropped
 * before stepping left.  On return, we have pin and read lock on the
 * returned page, instead.
 *
 * Returns InvalidBuffer if there is no page to the left (no lock is held
 * in that case).
 *
 * When working on a non-leaf level, it is possible for the returned page
 * to be half-dead; the caller should check that condition and step left
 * again if it's important.
 */
static Buffer
sortheap_bt_walk_left(Relation rel, Buffer buf, Snapshot snapshot)
{
	Page page;
	BTPageOpaque opaque;

	page = BufferGetPage(buf);
	opaque = (BTPageOpaque)PageGetSpecialPointer(page);

	for (;;)
	{
		BlockNumber obknum;
		BlockNumber lblkno;
		BlockNumber blkno;
		int tries;

		/* if we're at end of tree, release buf and return failure */
		if (P_LEFTMOST(opaque))
		{
			sortheap_bt_relbuf(rel, buf);
			break;
		}
		/* remember original page we are stepping left from */
		obknum = BufferGetBlockNumber(buf);
		/* step left */
		blkno = lblkno = opaque->btpo_prev;
		sortheap_bt_relbuf(rel, buf);
		/* check for interrupts while we're not holding any buffer lock */
		CHECK_FOR_INTERRUPTS();
		buf = sortheap_bt_getbuf(rel, blkno, BT_READ);
		page = BufferGetPage(buf);
		TestForOldSnapshot(snapshot, rel, page);
		opaque = (BTPageOpaque)PageGetSpecialPointer(page);

		/*
		 * If this isn't the page we want, walk right till we find what we
		 * want --- but go no more than four hops (an arbitrary limit). If we
		 * don't find the correct page by then, the most likely bet is that
		 * the original page got deleted and isn't in the sibling chain at all
		 * anymore, not that its left sibling got split more than four times.
		 *
		 * Note that it is correct to test P_ISDELETED not P_IGNORE here,
		 * because half-dead pages are still in the sibling chain.  Caller
		 * must reject half-dead pages if wanted.
		 */
		tries = 0;
		for (;;)
		{
			if (!P_ISDELETED(opaque) && opaque->btpo_next == obknum)
			{
				/* Found desired page, return it */
				return buf;
			}
			if (P_RIGHTMOST(opaque) || ++tries > 4)
				break;
			blkno = opaque->btpo_next;
			buf = sortheap_bt_relandgetbuf(rel, buf, blkno, BT_READ);
			page = BufferGetPage(buf);
			TestForOldSnapshot(snapshot, rel, page);
			opaque = (BTPageOpaque)PageGetSpecialPointer(page);
		}

		/* Return to the original page to see what's up */
		buf = sortheap_bt_relandgetbuf(rel, buf, obknum, BT_READ);
		page = BufferGetPage(buf);
		TestForOldSnapshot(snapshot, rel, page);
		opaque = (BTPageOpaque)PageGetSpecialPointer(page);
		if (P_ISDELETED(opaque))
		{
			/*
			 * It was deleted.  Move right to first nondeleted page (there
			 * must be one); that is the page that has acquired the deleted
			 * one's keyspace, so stepping left from it will take us where we
			 * want to be.
			 */
			for (;;)
			{
				if (P_RIGHTMOST(opaque))
					elog(ERROR, "fell off the end of index \"%s\"",
						 RelationGetRelationName(rel));
				blkno = opaque->btpo_next;
				buf = sortheap_bt_relandgetbuf(rel, buf, blkno, BT_READ);
				page = BufferGetPage(buf);
				TestForOldSnapshot(snapshot, rel, page);
				opaque = (BTPageOpaque)PageGetSpecialPointer(page);
				if (!P_ISDELETED(opaque))
					break;
			}

			/*
			 * Now return to top of loop, resetting obknum to point to this
			 * nondeleted page, and try again.
			 */
		}
		else
		{
			/*
			 * It wasn't deleted; the explanation had better be that the page
			 * to the left got split or deleted. Without this check, we'd go
			 * into an infinite loop if there's anything wrong.
			 */
			if (opaque->btpo_prev == lblkno)
				elog(ERROR, "could not find left sibling of block %u in index \"%s\"",
					 obknum, RelationGetRelationName(rel));
			/* Okay to try again with new lblkno value */
		}
	}

	return InvalidBuffer;
}

/*
 * sortheap_bt_get_endpoint() -- Find the first or last page on a given tree level
 *
 * If the index is empty, we will return InvalidBuffer; any other failure
 * condition causes ereport().  We will not return a dead page.
 *
 * The returned buffer is pinned and read-locked.
 */
static Buffer
sortheap_bt_get_endpoint(IndexScanDesc scan, uint32 level, bool rightmost,
						 Snapshot snapshot)
{
	Buffer buf;
	Page page;
	BTPageOpaque opaque;
	OffsetNumber offnum;
	BlockNumber blkno;
	IndexTuple itup;

	/*
	 * If we are looking for a leaf page, okay to descend from fast root;
	 * otherwise better descend from true root.  (There is no point in being
	 * smarter about intermediate levels.)
	 */
	buf = sortheap_bt_getroot(scan, BT_READ);

	if (!BufferIsValid(buf))
		return InvalidBuffer;

	page = BufferGetPage(buf);
	TestForOldSnapshot(snapshot, scan->heapRelation, page);
	opaque = (BTPageOpaque)PageGetSpecialPointer(page);

	for (;;)
	{
		/*
		 * If we landed on a deleted page, step right to find a live page
		 * (there must be one).  Also, if we want the rightmost page, step
		 * right if needed to get to it (this could happen if the page split
		 * since we obtained a pointer to it).
		 */
		while (P_IGNORE(opaque) ||
			   (rightmost && !P_RIGHTMOST(opaque)))
		{
			blkno = opaque->btpo_next;
			if (blkno == P_NONE)
				elog(ERROR, "fell off the end of index \"%s\"",
					 RelationGetRelationName(scan->heapRelation));
			buf = sortheap_bt_relandgetbuf(scan->heapRelation, buf, blkno, BT_READ);
			page = BufferGetPage(buf);
			TestForOldSnapshot(snapshot, scan->heapRelation, page);
			opaque = (BTPageOpaque)PageGetSpecialPointer(page);
		}

		/* Done? */
		if (opaque->btpo.level == level)
			break;
		if (opaque->btpo.level < level)
			elog(ERROR, "btree level %u not found in index \"%s\"",
				 level, RelationGetRelationName(scan->heapRelation));

		/* Descend to leftmost or rightmost child page */
		if (rightmost)
			offnum = PageGetMaxOffsetNumber(page);
		else
			offnum = P_FIRSTDATAKEY(opaque);

		itup = (IndexTuple)PageGetItem(page, PageGetItemId(page, offnum));
		blkno = BTreeInnerTupleGetDownLink(itup);

		buf = sortheap_bt_relandgetbuf(scan->heapRelation, buf, blkno, BT_READ);
		page = BufferGetPage(buf);
		opaque = (BTPageOpaque)PageGetSpecialPointer(page);
	}

	return buf;
}

/*
 *	_bt_endpoint() -- Find the first or last page in the index, and scan
 * from there to the first key satisfying all the quals.
 *
 * This is used by _bt_first() to set up a scan when we've determined
 * that the scan must start at the beginning or end of the index (for
 * a forward or backward scan respectively).  Exit conditions are the
 * same as for _bt_first().
 */
static bool
sortheap_bt_endpoint(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;
	Buffer buf;
	Page page;
	BTPageOpaque opaque;
	OffsetNumber start;
	BTScanPosItem *currItem;

	/*
	 * Scan down to the leftmost or rightmost leaf page.  This is a simplified
	 * version of _bt_search().  We don't maintain a stack since we know we
	 * won't need it.
	 */
	buf = sortheap_bt_get_endpoint(scan, 0, ScanDirectionIsBackward(dir), scan->xs_snapshot);

	if (!BufferIsValid(buf))
	{
		/*
		 * Empty index. Lock the whole relation, as nothing finer to lock
		 * exists.
		 */
		PredicateLockRelation(scan->indexRelation, scan->xs_snapshot);
		BTScanPosInvalidate(so->currPos);
		return false;
	}

	PredicateLockPage(scan->heapRelation, BufferGetBlockNumber(buf), scan->xs_snapshot);
	page = BufferGetPage(buf);
	opaque = (BTPageOpaque)PageGetSpecialPointer(page);
	Assert(P_ISLEAF(opaque));

	if (ScanDirectionIsForward(dir))
	{
		/* There could be dead pages to the left, so not this: */
		/* Assert(P_LEFTMOST(opaque)); */

		start = P_FIRSTDATAKEY(opaque);
	}
	else if (ScanDirectionIsBackward(dir))
	{
		Assert(P_RIGHTMOST(opaque));

		start = PageGetMaxOffsetNumber(page);
	}
	else
	{
		elog(ERROR, "invalid scan direction: %d", (int)dir);
		start = 0; /* keep compiler quiet */
	}

	/* remember which buffer we have pinned */
	so->currPos.buf = buf;

	sortheap_bt_initialize_more_data(so, dir);

	/*
	 * Now load data from the first page of the scan.
	 */
	if (!sortheap_bt_readpage(scan, dir, start))
	{
		/*
		 * There's no actually-matching data on this page.  Try to advance to
		 * the next page.  Return false if there's no matching data at all.
		 */
		LockBuffer(so->currPos.buf, BUFFER_LOCK_UNLOCK);
		if (!sortheap_bt_steppage(scan, dir))
			return false;
	}
	else
	{
		/* Drop the lock, and maybe the pin, on the current page */
		sortheap_bt_drop_lock_and_maybe_pin(scan, &so->currPos);
	}

	/* OK, itemIndex says what to return */
	currItem = &so->currPos.items[so->currPos.itemIndex];
	scan->xs_heaptid = currItem->heapTid;
	if (scan->xs_want_itup)
		scan->xs_itup = (IndexTuple)(so->currTuples + currItem->tupleOffset);

	return true;
}

/*
 * _bt_initialize_more_data() -- initialize moreLeft/moreRight appropriately
 * for scan direction
 */
static inline void
sortheap_bt_initialize_more_data(BTScanOpaque so, ScanDirection dir)
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
	so->numKilled = 0;		/* just paranoia */
	so->markItemIndex = -1; /* ditto */
}

/*
 * slide an array of ItemIds back one slot (from P_FIRSTKEY to
 * P_HIKEY, overwriting P_HIKEY).  we need to do this when we discover
 * that we have built an ItemId array in what has turned out to be a
 * P_RIGHTMOST page.
 */
static void
sortheap_bt_slideleft(Page page)
{
	OffsetNumber off;
	OffsetNumber maxoff;
	ItemId previi;
	ItemId thisii;

	if (!PageIsEmpty(page))
	{
		maxoff = PageGetMaxOffsetNumber(page);
		previi = PageGetItemId(page, P_HIKEY);
		for (off = P_FIRSTKEY; off <= maxoff; off = OffsetNumberNext(off))
		{
			thisii = PageGetItemId(page, off);
			*previi = *thisii;
			previi = thisii;
		}
		((PageHeader)page)->pd_lower -= sizeof(ItemIdData);
	}
}

static bool
sortheap_bt_advance_runs(IndexScanDesc scan, ScanDirection dir)
{
	IndexFetchSortHeapData *sortheap_fetch;
	SortHeapState *shstate;
	TapeSetsState *tsstate;
	TapeSetsMeta *tsmetad;
	TapeSetsStatus tsstatus;

	sortheap_fetch = (IndexFetchSortHeapData *)scan->xs_heapfetch;
	shstate = sortheap_fetch->sortstate;
	tsstate = TAPESETSSTATE(shstate, shstate->cur_readtapesets);
	tsmetad = tapesets_getmeta_snapshot(tsstate);
	tsstatus = tapesets_getstatus_forread(tsstate);

	tsstate->cur_readrun++;

	/* Switch to next tape if all runs in current tape are read */
	if (tsstate->cur_readrun >= tsmetad->tapes[tsstate->cur_readtape].runs)
		tsstate->cur_readrun = 0;
	else
		return true;

	/* Find the next non-empty tape */
	while (true)
	{
		tsstate->cur_readtape++;

		/* Switch to next tapesets if all tapes in current tape are readed */
		if (tsstate->cur_readtape >= tsstate->end_readtape)
		{
			tsstate->cur_readtape = 0;
			shstate->cur_readtapesets++;
			break;
		}

		if (tsmetad->tapes[tsstate->cur_readtape].runs == 0)
			continue;
		else
			return true;
	}

	if (shstate->cur_readtapesets >= MAX_TAPESETS)
		return false;

	/* Advance to the next tapesets */
	tsstate = TAPESETSSTATE(shstate, shstate->cur_readtapesets);
	tsmetad = tapesets_getmeta_snapshot(tsstate);
	tsstatus = tapesets_getstatus_forread(tsstate);

	/* Return false if the tapesets has no data */
	if (tsstatus == TS_INITIAL)
		return false;

	/* Find the next non-empty tape */
	while (true)
	{
		/* Switch to next tapesets if all tapes in current tape are readed */
		if (tsstate->cur_readtape >= tsstate->end_readtape)
			return false;

		if (tsmetad->tapes[tsstate->cur_readtape].runs == 0)
		{
			tsstate->cur_readtape++;
			continue;
		}
		else
			return true;
	}

	return false;
}

/*
 * allocate and initialize a new BTPageState.  the returned structure
 * is suitable for immediate use by _bt_buildadd.
 */
BTPageState *
sortheap_bt_pagestate(Relation relation, TapeInsertState *insertstate, uint32 level)
{
	BTPageState *state = (BTPageState *)palloc0(sizeof(BTPageState));

	/* create initial page for level */
	state->btps_buf = tape_get_new_bt_buffer(insertstate, level);

	state->btps_page = BufferGetPage(state->btps_buf);
	/* and assign it a page position */
	state->btps_blkno = BufferGetBlockNumber(state->btps_buf);

	state->btps_minkey = NULL;
	/* initialize lastoff so first item goes into P_FIRSTKEY */
	state->btps_lastoff = P_HIKEY;
	state->btps_level = level;
	/* set "full" threshold based on level.  See notes at head of file. */
	if (level > 0)
		state->btps_full = (BLCKSZ * (100 - BTREE_NONLEAF_FILLFACTOR) / 100);
	else
		state->btps_full = BTREE_DEFAULT_FILLFACTOR;
	/* no parent level, yet */
	state->btps_next = NULL;

	return state;
}

static void
sortheap_bt_sortaddtup(Page page,
					   Size itemsize,
					   IndexTuple itup,
					   OffsetNumber itup_off)
{
	BTPageOpaque opaque = (BTPageOpaque)PageGetSpecialPointer(page);
	IndexTupleData trunctuple;

	if (!P_ISLEAF(opaque) && itup_off == P_FIRSTKEY)
	{
		trunctuple = *itup;
		trunctuple.t_info = sizeof(IndexTupleData);
		/* Deliberately zero INDEX_ALT_TID_MASK bits */
		BTreeTupleSetNAtts(&trunctuple, 0);
		itup = &trunctuple;
		itemsize = sizeof(IndexTupleData);
	}

	if (PageAddItem(page, (Item)itup, itemsize, itup_off,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "failed to add item to the index page");
}

/*----------
 * Add an item to a disk page from the sort output.
 *
 * We must be careful to observe the page layout conventions of nbtsearch.c:
 * - rightmost pages start data items at P_HIKEY instead of at P_FIRSTKEY.
 * - on non-leaf pages, the key portion of the first item need not be
 *	 stored, we should store only the link.
 *
 * A leaf page being built looks like:
 *
 * +----------------+---------------------------------+
 * | PageHeaderData | linp0 linp1 linp2 ...           |
 * +-----------+----+---------------------------------+
 * | ... linpN |									  |
 * +-----------+--------------------------------------+
 * |	 ^ last										  |
 * |												  |
 * +-------------+------------------------------------+
 * |			 | itemN ...                          |
 * +-------------+------------------+-----------------+
 * |		  ... item3 item2 item1 | "special space" |
 * +--------------------------------+-----------------+
 *
 * Contrast this with the diagram in bufpage.h; note the mismatch
 * between linps and items.  This is because we reserve linp0 as a
 * placeholder for the pointer to the "high key" item; when we have
 * filled up the page, we will set linp0 to point to itemN and clear
 * linpN.  On the other hand, if we find this is the last (rightmost)
 * page, we leave the items alone and slide the linp array over.  If
 * the high key is to be truncated, offset 1 is deleted, and we insert
 * the truncated high key at offset 1.
 *
 * 'last' pointer indicates the last offset added to the page.
 *----------
 */
void sortheap_bt_buildadd(Relation relation,
						  TapeInsertState *insertstate,
						  BTPageState *state,
						  IndexTuple itup)
{
	Page npage;
	BlockNumber nblkno;
	Buffer nbuffer;
	OffsetNumber last_off;
	Size pgspc;
	Size itupsz;
	bool isleaf;

	/*
	 * This is a handy place to check for cancel interrupts during the btree
	 * load phase of index creation.
	 */
	CHECK_FOR_INTERRUPTS();

	npage = state->btps_page;
	nblkno = state->btps_blkno;
	nbuffer = state->btps_buf;
	last_off = state->btps_lastoff;

	pgspc = PageGetFreeSpace(npage);

	/* FIXME : index_form_tuple */
	itupsz = IndexTupleSize(itup);
	itupsz = MAXALIGN(itupsz);
	/* Leaf case has slightly different rules due to suffix truncation */
	isleaf = (state->btps_level == 0);

	if (unlikely(itupsz > BTMaxItemSize(npage)))
		elog(PANIC, "the index aggheap  tuple is too large");

	/*
	 * Check to see if current page will fit new item, with space left over to
	 * append a heap TID during suffix truncation when page is a leaf page.
	 *
	 * It is guaranteed that we can fit at least 2 non-pivot tuples plus a
	 * high key with heap TID when finishing off a leaf page, since we rely on
	 * _bt_check_third_page() rejecting oversized non-pivot tuples.  On
	 * internal pages we can always fit 3 pivot tuples with larger internal
	 * page tuple limit (includes page high key).
	 *
	 * Most of the time, a page is only "full" in the sense that the soft
	 * fillfactor-wise limit has been exceeded.  However, we must always leave
	 * at least two items plus a high key on each page before starting a new
	 * page.  Disregard fillfactor and insert on "full" current page if we
	 * don't have the minimum number of items yet.  (Note that we deliberately
	 * assume that suffix truncation neither enlarges nor shrinks new high key
	 * when applying soft limit.)
	 */
	if (pgspc < itupsz + (isleaf ? MAXALIGN(sizeof(ItemPointerData)) : 0) ||
		(pgspc < state->btps_full && last_off > P_FIRSTKEY))
	{
		/*
		 * Finish off the page and write it out.
		 */
		Page opage = npage;
		BlockNumber oblkno = nblkno;
		Buffer obuffer = nbuffer;
		ItemId ii;
		ItemId hii;
		IndexTuple oitup;

		/* Create new page of same level */
		nbuffer = tape_get_new_bt_buffer(insertstate, state->btps_level);

		npage = BufferGetPage(nbuffer);
		nblkno = BufferGetBlockNumber(nbuffer);

		/*
		 * We copy the last item on the page into the new page, and then
		 * rearrange the old page so that the 'last item' becomes its high key
		 * rather than a true data item.  There had better be at least two
		 * items on the page already, else the page would be empty of useful
		 * data.
		 */
		Assert(last_off > P_FIRSTKEY);
		ii = PageGetItemId(opage, last_off);
		oitup = (IndexTuple)PageGetItem(opage, ii);
		sortheap_bt_sortaddtup(npage, ItemIdGetLength(ii), oitup, P_FIRSTKEY);

		/*
		 * Move 'last' into the high key position on opage.  _bt_blnewpage()
		 * allocated empty space for a line pointer when opage was first
		 * created, so this is a matter of rearranging already-allocated space
		 * on page, and initializing high key line pointer. (Actually, leaf
		 * pages must also swap oitup with a truncated version of oitup, which
		 * is sometimes larger than oitup, though never by more than the space
		 * needed to append a heap TID.)
		 */
		hii = PageGetItemId(opage, P_HIKEY);
		*hii = *ii;
		ItemIdSetUnused(ii); /* redundant */
		((PageHeader)opage)->pd_lower -= sizeof(ItemIdData);

		if (isleaf)
		{
			IndexTuple lastleft;
			IndexTuple truncated;
			Size truncsz;

			/*
			 * Truncate away any unneeded attributes from high key on leaf
			 * level.  This is only done at the leaf level because downlinks
			 * in internal pages are either negative infinity items, or get
			 * their contents from copying from one level down.  See also:
			 * _bt_split().
			 *
			 * We don't try to bias our choice of split point to make it more
			 * likely that _bt_truncate() can truncate away more attributes,
			 * whereas the split point passed to _bt_split() is chosen much
			 * more delicately.  Suffix truncation is mostly useful because it
			 * improves space utilization for workloads with random
			 * insertions.  It doesn't seem worthwhile to add logic for
			 * choosing a split point here for a benefit that is bound to be
			 * much smaller.
			 *
			 * Since the truncated tuple is often smaller than the original
			 * tuple, it cannot just be copied in place (besides, we want to
			 * actually save space on the leaf page).  We delete the original
			 * high key, and add our own truncated high key at the same
			 * offset.
			 *
			 * Note that the page layout won't be changed very much.  oitup is
			 * already located at the physical beginning of tuple space, so we
			 * only shift the line pointer array back and forth, and overwrite
			 * the tuple space previously occupied by oitup.  This is fairly
			 * cheap.
			 */
			ii = PageGetItemId(opage, OffsetNumberPrev(last_off));
			lastleft = (IndexTuple)PageGetItem(opage, ii);

			truncated = _bt_truncate(insertstate->indexrel, lastleft, oitup,
									 insertstate->inskey);
			truncsz = IndexTupleSize(truncated);
			PageIndexTupleDelete(opage, P_HIKEY);
			sortheap_bt_sortaddtup(opage, truncsz, truncated, P_HIKEY);
			pfree(truncated);

			/* oitup should continue to point to the page's high key */
			hii = PageGetItemId(opage, P_HIKEY);
			oitup = (IndexTuple)PageGetItem(opage, hii);
		}

		/*
		 * Link the old page into its parent, using its minimum key. If we
		 * don't have a parent, we have to create one; this adds a new btree
		 * level.
		 */
		if (state->btps_next == NULL)
			state->btps_next = sortheap_bt_pagestate(relation,
													 insertstate,
													 state->btps_level + 1);

		BTreeInnerTupleSetDownLink(state->btps_minkey, oblkno);
		sortheap_bt_buildadd(relation, insertstate, state->btps_next, state->btps_minkey);
		pfree(state->btps_minkey);

		/*
		 * Save a copy of the high key from the old page.  It is also used as
		 * the minimum key for the new page.
		 */
		state->btps_minkey = CopyIndexTuple(oitup);

		/*
		 * Set the sibling links for both pages.
		 */
		{
			BTPageOpaque oopaque = (BTPageOpaque)PageGetSpecialPointer(opage);
			BTPageOpaque nopaque = (BTPageOpaque)PageGetSpecialPointer(npage);

			oopaque->btpo_next = nblkno;
			nopaque->btpo_prev = oblkno;
			nopaque->btpo_next = P_NONE; /* redundant */
		}

		MarkBufferDirty(obuffer);

		/* XLOG STUFF */
		{
			XLogRecPtr recptr;

			recptr = log_full_page_update(obuffer);
			PageSetLSN(opage, recptr);
		}

		UnlockReleaseBuffer(obuffer);

		/*
		 * Reset last_off to point to new page
		 */
		last_off = P_FIRSTKEY;
	}

	/*
	 * By here, either original page is still the current page, or a new page
	 * was created that became the current page.  Either way, the current page
	 * definitely has space for new item.
	 *
	 * If the new item is the first for its page, stash a copy for later. Note
	 * this will only happen for the first item on a level; on later pages,
	 * the first item for a page is copied from the prior page in the code
	 * above.  The minimum key for an entire level is nothing more than a
	 * minus infinity (downlink only) pivot tuple placeholder.
	 */
	if (last_off == P_HIKEY)
	{
		Assert(state->btps_minkey == NULL);
		state->btps_minkey = CopyIndexTuple(itup);
		/* _bt_sortaddtup() will perform full truncation later */
		BTreeTupleSetNAtts(state->btps_minkey, 0);
	}

	/*
	 * Add the new item into the current page.
	 */
	last_off = OffsetNumberNext(last_off);
	sortheap_bt_sortaddtup(npage, itupsz, itup, last_off);

	state->btps_buf = nbuffer;
	state->btps_page = npage;
	state->btps_blkno = nblkno;
	state->btps_lastoff = last_off;
}

/*
 * Specialized version of _bt_checkkeys which accept the SortTuple as the input.
 */
bool sortheap_bt_checkkeys(Relation heaprel, IndexScanDesc scan,
						   SortTuple *stup,
						   ScanDirection dir,
						   bool *continuescan)
{
	IndexFetchSortHeapData *sortheap_fetch;
	TupleDesc tupdesc;
	BTScanOpaque so;
	int keysz;
	int ikey;
	ScanKey key;

	sortheap_fetch = (IndexFetchSortHeapData *)scan->xs_heapfetch;

	*continuescan = true; /* default assumption */

	tupdesc = RelationGetDescr(scan->indexRelation);
	so = (BTScanOpaque)scan->opaque;
	keysz = so->numberOfKeys;

	for (key = so->keyData, ikey = 0; ikey < keysz; key++, ikey++)
	{
		Datum datum;
		bool isNull;
		Datum test;

		/* row-comparison keys need special processing */
		if (key->sk_flags & SK_ROW_HEADER)
			elog(ERROR, "sortheap: row compare is not supported");

		datum = get_sorttuple_cache(sortheap_fetch->sortstate, stup, key->sk_attno - 1, &isNull);

		if (key->sk_flags & SK_ISNULL)
		{
			/* Handle IS NULL/NOT NULL tests */
			if (key->sk_flags & SK_SEARCHNULL)
			{
				if (isNull)
					continue; /* tuple satisfies this qual */
			}
			else
			{
				Assert(key->sk_flags & SK_SEARCHNOTNULL);
				if (!isNull)
					continue; /* tuple satisfies this qual */
			}

			/*
			 * Tuple fails this qual.  If it's a required qual for the current
			 * scan direction, then we can conclude no further tuples will
			 * pass, either.
			 */
			if ((key->sk_flags & SK_BT_REQFWD) &&
				ScanDirectionIsForward(dir))
				*continuescan = false;
			else if ((key->sk_flags & SK_BT_REQBKWD) &&
					 ScanDirectionIsBackward(dir))
				*continuescan = false;

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}

		if (isNull)
		{
			if (key->sk_flags & SK_BT_NULLS_FIRST)
			{
				/*
				 * Since NULLs are sorted before non-NULLs, we know we have
				 * reached the lower limit of the range of values for this
				 * index attr.  On a backward scan, we can stop if this qual
				 * is one of the "must match" subset.  We can stop regardless
				 * of whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a forward scan, however, we must keep going, because we may
				 * have initially positioned to the start of the index.
				 */
				if ((key->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) &&
					ScanDirectionIsBackward(dir))
					*continuescan = false;
			}
			else
			{
				/*
				 * Since NULLs are sorted after non-NULLs, we know we have
				 * reached the upper limit of the range of values for this
				 * index attr.  On a forward scan, we can stop if this qual is
				 * one of the "must match" subset.  We can stop regardless of
				 * whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a backward scan, however, we must keep going, because we
				 * may have initially positioned to the end of the index.
				 */
				if ((key->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) &&
					ScanDirectionIsForward(dir))
					*continuescan = false;
			}

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}

		test = FunctionCall2Coll(&key->sk_func, key->sk_collation,
								 datum, key->sk_argument);

		if (!DatumGetBool(test))
		{
			/*
			 * Tuple fails this qual.  If it's a required qual for the current
			 * scan direction, then we can conclude no further tuples will
			 * pass, either.
			 *
			 * Note: because we stop the scan as soon as any required equality
			 * qual fails, it is critical that equality quals be used for the
			 * initial positioning in _bt_first() when they are available. See
			 * comments in _bt_first().
			 */
			if ((key->sk_flags & SK_BT_REQFWD) &&
				ScanDirectionIsForward(dir))
				*continuescan = false;
			else if ((key->sk_flags & SK_BT_REQBKWD) &&
					 ScanDirectionIsBackward(dir))
				*continuescan = false;

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}
	}

	/* If we get here, the tuple passes all index quals. */
	return true;
}

/*
 * Finish writing out the completed btree.
 */
BlockNumber
sortheap_bt_uppershutdown(Relation relation, TapeInsertState *insertstate, int curRun)
{
	BTPageState *s;
	BlockNumber rootblkno = P_NONE;
	uint32 rootlevel = 0;

	/*
	 * Each iteration of this loop completes one more level of the tree.
	 */
	for (s = insertstate->btstate; s != NULL; s = s->btps_next)
	{
		BlockNumber blkno;
		BTPageOpaque opaque;

		blkno = s->btps_blkno;
		opaque = (BTPageOpaque)PageGetSpecialPointer(s->btps_page);

		/*
		 * We have to link the last page on this level to somewhere.
		 *
		 * If we're at the top, it's the root, so attach it to the metapage.
		 * Otherwise, add an entry for it to its parent using its minimum key.
		 * This may cause the last page of the parent level to split, but
		 * that's not a problem -- we haven't gotten to it yet.
		 */
		if (s->btps_next == NULL)
		{
			opaque->btpo_flags |= BTP_ROOT;
			rootblkno = blkno;
			rootlevel = s->btps_level;
		}
		else
		{
			BTreeInnerTupleSetDownLink(s->btps_minkey, blkno);
			sortheap_bt_buildadd(relation, insertstate, s->btps_next, s->btps_minkey);
			pfree(s->btps_minkey);
			s->btps_minkey = NULL;
		}

		/*
		 * This is the rightmost page, so the ItemId array needs to be slid
		 * back one slot.  Then we can dump out the page.
		 */
		sortheap_bt_slideleft(s->btps_page);
		MarkBufferDirty(s->btps_buf);

		/* XLOG STUFF */
		{
			XLogRecPtr recptr;

			recptr = log_full_page_update(s->btps_buf);
			PageSetLSN(s->btps_page, recptr);
		}

		UnlockReleaseBuffer(s->btps_buf);
		s->btps_page = NULL; /* writepage freed the workspace */
	}

	/*
	 * write an empty page to the relfilenode of index to make the analyze
	 * happy.
	 */
	RelationOpenSmgr(insertstate->indexrel);
	if (smgrnblocks(insertstate->indexrel->rd_smgr, MAIN_FORKNUM) == 0)
	{
		char *dummy = (char *)palloc(BLCKSZ);

		smgrwrite(insertstate->indexrel->rd_smgr, MAIN_FORKNUM, 0, dummy, false);
	}

	return rootblkno;
}

/*
 *	btbuild() -- build a new btree index.
 */
static IndexBuildResult *
sortheap_btbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	double reltuples;

	IndexBuildResult *buildstate = (IndexBuildResult *)palloc(sizeof(IndexBuildResult));

	if (heap->rd_tableam->slot_callbacks != sortheapam_methods.slot_callbacks ||
		heap->rd_tableam->event_notify != sortheapam_methods.event_notify ||
		heap->rd_tableam->tuple_insert != sortheapam_methods.tuple_insert)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("sortheap_btree can only be built on %s",
						SORTHEAPAMNAME)));

	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   NULL, (void *)buildstate,
									   NULL);

	buildstate->heap_tuples = reltuples;

	return buildstate;
}

/*
 *	sortheap_btbuildempty() -- build an empty btree index in the initialization fork
 */
static void
sortheap_btbuildempty(Relation index)
{
	/*
	 * the sortheap_btree don't store any data actually, so this is
	 * unnecessary
	 */
	return;
}

static bool
sortheap_btinsert(Relation rel, Datum *values, bool *isnull,
				  ItemPointer ht_ctid, Relation heapRel,
				  IndexUniqueCheck checkUnique,
				  IndexInfo *indexInfo)
{
	return true;
}

/*
 *	sortheap_btgettuple() -- Get the next tuple in the scan.
 */
static bool
sortheap_btgettuple(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;
	bool res;

	/* btree indexes are never lossy */
	scan->xs_recheck = false;

	/*
	 * If we have any array keys, initialize them during first call for a
	 * scan.  We can't do this in btrescan because we don't know the scan
	 * direction at that time.
	 */
	if (so->numArrayKeys && !BTScanPosIsValid(so->currPos))
	{
		/* punt if we have any unsatisfiable array keys */
		if (so->numArrayKeys < 0)
			return false;

		_bt_start_array_keys(scan, dir);
	}

	/* each run has it's own sub tree, so we need to interation all runs */
	do
	{
		/* This loop handles advancing to the next array elements, if any */
		do
		{
			/*
			 * If we've already initialized this scan, we can just advance it
			 * in the appropriate direction.  If we haven't done so yet, we
			 * call _bt_first() to get the first item in the scan.
			 */
			if (!BTScanPosIsValid(so->currPos))
				res = sortheap_bt_first(scan, dir);
			else
			{
				/*
				 * Now continue the scan.
				 */
				res = sortheap_bt_next(scan, dir);
			}

			/* If we have a tuple, return it ... */
			if (res)
				return res;
			/* ... otherwise see if we have more array keys to deal with */
		} while (so->numArrayKeys && _bt_advance_array_keys(scan, dir));
	} while (sortheap_bt_advance_runs(scan, dir));

	return res;
}

/*
 * btgetbitmap() -- construct a TIDBitmap.
 */
static Node *
sortheap_btgetbitmap(IndexScanDesc scan, Node *n)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;
	ItemPointer heapTid;
	TIDBitmap *tbm;
	int64 ntids = 0;

	if (n == NULL)
	{
		/* XXX should we use less than work_mem for this? */
		tbm = tbm_create(work_mem * 1024L, NULL);
	}
	else if (!IsA(n, TIDBitmap))
	{
		elog(ERROR, "non hash bitmap");
	}
	else
	{
		tbm = (TIDBitmap *)n;
	}

	if (!scan->heapRelation)
	{
		HeapTuple htup;
		Form_pg_index index;
		Oid heapoid;

		/*
		 * For a normal bitmapscan, it doesn't need the scan->heapRelation,
		 * but for a sortheap btree bitmapscan, we need that because the real
		 * btree pages are stored on the sortheap storage.
		 */
		Assert(scan->heapRelation == NULL);
		htup = SearchSysCache1(INDEXRELID,
							   ObjectIdGetDatum(scan->indexRelation->rd_id));

		Assert(HeapTupleIsValid(htup));
		index = (Form_pg_index)GETSTRUCT(htup);
		heapoid = index->indrelid;
		ReleaseSysCache(htup);

		scan->heapRelation = relation_open(index->indrelid, AccessShareLock);
		scan->xs_heapfetch = table_index_fetch_begin(scan->heapRelation);
	}

	/*
	 * If we have any array keys, initialize them.
	 */
	if (so->numArrayKeys)
	{
		/* punt if we have any unsatisfiable array keys */
		if (so->numArrayKeys < 0)
			return (Node *)tbm;

		_bt_start_array_keys(scan, ForwardScanDirection);
	}

	do
	{
		/* This loop handles advancing to the next array elements, if any */
		do
		{
			/* Fetch the first page & tuple */
			if (sortheap_bt_first(scan, ForwardScanDirection))
			{
				/* Save tuple ID, and continue scanning */
				heapTid = &scan->xs_heaptid;
				tbm_add_tuples(tbm, heapTid, 1, false);
				ntids++;

				for (;;)
				{
					/*
					 * Advance to next tuple within page.  This is the same as
					 * the easy case in _bt_next().
					 */
					if (++so->currPos.itemIndex > so->currPos.lastItem)
					{
						/* let _bt_next do the heavy lifting */
						if (!sortheap_bt_next(scan, ForwardScanDirection))
							break;
					}

					/* Save tuple ID, and continue scanning */
					heapTid = &so->currPos.items[so->currPos.itemIndex].heapTid;
					tbm_add_tuples(tbm, heapTid, 1, false);
					ntids++;
				}
			}
			/* Now see if we have more array keys to deal with */
		} while (so->numArrayKeys && _bt_advance_array_keys(scan, ForwardScanDirection));
	} while (sortheap_bt_advance_runs(scan, ForwardScanDirection));

	/* We have got all tids, it's safe to close the scan->heapRelation */
	if (scan->heapRelation)
	{
		table_index_fetch_end(scan->xs_heapfetch);
		scan->xs_heapfetch = NULL;
		scan->heapRelation->rd_amcache = NULL;
		table_close(scan->heapRelation, AccessShareLock);
		scan->heapRelation = NULL;
	}

	return (Node *)tbm;
}

/*
 *	btbeginscan() -- start a scan on a btree index
 */
static IndexScanDesc
sortheap_btbeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	BTScanOpaque so;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* get the scan */
	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	/* allocate private workspace */
	so = (BTScanOpaque)palloc(sizeof(BTScanOpaqueData));
	BTScanPosInvalidate(so->currPos);
	BTScanPosInvalidate(so->markPos);
	if (scan->numberOfKeys > 0)
		so->keyData = (ScanKey)palloc(scan->numberOfKeys * sizeof(ScanKeyData));
	else
		so->keyData = NULL;

	so->arrayKeyData = NULL; /* assume no array keys for now */
	so->numArrayKeys = 0;
	so->arrayKeys = NULL;
	so->arrayContext = NULL;

	so->killedItems = NULL; /* until needed */
	so->numKilled = 0;

	/*
	 * We don't know yet whether the scan will be index-only, so we do not
	 * allocate the tuple workspace arrays until btrescan.  However, we set up
	 * scan->xs_itupdesc whether we'll need it or not, since that's so cheap.
	 */
	so->currTuples = so->markTuples = NULL;

	scan->xs_itupdesc = RelationGetDescr(rel);

	scan->opaque = so;

	return scan;
}

/*
 *	btrescan() -- rescan an index relation
 */
static void
sortheap_btrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
				  ScanKey orderbys, int norderbys)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;

	/* we aren't holding any read locks, but gotta drop the pins */
	if (BTScanPosIsValid(so->currPos))
	{
		BTScanPosUnpinIfPinned(so->currPos);
		BTScanPosInvalidate(so->currPos);
	}

	so->markItemIndex = -1;
	so->arrayKeyCount = 0;
	BTScanPosUnpinIfPinned(so->markPos);
	BTScanPosInvalidate(so->markPos);

	/*
	 * Allocate tuple workspace arrays, if needed for an index-only scan and
	 * not already done in a previous rescan call.  To save on palloc
	 * overhead, both workspaces are allocated as one palloc block; only this
	 * function and btendscan know that.
	 *
	 * NOTE: this data structure also makes it safe to return data from a
	 * "name" column, even though btree name_ops uses an underlying storage
	 * datatype of cstring.  The risk there is that "name" is supposed to be
	 * padded to NAMEDATALEN, but the actual index tuple is probably shorter.
	 * However, since we only return data out of tuples sitting in the
	 * currTuples array, a fetch of NAMEDATALEN bytes can at worst pull some
	 * data out of the markTuples array --- running off the end of memory for
	 * a SIGSEGV is not possible.  Yeah, this is ugly as sin, but it beats
	 * adding special-case treatment for name_ops elsewhere.
	 */
	if (scan->xs_want_itup && so->currTuples == NULL)
	{
		so->currTuples = (char *)palloc(BLCKSZ * 2);
		so->markTuples = so->currTuples + BLCKSZ;
	}

	/*
	 * Reset the scan keys. Note that keys ordering stuff moved to _bt_first.
	 * - vadim 05/05/97
	 */
	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData,
				scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	so->numberOfKeys = 0; /* until _bt_preprocess_keys sets it */

	/* If any keys are SK_SEARCHARRAY type, set up array-key info */
	_bt_preprocess_array_keys(scan);
}

/*
 *	btendscan() -- close down a scan
 */
static void
sortheap_btendscan(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;

	/* we aren't holding any read locks, but gotta drop the pins */
	if (BTScanPosIsValid(so->currPos))
	{
		BTScanPosUnpinIfPinned(so->currPos);
	}

	so->markItemIndex = -1;
	BTScanPosUnpinIfPinned(so->markPos);

	/* No need to invalidate positions, the RAM is about to be freed. */

	/* Release storage */
	if (so->keyData != NULL)
		pfree(so->keyData);
	/* so->arrayKeyData and so->arrayKeys are in arrayContext */
	if (so->arrayContext != NULL)
		MemoryContextDelete(so->arrayContext);
	if (so->killedItems != NULL)
		pfree(so->killedItems);
	if (so->currTuples != NULL)
		pfree(so->currTuples);
	/* so->markTuples should not be pfree'd, see btrescan */
	pfree(so);
}

/*
 *	btmarkpos() -- save current scan position
 */
static void
sortheap_btmarkpos(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;

	/* There may be an old mark with a pin (but no lock). */
	BTScanPosUnpinIfPinned(so->markPos);

	/*
	 * Just record the current itemIndex.  If we later step to next page
	 * before releasing the marked position, _bt_steppage makes a full copy of
	 * the currPos struct in markPos.  If (as often happens) the mark is moved
	 * before we leave the page, we don't have to do that work.
	 */
	if (BTScanPosIsValid(so->currPos))
		so->markItemIndex = so->currPos.itemIndex;
	else
	{
		BTScanPosInvalidate(so->markPos);
		so->markItemIndex = -1;
	}

	/* Also record the current positions of any array keys */
	if (so->numArrayKeys)
		_bt_mark_array_keys(scan);
}

/*
 *	btrestrpos() -- restore scan to last saved position
 */
static void
sortheap_btrestrpos(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque)scan->opaque;

	/* Restore the marked positions of any array keys */
	if (so->numArrayKeys)
		_bt_restore_array_keys(scan);

	if (so->markItemIndex >= 0)
	{
		/*
		 * The scan has never moved to a new page since the last mark.  Just
		 * restore the itemIndex.
		 *
		 * NB: In this case we can't count on anything in so->markPos to be
		 * accurate.
		 */
		so->currPos.itemIndex = so->markItemIndex;
	}
	else
	{
		/*
		 * The scan moved to a new page after last mark or restore, and we are
		 * now restoring to the marked page.  We aren't holding any read
		 * locks, but if we're still holding the pin for the current position,
		 * we must drop it.
		 */
		if (BTScanPosIsValid(so->currPos))
		{
			BTScanPosUnpinIfPinned(so->currPos);
		}

		if (BTScanPosIsValid(so->markPos))
		{
			/* bump pin on mark buffer for assignment to current buffer */
			if (BTScanPosIsPinned(so->markPos))
				IncrBufferRefCount(so->markPos.buf);
			memcpy(&so->currPos, &so->markPos,
				   offsetof(BTScanPosData, items[1]) +
					   so->markPos.lastItem * sizeof(BTScanPosItem));
			if (so->currTuples)
				memcpy(so->currTuples, so->markTuples,
					   so->markPos.nextTupleOffset);
		}
		else
			BTScanPosInvalidate(so->currPos);
	}
}

static IndexBulkDeleteResult *
sortheap_btvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	return stats;
}

IndexAmRoutine sortheap_btmethods =
	{
		.type = T_IndexAmRoutine,
		.amstrategies = BTMaxStrategyNumber,
		.amsupport = BTNProcs,
		.amcanorder = true,
		.amcanorderbyop = false,
		.amcanbackward = true,
		.amcanunique = false,
		.amcanmulticol = true,
		.amoptionalkey = true,
		.amsearcharray = true,
		.amsearchnulls = true,
		.amstorage = false,
		.amclusterable = false,
		.ampredlocks = true,
		.amcanparallel = false,
		.amcaninclude = false,
		.amkeytype = InvalidOid,
		.ambuild = sortheap_btbuild,
		.ambuildempty = sortheap_btbuildempty,
		.aminsert = sortheap_btinsert,
		.ambulkdelete = NULL,
		.amvacuumcleanup = sortheap_btvacuumcleanup,
		.amcanreturn = NULL,
		.amcostestimate = btcostestimate,
		.amoptions = btoptions,
		.amproperty = NULL,
		.ambuildphasename = btbuildphasename,
		.amvalidate = btvalidate,
		.ambeginscan = sortheap_btbeginscan,
		.amrescan = sortheap_btrescan,
		.amgettuple = sortheap_btgettuple,
		.amgetbitmap = sortheap_btgetbitmap,
		.amendscan = sortheap_btendscan,
		.ammarkpos = sortheap_btmarkpos,
		.amrestrpos = sortheap_btrestrpos,
		.amestimateparallelscan = NULL,
		.aminitparallelscan = NULL,
		.amparallelrescan = NULL};

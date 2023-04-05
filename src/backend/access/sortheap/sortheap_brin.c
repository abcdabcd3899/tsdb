/*-------------------------------------------------------------------------
 *
 * sortheap_brin.c
 *	  Internal BRIN index for sort heap.
 *
 *
 * Sort Heap build a BRIN like index automatically and internally, it is
 * mainly to make up for the deficiency of the main btree index which has
 * poor performance on the secondary/third index scan keys.
 *
 * Same as the btree index, the brin index tuples are also stored in the
 * same file with the data.
 *
 * We build min/max info for each logical tape run by every 128 blocks by
 * default, we also build min/max info for the whole logical tape run to
 * filter tuples in a larger granularity.
 *
 * Because the blocks of logical tape run are not always with a sequential
 * block numbers, the format of brin index tuple is also different, the
 * ctid of the startup tuple of the blocks range is also recored in the
 * index tuple.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_brin.c
 *
 *-------------------------------------------------------------------------
 */

#include "sortheap.h"
#include "sortheap_tapesets.h"
#include "sortheap_tape.h"
#include "sortheap_brin.h"

static FmgrInfo *
sortheap_get_strategy_procinfo(TapeScanDesc tapescandesc,
							   Relation indexrel,
							   uint16 attno, Oid subtype,
							   uint16 strategynum)
{
	int nattrs = RelationGetDescr(indexrel)->natts;

	Assert(strategynum >= 1 &&
		   strategynum <= BTMaxStrategyNumber);

	if (!tapescandesc->strategy_procinfos)
	{
		tapescandesc->strategy_procinfos = (FmgrInfo **)
			palloc0(sizeof(FmgrInfo *) * nattrs);
	}

	if (!tapescandesc->strategy_procinfos[attno - 1])
	{
		tapescandesc->strategy_procinfos[attno - 1] = (FmgrInfo *)
			palloc0(sizeof(FmgrInfo) * RTMaxStrategyNumber);
	}

	if (tapescandesc->strategy_procinfos[attno - 1][strategynum].fn_oid == InvalidOid)
	{
		Form_pg_attribute attr;
		HeapTuple tuple;
		Oid opfamily,
			oprid;
		bool isNull;

		opfamily = indexrel->rd_opfamily[attno - 1];
		attr = TupleDescAttr(RelationGetDescr(indexrel), attno - 1);
		tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(opfamily),
								ObjectIdGetDatum(attr->atttypid),
								ObjectIdGetDatum(subtype),
								Int16GetDatum(strategynum));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 strategynum, attr->atttypid, subtype, opfamily);

		oprid = DatumGetObjectId(SysCacheGetAttr(AMOPSTRATEGY, tuple,
												 Anum_pg_amop_amopopr, &isNull));
		ReleaseSysCache(tuple);
		Assert(!isNull && RegProcedureIsValid(oprid));

		fmgr_info_cxt(get_opcode(oprid),
					  &tapescandesc->strategy_procinfos[attno - 1][strategynum],
					  CurrentMemoryContext);
	}

	return &tapescandesc->strategy_procinfos[attno - 1][strategynum];
}

/*
 * Initialize the brin tuple descriptor,
 * |min1|min2|...|max1|max2|blckno
 */
TupleDesc
sortheap_brin_builddesc(TupleDesc btdesc)
{
	int i;
	int nkeys;
	TupleDesc brindesc;

	nkeys = btdesc->natts;
	brindesc = CreateTemplateTupleDesc(nkeys * 2 + 1);

	for (i = 0; i < nkeys; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(btdesc, i);

		TupleDescInitEntry(brindesc, i + 1, NULL,
						   attr->atttypid, -1, 0);
		TupleDescInitEntry(brindesc, nkeys + i + 1, NULL,
						   attr->atttypid, -1, 0);
	}

	TupleDescInitEntry(brindesc, nkeys * 2 + 1, NULL,
					   INT8OID, -1, 0);
	return brindesc;
}

void sortheap_brin_update_minmax(TupleDesc tupdesc, int nkeys, SortSupport sortKey,
								 Datum *datum_minmax, bool *nulls_minmax,
								 Datum *datum_new, bool *nulls_new, bool initminmax)
{
	int i;
	int32 compare;

	for (i = 0; i < nkeys; i++, sortKey++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		/* If the min is not set, set it to datum2 without comparison */
		if (initminmax)
		{
			if (nulls_new[i])
			{
				/* update min */
				nulls_minmax[i] = true;
				/* update max */
				nulls_minmax[nkeys + i] = true;
			}
			else
			{
				/* update min */
				datum_minmax[i] = datumCopy(datum_new[i], att->attbyval, att->attlen);
				/* update max */
				datum_minmax[nkeys + i] = datumCopy(datum_new[i], att->attbyval, att->attlen);
			}
			continue;
		}

		/* Must compare */
		compare = ApplySortComparator(datum_minmax[i], nulls_minmax[i],
									  datum_new[i], nulls_new[i],
									  sortKey);

		/* min > new */
		if (compare > 0)
		{
			if (nulls_new[i])
				nulls_minmax[i] = true;
			else
				datum_minmax[i] = datumCopy(datum_new[i], att->attbyval, att->attlen);

			/* new < min < max, no need to check the max */
			continue;
		}

		compare = ApplySortComparator(datum_minmax[nkeys + i], nulls_minmax[nkeys + i],
									  datum_new[i], nulls_new[i],
									  sortKey);

		/* max < new */
		if (compare < 0)
		{
			if (nulls_new[i])
				nulls_minmax[nkeys + i] = true;
			else
				datum_minmax[nkeys + i] = datumCopy(datum_new[i], att->attbyval, att->attlen);
		}
	}
}

/* Form a min/max tuple and insert it into the buffer */
void sortheap_brin_flush_minmax(TapeInsertState *tapeinsertstate,
								Datum *datum_minmax, bool *nulls_minmax,
								bool runlevel)
{
	Size itemsz;
	Page brinpage;
	Relation relation;
	IndexTuple itup;
	BlockNumber newblk;
	OffsetNumber offnum;
	ItemPointerData ctid;

	relation = tapeinsertstate->relation;

	/* Form the the brin index tuple */
	itup = index_form_tuple(tapeinsertstate->brindesc, datum_minmax, nulls_minmax);

	itemsz = IndexTupleSize(itup);
	itemsz = MAXALIGN(itemsz);

	if (tapeinsertstate->brinbuf == InvalidBuffer)
	{
		newblk = tape_extend(tapeinsertstate, InvalidBuffer, SORTHEAP_BRINPAGE, -1);

		tapeinsertstate->brinbuf = ReadBuffer(relation, newblk);
		LockBuffer(tapeinsertstate->brinbuf, BUFFER_LOCK_EXCLUSIVE);
	}

	brinpage = BufferGetPage(tapeinsertstate->brinbuf);

	/* switch to a new brin block if current one is full */
	if (itemsz > PageGetFreeSpace(brinpage))
	{
		newblk = tape_extend(tapeinsertstate, tapeinsertstate->brinbuf,
							 SORTHEAP_BRINPAGE, -1);

		/* XLOG stuff */
		{
			log_full_page_update(tapeinsertstate->brinbuf);
		}

		UnlockReleaseBuffer(tapeinsertstate->brinbuf);

		tapeinsertstate->brinbuf = ReadBuffer(relation, newblk);
		LockBuffer(tapeinsertstate->brinbuf, BUFFER_LOCK_EXCLUSIVE);
		brinpage = BufferGetPage(tapeinsertstate->brinbuf);
	}

	START_CRIT_SECTION();

	offnum = PageAddItem(brinpage, (Item)itup, itemsz,
						 InvalidOffsetNumber, false, false);

	if (offnum == InvalidOffsetNumber)
		elog(PANIC, "failed to add tuple to page");

	ItemPointerSet(&ctid, BufferGetBlockNumber(tapeinsertstate->brinbuf), offnum);

	MarkBufferDirty(tapeinsertstate->brinbuf);

	END_CRIT_SECTION();

	if (runlevel)
	{
		LockBuffer(tapeinsertstate->runsmetabuf, BUFFER_LOCK_EXCLUSIVE);
		tapeinsertstate->runmetad->run_minmax_tuple = ctid;
		tapeinsertstate->runmetad->nranges = tapeinsertstate->nranges;

		MarkBufferDirty(tapeinsertstate->runsmetabuf);

		/* XLOG stuff */
		{
			XLogRecPtr recptr;
			Page page;

			recptr = log_full_page_update(tapeinsertstate->brinbuf);
			page = BufferGetPage(tapeinsertstate->brinbuf);
			PageSetLSN(page, recptr);

			recptr = log_full_page_update(tapeinsertstate->runsmetabuf);
			page = BufferGetPage(tapeinsertstate->runsmetabuf);
			PageSetLSN(page, recptr);
		}
		LockBuffer(tapeinsertstate->runsmetabuf, BUFFER_LOCK_UNLOCK);
	}
	else
	{
		tapeinsertstate->nranges++;

		if (!ItemPointerIsValid(&tapeinsertstate->runmetad->range_minmax_tuple))
		{
			LockBuffer(tapeinsertstate->runsmetabuf, BUFFER_LOCK_EXCLUSIVE);
			tapeinsertstate->runmetad->range_minmax_tuple = ctid;
			MarkBufferDirty(tapeinsertstate->runsmetabuf);
			LockBuffer(tapeinsertstate->runsmetabuf, BUFFER_LOCK_UNLOCK);
		}
	}
}

/*
 * Get the first block number of the next range.
 */
int sortheap_brin_get_next_range(Relation relation, TapeScanDesc tapescandesc,
								 ItemPointer rangestart)
{
	int lines;
	int ikey;
	int nbtkeys;
	Page page;
	bool isnull;
	bool checkrunlevel;
	Datum d;
	ItemId lpp;
	ScanKey key;
	IndexTuple itup;
	BlockNumber brinblk = InvalidBlockNumber;
	IndexScanDesc idesc;
	OffsetNumber lineoff;
	BTScanOpaque so;
	SortHeapState *shstate = tapescandesc->parent->parent->parent;
	TapeRunsMetaEntry *runmetad = &tapescandesc->runmetad;

	Assert(tapescandesc->idesc);

	tapescandesc->brindesc =
		sortheap_brin_builddesc(RelationGetDescr(shstate->indexrel));

	nbtkeys = RelationGetDescr(shstate->indexrel)->natts;

	checkrunlevel = true;

retry:
	if (tapescandesc->range == -1)
	{
		ItemPointerData *start;

		/* find the first */
		if (checkrunlevel)
		{
			if (!ItemPointerIsValid(&runmetad->run_minmax_tuple))
				return -1;

			start = &runmetad->run_minmax_tuple;
			brinblk = ItemPointerGetBlockNumber(start);
			tapescandesc->brinbuf = ReadBuffer(relation, brinblk);
			lineoff = ItemPointerGetOffsetNumber(start);
		}
		else
		{
			Assert(ItemPointerIsValid(&runmetad->range_minmax_tuple));
			start = &runmetad->range_minmax_tuple;

			brinblk = ItemPointerGetBlockNumber(start);
			lineoff = ItemPointerGetOffsetNumber(start);

			if (ItemPointerGetBlockNumber(&runmetad->run_minmax_tuple) !=
				ItemPointerGetBlockNumber(&runmetad->range_minmax_tuple))
			{
				ReleaseBuffer(tapescandesc->brinbuf);
				tapescandesc->brinbuf = ReadBuffer(relation, brinblk);
			}

			tapescandesc->range = 0;
		}
	}
	else
	{
		checkrunlevel = false;
		tapescandesc->range++;

		/* Run out of brin range index */
		if (tapescandesc->range >= runmetad->nranges)
			return -1;

		/* Get the next brin index tuple */
		LockBuffer(tapescandesc->brinbuf, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(tapescandesc->brinbuf);
		lineoff = ItemPointerGetOffsetNumber(&tapescandesc->brinindex);
		lines = PageGetMaxOffsetNumber(page);

		/* Check whether the next brin index tuple is in the same brin page */
		if ((lines - lineoff) <= 0)
		{
			SortHeapPageOpaqueData *opaque;

			opaque = (SortHeapPageOpaqueData *)PageGetSpecialPointer(page);

			brinblk = opaque->next;

			Assert(brinblk != InvalidBlockNumber);

			UnlockReleaseBuffer(tapescandesc->brinbuf);

			tapescandesc->brinbuf = ReadBuffer(relation, tapescandesc->brinbuf);
			lineoff = FirstOffsetNumber;
		}
		else
		{
			lineoff = OffsetNumberNext(lineoff);
			LockBuffer(tapescandesc->brinbuf, BUFFER_LOCK_UNLOCK);
		}
	}

	/* Update the current brin index using */
	ItemPointerSet(&tapescandesc->brinindex, brinblk, lineoff);

	page = BufferGetPage(tapescandesc->brinbuf);
	lpp = PageGetItemId(page, lineoff);

	itup = (IndexTuple)PageGetItem(page, lpp);

	idesc = tapescandesc->idesc;

	so = (BTScanOpaque)idesc->opaque;

	for (key = so->keyData, ikey = 0; ikey < idesc->numberOfKeys; key++, ikey++)
	{
		Datum min;
		Datum max;
		bool min_isnull;
		bool max_isnull;
		Datum test;
		FmgrInfo *finfo;

		/* Check the min first */
		switch (key->sk_strategy)
		{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			/* Check whether min < key or min <= key */
			min = index_getattr(itup, key->sk_attno,
								tapescandesc->brindesc,
								&min_isnull);

			test = FunctionCall2Coll(&key->sk_func, key->sk_collation,
									 min, key->sk_argument);

			if (!DatumGetBool(test))
			{
				/*
				 * Negative: min >= key or min > key, which means max >=
				 * key or max > key. This range has no tuples match the
				 * scankey, let's advance to next range.
				 */
				if (checkrunlevel)
					return -1;

				goto retry;
			}
			break;
		case BTEqualStrategyNumber:

			/*
			 * If the scankey is c1 = 100, we need to check the min > 100
			 * and max < 100. So according BTLessEqualStrategyNumber and
			 * BTGreaterEqualStrategyNumber info for min > 100 or max <
			 * 100 need to be found.
			 */
			min = index_getattr(itup, key->sk_attno,
								tapescandesc->brindesc,
								&min_isnull);
			finfo = sortheap_get_strategy_procinfo(tapescandesc,
												   shstate->indexrel,
												   key->sk_attno,
												   key->sk_subtype,
												   BTLessEqualStrategyNumber);
			test = FunctionCall2Coll(finfo, key->sk_collation, min, key->sk_argument);

			if (!DatumGetBool(test))
			{
				/* min > 100, the range contains no tuples match == 100 */
				if (checkrunlevel)
					return -1;

				goto retry;
			}

			/* min <= 100, futher check max >= 100 */
			max = index_getattr(itup, nbtkeys + key->sk_attno,
								tapescandesc->brindesc,
								&max_isnull);
			finfo = sortheap_get_strategy_procinfo(tapescandesc,
												   shstate->indexrel,
												   key->sk_attno,
												   key->sk_subtype,
												   BTGreaterEqualStrategyNumber);
			test = FunctionCall2Coll(finfo, key->sk_collation, max, key->sk_argument);

			if (!DatumGetBool(test))
			{
				/* max < 100, the range contains no tuples match == 100 */
				if (checkrunlevel)
					return -1;
				goto retry;
			}

			break;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			/* Check whether max > key or max >= key */
			max = index_getattr(itup, nbtkeys + key->sk_attno,
								tapescandesc->brindesc,
								&max_isnull);

			test = FunctionCall2Coll(&key->sk_func, key->sk_collation,
									 max, key->sk_argument);

			if (!DatumGetBool(test))
			{
				/*
				 * Negative: max <= key or max < key, which means min <=
				 * key or min < key. This range has no tuples match the
				 * scankey, let's advance to next range.
				 */
				if (checkrunlevel)
					return -1;
				goto retry;
			}
			break;
		default:
			elog(ERROR, "invalid strategy number %d", key->sk_strategy);
			break;
		}
	}

	if (checkrunlevel)
	{
		/* The whole run has tuple matches */
		checkrunlevel = false;

		/*
		 * If we have 1+ ranges, start to further check each ranges,
		 * otherwise, just start at range 0.
		 */
		if (runmetad->nranges > 1)
			goto retry;
		else
		{
			*rangestart = runmetad->firsttuple;
			tapescandesc->range = 0;
		}
	}
	else
	{
		d = index_getattr(itup, 2 * nbtkeys + 1,
						  tapescandesc->brindesc,
						  &isnull);

		ItemPointerSet(rangestart, DatumGetUInt32(d), FirstOffsetNumber);
	}

	return tapescandesc->range;
}

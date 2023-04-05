/*-------------------------------------------------------------------------
 *
 * sortheap_common.c
 *	  common routines for sort heap.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_common.c
 *
 *-------------------------------------------------------------------------
 */

#include "sortheap_common.h"
#include "sortheap.h"
#include "sortheap_tapesets.h"
#include "sortheap_tape.h"
#include "sortheap_btree.h"

/*
 * Insert a new tuple into an empty or existing heap, maintaining the
 * heap invariant.  Caller is responsible for ensuring there's room.
 *
 * Note: For some callers, tuple points to a memtuples[] entry above the
 * end of the heap.  This is safe as long as it's not immediately adjacent
 * to the end of the heap (ie, in the [memtupcount] array entry) --- if it
 * is, it might get overwritten before being moved into the heap!
 */
void minheap_insert(SortHeapState *shstate,
					SortTuple *memtuples,
					int *memtupcount,
					SortTuple *tuple)
{
	int j;

	CHECK_FOR_INTERRUPTS();

	/*
	 * Sift-up the new entry, per Knuth 5.2.3 exercise 16. Note that Knuth is
	 * using 1-based array indexes, not 0-based.
	 */
	j = (*memtupcount)++;
	while (j > 0)
	{
		int i = (j - 1) >> 1;

		if (COMPARETUP(shstate, tuple, &memtuples[i]) >= 0)
			break;
		memtuples[j] = memtuples[i];
		j = i;
	}
	memtuples[j] = *tuple;
}

/*
 * Remove the tuple at state->memtuples[0] from the heap.  Decrement
 * memtupcount, and sift up to maintain the heap invariant.
 *
 * The caller has already free'd the tuple the top node points to,
 * if necessary.
 */
void minheap_delete_top(SortHeapState *shstate, SortTuple *memtuples, int *memtupcount)
{
	SortTuple *tuple;

	if (--(*memtupcount) <= 0)
		return;

	/*
	 * Remove the last tuple in the heap, and re-insert it, by replacing the
	 * current top node with it.
	 */
	tuple = &memtuples[*memtupcount];
	minheap_replace_top(shstate, memtuples, memtupcount, tuple);
}

/*
 * Replace the tuple at state->memtuples[0] with a new tuple.  Sift up to
 * maintain the heap invariant.
 *
 * This corresponds to Knuth's "sift-up" algorithm (Algorithm 5.2.3H,
 * Heapsort, steps H3-H8).
 */
void minheap_replace_top(SortHeapState *shstate,
						 SortTuple *memtuples,
						 int *memtupcount,
						 SortTuple *tuple)
{
	unsigned int i,
		n;

	Assert((*memtupcount) >= 1);

	CHECK_FOR_INTERRUPTS();

	/*
	 * state->memtupcount is "int", but we use "unsigned int" for i, j, n.
	 * This prevents overflow in the "2 * i + 1" calculation, since at the top
	 * of the loop we must have i < n <= INT_MAX <= UINT_MAX/2.
	 */
	n = (*memtupcount);
	i = 0; /* i is where the "hole" is */
	for (;;)
	{
		unsigned int j = 2 * i + 1;

		if (j >= n)
			break;
		if (j + 1 < n &&
			COMPARETUP(shstate, &memtuples[j], &memtuples[j + 1]) > 0)
			j++;
		if (COMPARETUP(shstate, tuple, &memtuples[j]) <= 0)
			break;
		memtuples[i] = memtuples[j];
		i = j;
	}
	memtuples[i] = *tuple;
}

Datum get_sorttuple_cache(SortHeapState *shstate, SortTuple *stup, int idx, bool *isnull)
{
	TupleDesc desc;
	Datum d;

	desc = RelationGetDescr(shstate->relation);

	if (idx < MAXKEYCACHE && stup->cached[idx])
	{
		*isnull = stup->isnulls[idx];
		return stup->datums[idx];
	}

	Assert(stup->tuple);

	/*
	 * If it is a commpressed column-oriented tuple, the scan key is already
	 * stored in the nth attr
	 */
	if (IsColumnStoreTuple(stup->tuple))
	{
		d = heap_getattr(stup->tuple, idx, desc, isnull);

		if (idx < MAXKEYCACHE)
		{
			stup->datums[idx] = d;
			stup->isnulls[idx] = *isnull;
			stup->cached[idx] = true;
		}
		return d;
	}

	/* Otherwise, calculate the key from the original attrs */
	if (shstate->indexinfo->ii_Expressions == NULL)
	{
		AttrNumber attno = shstate->indexinfo->ii_IndexAttrNumbers[idx];

		d = heap_getattr(stup->tuple, attno, desc, isnull);

		if (idx < MAXKEYCACHE)
		{
			stup->datums[idx] = d;
			stup->isnulls[idx] = *isnull;
			stup->cached[idx] = true;
		}

		return d;
	}
	else
	{
		int i;
		Datum index_values[INDEX_MAX_KEYS];
		bool index_isnull[INDEX_MAX_KEYS];
		TupleDesc indexdesc;
		TupleTableSlot *ecxt_scantuple;

		indexdesc = RelationGetDescr(shstate->indexrel);

		/* Reset context each time to prevent memory leakage */
		ResetPerTupleExprContext(shstate->estate);

		ecxt_scantuple = GetPerTupleExprContext(shstate->estate)->ecxt_scantuple;

		ExecStoreHeapTuple(stup->tuple, ecxt_scantuple, false);
		FormIndexDatum(shstate->indexinfo, ecxt_scantuple, shstate->estate,
					   index_values, index_isnull);

		for (i = 0; i < shstate->nkeys; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(indexdesc, i);

			d = datumCopy(index_values[i], attr->attbyval, attr->attlen);
			*isnull = index_isnull[i];

			if (i < MAXKEYCACHE)
			{
				stup->datums[i] = d;
				stup->isnulls[i] = *isnull;
				stup->cached[i] = true;
			}

			if (i == idx)
				return d;
		}
	}

	*isnull = true;
	return PointerGetDatum(NULL);
}

bool sortheap_match_scankey_quals(Relation relation,
								  TapeScanDesc scandesc,
								  SortTuple *stup,
								  TupleTableSlot *slot,
								  bool *continuescan,
								  bool comparescankey,
								  bool comparequal)
{
	ExprState *qual;

	*continuescan = true;

	/*
	 * If an auxiliary indexscan is supplied, the original qual of the seqscan
	 * is removed, we must do qual filter here with the scankey and qual of
	 * the indexscan.
	 */
	if (comparescankey && scandesc->idesc)
	{
		/* Check whether the stup matches the all scan keys */
		if (!sortheap_bt_checkkeys(relation, scandesc->idesc,
								   stup, ForwardScanDirection, continuescan))
			return false;
	}

	if (comparequal && scandesc->tablescandesc && scandesc->tablescandesc->qual)
	{
		ExprContext *econtext;

		/*
		 * If the seqscan pushed down the qual filter, we must eval it here.
		 * Eg, customscan want to keep order and want the tuples are filtered
		 * before sorting.
		 */
		econtext = scandesc->tablescandesc->qualcontext;
		qual = scandesc->tablescandesc->qual;

		ResetExprContext(econtext);
		Assert(slot);
		econtext->ecxt_scantuple = slot;

		if (ExecQual(qual, econtext))
			return true;

		/* qual fails, retry */
		return false;
	}

	return true;
}

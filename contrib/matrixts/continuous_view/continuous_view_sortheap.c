#include "postgres.h"

#include "executor/nodeAgg.h"
#include "executor/executor.h"

#include "access/sortheapam.h"
#include "continuous_view.h"

PG_FUNCTION_INFO_V1(cvsortheap_tableam_handler);

/* ------------------------------------------------------------------------
 * Definition of the sort heap table access method.
 * ------------------------------------------------------------------------
 */

static TableAmRoutine cv_sortheapam_methods;
static ExternalSortMethods cv_externalsort_methods;

static bool
cv_copytup(SortHeapState *shstate, SortTuple *stup, TupleTableSlot *slot, int64 *availmem)
{
	MemoryContext oldcontext;
	ScanState *scanstate;
	ExprState *qual;
	ExprContext *econtext;
	ProjectionInfo *projectinfo;
	CvExecutorState *cvestate;

	oldcontext = MemoryContextSwitchTo(shstate->tuplecontext);

	if (!shstate->private)
		shstate->private = (void *)
			continuous_view_executor_start(shstate,
										   shstate->args ? 
										   ((Relation) shstate->args)->rd_id :
										   InvalidOid);
	cvestate = (CvExecutorState *) shstate->private;

	scanstate = cvestate->scanstate;

	qual = scanstate->ps.qual;
	projectinfo = scanstate->ps.ps_ProjInfo;
	econtext = scanstate->ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	if (qual || projectinfo)
	{
		if (cvestate->scanslot->tts_ops == slot->tts_ops)
			econtext->ecxt_scantuple = slot;
		else
		{
			ExecClearTuple(cvestate->scanslot);
			ExecCopySlot(cvestate->scanslot, slot);
			econtext->ecxt_scantuple = cvestate->scanslot;
		}

		if (qual && !ExecQual(qual, econtext))
			return false;

		if (projectinfo)
		{
			TupleTableSlot *resultslot;

			resultslot = ExecProject(projectinfo);
			stup->tuple = ExecCopySlotHeapTuple(resultslot);
		}
		else
		{
			/* copy the tuple into sort storage */
			stup->tuple = ExecCopySlotHeapTuple(slot);
		}
	}
	else
	{
		stup->tuple = ExecCopySlotHeapTuple(slot);
	}

	MemSet(stup->cached, false, sizeof(bool) * MAXKEYCACHE);

	MemoryContextSwitchTo(oldcontext);

	*availmem -= GetMemoryChunkSpace(stup->tuple);

	return true;
}

/*
 * tuples are merging sorting into one tape, do a group aggregate before writing
 * into the target tape.
 *
 * Everytime the sort_advance_aggregate returns one tuple when across one group
 * boundary, insert it to the target tape.
 */
static void
cv_mergetup(SortHeapState *shstate, SortTuple *stup)
{
	TupleTableSlot *slot;
	CvExecutorState *cvestate;

	if (!shstate->private)
		shstate->private = (void *)
			continuous_view_executor_start(shstate,
										   shstate->args ? 
										   ((Relation) shstate->args)->rd_id :
										   InvalidOid);

	cvestate = (CvExecutorState *) shstate->private;

	if (stup)
	{
		PlanState 	*substate = outerPlanState(cvestate->aggstate);

		if (IsA(substate, GatherState))
			slot = ((GatherState *) substate)->funnel_slot;
		else
			slot = substate->ps_ResultTupleSlot;

		ExecClearTuple(slot);
		ExecForceStoreHeapTuple(stup->tuple,
								slot,
								false);
	}
	else
	{
		/* End of run, set slot to NULL to finialize the last group */
		slot = NULL;
	}

	slot = sort_advance_aggregate(cvestate->aggstate, slot);

	if (slot)
	{
		bool		shouldFree;
		SortTuple	tmp;
		HeapTuple	htup;

		htup = ExecFetchSlotHeapTuple(slot,
									  false,
									  &shouldFree);
		tmp.tuple = htup;
		MemSet(tmp.cached, false, sizeof(bool) * MAXKEYCACHE);

		WRITETUP(shstate, &tmp);

		if (shouldFree)
			pfree(htup);

		ExecClearTuple(slot);
	}

	/* End of run */
	if (!stup)
	{
		WRITETUP(shstate, NULL);
		continuous_view_executor_end(cvestate);
		shstate->private = NULL;
	}
}

/*
 * memtuples array stores the sorted tuples that has been transformed by
 * cv_copytup, do a group aggregate before writing it into the target tape.
 */
static void
cv_dumptups(SortHeapState *shstate, SortTuple *memtuples,
			int memtupcount, int64 *availmem, bool lastdump)
{
	CvExecutorState *cvestate;

	if (!shstate->private)
		shstate->private = (void *)
			continuous_view_executor_start(shstate,
										   shstate->args ? 
										   ((Relation) shstate->args)->rd_id :
										   InvalidOid);

	cvestate = (CvExecutorState *) shstate->private;

	if (memtuples)
	{
		int		i;
		TupleTableSlot 	*slot;

		for (i = 0; i < memtupcount; i++)
		{
			PlanState *planstate;
			SortTuple *stup = &memtuples[i];

			planstate = outerPlanState(cvestate->aggstate);

			if (planstate->ps_ResultTupleSlot)
				slot = planstate->ps_ResultTupleSlot;
			else
				slot = ((ScanState *) planstate)->ss_ScanTupleSlot;

			ExecClearTuple(slot);
			ExecForceStoreHeapTuple(stup->tuple,
									slot,
									false);

			slot = sort_advance_aggregate(cvestate->aggstate, slot);

			/* Reach the boundary of a group, finialize and insert it to the table */
			if (slot)
			{
				bool		shouldFree;
				SortTuple	tmp;
				HeapTuple	htup;

				htup = ExecFetchSlotHeapTuple(slot,
											  false,
											  &shouldFree);
				tmp.tuple = htup;
				MemSet(tmp.cached, false, sizeof(bool) * MAXKEYCACHE);

				WRITETUP(shstate, &tmp);

				if (shouldFree)
					pfree(htup);

				ExecClearTuple(slot);
			}

			*availmem += GetMemoryChunkSpace(stup->tuple);
			heap_freetuple(stup->tuple);
		}

		/* Finalize the last group */
		slot = sort_advance_aggregate(cvestate->aggstate, NULL);
		if (slot)
		{
			bool		shouldFree;
			SortTuple	tmp;
			HeapTuple	htup;

			htup = ExecFetchSlotHeapTuple(slot,
										  false,
										  &shouldFree);
			tmp.tuple = htup;
			MemSet(tmp.cached, false, sizeof(bool) * MAXKEYCACHE);

			WRITETUP(shstate, &tmp);

			if (shouldFree)
				pfree(htup);

			ExecClearTuple(slot);
		}

		/* Write up a end of run marker */
		WRITETUP(shstate, NULL);
	}

	if (lastdump)
	{
		continuous_view_executor_end(cvestate);
		shstate->private = NULL;
	}
}

Datum
cvsortheap_tableam_handler(PG_FUNCTION_ARGS)
{
	cv_externalsort_methods = externalsort_default_methods;
	cv_externalsort_methods.copytup = cv_copytup;
	cv_externalsort_methods.mergetup = cv_mergetup;
	cv_externalsort_methods.dumptups = cv_dumptups;
	cv_sortheapam_methods = sortheapam_methods;
	cv_sortheapam_methods.priv = (void *) &cv_externalsort_methods;
	PG_RETURN_POINTER(&cv_sortheapam_methods);
}


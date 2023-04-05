/*---------------------------------------------------------------------------
 *
 * sortheap_customscan.c
 *  	Implement custom scan for the sortheap table.
 *
 * The custom scan provide:
 * 1) a seqscan now has a pathkeys
 * 2) the pathkeys of the index scan is removed (btree forest keep no order)
 * 3) the index paths are passed down as an auxilary path of seqscan, this
 * gives the seqscan some abilities indexscan has (eg, quick locate the
 * first tuple, stop scan earlier).
 * 4) filters are pushed down before sorting.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_customscan.c
 *
 *-------------------------------------------------------------------------
 */

#include "sortheap.h"

set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;

typedef struct SortHeapCustomScanState
{
	CustomScanState csstate;
	SeqScanState *seqscanstate;
	IndexScanState *indexscanstate;
} SortHeapCustomScanState;

/* Callbacks for execution of sortheap agg scan */
static void begin_sortheap_scan(CustomScanState *node,
								EState *estate,
								int eflags);
static TupleTableSlot *exec_sortheap_scan(CustomScanState *node);
static void end_sortheap_scan(CustomScanState *node);
static void rescan_sortheap_scan(CustomScanState *node);
static Size estimate_dsm_sortheap_scan(CustomScanState *node,
									   ParallelContext *pcxt);
static void initialize_dsm_sortheap_scan(CustomScanState *node,
										 ParallelContext *pcxt,
										 void *coordinate);
static void reinitialize_dsm_sortheap_scan(CustomScanState *node,
										   ParallelContext *pcxt,
										   void *coordinate);
static void initialize_worker_sortheap_scan(CustomScanState *node,
											shm_toc *toc,
											void *coordinate);
static void explain_sortheap_scan(CustomScanState *node,
								  List *ancestors,
								  ExplainState *es);
static void shutdown_sortheap_scan(CustomScanState *node);
static Node *create_sortheap_scan_state(CustomScan *cscan);

/* Callbacks for planner */
static Plan *create_sortheap_scan_plan(PlannerInfo *root,
									   RelOptInfo *rel,
									   CustomPath *best_path,
									   List *tlist,
									   List *clauses,
									   List *custom_plans);

/* Method to convert a sortheap scan path to plan */
static const CustomPathMethods sortheap_scan_path_methods =
	{
		.CustomName = "sortheapscan",
		.PlanCustomPath = create_sortheap_scan_plan,
};

/* Method to initialize a sortheap scan plan node */
static CustomScanMethods sortheap_scan_plan_methods =
	{
		.CustomName = "sortheapscan",
		.CreateCustomScanState = create_sortheap_scan_state,
};

/* Method to define the callbacks for execution */
static CustomExecMethods sortheap_sort_scan_exec_methods =
	{
		.CustomName = "sortheap Sort Scan",
		.BeginCustomScan = begin_sortheap_scan,
		.ExecCustomScan = exec_sortheap_scan,
		.EndCustomScan = end_sortheap_scan,
		.ReScanCustomScan = rescan_sortheap_scan,
		/* We need to fill all the fields to compile with gcc-7 or earlier */
		.MarkPosCustomScan = NULL,
		.RestrPosCustomScan = NULL,
		.EstimateDSMCustomScan = estimate_dsm_sortheap_scan,
		.InitializeDSMCustomScan = initialize_dsm_sortheap_scan,
		.ReInitializeDSMCustomScan = reinitialize_dsm_sortheap_scan,
		.InitializeWorkerCustomScan = initialize_worker_sortheap_scan,
		.ShutdownCustomScan = shutdown_sortheap_scan,
		.ExplainCustomScan = explain_sortheap_scan,
};

static Size
estimate_dsm_sortheap_scan(CustomScanState *node,
						   ParallelContext *pcxt)
{
	elog(ERROR, "sortheap doesn't support parallel mode");
}

static void
initialize_dsm_sortheap_scan(CustomScanState *node,
							 ParallelContext *pcxt,
							 void *coordinate)
{
	elog(ERROR, "sortheap doesn't support parallel mode");
}

static void
reinitialize_dsm_sortheap_scan(CustomScanState *node,
							   ParallelContext *pcxt,
							   void *coordinate)
{
	elog(ERROR, "sortheap doesn't support parallel mode");
}

static void
initialize_worker_sortheap_scan(CustomScanState *node,
								shm_toc *toc,
								void *coordinate)
{
	elog(ERROR, "sortheap doesn't support parallel mode");
}

/* show custom ps */
static void
explain_sortheap_scan(CustomScanState *node,
					  List *ancestors,
					  ExplainState *es)
{
	return;
}

static void
shutdown_sortheap_scan(CustomScanState *node)
{
	// sortheap_endsort(node->ss.ss_currentRelation);
}

static Node *
create_sortheap_scan_state(CustomScan *cscan)
{
	SortHeapCustomScanState *state;

	state = (SortHeapCustomScanState *)newNode(sizeof(SortHeapCustomScanState),
											   T_CustomScanState);

	state->csstate.methods = &sortheap_sort_scan_exec_methods;
	state->seqscanstate = NULL;
	state->indexscanstate = NULL;

	return (Node *)state;
}

/*
 * This function tells how to convert a CustomPath to sortheapscan plan, the key
 * is CustomScanMethods which defines the function to create a CustomScanState.
 */
static Plan *
create_sortheap_scan_plan(PlannerInfo *root, RelOptInfo *rel,
						  CustomPath *best_path,
						  List *tlist,
						  List *scan_clauses,
						  List *custom_plans)
{
	CustomScan *cscan;
	Index scanrelid = best_path->path.parent->relid;
	Plan *plan;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */

	cscan = makeNode(CustomScan);
	cscan->scan.scanrelid = scanrelid;
	cscan->scan.plan.targetlist = tlist;
	cscan->flags = best_path->flags;
	cscan->custom_plans = custom_plans;

	plan = (Plan *)linitial(custom_plans);
	cscan->custom_scan_tlist = plan->targetlist;

	/* Tell how to create a CustomScanState */
	cscan->methods = &sortheap_scan_plan_methods;

	return &cscan->scan.plan;
}

static void
begin_sortheap_scan(CustomScanState *node,
					EState *estate, int eflags)
{
	int idx;
	ListCell *lc;
	CustomScan *cs;
	SortHeapCustomScanState *state = (SortHeapCustomScanState *)node;

	cs = (CustomScan *)node->ss.ps.plan;

	foreach_with_count(lc, cs->custom_plans, idx)
	{
		PlanState *subplanstate;
		Plan *subplan = (Plan *)lfirst(lc);

		subplanstate = ExecInitNode(subplan, estate, eflags);

		/*
		 * Set custom_ps for explain, we only show the main planstate for
		 * explain
		 */
		node->custom_ps = lappend(node->custom_ps, subplanstate);

		/* Set the internal seqscan state */
		if (IsA(subplanstate, SeqScanState))
			state->seqscanstate = (SeqScanState *)subplanstate;
		else if (IsA(subplanstate, IndexScanState))
			state->indexscanstate = (IndexScanState *)subplanstate;
	}

	/* Re-initialize the result tuple slot to TTSOpsHeapTuple */
	ExecInitResultTupleSlotTL(&node->ss.ps, &TTSOpsHeapTuple);

	/* Re-initialize the scan tuple slot to TTSOpsHeapTuple */
	node->ss.ss_ScanTupleSlot = ExecAllocTableSlot(&estate->es_tupleTable,
												   node->ss.ps.scandesc,
												   &TTSOpsHeapTuple);
	node->ss.ps.scanops = &TTSOpsHeapTuple;

	ExecAssignScanProjectionInfoWithVarno(&node->ss, INDEX_VAR);

	return;
}

static TupleTableSlot *
sortheap_seqnext(SortHeapCustomScanState *node)
{
	SeqScanState *seqscanstate = node->seqscanstate;
	IndexScanState *indexscanstate = node->indexscanstate;

	if (seqscanstate->ss.ss_currentScanDesc == NULL)
	{
		SortHeapScanDesc scandesc;
		EState *estate;

		estate = seqscanstate->ss.ps.state;

		/*
		 * GPDB: we are using table_beginscan_es in order to also initialize
		 * the scan state with the column info needed for AOCO relations.
		 * Check the comment in table_beginscan_es() for more info.
		 */
		seqscanstate->ss.ss_currentScanDesc =
			table_beginscan_es(seqscanstate->ss.ss_currentRelation,
							   estate->es_snapshot,
							   seqscanstate->ss.ps.plan->targetlist,
							   seqscanstate->ss.ps.plan->qual);
		scandesc = (SortHeapScanDesc)seqscanstate->ss.ss_currentScanDesc;
		scandesc->keeporder = true;

		/* In custom scan, all quals are pushed down to the scan node */
		scandesc->iss = NULL;
		scandesc->qual = seqscanstate->ss.ps.qual;
		scandesc->qualcontext = seqscanstate->ss.ps.ps_ExprContext;
		seqscanstate->ss.ps.qual = NULL;

		/* If we have an auxiliary index search state, use its qual instead */
		if (indexscanstate)
		{
			ScanKey cur;
			int i;
			bool arraykeys = false;

			/*
			 * SeqScan cannot handle array keys, give up auxiliary index
			 * search if found one
			 */
			for (i = 0; i < indexscanstate->iss_NumScanKeys; i++)
			{
				cur = &indexscanstate->iss_ScanKeys[i];
				if (cur->sk_flags & SK_SEARCHARRAY)
				{
					arraykeys = true;
					break;
				}
			}

			if (!arraykeys)
			{
				scandesc->iss = indexscanstate;
				scandesc->qual = indexscanstate->ss.ps.qual;
				scandesc->qualcontext = indexscanstate->ss.ps.ps_ExprContext;
			}
		}
	}

	return ExecProcNode((PlanState *)seqscanstate);
}

static bool
sortheap_seqrecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

static TupleTableSlot *
exec_sortheap_scan(CustomScanState *node)
{
	TupleTableSlot *slot;
	SortHeapCustomScanState *csstate = (SortHeapCustomScanState *)node;

	if (csstate->seqscanstate)
	{
		/* Fetch a slot from the underlying table */
		slot = ExecScan((ScanState *)csstate,
						(ExecScanAccessMtd)sortheap_seqnext,
						(ExecScanRecheckMtd)sortheap_seqrecheck);
	}
	else
	{
		ProjectionInfo *projInfo = node->ss.ps.ps_ProjInfo;

		Assert(csstate->indexscanstate);
		slot = ExecProcNode((PlanState *)csstate->indexscanstate);

		if (TupIsNull(slot))
		{
			if (projInfo)
				return ExecClearTuple(projInfo->pi_state.resultslot);
			else
				return slot;
		}

		if (projInfo)
		{
			ExprContext *econtext;
			econtext = node->ss.ps.ps_ExprContext;
			/* place the current tuple into the expr context */
			ResetExprContext(econtext);
			econtext->ecxt_scantuple = slot;
			return ExecProject(projInfo);
		}
	}

	return slot;
}

static void
end_sortheap_scan(CustomScanState *node)
{
	TableScanDesc scanDesc;
	SortHeapCustomScanState *csstate = (SortHeapCustomScanState *)node;

	if (csstate->seqscanstate)
	{
		ExecEndNode((PlanState *)csstate->seqscanstate);
		csstate->seqscanstate = NULL;
	}

	if (csstate->indexscanstate)
	{
		ExecEndNode((PlanState *)csstate->indexscanstate);
		csstate->indexscanstate = NULL;
	}

	node->custom_ps = NIL;

	/*
	 * get information from node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	if (scanDesc != NULL)
		table_endscan(scanDesc);
}

static void
rescan_sortheap_scan(CustomScanState *node)
{
	SortHeapCustomScanState *state = (SortHeapCustomScanState *)node;

	if (state->seqscanstate)
	{
		UpdateChangedParamSet((PlanState *)state->seqscanstate, node->ss.ps.chgParam);
		ExecReScan((PlanState *)state->seqscanstate);
	}

	if (state->indexscanstate)
	{
		UpdateChangedParamSet((PlanState *)state->indexscanstate, node->ss.ps.chgParam);
		ExecReScan((PlanState *)state->indexscanstate);
	}
}

static Path *
create_sortheap_customscan_path(PlannerInfo *root,
								RelOptInfo *rel,
								Path *seqscan_path,
								Path *indexscan_path)
{
	CustomPath *path;
	Path *copy;

	Assert(seqscan_path || indexscan_path);

	/* Create a custom scan path and set the method */
	path = (CustomPath *)newNode(sizeof(CustomPath), T_CustomPath);

	/* Set common path fields */
	path->path.pathtype = T_CustomScan;
	path->path.parent = rel;
	path->path.pathtarget = indexscan_path ? indexscan_path->pathtarget : seqscan_path->pathtarget;
	path->path.param_info = indexscan_path ? indexscan_path->param_info : seqscan_path->param_info;
	/* Don't consider parallel for now */
	path->path.parallel_aware = false;
	path->path.parallel_safe = false;
	path->path.parallel_workers = 0;
	path->path.locus = seqscan_path ? seqscan_path->locus : indexscan_path->locus;
	path->path.motionHazard = false;
	path->path.rescannable = true;
	path->path.sameslice_relids = rel->relids;

	if (seqscan_path != NULL)
	{
		if (seqscan_path->pathtarget->exprs != NULL && rel->indexlist)
		{
			IndexOptInfo *index;

			index = (IndexOptInfo *)linitial(rel->indexlist);

			/* Decorate a pathkey above seqscan */
			path->path.pathkeys =
				build_index_pathkeys(root, index, ForwardScanDirection);
		}

		/*
		 * If the internal index can predicate the filter, give a reward,
		 * otherwise, give a penalty to keep the order.
		 */
		if (indexscan_path &&
			IsA(indexscan_path, IndexPath) &&
			((IndexPath *)indexscan_path)->indexclauses)
		{
			double factor;

			if (indexscan_path->param_info)
				factor = 0.5;
			else
				factor = 0.7;

			path->path.total_cost = seqscan_path->total_cost * factor;
			path->path.startup_cost = seqscan_path->startup_cost * factor;
			path->path.rows = indexscan_path->rows;
		}
		else
		{
			path->path.total_cost = seqscan_path->total_cost * 1.2;
			path->path.startup_cost = seqscan_path->startup_cost * 1.2;
			path->path.rows = seqscan_path->rows;
		}
	}
	else
	{
		Assert(indexscan_path != NULL);

		/*
		 * Remove pathkey on the indexscan because sortheapam_btree cannot
		 * guarantee the order
		 */
		path->path.pathkeys = NIL;

		/* Keep the same cost with index scan */
		path->path.total_cost = indexscan_path->total_cost;
		path->path.startup_cost = indexscan_path->startup_cost;
		path->path.rows = indexscan_path->rows;
	}

	/*
	 * Setup the custom scan method, this is mainly to set the callback to
	 * convert a custom path to custom plan, in that callback you can setup
	 * your callback for execution, to be more accurate, for initialize of
	 * CustomScanState.
	 */
	path->methods = &sortheap_scan_path_methods;

	if (seqscan_path != NULL)
	{
		copy = makeNode(Path);
		memcpy(copy, seqscan_path, sizeof(Path));

		path->custom_paths = lappend(path->custom_paths, copy);
	}

	if (indexscan_path &&
		(seqscan_path == NULL ||
		 (IsA(indexscan_path, IndexPath) &&
		  ((IndexPath *)indexscan_path)->indexclauses)))
	{
		copy = (Path *)makeNode(IndexPath);
		memcpy(copy, indexscan_path, sizeof(IndexPath));

		path->custom_paths = lappend(path->custom_paths, copy);
	}

	return (Path *)path;
}

/*
 * Decorate the normal seqscan/index scan within a customsca path.
 * 1) normal seqscan has no order info (pathkeys), customscan can hold pathkeys.
 * 2) index scan using sortheap_btree which cannot guarantee the same order with the
 * index key, a customscan can correct it.
 */
static void
sortheap_set_rel_pathlist_hook(PlannerInfo *root,
							   RelOptInfo *input_rel,
							   Index rti,
							   RangeTblEntry *rte)
{
	ListCell *lc;
	List *aux_indexpaths = NIL;
	Path *subpath;
	List *new_pathlist = NIL;
	List *indexscan_paths = NIL;
	List *seqscan_paths = NIL;
	Relation rel;

	if (prev_set_rel_pathlist_hook != NULL)
		(*prev_set_rel_pathlist_hook)(root, input_rel, rti, rte);

	if (!IS_SIMPLE_REL(input_rel) || input_rel->rtekind != RTE_RELATION)
		return;

	/* Quit immediately if it's not a sortheap or sortheap like storage */
	rel = RelationIdGetRelation(rte->relid);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
	{
		RelationClose(rel);
		return;
	}

	if (rel->rd_tableam->event_notify != sortheapam_methods.event_notify ||
		rel->rd_tableam->slot_callbacks != sortheapam_methods.slot_callbacks ||
		rel->rd_tableam->tuple_insert != sortheapam_methods.tuple_insert)
	{
		RelationClose(rel);
		return;
	}

	RelationClose(rel);

	/*
	 * Find out all seqscan and indexscan paths.
	 *
	 * Sort heap doesn't support parallel scan now, so only consider pathlist
	 */
	foreach (lc, input_rel->pathlist)
	{
		subpath = (Path *)lfirst(lc);

		if (subpath->pathtype == T_SeqScan)
		{
			seqscan_paths = lappend(seqscan_paths, subpath);
			/* the original seqscan should still kept in the pathlist */
			new_pathlist = lappend(new_pathlist, subpath);
		}
		else if (IsA(subpath, IndexPath))
		{
			indexscan_paths = lappend(indexscan_paths, subpath);
		}
		else
			new_pathlist = lappend(new_pathlist, subpath);
	}

	/* Empty the old pathlist, we will rebuild the pathlist */
	input_rel->pathlist = NIL;

	/*
	 * Decorate seqscan paths within custom scan with pathkeys and scankeys.
	 *
	 * Firstly, find any useful scankeys to speed up the tuples filter within
	 * custom seqscan.
	 *
	 * If indexscan exists, we can derive the keys from them. Otherwise, call
	 * create_index_paths() to generate index scan paths again. We have
	 * emptied input_rel->pathlist, so those index paths that got rid due to
	 * high costs can be added to the pathlist now.
	 *
	 * To be simple, the whole index path is kept as child path of custom scan
	 */
	if (seqscan_paths)
	{
		if (indexscan_paths)
			aux_indexpaths = indexscan_paths;
		else
		{
			bool saved_enable_bitmapscan;

			saved_enable_bitmapscan = enable_bitmapscan;
			enable_bitmapscan = false;

			create_index_paths(root, input_rel);

			enable_bitmapscan = saved_enable_bitmapscan;

			if (input_rel->pathlist)
				aux_indexpaths = input_rel->pathlist;
			else
				aux_indexpaths = list_make1(NULL);
		}
	}

	/* Start to rebuild the pathlist */
	input_rel->pathlist = new_pathlist;

	/*
	 * Decorate seqscan paths within custom scan with pathkeys and auxiliary
	 * index scan
	 */
	foreach (lc, seqscan_paths)
	{
		ListCell *lc1;

		subpath = (Path *)lfirst(lc);

		foreach (lc1, aux_indexpaths)
		{
			Path *indexpath = (Path *)lfirst(lc1);

			add_path(input_rel, create_sortheap_customscan_path(root,
																input_rel,
																subpath,
																indexpath));
		}
	}

	/* Decorate indexscan paths within custom scan without pathkeys */
	foreach (lc, indexscan_paths)
	{
		subpath = (Path *)lfirst(lc);

		add_path(input_rel, create_sortheap_customscan_path(root,
															input_rel,
															NULL,
															subpath));
	}
}

void _sortheapam_customscan_init(void)
{
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = sortheap_set_rel_pathlist_hook;

	RegisterCustomScanMethods(&sortheap_scan_plan_methods);
}

void _sortheapam_customscan_fini(void)
{
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
}

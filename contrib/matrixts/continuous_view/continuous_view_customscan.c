/*-------------------------------------------------------------------------
 *
 * continuous_viewcustomscan.cc
 *	  The continuous_view Sort Scan is a normal sequential scan that has
 *	  pathkeys (order info).
 *
 *	  continuous_view can provide mulitple pathkeys, eg, table ordering
 *	  keys, time_bucket expressions on table ordering keys.
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "commands/explain.h"
#include "nodes/makefuncs.h"
#include "nodes/pathnodes.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"

#include "continuous_view.h"

static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
static create_upper_paths_hook_type prev_create_upper_paths_hook;

typedef struct PrivateCustomData
{
	Oid scanrelid;
	List *custom_scan_tlist;
	List *scanclauses;
} PrivateCustomData;

typedef struct ContinuousViewCSState
{
	CustomScanState csstate;
	PlanState *sub_planstate;
} ContinuousViewCSState;

/* Callbacks for execution of continuous_view agg scan */
static void begin_continuous_view_scan(CustomScanState *node,
									   EState *estate, int eflags);
static TupleTableSlot *exec_continuous_view_scan(CustomScanState *node);
static void end_continuous_view_scan(CustomScanState *node);
static void rescan_continuous_view_scan(CustomScanState *node);
static Size estimate_dsm_continuous_view_scan(CustomScanState *node,
											  ParallelContext *pcxt);
static void initialize_dsm_continuous_view_scan(CustomScanState *node,
												ParallelContext *pcxt,
												void *coordinate);
static void reinitialize_dsm_continuous_view_scan(CustomScanState *node,
												  ParallelContext *pcxt,
												  void *coordinate);
static void initialize_worker_continuous_view_scan(CustomScanState *node,
												   shm_toc *toc,
												   void *coordinate);
static void explain_continuous_view_scan(CustomScanState *node,
										 List *ancestors,
										 ExplainState *es);
static Node *create_continuous_view_scan_state(CustomScan *cscan);

/* Callbacks for planner */
static Plan *create_continuous_view_scan_plan(PlannerInfo *root,
											  RelOptInfo *rel,
											  CustomPath *best_path,
											  List *tlist,
											  List *clauses,
											  List *custom_plans);

/* Method to convert a continuous_view scan path to plan */
static const CustomPathMethods continuous_view_scan_path_methods =
	{
		.CustomName = "continuous_viewscan",
		.PlanCustomPath = create_continuous_view_scan_plan,
};

/* Method to initialize a continuous_view scan plan node */
static CustomScanMethods continuous_view_scan_plan_methods =
	{
		.CustomName = "continuous_viewscan",
		.CreateCustomScanState = create_continuous_view_scan_state,
};

/* Method to define the callbacks for execution */
static CustomExecMethods continuous_view_sort_scan_exec_methods =
	{
		.CustomName = "continuous_view Sort Scan",
		.BeginCustomScan = begin_continuous_view_scan,
		.ExecCustomScan = exec_continuous_view_scan,
		.EndCustomScan = end_continuous_view_scan,
		.ReScanCustomScan = rescan_continuous_view_scan,
		/* We need to fill all the fields to compile with gcc-7 or earlier */
		.MarkPosCustomScan = NULL,
		.RestrPosCustomScan = NULL,
		.EstimateDSMCustomScan = estimate_dsm_continuous_view_scan,
		.InitializeDSMCustomScan = initialize_dsm_continuous_view_scan,
		.ReInitializeDSMCustomScan = reinitialize_dsm_continuous_view_scan,
		.InitializeWorkerCustomScan = initialize_worker_continuous_view_scan,
#if 0
	.ShutdownCustomScan = shutdown_continuous_view_scan,
#else
		.ShutdownCustomScan = NULL,
#endif
		.ExplainCustomScan = explain_continuous_view_scan,
};

static Size
estimate_dsm_continuous_view_scan(CustomScanState *node,
								  ParallelContext *pcxt)
{
	elog(ERROR, "continuous_view doesn't support parallel mode");
}

static void
initialize_dsm_continuous_view_scan(CustomScanState *node,
									ParallelContext *pcxt,
									void *coordinate)
{
	elog(ERROR, "continuous_view doesn't support parallel mode");
}

static void
reinitialize_dsm_continuous_view_scan(CustomScanState *node,
									  ParallelContext *pcxt,
									  void *coordinate)
{
	elog(ERROR, "continuous_view doesn't support parallel mode");
}

static void
initialize_worker_continuous_view_scan(CustomScanState *node,
									   shm_toc *toc,
									   void *coordinate)
{
	elog(ERROR, "continuous_view doesn't support parallel mode");
}

/* show sort keys */
static void
explain_continuous_view_scan(CustomScanState *node,
							 List *ancestors,
							 ExplainState *es)
{
	return;
}

static Node *
create_continuous_view_scan_state(CustomScan *cscan)
{
	CustomScanState *state;
	Plan *plan;

	state = (CustomScanState *)newNode(sizeof(CustomScanState),
									   T_CustomScanState);

	state->methods = &continuous_view_sort_scan_exec_methods;

	plan = (Plan *)linitial(cscan->custom_plans);

	return (Node *)state;
}

/*
 * This function tells how to convert a CustomPath to
 * continuous_viewscan plan, the key is CustomScanMethods which
 * defines the function to create a CustomScanState.
 */
static Plan *
create_continuous_view_scan_plan(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path,
								 List *tlist,
								 List *scan_clauses,
								 List *custom_plans)
{
	CustomScan *cscan;
	PrivateCustomData *private =
		(PrivateCustomData *)linitial(best_path->custom_private);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(private->scanclauses, false);

	cscan = makeNode(CustomScan);
	cscan->scan.scanrelid = private->scanrelid;
	cscan->scan.plan.targetlist = tlist;
	cscan->flags = best_path->flags;
	cscan->custom_scan_tlist = private->custom_scan_tlist;
	cscan->custom_plans = custom_plans;

	/* Tell how to create a CustomScanState */
	cscan->methods = &continuous_view_scan_plan_methods;

	return &cscan->scan.plan;
}

static void
begin_continuous_view_scan(CustomScanState *node,
						   EState *estate, int eflags)
{
	CustomScan *cs;
	Plan *subplan;

	cs = (CustomScan *)node->ss.ps.plan;

	Assert(list_length(cs->custom_plans) == 1);
	subplan = (Plan *)linitial(cs->custom_plans);

	node->custom_ps = list_make1(ExecInitNode(subplan, estate, eflags));

	/* Re-initialize the result tuple slot to TTSOpsHeapTuple */
	ExecInitResultTupleSlotTL(&node->ss.ps, &TTSOpsHeapTuple);

	/* Re-initialize the scan tuple slot to TTSOpsHeapTuple */
	node->ss.ss_ScanTupleSlot = ExecAllocTableSlot(&estate->es_tupleTable,
												   node->ss.ps.scandesc,
												   &TTSOpsHeapTuple);
	node->ss.ps.scanops = &TTSOpsHeapTuple;

	return;
}

static TupleTableSlot *
exec_continuous_view_scan(CustomScanState *node)
{
	PlanState *sub_planstate;
	ProjectionInfo *projInfo;
	TupleTableSlot *slot;

	sub_planstate = (PlanState *)linitial(node->custom_ps);
	projInfo = node->ss.ps.ps_ProjInfo;

	slot = ExecProcNode(sub_planstate);

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
		/*
		 * place the current tuple into the expr context
		 */
		ResetExprContext(econtext);
		econtext->ecxt_scantuple = slot;

		return ExecProject(projInfo);
	}
	else
		return slot;
}

static void
end_continuous_view_scan(CustomScanState *node)
{
	TableScanDesc scanDesc;
	PlanState *sub_planstate = (PlanState *)linitial(node->custom_ps);

	ExecEndNode(sub_planstate);

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
rescan_continuous_view_scan(CustomScanState *node)
{
	TableScanDesc scan;

	scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,	/* scan desc */
					 NULL); /* new scan keys */

	ExecScanReScan((ScanState *)node);
}

static Path *
create_cv_customscan_path(PlannerInfo *root,
						  RelOptInfo *rel,
						  RelOptInfo *input_rel,
						  Path *subpath,
						  Relids required_outer,
						  PathTarget *reltarget)
{
	int idx;
	List *custom_scan_tlist = NIL;
	Expr **mock_args;
	ListCell *lc;
	CustomPath *path;
	PrivateCustomData *private;

	/* Create a custom scan path and set the method */
	path = (CustomPath *)newNode(sizeof(CustomPath), T_CustomPath);

	/* Set common path fields */
	path->path.pathtype = T_CustomScan;
	path->path.parent = rel;
	path->path.pathtarget = reltarget;
	path->path.param_info =
		get_baserel_parampathinfo(root, rel, required_outer);

	/* Don't consider parallel for now */
	path->path.parallel_aware = false;
	path->path.parallel_safe = false;
	path->path.parallel_workers = 0;
	path->path.locus = subpath->locus;

	path->path.motionHazard = false;
	path->path.rescannable = true;
	path->path.sameslice_relids = rel->relids;

	/*
	 * Setup the custom scan method, this is mainly to set the callback to
	 * convert a custom path to custom plan, in that callback you can setup
	 * your callback for execution, to be more accurate, for initialize of
	 * CustomScanState.
	 */
	path->methods = &continuous_view_scan_path_methods;
	path->custom_paths = list_make1(subpath);
	path->path.pathkeys = subpath->pathkeys;

	custom_scan_tlist = make_tlist_from_pathtarget(subpath->pathtarget);

	mock_args = (Expr **)
		palloc0(sizeof(Expr *) * list_length(build_physical_tlist(root, input_rel)));

	foreach_with_count(lc, reltarget->exprs, idx)
	{
		Expr *expr = (Expr *)lfirst(lc);
		List *vars;
		Var *var;

		vars = pull_var_clause((Node *)expr, PVC_RECURSE_AGGREGATES);

		Assert(list_length(vars) == 1);
		var = linitial_node(Var, vars);

		mock_args[var->varattno - 1] = expr;
	}

	foreach (lc, custom_scan_tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var *var = (Var *)tle->expr;

		if (mock_args[var->varattno - 1])
			tle->expr = mock_args[var->varattno - 1];
	}

	private = palloc0(sizeof(PrivateCustomData));
	private->custom_scan_tlist = custom_scan_tlist;
	private->scanrelid = input_rel->relid;

	path->custom_private = list_make1(private);

	path->path.total_cost = subpath->total_cost;
	path->path.startup_cost = subpath->startup_cost;
	path->path.rows = subpath->rows;

	return (Path *)path;
}

static void
create_continuous_view_scan_paths(PlannerInfo *root,
								  RelOptInfo *rel,
								  RelOptInfo *input_rel,
								  Relids required_outer,
								  PathTarget *reltarget)
{
	Path *cheapest_total_path;
	ListCell *lc;

	cheapest_total_path = input_rel->cheapest_total_path;

	foreach (lc, input_rel->pathlist)
	{
		Path *subpath = (Path *)lfirst(lc);

		if (subpath == cheapest_total_path || subpath->pathkeys)
			add_path(rel, create_cv_customscan_path(root,
													rel,
													input_rel,
													subpath,
													required_outer,
													reltarget));
	}
}

/*
 * Hook the the creation of upper paths on the auxiliay table of continuous view.
 *
 * 1) adjust the cost of input_rel, increase the cost of all paths in the base
 * rel to make (simple agg + scan) be a loser and (final agg + custom scan) to
 * be a winner.
 * 2) hook the partial aggregate, squeeze the (partial agg + scan) nodes to a
 * single custom scan on the auxiliary table.
 */
static void
cv_create_upper_paths_hook(PlannerInfo *root,
						   UpperRelationKind stage,
						   RelOptInfo *input_rel,
						   RelOptInfo *output_rel,
						   void *extra)
{
	RangeTblEntry *rte;
	Path *path;
	Relation rel;
	ListCell *lc;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (stage != UPPERREL_PARTIAL_GROUP_AGG)
		return;

	/* We can only support agg pushdown on single base rel */
	if ((input_rel->reloptkind != RELOPT_BASEREL &&
		 input_rel->reloptkind != RELOPT_OTHER_MEMBER_REL) ||
		input_rel->rtekind != RTE_RELATION)
		return;

	/* Return immediately if it's not aggheap storage */
	rte = planner_rt_fetch(input_rel->relid, root);

	rel = RelationIdGetRelation(rte->relid);

	if (!external_amcheck(rel->rd_rel->relam, CVAMNAME))
	{
		RelationClose(rel);
		return;
	}

	RelationClose(rel);

	/* Cleanup already planned partial group agg path */
	output_rel->partial_pathlist = NIL;
	output_rel->pathlist = NIL;

	/* Generate continuous view custom scan paths */
	create_continuous_view_scan_paths(root,
									  output_rel,
									  input_rel,
									  output_rel->lateral_relids,
									  output_rel->reltarget);
	/*
	 * Increase the cost of the paths in the base rel, so (simple Agg + Scan)
	 * type plan will have a very high cost to be not chosen
	 */
	foreach (lc, input_rel->pathlist)
	{
		path = (Path *)lfirst(lc);
		path->total_cost *= 10000;
		path->startup_cost *= 10000;
		path->rows *= 10000;
	}
}

static void
cv_set_rel_pathlist_hook(PlannerInfo *root,
						 RelOptInfo *input_rel,
						 Index rti,
						 RangeTblEntry *rte)
{
	Relation rel;

	if (prev_set_rel_pathlist_hook != NULL)
		(*prev_set_rel_pathlist_hook)(root, input_rel, rti, rte);

	if (!IS_SIMPLE_REL(input_rel) || input_rel->rtekind != RTE_RELATION)
		return;

	/* Quit immediately if it's not a cv sortheap like storage */
	rel = RelationIdGetRelation(rte->relid);

	if (!external_amcheck(rel->rd_rel->relam, CVAMNAME))
	{
		RelationClose(rel);
		return;
	}

	RelationClose(rel);

	/*
	 * Cv sortheap stored the partially aggregated tuples, so we need to force
	 * the planner to create a partial aggregate path, then we can use the
	 * cv_create_upper_paths_hook to replace the plain scan to a customscan.
	 */
	root->force_partial_aggregate_path = true;
}

void init_cv_create_upper_paths_hook(void)
{
	prev_create_upper_paths_hook = create_upper_paths_hook;
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;

	set_rel_pathlist_hook = cv_set_rel_pathlist_hook;
	create_upper_paths_hook = cv_create_upper_paths_hook;
}

void fini_cv_create_upper_paths_hook(void)
{
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
	create_upper_paths_hook = prev_create_upper_paths_hook;
}

void register_continuous_view_methods(void)
{
	RegisterCustomScanMethods(&continuous_view_scan_plan_methods);
}

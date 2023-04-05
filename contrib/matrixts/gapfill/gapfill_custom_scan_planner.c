/*-------------------------------------------------------------------------
 *
 * gapfill/gapfill_custom_scan.c
 *
 *	  Gapfill scan can create new tuples to fill gaps of time bucket.
 *	  It will also call interpolate(or locf) function.
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "gapfill_custom_scan.h"

static create_upper_paths_hook_type prev_create_upper_paths_hook;
/* Method to convert a gapfill custom scan path to plan */
static CustomPathMethods gapfill_scan_path_methods = {
	.CustomName = "GapfillScan",
	.PlanCustomPath = create_gapfill_cscan_plan,
};

/* Method to initialize a mars aggscan plan node */
static CustomScanMethods gapfill_cscan_plan_methods = {
	.CustomName = "GapfillScan",
	.CreateCustomScanState = create_gapfill_cscan_state,
};

/* Method to define the calllbacks for execution */
static CustomExecMethods gapfill_cscan_exec_methods = {
	.CustomName = "GapfillScan",
	.BeginCustomScan = begin_gapfill_cscan,
	.ExecCustomScan = exec_gapfill_cscan,
	.EndCustomScan = end_gapfill_cscan,
	.ReScanCustomScan = rescan_gapfill_cscan,
	/* We need to fill all the fileds to compile with gcc-7 or earlier */
	.MarkPosCustomScan = NULL,
	.RestrPosCustomScan = NULL,
	.EstimateDSMCustomScan = estimate_dsm_gapfill_cscan,
	.InitializeDSMCustomScan = initialize_dsm_gapfill_cscan,
	.ReInitializeDSMCustomScan = reinitialize_dsm_gapfill_cscan,
	.InitializeWorkerCustomScan = initialize_worker_gapfill_cscan,
	.ShutdownCustomScan = NULL,
	.ExplainCustomScan = NULL,
};

Size estimate_dsm_gapfill_cscan(CustomScanState *node, ParallelContext *pcxt)
{
	elog(ERROR, "Gapfill doesn't support parallel mode");
}
void initialize_dsm_gapfill_cscan(CustomScanState *node, ParallelContext *pcxt,
								  void *coordinate)
{
	elog(ERROR, "Gapfill doesn't support parallel mode");
}

void reinitialize_dsm_gapfill_cscan(CustomScanState *node, ParallelContext *pcxt,
									void *coordinate)
{
	elog(ERROR, "Gapfill doesn't support parallel mode");
}

void initialize_worker_gapfill_cscan(CustomScanState *node, shm_toc *toc,
									 void *coordinate)
{
	elog(ERROR, "Gapfill doesn't support parallel mode");
}

Node *
create_gapfill_cscan_state(CustomScan *cscan)
{
	GapfillScanState *state;

	state = (GapfillScanState *)newNode(sizeof(GapfillScanState),
										T_CustomScanState);
	state->ccstate.methods = &gapfill_cscan_exec_methods;
	return (Node *)state;
}

/*
 * This function tells how to convert a GapfillScanPath(path) to
 * GapfillScanState(plan).
 * The key is CustomScanMethods which defines the function to create
 * a GapfillScanState.
 */
Plan *
create_gapfill_cscan_plan(PlannerInfo *root, RelOptInfo *rel,
						  CustomPath *best_path, List *tlist,
						  List *scan_clauses, List *custom_plans)
{
	CustomScan *cscan;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	cscan = makeNode(CustomScan);
	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = scan_clauses;
	cscan->flags = best_path->flags;
	cscan->custom_scan_tlist = tlist;
	cscan->custom_plans = custom_plans;
	/* Tell how to create a MarsSortScanState */
	cscan->methods = &gapfill_cscan_plan_methods;
	cscan->custom_private = list_make2(linitial(best_path->custom_private),
									   root->parse->groupClause);

	return &cscan->scan.plan;
}

static Path *
create_gapfill_cscan_path(PlannerInfo *root, RelOptInfo *parent, Path *subpath,
						  FuncExpr **func)
{
	CustomPath *path;

	/* Create a custom scan path and set the method */
	path = (CustomPath *)newNode(sizeof(struct CustomPath), T_CustomPath);

	/* Set common path fields */
	path->path.pathtype = T_CustomScan;
	path->path.parent = parent;
	path->path.pathtarget = parent->reltarget;
	path->path.param_info = NULL;
	/* Dont't consider parallel for now */
	path->path.parallel_aware = false;
	path->path.parallel_safe = false;
	path->path.parallel_workers = 0;
	/* The GapfillScan has the same locus with input base rel */
	path->path.locus = subpath->locus;
	path->path.motionHazard = false;
	path->path.rescannable = true;
	path->path.pathkeys = root->group_pathkeys;

	/*
	 * setup the custom scan method, this is mainly to
	 * set the callback to convert a custom path to custom
	 * plan, in that callback you can setup your callback
	 * for execution, to be more accurate, for initialize
	 * of marssortscanstate.
	 */
	path->flags = 0;
	path->methods = &gapfill_scan_path_methods;
	path->custom_paths = lcons(subpath, NULL);

	/* Setup the cost of gapfill*/
	path->path.rows = subpath->rows;
	path->path.startup_cost = subpath->startup_cost;
	path->path.total_cost = subpath->total_cost;
	path->custom_private = list_make1(*func);
	return (Path *)path;
}

static bool
if_has_interpolate(FuncExpr *func_expr)
{
	char *func_name;

	func_name = get_func_name(func_expr->funcid);
	return pg_strcasecmp(func_name, MATRIXTS_GAPFILL_INTERPOLATE_UDF_NAME) == 0 ||
		   pg_strcasecmp(func_name, MATRIXTS_GAPFILL_LOCF_UDF_NAME) == 0;
}

static bool
interpolate_expression_walker(Node *node, GapfillUDFCount *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr && if_has_interpolate(castNode(FuncExpr, node))))
	{
		context->deep_interpoate_count++;
		return false;
	}

	return expression_tree_walker(node, interpolate_expression_walker, context);
}

static bool
gapfill_func_check(List *target_list, FuncExpr **func)
{
	ListCell *cell;
	TargetEntry *entry;
	FuncExpr *funcExpr;
	char *func_name;
	GapfillUDFCount context;

	context.gapfill_count = 0;
	context.deep_interpoate_count = 0;

	foreach (cell, target_list)
	{
		entry = (TargetEntry *)lfirst(cell);
		if (IsA(entry->expr, FuncExpr))
		{
			funcExpr = (FuncExpr *)entry->expr;
			func_name = get_func_name(funcExpr->funcid);
			if (func_name != NULL &&
				pg_strcasecmp(func_name, MATRIXTS_GAPFILL_UDF_NAME) == 0)
			{
				*func = funcExpr;
				context.gapfill_count++;
			}
		}
		else
			interpolate_expression_walker((Node *)entry->expr, &context);
	}

	if (context.gapfill_count > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("do not support multiple time_bucket_gapfill() function")));
	if (context.deep_interpoate_count > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interpolate/locf must be toplevel function")));

	if (context.gapfill_count == 1)
		return true;

	return false;
}

static bool
gapfill_cscan_precheck(PlannerInfo *root, FuncExpr **func)
{
	Query *parse;
	List *target_list;

	parse = root->parse;
	target_list = parse->targetList;
	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return false;

	if (!gapfill_func_check(target_list, func))
		return false;

	return true;
}

void gapfill_create_upper_paths(PlannerInfo *root, UpperRelationKind stage,
								RelOptInfo *input_rel, RelOptInfo *output_rel,
								void *extra)
{
	ListCell *cell;
	FuncExpr *func;
	List *pathlist_copy;
	Path *path;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (stage != UPPERREL_GROUP_AGG)
		return;

	func = (FuncExpr *)newNode(sizeof(FuncExpr), T_FuncExpr);
	/* Check 1: if query is 'SELECT ' */
	if (!gapfill_cscan_precheck(root, &func))
		return;

	/*
	 * Concat "pathlist" and "partial_pathlist" first.
	 * Then append "Gapfill Path" to them.
	 * New paths will be set to "output_rel->pathlist".
	 */
	pathlist_copy = list_concat(output_rel->pathlist, output_rel->partial_pathlist);
	output_rel->pathlist = NIL;
	output_rel->partial_pathlist = NIL;
	foreach (cell, pathlist_copy)
	{
		CdbPathLocus locus;

		path = (Path *)lfirst(cell);

		if (!CdbPathLocus_IsBottleneck(path->locus))
		{
			/* add gather motion */
			CdbPathLocus_MakeSingleQE(&locus, path->locus.numsegments);

			path =
				apply_projection_to_path(root,
										 output_rel,
										 path,
										 root->upper_targets[UPPERREL_FINAL]);

			path = cdbpath_create_motion_path(root,
											  path,
											  path->pathkeys,
											  false,
											  locus);
		}
		if (!pathkeys_contained_in(root->group_pathkeys, path->pathkeys))
		{
			path = (Path *)create_sort_path(root,
											output_rel,
											path,
											root->group_pathkeys,
											-1.0);
		}
		path = apply_projection_to_path(root,
										output_rel,
										path,
										root->upper_targets[UPPERREL_FINAL]);

		path = create_gapfill_cscan_path(root, output_rel, path, &func);
		add_path(output_rel, path);
	}
	return;
}

void gapfill_register_customscan_methods(void)
{
	RegisterCustomScanMethods(&gapfill_cscan_plan_methods);

	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = gapfill_create_upper_paths;
}

void gapfill_unregister_customscan_methods(void)
{
	create_upper_paths_hook = prev_create_upper_paths_hook;
}

/*-------------------------------------------------------------------------
 *
 * marscustomaggscan.cc
 *	  The MARS Agg Scan is a plan node that execute aggregate
 *	  and scan operations within one single node. The difference
 *	  to pure node is that it may get the pre-aggregate result
 *	  from Mars footers directly.
 *
 *	  The mars aggscan is implemented by PG custom scan framework.
 *	  This file contains the implementation of all kinds of methods
 *	  that a custom scan needs.
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "wrapper.hpp"

#include "FileManager.h"
#include "Footer.h"
#include "ParquetWriter.h"
#include "ParquetReader.h"
#include "Tle.h"
#include "marsam.h"
#include "marscustomscan.h"
#include "guc.h"

#include <memory>

/*
 * The overall execution logical of a custom mars agg scan can be
 * explained by a plan like:
 *
 * Custom Scan (MarsAggScan)
 *    Internal Plan :
 *    ->  Footer Scan (Block Header Scan)
 *    ->  Partial GroupAggregate
 *       ->  Content Scan
 * OR
 * Custom Scan (MarsAggScan)
 *    Internal Plan :
 *    Finalize GroupAggregate
 *      ->  Footer Scan
 *      ->  Partial GroupAggregate
 *         ->  Content Scan (Block Scan)
 *
 * Whether adding Finalize GroupAggregate depends on which upper stage
 * we are planning, if it's UPPERREL_PARTIAL_GROUP_AGG, it's unnecessary.
 */

extern "C"
{

	static create_upper_paths_hook_type prev_create_upper_paths_hook;

	extern PathKey *make_pathkey_from_sortop(PlannerInfo *root,
											 Expr *expr,
											 Relids nullable_relids,
											 Oid ordering_op,
											 bool nulls_first,
											 Index sortref,
											 bool create_it);

	typedef enum MarsAggScanPhase
	{
		READING_NORMAL = 1, /* reading from subpath or mars content */
		READING_FOOTER,		/* reading from mars footer */
		READING_TUPLESORT	/* reading from TupleSortState */
	} MarsAggScanPhase;

	typedef struct MarsAggScanPath
	{
		CustomPath cpath;
	} MarsAggScanPath;

	typedef struct MarsAggState
	{
		AggState *aggstate;
		Tuplesortstate *tuple_sort;
		MarsAggScanPhase phase;
	} MarsAggState;

	typedef struct MarsAggScanState
	{
		CustomScanState csstate;
		PlanState *child_ps;
		/* more details about child planstate */
		MarsAggState *final_aggstate;
		MarsAggState *partial_aggstate;
		ScanState *scanstate;
	} MarsAggScanState;

	static bool mars_aggscan_precheck(PlannerInfo *root,
									  UpperRelationKind stage,
									  RelOptInfo *input_rel,
									  RelOptInfo *output_rel,
									  void *extra);
	static bool mars_aggscan_groupkey_check(PlannerInfo *root,
											RelOptInfo *input_rel,
											RelOptInfo *output_rel,
											Path **subpath);
	static bool mars_aggscan_agg_check_internal(Node *node, bool *all_supported);
	static bool mars_aggscan_agg_check(List *targetlist);

	static bool explain_mars_aggscan_internal(PlanState *planstate,
											  MarsExplainContext *context);
	static bool fill_mars_aggscanstate(PlanState *state, MarsAggScanState *mass);
	static bool TransformPGTargetEntry(Node *node, mars::TleContext *context);

	static TableScanDesc mars_aggscan_begin(AggState *aggstate,
											ScanState *scanstate,
											Snapshot snapshot);

	/* Callbacks for execution of mars agg scan */
	static void begin_mars_aggscan(CustomScanState *node,
								   EState *estate, int eflags);
	static TupleTableSlot *exec_mars_aggscan(CustomScanState *node);
	static void end_mars_aggscan(CustomScanState *node);
	static void rescan_mars_aggscan(CustomScanState *node);
	static Size estimate_dsm_mars_aggscan(CustomScanState *node,
										  ParallelContext *pcxt);
	static void initialize_dsm_mars_aggscan(CustomScanState *node,
											ParallelContext *pcxt,
											void *coordinate);
	static void reinitialize_dsm_mars_aggscan(CustomScanState *node,
											  ParallelContext *pcxt,
											  void *coordinate);
	static void initialize_worker_mars_aggscan(CustomScanState *node,
											   shm_toc *toc,
											   void *coordinate);
#if 0
static void	shutdown_mars_aggscan(CustomScanState *node);
#endif
	static void explain_mars_aggscan(CustomScanState *node,
									 List *ancestors,
									 ExplainState *es);
	static Node *create_mars_aggscan_state(CustomScan *cscan);

	/* Callbacks for planner */
	static Plan *create_mars_aggscan_plan(PlannerInfo *root,
										  RelOptInfo *rel,
										  CustomPath *best_path,
										  List *tlist,
										  List *clauses,
										  List *custom_plans);

	static TupleTableSlot *exec_partial_mars_aggscan(CustomScanState *node);
	static TupleTableSlot *exec_final_mars_aggscan(CustomScanState *node);
	static Path *create_mars_agg_path(PlannerInfo *root,
									  RelOptInfo *parent,
									  Path *subpath,
									  UpperRelationKind stage,
									  AggSplit aggsplit,
									  List *havingQual,
									  GroupPathExtraData *extra);

	/* Method to convert a mars aggscan path to plan */
	static CustomPathMethods mars_aggscan_path_methods =
		{
			.CustomName = "MarsAggScan",
			.PlanCustomPath = create_mars_aggscan_plan,
	};

	/* Method to initialize a mars aggscan plan node */
	static CustomScanMethods mars_aggscan_plan_methods =
		{
			.CustomName = "MarsAggScan",
			.CreateCustomScanState = create_mars_aggscan_state,
	};

	/* Method to define the callbacks for execution */
	static CustomExecMethods mars_agg_scan_exec_methods =
		{
			.CustomName = "Mars Agg Scan",
			.BeginCustomScan = begin_mars_aggscan,
			.ExecCustomScan = exec_mars_aggscan,
			.EndCustomScan = end_mars_aggscan,
			.ReScanCustomScan = rescan_mars_aggscan,
			/* We need to fill all the fields to compile with gcc-7 or earlier */
			.MarkPosCustomScan = NULL,
			.RestrPosCustomScan = NULL,
			.EstimateDSMCustomScan = estimate_dsm_mars_aggscan,
			.InitializeDSMCustomScan = initialize_dsm_mars_aggscan,
			.ReInitializeDSMCustomScan = reinitialize_dsm_mars_aggscan,
			.InitializeWorkerCustomScan = initialize_worker_mars_aggscan,
#if 0
	.ShutdownCustomScan = shutdown_mars_aggscan,
#else
			.ShutdownCustomScan = NULL,
#endif
			.ExplainCustomScan = explain_mars_aggscan,
	};

	static Size
	estimate_dsm_mars_aggscan(CustomScanState *node,
							  ParallelContext *pcxt)
	{
		elog(ERROR, "Mars doesn't support parallel mode");
	}

	static void
	initialize_dsm_mars_aggscan(CustomScanState *node,
								ParallelContext *pcxt,
								void *coordinate)
	{
		elog(ERROR, "Mars doesn't support parallel mode");
	}

	static void
	reinitialize_dsm_mars_aggscan(CustomScanState *node,
								  ParallelContext *pcxt,
								  void *coordinate)
	{
		elog(ERROR, "Mars doesn't support parallel mode");
	}

	static void
	initialize_worker_mars_aggscan(CustomScanState *node,
								   shm_toc *toc,
								   void *coordinate)
	{
		elog(ERROR, "Mars doesn't support parallel mode");
	}

#if 0
static void
shutdown_mars_aggscan (CustomScanState *node)
{
    //MarsAggScanState	*as = (MarsAggScanState *) node;

	elog(ERROR, "Mars doesn't support parallel mode");
}
#endif

	static bool
	explain_mars_aggscan_internal(PlanState *planstate, MarsExplainContext *context)
	{
		ExplainState *es = context->es;
		List *l_resno = NULL;
		ListCell *lc;
		const char *label;
		int current_indent = 0;

		if (planstate == NULL)
			return false;

		if (es->indent)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			current_indent = es->indent * 2;
			appendStringInfoString(es->str, ">  ");
			es->indent += 2;
		}

		if (IsA(planstate, AggState))
		{
			AggState *aggstate = (AggState *)planstate;
			Agg *agg = (Agg *)aggstate->ss.ps.plan;

			if (aggstate->aggsplit == AGGSPLIT_FINAL_DESERIAL)
				appendStringInfoString(es->str, "Finalize GroupAggregate");
			else if (aggstate->aggsplit == AGGSPLIT_INITIAL_SERIAL)
			{
				/* print footerscan node */
				appendStringInfoString(es->str, "Block Header Scan");
				appendStringInfoChar(es->str, '\n');
				/* show targetlist */
				label = "Output";
				foreach (lc, planstate->plan->targetlist)
				{
					TargetEntry *tle = (TargetEntry *)lfirst(lc);
					l_resno = lappend_int(l_resno, tle->resno);
				}
				explain_targetlist_exprs(planstate, l_resno, label,
										 context->ancestors,
										 context->es);
				l_resno = NULL;

				/* print Partial agg node */
				appendStringInfoSpaces(es->str, current_indent);
				appendStringInfoString(es->str, ">  Partial GroupAggregate");
			}

			appendStringInfoChar(es->str, '\n');
			/* show group keys */
			label = "Group Key";
			for (int i = 0; i < agg->numCols; i++)
				l_resno = lappend_int(l_resno, agg->grpColIdx[i]);

			explain_targetlist_exprs(planstate,
									 l_resno, label,
									 context->ancestors,
									 context->es);

			l_resno = NULL;
		}
		else if (IsA(planstate, SortState))
		{
			SortState *sortstate = (SortState *)planstate;
			Sort *sort = (Sort *)sortstate->ss.ps.plan;

			appendStringInfoString(es->str, "Sort");

			appendStringInfoChar(es->str, '\n');
			/* show sort keys */
			label = "Sort Key";
			for (int i = 0; i < sort->numCols; i++)
				l_resno = lappend_int(l_resno, sort->sortColIdx[i]);

			explain_targetlist_exprs(planstate, l_resno, label,
									 context->ancestors,
									 context->es);

			l_resno = NULL;
		}
		else if (IsA(planstate, IndexScanState) ||
				 IsA(planstate, SeqScanState) ||
				 IsA(planstate, IndexOnlyScanState) ||
				 IsA(planstate, CustomScanState))
		{
			appendStringInfoString(es->str, "Block Scan");
			appendStringInfoChar(es->str, '\n');

			if (IsA(planstate, CustomScanState))
			{
				CustomScan *plan = (CustomScan *)planstate->plan;
				Sort *sort = (Sort *)linitial(plan->custom_plans);

				/* it is a seqscan in the unordered case */
				if (IsA(sort, Sort))
				{
					/* show sort keys */
					label = "Sort Key";
					for (int i = 0; i < sort->numCols; i++)
						l_resno = lappend_int(l_resno, sort->sortColIdx[i]);

					explain_targetlist_exprs(planstate, l_resno, label,
											 context->ancestors,
											 context->es);
					l_resno = NULL;
				}
			}
		}

		/* show filter */
		label = "Filter";
		explain_qual_exprs(planstate->plan->qual,
						   label,
						   planstate,
						   context->ancestors,
						   false,
						   context->es);

		/* show targetlist */
		label = "Output";
		foreach (lc, planstate->plan->targetlist)
		{
			TargetEntry *tle = (TargetEntry *)lfirst(lc);
			l_resno = lappend_int(l_resno, tle->resno);
		}

		explain_targetlist_exprs(planstate, l_resno, label,
								 context->ancestors,
								 context->es);

		return planstate_tree_walker(planstate,
									 (bool (*)())explain_mars_aggscan_internal,
									 context);
	}

	static void
	explain_mars_aggscan(CustomScanState *node,
						 List *ancestors,
						 ExplainState *es)
	{
		MarsAggScanState *mass = (MarsAggScanState *)node;
		MarsExplainContext context;

		/* Only show customscan details in verbose mode */
		if (!es->verbose)
			return;

		context.es = es;
		context.ancestors = ancestors;

		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfoString(es->str, "Internal Plan :");
		appendStringInfoChar(es->str, '\n');

		explain_mars_aggscan_internal(mass->child_ps, &context);
	}

	static Path *
	create_mars_agg_path(PlannerInfo *root,
						 RelOptInfo *parent,
						 Path *subpath,
						 UpperRelationKind stage,
						 AggSplit aggsplit,
						 List *havingQual,
						 GroupPathExtraData *extra)
	{
		PathTarget *reltarget;
		AggClauseCosts *agg_costs;

		Assert(extra->partial_costs_set);

		agg_costs = (aggsplit == AGGSPLIT_INITIAL_SERIAL ? &extra->agg_partial_costs : &extra->agg_final_costs);

		if (stage == UPPERREL_GROUP_AGG && aggsplit == AGGSPLIT_INITIAL_SERIAL)
			reltarget = make_partial_grouping_target(root, parent->reltarget, NULL);
		else
			reltarget = parent->reltarget;

		subpath = (Path *)create_agg_path(root,
										  parent,
										  subpath,
										  reltarget,
										  root->parse->groupClause ? AGG_SORTED : AGG_PLAIN,
										  aggsplit /* FIXME */,
										  false,
										  root->parse->groupClause,
										  havingQual,
										  agg_costs /* agg_costs */,
										  1 /* dNumGroups */);

		return subpath;
	}

	/*
	 * Quick precheck whether an agg can be pushed down
	 */
	static bool
	mars_aggscan_precheck(PlannerInfo *root,
						  UpperRelationKind stage,
						  RelOptInfo *input_rel,
						  RelOptInfo *output_rel,
						  void *extra)
	{
		List *group_tles;
		RangeTblEntry *rte;

		if (!mars_enable_customscan)
			return false;

		if (stage != UPPERREL_GROUP_AGG &&
			stage != UPPERREL_PARTIAL_GROUP_AGG)
			return false;

		/* We can only support agg pushdown on single base rel */
		if ((input_rel->reloptkind != RELOPT_BASEREL &&
			 input_rel->reloptkind != RELOPT_OTHER_MEMBER_REL) ||
			input_rel->rtekind != RTE_RELATION)
			return false;

		if (root->parse->groupingSets ||
			root->parse->distinctClause ||
			root->parse->windowClause ||
			root->parse->scatterClause)
			return false;

		/* Return immediately if is not a Mars */
		rte = planner_rt_fetch(input_rel->relid, root);
		if (!IsMarsAM(rte->relid))
			return false;

		/*
		 * For one-stage aggregate, we only consider the mars aggscan when the
		 * group key is co-located with data distribution.
		 */
		if (stage == UPPERREL_GROUP_AGG)
		{
			bool need_redistribute;

			group_tles = get_common_group_tles(input_rel->cheapest_total_path->pathtarget,
											   root->parse->groupClause,
											   NULL);

			choose_grouping_locus(root,
								  input_rel->cheapest_total_path,
								  group_tles,
								  &need_redistribute);

			if (need_redistribute)
				return false;
		}

		return true;
	}

	/*
	 * Return true iff:
	 * 1. input_rel contains the mars_customsortscan path we need, we don't
	 * consider partial_pathlist for now.
	 * 2. the group keys contain in mars table order key (we also allow time_bucket
	 * to be the last group key).
	 */
	static bool
	mars_aggscan_groupkey_check(PlannerInfo *root,
								RelOptInfo *input_rel,
								RelOptInfo *output_rel,
								Path **subpath)
	{
		ListCell *lc;

		foreach (lc, input_rel->pathlist)
		{
			Path *path = (Path *)lfirst(lc);
			bool is_sorted;

			if (!IsMarsCustomSortScan(path))
				continue;

			if (root->mx_homo_group_pathkeys)
				is_sorted = pathkeys_contained_in(root->mx_homo_group_pathkeys,
												  path->pathkeys);
			else
				is_sorted = pathkeys_contained_in(root->group_pathkeys,
												  path->pathkeys);

			if (is_sorted)
			{
				*subpath = path;
				return true;
			}
		}

		return false;
	}

	/*
	 * Walker to check whether all aggregate is pre-aggregated
	 */
	static bool
	mars_aggscan_agg_check_internal(Node *node, bool *all_supported)
	{
		if (node == NULL)
			return false;

		if (IsA(node, Aggref))
		{
			Aggref *agg = (Aggref *)node;
			ListCell *lc;
			char *func_name;

			/*
			 * first() and last() have 2 args.
			 * Aggregates with DISTINCT / ORDER BY / FILTER are not supported now.
			 */
			if (list_length(agg->args) > 2 || agg->aggdistinct != nullptr || agg->aggfilter != nullptr || agg->aggorder != nullptr)
			{
				*all_supported = false;
				return false;
			}

			func_name = get_func_name(agg->aggfnoid);

			if (mars::AggregateTLE::GetAggType(func_name) ==
				mars::AggType::UNKNOWN)
			{
				*all_supported = false;
				return false;
			}

			/* Only support simple Var as args */
			foreach (lc, agg->args)
			{
				Node *expr = (Node *)lfirst(lc);

				if (!IsA(expr, TargetEntry))
				{
					*all_supported = false;
					return false;
				}
				TargetEntry *entry = (TargetEntry *)expr;
				bool is_cnt_const = (strcmp(func_name, "count") == 0) &&
									IsA(entry->expr, Const) &&
									!castNode(Const, entry->expr)->constisnull;
				if (!IsA(entry->expr, Var) && !is_cnt_const)
				{
					*all_supported = false;
					return false;
				}
			}
		}

		return expression_tree_walker(node,
									  (bool (*)())mars_aggscan_agg_check_internal,
									  (void *)all_supported);
	}

	static bool
	mars_aggscan_agg_check(List *targetlist)
	{
		bool support = true;

		mars_aggscan_agg_check_internal((Node *)targetlist, &support);

		return support;
	}

	static Path *
	create_mars_aggscan_path(PlannerInfo *root,
							 UpperRelationKind stage,
							 RelOptInfo *parent,
							 Path *subpath,
							 bool need_resort,
							 GroupPathExtraData *extra)
	{
		MarsAggScanPath *path;

		/* Create a custom scan path and set the method */
		path = (MarsAggScanPath *)newNode(sizeof(MarsAggScanPath), T_CustomPath);
		path->cpath.path.sameslice_relids = subpath->sameslice_relids;

		subpath = create_mars_agg_path(root,
									   parent,
									   subpath,
									   stage,
									   AGGSPLIT_INITIAL_SERIAL,
									   NULL,
									   extra);

		if (need_resort)
			subpath = (Path *)create_sort_path(root,
											   parent,
											   subpath,
											   root->group_pathkeys,
											   -1.0);

		if (stage == UPPERREL_GROUP_AGG)
			subpath = create_mars_agg_path(root,
										   parent,
										   subpath,
										   stage,
										   AGGSPLIT_FINAL_DESERIAL,
										   (List *)extra->havingQual,
										   extra);

		/* Set common path fields */
		path->cpath.path.pathtype = T_CustomScan;
		path->cpath.path.parent = parent;
		path->cpath.path.pathtarget = parent->reltarget;
		path->cpath.path.param_info = NULL;
		/* Don't consider parallel for now*/
		path->cpath.path.parallel_aware = false;
		path->cpath.path.parallel_safe = false;
		path->cpath.path.parallel_workers = 0;
		/* The AggScan has the same locus with input base rel */
		path->cpath.path.locus = subpath->locus;
		path->cpath.path.motionHazard = false;
		path->cpath.path.rescannable = true;
		path->cpath.path.pathkeys = root->group_pathkeys;

		/*
		 * Setup the custom scan method, this is mainly to set the callback to
		 * convert a custom path to custom plan, in that callback you can setup
		 * your callback for execution, to be more accurate, for initialize of
		 * MarsAggScanState.
		 */
		path->cpath.flags = 0;
		path->cpath.methods = &mars_aggscan_path_methods;
		path->cpath.custom_paths = lcons(subpath, NULL);

		/* Setup the cost of mars_aggscan */
		path->cpath.path.rows = subpath->rows;
		path->cpath.path.startup_cost = subpath->startup_cost;
		/*
		 * FIXME: now we cannot tell the percentage of footers that we can read
		 * pre-aggregate result directly, so set a factor that is small enough to
		 * win the hash aggregate.
		 */
		path->cpath.path.total_cost = subpath->total_cost * 0.2;

		return (Path *)path;
	}

	/*
	 * Hook to create a mars aggscan path.
	 *
	 * FIXME: Now we only generate the custom aggscan when
	 * 1) is a plain agg.
	 * 2) the mars has order keys.
	 *
	 * But actually, we can even make a custom aggscan for not properly sorted
	 * tables, the key point is to avoid reading row group content directly,
	 * sometimes, with a proper granularity, row groups has very big chance that
	 * being locally unique on the group keys even the table are not specified a
	 * order key. We kept the ability to support such case with some additional
	 * resort paths.
	 */
	void
	mars_create_upper_paths(PlannerInfo *root,
							UpperRelationKind stage,
							RelOptInfo *input_rel,
							RelOptInfo *output_rel,
							void *extra)
	{
		Path *path;
		Path *subpath;
		bool need_resort = false;

		if (prev_create_upper_paths_hook != NULL)
			prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

		/* Check1: quick agg pushdown precheck */
		if (!mars_aggscan_precheck(root, stage,
								   input_rel, output_rel,
								   extra))
			return;

		/*
		 * Check 2:
		 * 1. input_rel contains the mars_customsortscan path we need, we don't
		 * consider partial_pathlist for now.
		 * 2. the group keys contain in mars table order key (we also allow
		 * time_bucket to be the last group key).
		 */
		subpath = input_rel->cheapest_total_path;
		if (root->parse->groupClause &&
			!mars_aggscan_groupkey_check(root,
										 input_rel,
										 output_rel,
										 &subpath))
			return;

		/* Check 3: all agg functions are supported by mars */
		if (!mars_aggscan_agg_check(output_rel->reltarget->exprs))
			return;

		/* Check 4: does not work on-top of an append */
		if (IsA(subpath, AppendPath))
			return;

		path = create_mars_aggscan_path(root, stage,
										output_rel,
										subpath,
										need_resort, /* FIXME */
										(GroupPathExtraData *)extra);

		add_path(output_rel, (Path *)path);
	}

	static Node *
	create_mars_aggscan_state(CustomScan *cscan)
	{
		MarsAggScanState *state;

		state = (MarsAggScanState *)newNode(sizeof(MarsAggScanState),
											T_CustomScanState);
		state->csstate.methods = &mars_agg_scan_exec_methods;

		return (Node *)state;
	}

	/*
	 * This function tells how to convert a MarsAggScanPath to MarsAggScan plan.
	 *
	 * The key is CustomScanMethods which defines the function to create a
	 * MarsAggScanState.
	 */
	static Plan *
	create_mars_aggscan_plan(PlannerInfo *root, RelOptInfo *rel,
							 CustomPath *best_path,
							 List *tlist,
							 List *clauses,
							 List *custom_plans)
	{
		CustomScan *cscan = makeNode(CustomScan);
		cscan->scan.plan.targetlist = tlist;
		cscan->flags = best_path->flags;
		cscan->custom_plans = custom_plans;
		cscan->custom_scan_tlist = tlist;

		/* Tell how to create a MarsAggScanState */
		cscan->methods = &mars_aggscan_plan_methods;

		return &cscan->scan.plan;
	}

	void
	mars_register_customaggscan_methods(void)
	{
		RegisterCustomScanMethods(&mars_aggscan_plan_methods);

		prev_create_upper_paths_hook = create_upper_paths_hook;
		create_upper_paths_hook = mars_create_upper_paths;
	}

	void
	mars_unregister_customaggscan_methods(void)
	{
		create_upper_paths_hook = prev_create_upper_paths_hook;
	}

} // extern "C"

static void
begin_mars_aggscan(CustomScanState *node,
				   EState *estate, int eflags)
{
	MarsAggScanState *mass = (MarsAggScanState *)node;
	CustomScan *cs = (CustomScan *)node->ss.ps.plan;
	Plan *child_plan = (Plan *)linitial(cs->custom_plans);

	/*
	 * We don't actually execute the child planstate tree as usual, we have our
	 * own logic to schedule each planstate node.  Init the child planstate
	 * anyway and fill each node into mars aggscanstate.
	 */
	mass->child_ps = ExecInitNode(child_plan, estate, eflags);
	fill_mars_aggscanstate(mass->child_ps, mass);
}

static TupleTableSlot *
exec_mars_aggscan(CustomScanState *node)
{
	MarsAggScanState *mass = (MarsAggScanState *)node;

	if (node->ss.ss_currentScanDesc == NULL)
		node->ss.ss_currentScanDesc =
			mars_aggscan_begin(mass->partial_aggstate->aggstate,
							   mass->scanstate,
							   node->ss.ps.state->es_snapshot);

	mass = (MarsAggScanState *)node;
	if (mass->final_aggstate)
		return exec_final_mars_aggscan(node);

	return exec_partial_mars_aggscan(node);
}

static TupleTableSlot *
exec_final_mars_aggscan(CustomScanState *node)
{
	MarsAggScanState *mass;
	Tuplesortstate *tuple_sort;
	TupleTableSlot *scan_slot;
	TupleTableSlot *result_slot;
	MarsAggState *final_;
	AggState *aggstate;

	mass = (MarsAggScanState *)node;
	final_ = mass->final_aggstate;
	aggstate = final_->aggstate;

	tuple_sort = final_->tuple_sort;

	while (!aggstate->agg_done)
	{
		scan_slot = NULL;

		if (final_->phase == READING_NORMAL)
		{
			scan_slot = exec_partial_mars_aggscan(node);
			if (scan_slot && tuple_sort)
			{
				tuplesort_puttupleslot(tuple_sort, scan_slot);
				continue;
			}
			else if (tuple_sort)
			{
				tuplesort_performsort(tuple_sort);
				final_->phase = READING_TUPLESORT;
				continue;
			}
		}

		if (final_->phase == READING_TUPLESORT)
		{
			scan_slot = outerPlanState(aggstate)->ps_ResultTupleSlot;
			if (!tuplesort_gettupleslot(tuple_sort, true, false,
										scan_slot, NULL))
				scan_slot = NULL;
		}

		/*
		 * If reaching a group boundary, output the aggregate result
		 */
		result_slot = sort_advance_aggregate(aggstate, scan_slot);

		if (result_slot)
			return result_slot;
	}

	return NULL;
}

static TupleTableSlot *
exec_partial_mars_aggscan(CustomScanState *node)
{
	MarsAggScanState *mass;
	ScanDirection direction;
	TupleTableSlot *scan_slot;
	TupleTableSlot *result_slot;
	Tuplesortstate *tuple_sort;
	bool plain_agg;
	MarsAggState *partial_;
	AggState *aggstate;
	ScanState *scanstate;

	mass = (MarsAggScanState *)node;
	partial_ = mass->partial_aggstate;
	aggstate = partial_->aggstate;
	tuple_sort = partial_->tuple_sort;
	scanstate = mass->scanstate;
	direction = aggstate->ss.ps.state->es_direction;
	plain_agg = aggstate->aggstrategy == AGG_PLAIN;

	auto scanWrapper = (mars::ScanWrapper *)node->ss.ss_currentScanDesc;
	auto preader = (mars::ParquetReader *)scanWrapper->preader_;

	while (!aggstate->agg_done)
	{
		scan_slot = NULL;

		if (partial_->phase == READING_FOOTER)
		{
			if (preader->NextBlock(direction))
			{
				/*
				 * Fast path, try to fetch agg result from footer directly
				 */
				scan_slot = aggstate->ss.ps.ps_ResultTupleSlot;
				if (preader->FooterRead(direction, scan_slot, plain_agg))
				{
					return scan_slot;
				}

				/* We need to read content */
				partial_->phase = READING_NORMAL;
				preader->LoadBlock();
			}
			/* EOF */
			else if (tuple_sort)
			{
				tuplesort_performsort(tuple_sort);
				partial_->phase = READING_TUPLESORT;
				continue;
			}
		}

		if (partial_->phase == READING_NORMAL)
		{
			scan_slot = scanstate->ss_ScanTupleSlot;

			if (preader->ReadNextInRowGroup(direction, scan_slot))
			{
				ExprContext *econtext = scanstate->ps.ps_ExprContext;
				econtext->ecxt_scantuple = scan_slot;

				// to filter rows because there may be some conditions
				// which are unable to be handled by mars
				ExprState *qual = scanstate->ps.qual;
				if (qual && !ExecQual(qual, econtext))
				{
					continue;
				}

				ProjectionInfo *projInfo = scanstate->ps.ps_ProjInfo;
				if (projInfo)
				{
					scan_slot = ExecProject(projInfo);
				}

				/*
				 * Put it into the tuple sort, we need all
				 * tuples before aggregation
				 */
				if (tuple_sort)
				{
					tuplesort_puttupleslot(tuple_sort, scan_slot);
					continue;
				}
			}
			else
			{
				/*
				 * All tuples in the current footer are read,
				 * advance to next footer.
				 */
				partial_->phase = READING_FOOTER;
				continue;
			}
		}

		if (partial_->phase == READING_TUPLESORT)
		{
			scan_slot = outerPlanState(aggstate)->ps_ResultTupleSlot;
			if (!tuplesort_gettupleslot(tuple_sort, true, false,
										scan_slot, NULL))
				scan_slot = NULL;
		}

		/* If reaching a group boundary, output the aggregate result */
		result_slot = sort_advance_aggregate(aggstate, scan_slot);

		if (result_slot)
			return result_slot;
	}

	if (mars_debug_customscan)
		elog(NOTICE, "FooterScan rate %f", preader->GetFooterScanRate());

	return NULL;
}

static void
end_mars_aggscan(CustomScanState *node)
{
	TableScanDesc scanDesc;
	MarsAggScanState *mass = (MarsAggScanState *)node;

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

	if (mass->child_ps)
		ExecEndNode(mass->child_ps);
}

static void
rescan_mars_aggscan(CustomScanState *node)
{
	elog(ERROR, "Mars doesn't support rescan");
}

/*
 * Initialize the TableScanDesc on the mars base rel and setup the FooterScan
 * targetlist and group keys.
 */
static TableScanDesc
mars_aggscan_begin(AggState *aggstate,
				   ScanState *scanstate,
				   Snapshot snapshot)
{
	ListCell *lc;
	Agg *agg = (Agg *)aggstate->ss.ps.plan;
	List *agg_targetlist = aggstate->ss.ps.plan->targetlist;
	auto tupdesc = RelationGetDescr(scanstate->ss_currentRelation);

	auto scan = table_beginscan(scanstate->ss_currentRelation,
								snapshot,
								0, NULL);

	auto scanWrapper = (mars::ScanWrapper *)scan;
	scanWrapper->preader_->BeginScan(scanstate);

	std::vector<std::shared_ptr<mars::TLE>> footerscan_tlist;
	footerscan_tlist.reserve(::list_length(agg_targetlist));

	foreach (lc, agg_targetlist)
	{
		TargetEntry *agg_tle = (TargetEntry *)lfirst(lc);
		mars::TleContext context(agg_tle->resno, scanWrapper->preader_,
								 aggstate, scanstate);
		std::shared_ptr<mars::TLE> tle = nullptr;

		TransformPGTargetEntry((Node *)agg_tle, &context);

		if (context.aggtype != mars::AggType::UNKNOWN)
			tle = mars::AggregateTLE::Make(context);
		else if (context.interval)
		{
			// this must be put after the call to TransformPGTargetEntry(),
			// otherwise the attnum is unknown.
			auto attr = &tupdesc->attrs[context.attnum - 1];

			tle = std::make_shared<mars::TimeBucketTLE>(attr,
														context.resno,
														context.attnum,
														context.interval);
		}
		else
		{
			// this must be put after the call to TransformPGTargetEntry(),
			// otherwise the attnum is unknown.
			auto attr = &tupdesc->attrs[context.attnum - 1];

			tle = std::make_shared<mars::AttributeTLE>(attr,
													   context.resno,
													   context.attnum);
		}

		footerscan_tlist.push_back(tle);
	}

	/* Setup GroupScanKeys which contains the index in footerscan_tlist */
	std::vector<std::shared_ptr<mars::TLE>> group_tlist;
	group_tlist.reserve(agg->numCols);

	for (int i = 0; i < agg->numCols; i++)
	{
		auto resno = agg->grpColIdx[i];
		for (unsigned int j = 0; j < footerscan_tlist.size(); j++)
		{
			auto tle = footerscan_tlist[j];
			if (tle->GetResno() == resno)
			{
				group_tlist.push_back(std::move(tle));
				break;
			}
		}
	}

	scanWrapper->preader_->SetFooterScanTlist(std::move(footerscan_tlist));
	scanWrapper->preader_->SetFooterScanGroupTlist(std::move(group_tlist));

	return scan;
}

static bool
TransformPGTargetEntry(Node *node, mars::TleContext *context)
{
	if (!node)
		return false;

	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *)node;
		char *funcname = get_func_name(aggref->aggfnoid);

		context->aggtype = mars::AggregateTLE::GetAggType(funcname);
		context->aggtypid = aggref->aggtype;
		context->aggref = aggref;

		/* For "avg()" or "sum()", the arg type specifies the real type */
		AssertImply(strcmp(funcname, "sum") == 0 ||
						strcmp(funcname, "avg") == 0,
					list_length(aggref->aggargtypes) > 0);
		if (aggref->aggargtypes)
			context->atttypid = linitial_oid(aggref->aggargtypes);
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr *func = (FuncExpr *)node;
		/* It must be a time_bucket */
		Const *c = (Const *)linitial(func->args);
		context->interval = DatumGetIntervalP(c->constvalue);
	}
	else if (IsA(node, Var))
	{
		Var *var = (Var *)node;

		if (IS_SPECIAL_VARNO(var->varno))
		{
			/*
			 * To find the real base scanning attribute, we need to follow into
			 * child (outer only) planstate.
			 */
			PlanState *saved_ps = context->ps;
			PlanState *child_ps = outerPlanState(saved_ps);
			List *child_tlist = child_ps->plan->targetlist;
			Node *child_expr = (Node *)list_nth(child_tlist,
												var->varattno - 1);
			/* recurse to child planstate */
			context->ps = child_ps;
			TransformPGTargetEntry(child_expr, context);
			/* restore the planstate */
			context->ps = saved_ps;
		}
		else
		{
			context->attnum = var->varattno;
			context->atttypid = var->vartype;
		}
	}

	return expression_tree_walker(node,
								  (bool (*)())TransformPGTargetEntry,
								  (void *)context);
}

static bool
fill_mars_aggscanstate(PlanState *planstate, MarsAggScanState *mass)
{
	if (planstate == NULL)
		return false;

	if (IsA(planstate, AggState))
	{
		AggState *aggstate = (AggState *)planstate;

		Assert(aggstate->aggsplit == AGGSPLIT_FINAL_DESERIAL ||
			   aggstate->aggsplit == AGGSPLIT_INITIAL_SERIAL);

		if (aggstate->aggsplit == AGGSPLIT_FINAL_DESERIAL)
		{
			Assert(!mass->final_aggstate);
			mass->final_aggstate =
				(MarsAggState *)palloc0(sizeof(MarsAggState));

			mass->final_aggstate->aggstate = aggstate;
			/* Set the initial phase to READING_NORMAL */
			mass->final_aggstate->phase = READING_NORMAL;
		}
		else if (aggstate->aggsplit == AGGSPLIT_INITIAL_SERIAL)
		{
			Assert(!mass->partial_aggstate);
			mass->partial_aggstate =
				(MarsAggState *)palloc0(sizeof(MarsAggState));

			mass->partial_aggstate->aggstate = aggstate;
			mass->partial_aggstate->phase = READING_FOOTER;
		}
	}
	else if (IsA(planstate, SortState))
	{
		Tuplesortstate *tuple_sort;
		SortState *sortstate = (SortState *)planstate;
		Sort *sortnode = (Sort *)sortstate->ss.ps.plan;
		TupleDesc tupDesc = ExecGetResultType((PlanState *)sortstate);

		tuple_sort = tuplesort_begin_heap(tupDesc,
										  sortnode->numCols,
										  sortnode->sortColIdx,
										  sortnode->sortOperators,
										  sortnode->collations,
										  sortnode->nullsFirst,
										  PlanStateOperatorMemKB((PlanState *)sortstate),
										  NULL, false);

		if (mass->partial_aggstate)
		{
			Assert(!mass->partial_aggstate->tuple_sort);
			mass->partial_aggstate->tuple_sort = tuple_sort;
		}
		else if (mass->final_aggstate)
		{
			Assert(!mass->final_aggstate->tuple_sort);
			mass->final_aggstate->tuple_sort = tuple_sort;
		}
	}
	else if (IsA(planstate, IndexScanState) ||
			 IsA(planstate, SeqScanState) ||
			 IsA(planstate, IndexOnlyScanState) ||
			 IsA(planstate, CustomScanState))
	{
		ScanState *scanstate = (ScanState *)planstate;
		mass->scanstate = scanstate;
	}
	else
		elog(ERROR, "unexpected internal customscan planstate node");

	return planstate_tree_walker(planstate,
								 (bool (*)())fill_mars_aggscanstate,
								 mass);
}

/*-------------------------------------------------------------------------
 *
 * marscustomsortscan.cc
 *	  The MARS Sort Scan is a normal sequential scan that has
 *	  pathkeys (order info).
 *
 *	  Mars can provide mulitple pathkeys, eg, table ordering
 *	  keys, time_bucket expressions on table ordering keys.
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
#include "marsam.h"
#include "marscustomscan.h"
#include "guc.h"

extern "C"
{

	static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;

	typedef struct MarsSortScanPath
	{
		CustomPath cpath;
	} MarsSortScanPath;

	typedef struct MarsSortScanState
	{
		CustomScanState csstate;

		ScanDirection direction;
		PlanState *child_ps;
	} MarsSortScanState;

	static Path *
	create_mars_sortscan_path(PlannerInfo *root,
							  RelOptInfo *rel,
							  Index parent_rti,
							  Path *subpath,
							  List *pathkeys,
							  List *orderlst);

	/* Callbacks for execution of mars agg scan */
	static void begin_mars_sortscan(CustomScanState *node,
									EState *estate, int eflags);
	static TupleTableSlot *exec_mars_sortscan(CustomScanState *node);
	static void end_mars_sortscan(CustomScanState *node);
	static void rescan_mars_sortscan(CustomScanState *node);
	static Size estimate_dsm_mars_sortscan(CustomScanState *node,
										   ParallelContext *pcxt);
	static void initialize_dsm_mars_sortscan(CustomScanState *node,
											 ParallelContext *pcxt,
											 void *coordinate);
	static void reinitialize_dsm_mars_sortscan(CustomScanState *node,
											   ParallelContext *pcxt,
											   void *coordinate);
	static void initialize_worker_mars_sortscan(CustomScanState *node,
												shm_toc *toc,
												void *coordinate);
#if 0
static void	shutdown_mars_sortscan(CustomScanState *node);
#endif
	static void explain_mars_sortscan(CustomScanState *node,
									  List *ancestors,
									  ExplainState *es);
	static Node *create_mars_sortscan_state(CustomScan *cscan);

	/* Callbacks for planner */
	static Plan *create_mars_sortscan_plan(PlannerInfo *root,
										   RelOptInfo *rel,
										   CustomPath *best_path,
										   List *tlist,
										   List *clauses,
										   List *custom_plans);

	/* Method to convert a mars sortscan path to plan */
	static const CustomPathMethods mars_sortscan_path_methods =
		{
			.CustomName = "MarsSortScan",
			.PlanCustomPath = create_mars_sortscan_plan,
	};

	/* Method to initialize a mars sortscan plan node */
	static CustomScanMethods mars_sortscan_plan_methods =
		{
			.CustomName = "MarsSortScan",
			.CreateCustomScanState = create_mars_sortscan_state,
	};

	/* Method to define the callbacks for execution */
	static CustomExecMethods mars_sort_scan_exec_methods =
		{
			.CustomName = "Mars Sort Scan",
			.BeginCustomScan = begin_mars_sortscan,
			.ExecCustomScan = exec_mars_sortscan,
			.EndCustomScan = end_mars_sortscan,
			.ReScanCustomScan = rescan_mars_sortscan,
			/* We need to fill all the fields to compile with gcc-7 or earlier */
			.MarkPosCustomScan = NULL,
			.RestrPosCustomScan = NULL,
			.EstimateDSMCustomScan = estimate_dsm_mars_sortscan,
			.InitializeDSMCustomScan = initialize_dsm_mars_sortscan,
			.ReInitializeDSMCustomScan = reinitialize_dsm_mars_sortscan,
			.InitializeWorkerCustomScan = initialize_worker_mars_sortscan,
#if 0
	.ShutdownCustomScan = shutdown_mars_sortscan,
#else
			.ShutdownCustomScan = NULL,
#endif
			.ExplainCustomScan = explain_mars_sortscan,
	};

	static Size
	estimate_dsm_mars_sortscan(CustomScanState *node,
							   ParallelContext *pcxt)
	{
		elog(ERROR, "Mars doesn't support parallel mode");
	}

	static void
	initialize_dsm_mars_sortscan(CustomScanState *node,
								 ParallelContext *pcxt,
								 void *coordinate)
	{
		elog(ERROR, "Mars doesn't support parallel mode");
	}

	static void
	reinitialize_dsm_mars_sortscan(CustomScanState *node,
								   ParallelContext *pcxt,
								   void *coordinate)
	{
		elog(ERROR, "Mars doesn't support parallel mode");
	}

	static void
	initialize_worker_mars_sortscan(CustomScanState *node,
									shm_toc *toc,
									void *coordinate)
	{
		elog(ERROR, "Mars doesn't support parallel mode");
	}

#if 0
static void
shutdown_mars_sortscan(CustomScanState *node)
{
    //MarsSortScanState	*as = (MarsSortScanState *) node;

	elog(ERROR, "Mars doesn't support parallel mode");
}
#endif

	/* show sort keys */
	static void
	explain_mars_sortscan(CustomScanState *node,
						  List *ancestors,
						  ExplainState *es)
	{
		MarsSortScanState *msss = (MarsSortScanState *)node;
		CustomScan *plan = (CustomScan *)node->ss.ps.plan;
		Sort *sort;
		List *l_resno = NULL;
		const char *label;
		const char *scandir;

		/* Only show customscan details in verbose mode */
		if (!es->verbose)
			return;

		/* describe scan direction */
		if (ScanDirectionIsBackward(msss->direction))
			scandir = "Backward";
		else
			scandir = "Forward";

		ExplainPropertyText("Scan Direction", scandir, es);

		/* show sort keys */
		sort = (Sort *)linitial(plan->custom_plans);
		/* it can also be a seqscan in the unordered case, must check it */
		if (IsA(sort, Sort))
		{
			label = "Sort Key";
			for (int i = 0; i < sort->numCols; i++)
				l_resno = lappend_int(l_resno, sort->sortColIdx[i]);

			explain_targetlist_exprs(msss->child_ps,
									 l_resno, label,
									 ancestors, es);
		}
	}

	static Node *
	create_mars_sortscan_state(CustomScan *cscan)
	{
		MarsSortScanState *state;

		state = (MarsSortScanState *)newNode(sizeof(MarsSortScanState),
											 T_CustomScanState);

		state->csstate.methods = &mars_sort_scan_exec_methods;

		/* Decide the global scan direction */
		state->direction = NoMovementScanDirection;
		/* TODO: a better and reliable way to detect the scandir. */
		if (list_length(cscan->custom_plans) > 0)
		{
			Sort *sort = (Sort *)linitial(cscan->custom_plans);

			/* it can also be a seqscan in the unordered case, must check it */
			if (IsA(sort, Sort))
			{
				Oid oper = sort->sortOperators[0];
				bool reverse;

				get_equality_op_for_ordering_op(oper, &reverse);

				if (reverse)
					state->direction = BackwardScanDirection;
				else
					state->direction = ForwardScanDirection;
			}
		}

		return (Node *)state;
	}

	/*
	 * This function tells how to convert a MarsSortScanPath to
	 * MarsSortScan plan, the key is CustomScanMethods which
	 * defines the function to create a MarsSortScanState.
	 */
	static Plan *
	create_mars_sortscan_plan(PlannerInfo *root, RelOptInfo *rel,
							  CustomPath *best_path,
							  List *tlist,
							  List *scan_clauses,
							  List *custom_plans)
	{
		CustomScan *cscan;
		Index scan_relid = best_path->path.parent->relid;

		/* it should be a base rel... */
		Assert(scan_relid > 0);
		Assert(best_path->path.parent->rtekind == RTE_RELATION);

		/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
		scan_clauses = extract_actual_clauses(scan_clauses, false);

		/* Replace any outer-relation variables with nestloop params */
		if (best_path->path.param_info)
		{
			scan_clauses = (List *)
				mars_replace_nestloop_params(root, (Node *)scan_clauses);
		}

		cscan = makeNode(CustomScan);
		cscan->scan.scanrelid = scan_relid;
		cscan->scan.plan.targetlist = tlist;
		cscan->scan.plan.qual = scan_clauses;
		cscan->flags = best_path->flags;
		cscan->custom_scan_tlist = NULL;
		cscan->custom_plans = custom_plans;

		/* Tell how to create a MarsSortScanState */
		cscan->methods = &mars_sortscan_plan_methods;

		return &cscan->scan.plan;
	}

	/*
	 * Derived from build_index_paths().
	 */
	static Relids
	build_param_relid(PlannerInfo *root, RelOptInfo *rel, Index parent_rti, List *orderkeys)
	{

		Relids outer_relids;
		ListCell *lc;
		RangeTblEntry *rte = root->simple_rte_array[rel->relid];
		Relation relation = relation_open(rte->relid, NoLock);
		MarsDimensionAttr *first_globalkey;

		relation_close(relation, NoLock);

		outer_relids = bms_copy(rel->lateral_relids);

		if (list_length(orderkeys) == 0)
			return outer_relids;

		first_globalkey = linitial_node(MarsDimensionAttr, orderkeys);

		Assert(root->parse->jointree->fromlist);

		foreach (lc, root->parse->jointree->fromlist)
		{
			JoinExpr *jExpr;
			ListCell *lc_qual;

			if (IsA(lfirst(lc), RangeTblRef))
				continue;

			jExpr = lfirst_node(JoinExpr, lc);

			if (jExpr->jointype != JOIN_SEMI && jExpr->jointype != JOIN_INNER)
				continue;

			if (!jExpr->quals ||
				!IsA(jExpr->quals, List))
				continue;

			foreach (lc_qual, (List *)jExpr->quals)
			{
				Var *var_mine;
				Var *var_other;
				RelOptInfo *rel_other;
				ListCell *lc_rinfo;
				OpExpr *opExpr = lfirst_node(OpExpr, lc_qual);

				if ((list_length(opExpr->args) != 2) ||
					!IsA(linitial(opExpr->args), Var) ||
					!IsA(llast(opExpr->args), Var))
					break;

				/* Check if the join is on me, and get the other side */
				if (linitial_node(Var, opExpr->args)->varno == parent_rti)
				{
					/* My rel is on the left, so the other is on the right */
					var_mine = linitial_node(Var, opExpr->args);
					var_other = llast_node(Var, opExpr->args);
				}
				else if (llast_node(Var, opExpr->args)->varno == parent_rti)
				{
					/* My rel is on the right, so the other is on the left */
					var_mine = llast_node(Var, opExpr->args);
					var_other = linitial_node(Var, opExpr->args);
				}
				else
				{
					/* The join is not on my rel */
					break;
				}

				/* The join is not on my first order key */
				if (var_mine->varattno != first_globalkey->attnum)
					break;

				rel_other = root->simple_rel_array[var_other->varno];
				Assert(rel_other);

				foreach (lc_rinfo, rel_other->baserestrictinfo)
				{
					RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc_rinfo);

					if (IsA(rinfo->clause, ScalarArrayOpExpr))
					{
						ScalarArrayOpExpr *saop =
							(ScalarArrayOpExpr *)rinfo->clause;

						/*
						 * We only support conditions like "c1 > 2":
						 * - 2 arguments;
						 * - one is Var;
						 * - the other is Const;
						 */
						if (list_length(saop->args) != 2 ||
							!IsA(linitial(saop->args), Var) ||
							!IsA(llast(saop->args), Const))
							continue;
					}

					/* OK to include this clause */
					outer_relids = bms_add_members(outer_relids,
												   rinfo->clause_relids);
				}
			}
		}

		/* We do not want the index's rel itself listed in outer_relids */
		outer_relids = bms_del_member(outer_relids, rel->relid);
		/* Enforce convention that outer_relids is exactly NULL if empty */
		if (bms_is_empty(outer_relids))
			outer_relids = NULL;

		return outer_relids;
	}

	static Path *
	create_mars_sortscan_path(PlannerInfo *root,
							  RelOptInfo *rel,
							  Index parent_rti,
							  Path *subpath,
							  List *pathkeys,
							  List *orderlst)
	{
		MarsSortScanPath *path;
		SortPath *sort = NULL;
		Relids outer_relids;

		outer_relids = build_param_relid(root, rel, parent_rti, orderlst);

		/* Create a custom scan path and set the method */
		path = (MarsSortScanPath *)newNode(sizeof(MarsSortScanPath), T_CustomPath);

		/* Set common path fields */
		path->cpath.path.pathtype = T_CustomScan;
		path->cpath.path.parent = rel;
		path->cpath.path.pathtarget = rel->reltarget;
		path->cpath.path.param_info =
			get_baserel_parampathinfo(root, rel, outer_relids);
		/* Don't consider parallel for now*/
		path->cpath.path.parallel_aware = false;
		path->cpath.path.parallel_safe = false;
		path->cpath.path.parallel_workers = 0;
		/* The sortscan has the same locus with baserel */
		path->cpath.path.locus = cdbpathlocus_from_baserel(root, rel, 0);
		path->cpath.path.motionHazard = false;
		path->cpath.path.rescannable = true;
		path->cpath.path.sameslice_relids = rel->relids;

		/* Setup the sort key */
		path->cpath.path.pathkeys = pathkeys;

		/*
		 * Setup the custom scan method, this is mainly to
		 * set the callback to convert a custom path to custom
		 * plan, in that callback you can setup your callback
		 * for execution, to be more accurate, for initialize
		 * of MarsSortScanState.
		 */
		path->cpath.flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
		path->cpath.methods = &mars_sortscan_path_methods;

		/*
		 * Setup a sort path for
		 * 1. explain (hint) purpose
		 * 2. a hint of what order we expect for this customscan,
		 * FIXME: this is not totally right, the pathkey may refer
		 * to a timebucket which is not an target entry of the subpath.
		 */
		if (pathkeys)
		{
			sort = create_sort_path(root, rel,
									subpath,
									pathkeys, -1.0);
			path->cpath.custom_paths = lappend(path->cpath.custom_paths, sort);

			/*
			 * A sort path assumes it's above any joins, so no parameterization,
			 * but we do need it.
			 */
			sort->path.param_info = path->cpath.path.param_info;
		}
		/*
		 * Otherwise it is just a unordered scan, however we can not adopt the
		 * subpath directly because it is shared by all the custom scan variants,
		 * if any of them are dropped the subpath is also destroyed, so we must
		 * duplicate it.
		 */
		else
		{
			Relids required_outer;

			Assert(subpath->pathtype == T_SeqScan);

			required_outer = bms_copy(subpath->param_info ? subpath->param_info->ppi_req_outer : NULL);
			subpath = create_seqscan_path(root, rel, required_outer,
										  subpath->parallel_workers);

			path->cpath.custom_paths = lappend(path->cpath.custom_paths,
											   subpath);
		}

		/*
		 * Cost like a seqscan.
		 *
		 * Comparing to seqscan, give it a tiny penalty for keeping
		 * the output ordering.
		 */
		cost_seqscan((Path *)path, root, rel, path->cpath.path.param_info);

		/*
		 * TODO: Turning the cost model.
		 * TODO: For the query is like: Sort -> Non-SPJ -> Sort -> Scan. If the
		 * scan node order meet Second Sort node requirement, we can not optimize.
		 * Since current rule only evaluate final result data order.
		 *
		 * So far, simply compare storage data order if it satisfies query.
		 */
		if (sort)
			path->cpath.path.total_cost = sort->path.total_cost;
		else
			/* Unordered scan. */
			path->cpath.path.total_cost = subpath->total_cost;

		if (root->parse->jointree->quals)
			/*
			 * When there are scan keys, we can usually beat seqscan because we
			 * have the ability to do block filtering.
			 */
			path->cpath.path.total_cost *= 0.1;
		else if (root->query_pathkeys &&
				 pathkeys_contained_in(root->query_pathkeys, pathkeys))
			/*
			 * When we have a proper scan order, we are usually more efficient than
			 * a seqscan.
			 */
			path->cpath.path.total_cost *= 0.1;
		else if (root->mx_homo_group_pathkeys &&
				 pathkeys_contained_in(root->mx_homo_group_pathkeys, pathkeys))
			path->cpath.path.total_cost *= 0.1;
		else if (pathkeys)
			/*
			 * When we have a improper scan order, it might requires extra effort
			 * to keep the output ordering, so give us a tiny penalty.
			 */
			path->cpath.path.total_cost *= 1.1;

		return (Path *)path;
	}

	/*
	 * Hook to create a mars sortscan path.
	 */
	void
	mars_set_rel_pathlist(PlannerInfo *root,
						  RelOptInfo *rel,
						  Index rti,
						  RangeTblEntry *rte)
	{
		Path *path;
		Path *subpath;
		List *pathkeys;
		ListCell *lc;
		Index parent_rti = rti;
		List *origin_pathlist;
		List *orderkeys_list;
		List *local_orderkey;

		if (prev_set_rel_pathlist_hook != NULL)
			(*prev_set_rel_pathlist_hook)(root, rel, rti, rte);

		if (!mars_enable_customscan)
			return;

		if (!IS_SIMPLE_REL(rel) ||
			rel->rtekind != RTE_RELATION)
			return;

		/* Return immediately if is not a Mars */
		if (!IsMarsAM(rte->relid))
			return;

		subpath = (Path *)linitial(rel->pathlist);

		/*
		 * I am a child rel, use the parent rel to generate pathkeys because
		 * parse->groupClause and parse->sortClause are all refer the parent
		 * rel.
		 */
		if (IS_OTHER_REL(rel))
		{
			foreach (lc, root->append_rel_list)
			{
				AppendRelInfo *appinfo = (AppendRelInfo *)lfirst(lc);

				/* append_rel_list contains all append rels */
				if (appinfo->child_relid == rti)
				{
					parent_rti = appinfo->parent_relid;
					break;
				}
			}
		}

		/*
		 * The subpath is not always a seqscan, for example it can be an append
		 * when it is a false-positive one-time filter.  Do not bother handling
		 * them.
		 */
		if (subpath->pathtype != T_SeqScan)
			return;

		/*
		 * For these unparam path, we believe it can not be faster than param path.
		 * But our cost model does not turning yet. only add these path back if no
		 * param path is generated.
		 */
		origin_pathlist = rel->pathlist;
		rel->pathlist = NULL;

		::Relation relation = relation_open(rte->relid, NoLock);

		mars::footer::GetListOfOrderKeys(relation, &orderkeys_list, &local_orderkey);

		relation_close(relation, NoLock);

		List *ppaths = NULL;
		List *paths = NULL;
		/* Report all the possible pathkeys */
		foreach (lc, orderkeys_list)
		{
			ScanDirection scandir[2] = {::ForwardScanDirection, ::BackwardScanDirection};
			List *orderkey = lfirst_node(List, lc);

			for (int i = 0; i < 2; i++)
			{
				pathkeys = mars_build_pathkeys_list(root,
													parent_rti,
													rti,
													subpath,
													rel,
													scandir[i],
													orderkey,
													local_orderkey);

				/*
				 * A custom sort scan beats a seqscan only if there are either
				 * useful order or scankeys.
				 */
				if (!pathkeys && !root->parse->jointree->quals)
					continue;

				Relids required_outer = NULL;
				if (subpath->param_info)
				{
					required_outer = subpath->param_info->ppi_req_outer;
				}

				Path *subpath_copy = create_seqscan_path(root,
														 subpath->parent,
														 required_outer,
														 subpath->parallel_workers);
				path = create_mars_sortscan_path(root, rel, parent_rti, subpath_copy, pathkeys, orderkey);

				/*
				 * TODO: Since SortCustomScan cost is not correct yet, we have to
				 * force add our path into pathlist. fix it and using add_path
				 * function to add the paths.
				 */
				if (path->param_info)
					ppaths = lappend(ppaths, path);
				else
					paths = lappend(paths, path);
			}
		}

		/* Accept seqscan only if no customscan paths contain param */
		rel->pathlist = ppaths ? ppaths : list_concat(paths, origin_pathlist);
	}

	/*
	 * Act like SeqNext.
	 */
	static TupleTableSlot *
	MarsSortScanNext(ScanState *ss)
	{
		MarsSortScanState *msss = (MarsSortScanState *)ss;
		TableScanDesc scandesc;
		EState *estate;
		ScanDirection direction;
		TupleTableSlot *slot;

		/*
		 * get information from the estate and scan state
		 */
		scandesc = ss->ss_currentScanDesc;
		slot = ss->ss_ScanTupleSlot;
		estate = ss->ps.state;
		direction = estate->es_direction;

		if (msss->direction != NoMovementScanDirection)
			direction = msss->direction;

		if (scandesc == NULL)
		{
			/*
			 * We reach here if the scan is not parallel, or if we're serially
			 * executing a scan that was planned to be parallel.
			 */
			scandesc = table_beginscan(ss->ss_currentRelation, estate->es_snapshot,
									   0, NULL);
			ss->ss_currentScanDesc = scandesc;

			auto scanWrapper = (mars::ScanWrapper *)scandesc;
			scanWrapper->preader_->BeginScan(ss);
		}

		/*
		 * get the next tuple from the table
		 */
		if (table_scan_getnextslot(scandesc, direction, slot))
		{
			return slot;
		}

		return NULL;
	}

	static bool
	MarsSortScanRecheck(ScanState *node, TupleTableSlot *slot)
	{
		return true;
	}

	void
	mars_register_customsortscan_methods(void)
	{
		RegisterCustomScanMethods(&mars_sortscan_plan_methods);

		prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
		set_rel_pathlist_hook = mars_set_rel_pathlist;
	}

	void
	mars_unregister_customsortscan_methods(void)
	{
		set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
	}

	bool
	IsMarsCustomSortScan(Path *path)
	{
		/* Ignore ProjectionPath,  */
		while (IsA(path, ProjectionPath))
			path = ((ProjectionPath *)path)->subpath;

		if (!IsA(path, CustomPath) ||
			((CustomPath *)path)->methods != &mars_sortscan_path_methods)
			return false;
		return true;
	}

} // extern "C"

static Relids build_param_relid(PlannerInfo *root, RelOptInfo *rel, Index parent_rti, List *orderlst);

static void
begin_mars_sortscan(CustomScanState *node,
					EState *estate, int eflags)
{
	MarsSortScanState *msss = (MarsSortScanState *)node;
	CustomScan *cs = (CustomScan *)node->ss.ps.plan;
	Plan *child_plan = (Plan *)linitial(cs->custom_plans);

	msss->child_ps = ExecInitNode(child_plan, estate, eflags);
}

static TupleTableSlot *
exec_mars_sortscan(CustomScanState *node)
{
	/*
	 * FIXME: Now we call the ExecScan here to do the projection and qual
	 * check. We know that Mars may have already done the qual check, so we can
	 * avoid rechecking the qual again, so then we can change the ExecScan with
	 * more customize code here.
	 */
	return ExecScan(&node->ss,
					(ExecScanAccessMtd)MarsSortScanNext,
					(ExecScanRecheckMtd)MarsSortScanRecheck);
}

static void
end_mars_sortscan(CustomScanState *node)
{
	TableScanDesc scanDesc;

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
rescan_mars_sortscan(CustomScanState *node)
{
	ScanState *ss = (ScanState *)node;
	TableScanDesc scanDesc = ss->ss_currentScanDesc;
	EState *estate = ss->ps.state;

	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	if (scanDesc == NULL)
	{
		scanDesc = table_beginscan(ss->ss_currentRelation, estate->es_snapshot,
								   0, NULL);
		ss->ss_currentScanDesc = scanDesc;

		auto scanWrapper = (mars::ScanWrapper *)scanDesc;
		scanWrapper->preader_->BeginScan(ss);
	}

	ResetExprContext(ss->ps.ps_ExprContext);

	auto scanWrapper = (mars::ScanWrapper *)scanDesc;
	auto &scanner = scanWrapper->preader_->GetScanner();
	auto runtimeKeys = scanner.GetRuntimeKeys();

	if (!runtimeKeys->empty())
	{
		::ExecIndexEvalRuntimeKeys(ss->ps.ps_ExprContext,
								   runtimeKeys->data(),
								   runtimeKeys->size());
	}

	table_rescan(scanDesc, NULL);
}

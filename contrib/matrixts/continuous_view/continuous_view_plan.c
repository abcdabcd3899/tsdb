#include "postgres.h"
#include "access/table.h"
#include "access/relation.h"
#include "catalog/pg_inherits.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "tcop/tcopprot.h"

#include "continuous_view.h"


/* ------------------------------------------------------------
 * Functions to generate a two-stage sort group aggregate plan
 * ------------------------------------------------------------
 */
static void
force_create_partial_seqscan(PlannerInfo *root,
							 RelOptInfo *rel,
							 Index rti,
							 RangeTblEntry *rte)
{
	rel->consider_parallel = true;
	add_partial_path(rel, create_seqscan_path(root, rel, NULL, 2));
}

static void
force_create_partial_agg(PlannerInfo *root,
						 UpperRelationKind stage,
						 RelOptInfo *input_rel,
						 RelOptInfo *output_rel,
						 void *extra)
{
	ListCell	*lc;

	if (stage != UPPERREL_PARTIAL_GROUP_AGG)
		return;

	/* minimize the cost of partial_pathlist to make partial agg a winner */
	foreach(lc, output_rel->partial_pathlist)
	{
		Path	*path = (Path *) lfirst(lc);

		path->rows = 0;
		path->total_cost = 0;
		path->startup_cost = 0;
	}
}

/*
 * Convert the query to plan, we must guarantee the planner produce a tow stage
 * sort group aggregate plan.
 */
CvPlannedStmt *
continuous_view_planner(Query *query)
{
	Plan 		*plan;
	Agg			*agg;
	CvPlannedStmt *result;
	PlannedStmt *planstmt;
	bool		saved_optimizer;
	bool		saved_enable_hashagg;
	bool		saved_enable_indexscan;
	bool		saved_enable_bitmapscan;
	bool		saved_enable_indexonlyscan;
	bool		saved_parallel_setup_cost;
	bool		saved_parallel_tuple_cost;
	bool		saved_enable_parallel_mode;
	bool		saved_enable_partitionwise_aggregate;
	bool		saved_gp_enable_multiphase_agg;
	bool		saved_force_partial_aggregate_path;
	set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
	create_upper_paths_hook_type prev_create_upper_paths_hook;

	saved_optimizer = optimizer;
	saved_enable_bitmapscan = enable_bitmapscan;
	saved_enable_hashagg = enable_hashagg;
	saved_enable_indexscan = enable_indexscan;
	saved_enable_indexonlyscan = enable_indexonlyscan;
	saved_enable_partitionwise_aggregate = enable_partitionwise_aggregate;
	saved_gp_enable_multiphase_agg = gp_enable_multiphase_agg;
	saved_parallel_setup_cost = parallel_setup_cost;
	saved_parallel_tuple_cost = parallel_tuple_cost;
	saved_enable_parallel_mode = enable_parallel_mode;
	saved_force_partial_aggregate_path = force_partial_aggregate_path;
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	prev_create_upper_paths_hook = create_upper_paths_hook;

	PG_TRY();
	{
		optimizer = false;
		enable_hashagg = false;
		enable_indexscan = false;
		enable_bitmapscan = false;
		enable_indexonlyscan = false;
		enable_partitionwise_aggregate = true;
		force_partial_aggregate_path = true;
		enable_parallel_mode = true;
		parallel_setup_cost = 0;
		parallel_tuple_cost = 0;
		gp_enable_multiphase_agg = true;
		set_rel_pathlist_hook = force_create_partial_seqscan;
		create_upper_paths_hook = force_create_partial_agg;

		planstmt = pg_plan_query(query, CURSOR_OPT_PARALLEL_OK, NULL);
	}
	PG_CATCH();
	{
		optimizer = saved_optimizer;
		enable_bitmapscan = saved_enable_bitmapscan;
		enable_hashagg = saved_enable_hashagg;
		enable_indexscan = saved_enable_indexscan;
		enable_indexonlyscan = saved_enable_indexonlyscan;
		enable_partitionwise_aggregate = saved_enable_partitionwise_aggregate;
		force_partial_aggregate_path = saved_force_partial_aggregate_path;
		gp_enable_multiphase_agg = saved_gp_enable_multiphase_agg;
		enable_parallel_mode = saved_enable_parallel_mode;
		parallel_setup_cost = saved_parallel_setup_cost;
		parallel_tuple_cost = saved_parallel_tuple_cost;
		set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
		create_upper_paths_hook = prev_create_upper_paths_hook;
		PG_RE_THROW();
	}
	PG_END_TRY();

	optimizer = saved_optimizer;
	enable_bitmapscan = saved_enable_bitmapscan;
	enable_hashagg = saved_enable_hashagg;
	enable_indexscan = saved_enable_indexscan;
	enable_indexonlyscan = saved_enable_indexonlyscan;
	enable_partitionwise_aggregate = saved_enable_partitionwise_aggregate;
	force_partial_aggregate_path = saved_force_partial_aggregate_path;
	gp_enable_multiphase_agg = saved_gp_enable_multiphase_agg;
	parallel_setup_cost = saved_parallel_setup_cost;
	parallel_tuple_cost = saved_parallel_tuple_cost;
	enable_parallel_mode = saved_enable_parallel_mode;
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
	create_upper_paths_hook = prev_create_upper_paths_hook;

	result = (CvPlannedStmt *) palloc0(sizeof(CvPlannedStmt));
	result->plannedStmt = planstmt;

	plan = planstmt->planTree;
	while (plan)
	{
		if (IsA(plan, Append))
		{
			Append *append = (Append *) plan;

			plan = (Plan *) linitial(append->appendplans);
		}

		if (IsA(plan, Agg))
		{
			agg = (Agg *) plan;

			if (agg->aggsplit == AGGSPLIT_SIMPLE ||
				agg->aggstrategy == AGG_HASHED ||
				agg->aggstrategy == AGG_MIXED)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("Unexpected continuous view error:"
									   "cannot produce two-stage group aggregate plan")));
			if (agg->aggsplit == AGGSPLIT_INITIAL_SERIAL)
				result->partial_agg = agg;
			else if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
				result->final_agg = agg;
		}
		plan = plan->lefttree;
	}

	if (result->partial_agg == NULL || result->final_agg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Unexpected continuous view error:"
							   "cannot produce two-stage group aggregate plan")));

	return result;
}


/* ------------------------------------------------------------
 * Functions to start a new executor to do runtime aggregation
 * ------------------------------------------------------------
 */
CvExecutorState *
continuous_view_executor_start(SortHeapState *shstate, Oid auxtblid)
{
	int				i;
	int 			nrecords;
	Oid 			*srcrelids;
	Oid 			*viewids;
	bool			partition;
	Relation		*partrels;
	Relation		srcrel;
	Query 			*query;
	QueryDesc		*desc;
	PlanState  		*planstate;
	CvPlannedStmt	*cvplan;
	CvExecutorState *cvestate;
	MemoryContext oldcontext; 
	oldcontext = MemoryContextSwitchTo(shstate->sortcontext);

	cvestate = (CvExecutorState *) palloc0(sizeof(CvExecutorState));

	nrecords =
		get_continuous_view_def(Anum_continuous_views_auxtblid,
								auxtblid != InvalidOid ?
								auxtblid : shstate->relation->rd_id,
								&viewids, NULL, &srcrelids, &query, false);

	Assert(nrecords == 1);

	/* Lock the source table */
	partition = has_superclass(srcrelids[0]);

	if (partition)
	{
		nrecords =
			get_continuous_view_def(Anum_continuous_views_viewid,
									viewids[0],
									NULL, NULL, &srcrelids, NULL, false);

		partrels = (Relation *) palloc(sizeof(Relation) * nrecords);

		for (i = 0; i < nrecords; i++)
			partrels[i] = relation_open(srcrelids[i], AccessShareLock);
	}
	else
		srcrel = relation_open(srcrelids[0], AccessShareLock);

	/* Lock the source table and do the customized planning */
	cvplan = continuous_view_planner(query);

	if (partition)
	{
		for (i = 0; i < nrecords; i++)
			relation_close(partrels[i], AccessShareLock);
	}
	else
		relation_close(srcrel, AccessShareLock);

	/*
	 * Before starting a executor, we need to modify the two stage plan for the
	 * MERGE operation.
	 *
	 * If we are performing a merge, we need an aggstate which calls the combine
	 * function but skips the final function, we prefer to use the final agg
	 * as the execution node for the merge, so we need to change the aggsplit
	 * of it. Meanwhile, the result of the merge is still the partial aggregate
	 * result, we need to change the targetlist of the final agg so that we can
	 * output the partial aggregate result.
	 */
	if (shstate->op == SHOP_MERGE)
	{
		int			idx = 0;
		ListCell	*lc;
		Agg			*fagg = cvplan->final_agg;
		Agg			*pagg = cvplan->partial_agg;

		fagg->aggsplit = AGGSPLITOP_COMBINE | AGGSPLITOP_SKIPFINAL |
						 AGGSPLITOP_SERIALIZE | AGGSPLITOP_DESERIALIZE;
		fagg->plan.targetlist = NULL;

		/* the entry of the partial agg must be column ref or aggref */
		foreach_with_count (lc, pagg->plan.targetlist, idx)
		{
			Var		*var;
			TargetEntry	*new_tle;
			TargetEntry *partial_tle = (TargetEntry *) lfirst(lc);

			var = makeVar(OUTER_VAR,
						  idx + 1,
						  exprType((Node *) partial_tle->expr),
						  exprTypmod((Node *) partial_tle->expr),
						  exprCollation((Node *) partial_tle->expr),
						  0);

			new_tle = makeTargetEntry((Expr *) var, idx + 1, "", false);

			if (IsA(partial_tle->expr, Aggref))
			{
				Aggref	*mock_aggref;
				mock_aggref = (Aggref *) copyObject(partial_tle->expr);
				mock_aggref->args = list_make1(new_tle);
				mock_aggref->aggsplit = fagg->aggsplit;

				new_tle = makeTargetEntry((Expr *) mock_aggref, idx + 1, "", false);
			}

			fagg->plan.targetlist = lappend(fagg->plan.targetlist, new_tle);
		}
	}

	/* Start a executor and get the planstate */
	desc = CreateQueryDesc(cvplan->plannedStmt,
						   "",
						   InvalidSnapshot, NULL,
						   NULL, NULL, NULL, 0);
	cvestate->querydesc = desc;

	ExecutorStart(desc, 0);
	planstate = desc->planstate;

	for (;;)
	{
		if (planstate == NULL)
			break;

		if (IsA(planstate, AggState))
		{
			AggState *candidate = (AggState *) planstate;

			if (shstate->op == SHOP_INSERT && candidate->aggsplit == AGGSPLIT_INITIAL_SERIAL)
				cvestate->aggstate = candidate;

			if (shstate->op != SHOP_INSERT && candidate->aggsplit == (AGGSPLIT_INITIAL_SERIAL | AGGSPLIT_FINAL_DESERIAL))
				cvestate->aggstate = candidate;
		}
		else if (IsA(planstate, IndexScanState) ||
				 IsA(planstate, SeqScanState) ||
				 IsA(planstate, IndexOnlyScanState) ||
				 IsA(planstate, BitmapHeapScanState) ||
				 IsA(planstate, CustomScanState))
			cvestate->scanstate = (ScanState *) planstate;
		else if (IsA(planstate, AppendState))
		{
			planstate = ((AppendState *) planstate)->appendplans[0];
			continue;
		}

		planstate = outerPlanState(planstate);
	}

	if (cvestate->aggstate == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Unexpected continuous view error:"
							   "cannot generate runtime aggregate planstate")));

	/* Initialize the scan slot */
	cvestate->scanslot =
		MakeSingleTupleTableSlot(cvestate->scanstate->ss_ScanTupleSlot->tts_tupleDescriptor,
								 cvestate->scanstate->ss_ScanTupleSlot->tts_ops);

	MemoryContextSwitchTo(oldcontext);

	return cvestate;
}

void
continuous_view_executor_end(CvExecutorState *cvestate)
{
	if (cvestate->scanslot)
	{
		ExecDropSingleTupleTableSlot(cvestate->scanslot);
		cvestate->scanslot = NULL;
	}

	if (cvestate->querydesc)
	{
		ExecutorFinish(cvestate->querydesc);
		ExecutorEnd(cvestate->querydesc);
		cvestate->querydesc = NULL;
	}
}

/*-------------------------------------------------------------------------
 *
 * cdbgroupingpaths.c
 *	  Routines to aid in planning grouping queries for parallel
 *    execution.  This is, essentially, an extension of the file
 *    optimizer/plan/planner.c, although some functions are not
 *    externalized.

 *
 * The general shape of the generated plan is similar to the parallel
 * aggregation plans in upstream:
 *
 * Finalize Aggregate [3]
 *    Motion             [2]
 *       Partial Aggregate  [1]
 *
 * but there are many different variants of this basic shape:
 *
 * [1] The Partial stage can be sorted or hashed. Furthermore,
 *     the sorted Agg can be construct from sorting the cheapest input Path,
 *     or from pre-sorted Paths.
 *
 * [2] The partial results need to be gathered for the second stage.
 *     For plain aggregation, with no GROUP BY, the results need to be
 *     gathered to a single node. With GROUP BY, they can be redistributed
 *     according to the GROUP BY columns.
 *
 * [3] Like the first tage, the second stage can likewise be sorted or hashed.
 *
 *
 * Things get more complicated if any of the aggregates have DISTINCT
 * arguments, also known as DQAs or Distinct-Qualified Aggregates. If there
 * is only one DQA, and the input path happens to be collocated with the
 * DISTINCT argument, then we can proceed with a two-stage path like above.
 * But otherwise, three stages and possibly TupleSplit node is needed. See
 * add_single_dqa_hash_agg_path() and add_multi_dqas_hash_agg_path() for
 * details.
 *
 * Portions Copyright (c) 2019-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbgroupingpaths.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "cdb/cdbgroup.h"
#include "cdb/cdbgroupingpaths.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbpath.h"
#include "cdb/cdbpathlocus.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/tlist.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"

typedef enum
{
	INVALID_DQA = -1,
	SINGLE_DQA, /* only one unique DQA expr */
	MULTI_DQAS, /* multiple DQA exprs */
	SINGLE_DQA_WITHAGG, /* only one unique DQA expr with agg */
	MULTI_DQAS_WITHAGG/* mixed DQA and normal aggregate */
} DQAType;

/*
 * For convenience, we collect various inputs and intermediate planning results
 * in this struct, instead of passing a dozen arguments to all subroutines.
 */
typedef struct
{
	/* Inputs from the caller */
	DQAType     type;
	List	   *havingQual;
	PathTarget *target;
	PathTarget *partial_grouping_target;
	double		dNumGroupsTotal;		/* total number of groups in the result, across all QEs */
	const AggClauseCosts *agg_costs;
	const AggClauseCosts *agg_partial_costs;
	const AggClauseCosts *agg_final_costs;
	List *rollups;
	List	   *group_tles;
} cdb_agg_planning_context;

typedef struct
{
	DQAType     type;

	PathTarget  *final_target;          /* finalize agg tlist */
	PathTarget  *partial_target;        /* partial agg tlist */
	PathTarget  *tup_split_target;      /* AggExprId + subpath_proj_target */
	PathTarget  *input_proj_target;     /* input tuple tlist + DQA expr */

	List        *dqa_group_clause;      /* DQA exprs + group by clause for remove duplication */

	List        *dqa_expr_lst;          /* DQAExpr list */
	double		 dNumDistinctGroups;	/* # of distinct combinations of GROUP BY and DISTINCT exprs */

} cdb_multi_dqas_info;

static void add_single_dqa_hash_agg_path(PlannerInfo *root,
										 Path *path,
										 cdb_agg_planning_context *ctx,
										 RelOptInfo *output_rel,
										 PathTarget *input_target,
										 List       *dqa_group_clause,
										 double dNumDistinctGroups);
static void add_single_mixed_dqa_hash_agg_path(PlannerInfo *root,
                                               Path *path,
                                               cdb_agg_planning_context *ctx,
                                               RelOptInfo *output_rel,
                                               PathTarget *input_target,
                                               List       *dqa_group_clause);
static void
add_multi_dqas_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *ctx,
							 RelOptInfo *output_rel,
							 cdb_multi_dqas_info *info);

static void
fetch_single_dqa_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_multi_dqas_info *info);

static void
fetch_multi_dqas_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_multi_dqas_info *info);

static DQAType
recognize_dqa_type(cdb_agg_planning_context *ctx);

static PathTarget *
strip_aggdistinct(PathTarget *target);

/*
 * CAUTION:
 *
 * This function now only consider multi-stage plans for DISTINCT.
 *
 * The original logical to generate multi-stage has been converted
 * to PG upstream style (see create_partial_grouping_paths);
 */
void
cdb_create_twostage_distinct_paths(PlannerInfo *root,
								   RelOptInfo *input_rel,
								   RelOptInfo *output_rel,
								   PathTarget *target,
								   PathTarget *partial_grouping_target,
								   List *havingQual,
								   bool can_sort,
								   bool can_hash,
								   double dNumGroupsTotal,
								   const AggClauseCosts *agg_costs,
								   const AggClauseCosts *agg_partial_costs,
								   const AggClauseCosts *agg_final_costs,
								   List *rollups)
{
	Query	   *parse = root->parse;
	Path	   *cheapest_path = input_rel->cheapest_total_path;
	bool		has_ordered_aggs = agg_costs->numPureOrderedAggs > 0;
	cdb_agg_planning_context ctx;

	/* The caller should've checked these already */
	Assert(parse->hasAggs || parse->groupClause);

	/*
	 * This prohibition could be relaxed if we tracked missing combine
	 * functions per DQA and were willing to plan some DQAs as single and
	 * some as multiple phases.  Not currently, however.
	 */
	Assert(!agg_costs->hasNonCombine && !agg_costs->hasNonSerial);
	Assert(root->config->gp_enable_multiphase_agg);

	/*
	 * Ordered aggregates need to run the transition function on the
	 * values in sorted order, which in turn translates into single phase
	 * aggregation.
	 */
	if (has_ordered_aggs)
		return;

	/*
	 * We are currently unwilling to redistribute a gathered intermediate
	 * across the cluster.  This might change one day.
	 */
	if (!CdbPathLocus_IsPartitioned(cheapest_path->locus))
		return;

	/*
	 * Prepare a struct to hold the arguments and intermediate results
	 * across subroutines.
	 */
	memset(&ctx, 0, sizeof(ctx));
	ctx.havingQual = havingQual;
	ctx.target = target;
	ctx.partial_grouping_target = partial_grouping_target;
	ctx.dNumGroupsTotal = dNumGroupsTotal;
	ctx.agg_costs = agg_costs;
	ctx.agg_partial_costs = agg_partial_costs;
	ctx.agg_final_costs = agg_final_costs;
	ctx.rollups = rollups;
	ctx.group_tles = get_common_group_tles(target,
										   parse->groupClause,
										   ctx.rollups);


	/*
	 * Aggregates with DISTINCT arguments are more complicated, and are not
	 * handled above (except for the case of a single DQA that happens to
	 * be collocated with the input, see add_first_stage_group_agg_path()).
	 * Consider ways to implement them, too.
	 */
	if ((can_hash || parse->groupClause == NIL) &&
		!parse->groupingSets &&
		list_length(agg_costs->distinctAggrefs) > 0)
	{
		/*
		 * Try possible plans for DISTINCT-qualified aggregate.
		 */
		cdb_multi_dqas_info info = {};
		DQAType type = recognize_dqa_type(&ctx);
		switch (type)
		{
		case SINGLE_DQA:
			{
				fetch_single_dqa_info(root, cheapest_path, &ctx, &info);

				add_single_dqa_hash_agg_path(root,
											 cheapest_path,
											 &ctx,
											 output_rel,
											 info.input_proj_target,
											 info.dqa_group_clause,
											 info.dNumDistinctGroups);
			}
			break;
		case SINGLE_DQA_WITHAGG:
			{
				fetch_single_dqa_info(root, cheapest_path, &ctx, &info);

				add_single_mixed_dqa_hash_agg_path(root,
				                                   cheapest_path,
				                                   &ctx,
				                                   output_rel,
				                                   info.input_proj_target,
				                                   info.dqa_group_clause);
			}
			break;
		case MULTI_DQAS:
			{
				fetch_multi_dqas_info(root, cheapest_path, &ctx, &info);

				add_multi_dqas_hash_agg_path(root,
											 cheapest_path,
											 &ctx,
											 output_rel,
											 &info);
			}
			break;
		case MULTI_DQAS_WITHAGG:
			break;
		default:
			break;
		}
	}
}

/*
 * Add a TargetEntry node of type GroupingSetId to the tlist.
 * Return its ressortgroupref.
 */
Index
add_gsetid_tlist(List *tlist)
{
	TargetEntry *tle;
	GroupingSetId *gsetid;
	ListCell *lc;

	foreach(lc, tlist)
	{
		tle = lfirst_node(TargetEntry, lc);
		if (IsA(tle->expr, GroupingSetId))
			elog(ERROR, "GROUPINGSET_ID already exists in tlist");
	}

	gsetid = makeNode(GroupingSetId);
	tle = makeTargetEntry((Expr *)gsetid, list_length(tlist) + 1,
			"GROUPINGSET_ID", true);
	assignSortGroupRef(tle, tlist);
	tlist = lappend(tlist, tle);

	return tle->ressortgroupref;
}

/*
 * Add a SortGroupClause node to the groupClause representing the GroupingSetId.
 * Note we insert the new node to the head of groupClause.
 */
SortGroupClause *
create_gsetid_groupclause(Index groupref)
{
	SortGroupClause *gc;
	Oid         sortop;
	Oid         eqop;
	bool        hashable;

	get_sort_group_operators(INT4OID,
							 false, true, false,
							 &sortop, &eqop, NULL,
							 &hashable);

	gc = makeNode(SortGroupClause);
	gc->tleSortGroupRef = groupref;
	gc->eqop = eqop;
	gc->sortop = sortop;
	gc->nulls_first = false;
	gc->hashable = hashable;

	return gc;
}

static Node *
strip_aggdistinct_mutator(Node *node, void *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		Aggref	   *newAggref = (Aggref *) copyObject(node);

		newAggref->aggdistinct = NIL;

		node = (Node *) newAggref;
	}
	return expression_tree_mutator(node, strip_aggdistinct_mutator, context);
}

static PathTarget *
strip_aggdistinct(PathTarget *target)
{
	PathTarget *result;

	result = copy_pathtarget(target);
	result->exprs = (List *) strip_aggdistinct_mutator((Node *) result->exprs, NULL);

	return result;
}

/*
 * Create Paths for an Aggregate with one DISTINCT-qualified aggregate and
 * multi normal aggregate.
 */
static void add_single_mixed_dqa_hash_agg_path(PlannerInfo *root,
                                               Path *path,
                                               cdb_agg_planning_context *ctx,
                                               RelOptInfo *output_rel,
                                               PathTarget *input_target,
                                               List       *dqa_group_clause)
{
	Query	   *parse = root->parse;
	List	   *dqa_group_tles;
	CdbPathLocus distinct_locus;
	bool		distinct_need_redistribute;
	CdbPathLocus singleQE_locus;

	if (!gp_enable_agg_distinct)
		return;

	if (parse->groupClause)
		return;

	/*
	 * If subpath is projection capable, we do not want to generate a
	 * projection plan. The reason is that the projection plan does not
	 * constrain a child tlist when it creates subplan. Thus, GROUP BY expr
	 * may not be found in the scan targetlist.
	 */
	path = apply_projection_to_path(root, path->parent, path, input_target);

	dqa_group_tles = get_common_group_tles(input_target, dqa_group_clause, NIL  );
	distinct_locus = choose_grouping_locus(root, path,
										   dqa_group_tles,
										   &distinct_need_redistribute);

	if (!CdbPathLocus_IsPartitioned(distinct_locus))
		return;

	if (distinct_need_redistribute)
		path = cdbpath_create_motion_path(root, path, NIL, false,
										  distinct_locus);

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									ctx->partial_grouping_target,
									AGG_PLAIN,
									AGGSPLIT_INITIAL_SERIAL,
									false, /* streaming */
									parse->groupClause,
									NIL,
									ctx->agg_partial_costs, /* FIXME */
									estimate_num_groups_on_segment(ctx->dNumGroupsTotal,
																   path->rows, path->locus));

	CdbPathLocus_MakeSingleQE(&singleQE_locus, getgpsegmentCount());
	path = cdbpath_create_motion_path(root, path, NIL, false, singleQE_locus);

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									ctx->target,
									AGG_PLAIN,
									AGGSPLIT_FINAL_DESERIAL,
									false, /* streaming */
									parse->groupClause,
									ctx->havingQual,
									ctx->agg_final_costs,
									ctx->dNumGroupsTotal);
	add_path(output_rel, path);
}

/*
 * Create Paths for an Aggregate with one DISTINCT-qualified aggregate.
 */
static void
add_single_dqa_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *ctx,
							 RelOptInfo *output_rel,
							 PathTarget *input_target,
							 List       *dqa_group_clause,
							 double dNumDistinctGroups)
{
	Query	   *parse = root->parse;
	List	   *dqa_group_tles;
	int			num_input_segments;
	List	   *group_tles;
	CdbPathLocus group_locus;
	double		dNumGroups;
	bool		group_need_redistribute;
	CdbPathLocus distinct_locus;
	bool		distinct_need_redistribute;

	if (!gp_enable_agg_distinct)
		return;

	/*
	 * If subpath is projection capable, we do not want to generate a
	 * projection plan. The reason is that the projection plan does not
	 * constrain a child tlist when it creates subplan. Thus, GROUP BY expr
	 * may not be found in the scan targetlist.
	 */
	path = apply_projection_to_path(root, path->parent, path, input_target);

	if (CdbPathLocus_IsPartitioned(path->locus))
		num_input_segments = CdbPathLocus_NumSegments(path->locus);
	else
		num_input_segments = 1;

	dqa_group_tles = get_common_group_tles(input_target, dqa_group_clause, NIL);
	distinct_locus = choose_grouping_locus(root, path,
										   dqa_group_tles,
										   &distinct_need_redistribute);

	/*
	 * Calculate the number of groups in the final stage, per segment.
	 * group_locus is the corresponding locus for the final stage.
	 */
	group_tles = get_common_group_tles(input_target, parse->groupClause, NIL);
	group_locus = choose_grouping_locus(root, path,
										group_tles,
										&group_need_redistribute);
	if (CdbPathLocus_IsPartitioned(group_locus))
		dNumGroups = clamp_row_est(ctx->dNumGroupsTotal /
								   CdbPathLocus_NumSegments(path->locus));
	else
		dNumGroups = ctx->dNumGroupsTotal;

	if (!distinct_need_redistribute || !group_need_redistribute)
	{
		/*
		 * 1. If the input's locus matches the DISTINCT, but not GROUP BY:
		 *
		 *  HashAggregate
		 *     -> Redistribute (according to GROUP BY)
		 *         -> HashAggregate (to eliminate duplicates)
		 *             -> input (hashed by GROUP BY + DISTINCT)
		 *
		 * 2. If the input's locus matches the GROUP BY:
		 *
		 *  HashAggregate (to aggregate)
		 *     -> HashAggregate (to eliminate duplicates)
		 *           -> input (hashed by GROUP BY)
		 *
		 * The main planner should already have created the single-stage
		 * Group Agg path.
		 *
		 * XXX: not sure if this makes sense. If hash distinct is a good
		 * idea, why doesn't PostgreSQL's agg node implement that?
		 */
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										clamp_row_est(dNumDistinctGroups / (double) num_input_segments));

		if (group_need_redistribute)
			path = cdbpath_create_motion_path(root, path, NIL, false,
											  group_locus);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										parse->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_DEDUPLICATED,
										false, /* streaming */
										parse->groupClause,
										ctx->havingQual,
										ctx->agg_final_costs,
										dNumGroups);
		add_path(output_rel, path);
	}
	else if (CdbPathLocus_IsHashed(group_locus))
	{
		double		input_rows = path->rows;

		/*
		 *  HashAgg (to aggregate)
		 *     -> HashAgg (to eliminate duplicates)
		 *          -> Redistribute (according to GROUP BY)
		 *               -> Streaming HashAgg (to eliminate duplicates)
		 *                    -> input
		 *
		 * It may seem silly to have two Aggs on top of each other like this,
		 * but the Agg node can't do DISTINCT-aggregation by hashing at the
		 * moment. So we have to do it with two separate Aggs steps.
		 */
		if (gp_enable_dqa_pruning)
			path = (Path *) create_agg_path(root,
											output_rel,
											path,
											input_target,
											AGG_HASHED,
											AGGSPLIT_SIMPLE,
											true, /* streaming */
											dqa_group_clause,
											NIL,
											ctx->agg_partial_costs, /* FIXME */
											estimate_num_groups_on_segment(dNumDistinctGroups,
																		   input_rows, path->locus));

		path = cdbpath_create_motion_path(root, path, NIL, false,
										  group_locus);
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										clamp_row_est(dNumDistinctGroups / CdbPathLocus_NumSegments(group_locus)));

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										parse->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_DEDUPLICATED,
										false, /* streaming */
										parse->groupClause,
										ctx->havingQual,
										ctx->agg_final_costs,
										dNumGroups);
		add_path(output_rel, path);
	}
	else if (CdbPathLocus_IsHashed(distinct_locus))
	{
		double			input_rows = path->rows;

		/*
		 *  Finalize Aggregate
		 *     -> Gather Motion
		 *          -> Partial Aggregate
		 *              -> HashAggregate, to remove duplicates
		 *                  -> Redistribute Motion (according to DISTINCT arg)
		 *                      -> Streaming HashAgg (to eliminate duplicates)
		 *                          -> input
		 */
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										true, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										estimate_num_groups_on_segment(dNumDistinctGroups,
																	   input_rows, path->locus));

		path = cdbpath_create_motion_path(root, path, NIL, false,
										  distinct_locus);
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										input_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										dqa_group_clause,
										NIL,
										ctx->agg_partial_costs, /* FIXME */
										clamp_row_est(dNumDistinctGroups / CdbPathLocus_NumSegments(distinct_locus)));

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										strip_aggdistinct(ctx->partial_grouping_target),
										parse->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_INITIAL_SERIAL | AGGSPLITOP_DEDUPLICATED,
										false, /* streaming */
										parse->groupClause,
										NIL,
										ctx->agg_partial_costs,
										estimate_num_groups_on_segment(ctx->dNumGroupsTotal, input_rows, path->locus));
		path = cdbpath_create_motion_path(root, path, NIL, false,
										  group_locus);

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										ctx->target,
										parse->groupClause ? AGG_HASHED : AGG_PLAIN,
										AGGSPLIT_FINAL_DESERIAL | AGGSPLITOP_DEDUPLICATED,
										false, /* streaming */
										parse->groupClause,
										ctx->havingQual,
										ctx->agg_final_costs,
										dNumGroups);

		add_path(output_rel, path);
	}
	else
		return;
}

/*
 * Create Paths for Multiple DISTINCT-qualified aggregates.
 *
 * The goal is that using a single execution path to handle all DQAs, so
 * before removing duplication a SplitTuple node is created. This node handles
 * each input tuple to n output tuples(n is DQA expr number). Each output tuple
 * only contains an AggExprId, one DQA expr and all GROUP by expr. For example,
 * SELECT DQA(a), DQA(b) FROM foo GROUP BY c;
 * After the tuple split, two tuples are generated:
 * -------------------
 * | 1 | a | n/a | c |
 * -------------------
 * -------------------
 * | 2 | n/a | b | c |
 * -------------------
 *
 * In an aggregate executor, if the input tuple contains AggExprId, that means
 * the tuple is split. Each value of AggExprId points to a bitmap set to
 * represent args AttrNumber. In the Agg executor, each transfunc also keeps
 * its own args bitmap set. The transfunc is invoked only if bitmapset matches
 * with each other.
 */
static void
add_multi_dqas_hash_agg_path(PlannerInfo *root,
							 Path *path,
							 cdb_agg_planning_context *ctx,
							 RelOptInfo *output_rel,
							 cdb_multi_dqas_info *info)
{
	List	   *dqa_group_tles;
	CdbPathLocus distinct_locus;
	bool		distinct_need_redistribute;

	/*
	 * If subpath is projection capable, we do not want to generate a
	 * projection plan. The reason is that the projection plan does not
	 * constrain a child tlist when it creates subplan. Thus, GROUP BY expr
	 * may not be found in the scan targetlist.
	 */
	path = apply_projection_to_path(root, path->parent, path, info->input_proj_target);

	/*
	 * Finalize Aggregate
	 *   -> Gather Motion
	 *        -> Partial Aggregate
	 *             -> HashAggregate, to remote duplicates
	 *                  -> Redistribute Motion
	 *                       -> TupleSplit (according to DISTINCT expr)
	 *                            -> input
	 */
	path = (Path *) create_tup_split_path(root,
										  output_rel,
										  path,
										  info->tup_split_target,
										  root->parse->groupClause,
										  info->dqa_expr_lst);

	AggClauseCosts DedupCost = {};
	get_agg_clause_costs(root, (Node *) info->tup_split_target->exprs,
						 AGGSPLIT_SIMPLE,
						 &DedupCost);

	if (gp_enable_dqa_pruning)
	{
		/*
		 * If we are grouping, we charge an additional cpu_operator_cost per
		 * **grouping column** per input tuple for grouping comparisons.
		 *
		 * But in the tuple split case, other columns not for this DQA are
		 * NULLs, the actual cost is way less than the number calculating based
		 * on the length of grouping clause.
		 *
		 * So here we create a dummy grouping clause whose length is 1 (the
		 * most common case of DQA), use it to calculate the cost, then set the
		 * actual one back into the path.
		 */
		List *dummy_group_clause = list_make1(list_head(info->dqa_group_clause));

		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										info->tup_split_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										true, /* streaming */
										dummy_group_clause, /* only its length 1 is being used here */
										NIL,
										&DedupCost,
										estimate_num_groups_on_segment(info->dNumDistinctGroups,
																	   path->rows, path->locus));

		/* set the actual group clause back */
		((AggPath *)path)->groupClause = info->dqa_group_clause;
	}

	dqa_group_tles = get_common_group_tles(info->tup_split_target,
										   info->dqa_group_clause, NIL);
	distinct_locus = choose_grouping_locus(root, path, dqa_group_tles,
										   &distinct_need_redistribute);

	if (distinct_need_redistribute)
		path = cdbpath_create_motion_path(root, path, NIL, false,
										  distinct_locus);

	AggStrategy split = AGG_PLAIN;
	unsigned long DEDUPLICATED_FLAG = 0;
	PathTarget *partial_target = info->partial_target;
	double		input_rows = path->rows;

	if (root->parse->groupClause)
	{
		path = (Path *) create_agg_path(root,
										output_rel,
										path,
										info->tup_split_target,
										AGG_HASHED,
										AGGSPLIT_SIMPLE,
										false, /* streaming */
										info->dqa_group_clause,
										NIL,
										&DedupCost,
										clamp_row_est(info->dNumDistinctGroups / CdbPathLocus_NumSegments(distinct_locus)));

		split = AGG_HASHED;
		DEDUPLICATED_FLAG = AGGSPLITOP_DEDUPLICATED;
		partial_target = strip_aggdistinct(info->partial_target);
	}

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									partial_target,
									split,
									AGGSPLIT_INITIAL_SERIAL | DEDUPLICATED_FLAG,
									false, /* streaming */
									root->parse->groupClause,
									NIL,
									ctx->agg_partial_costs,
									estimate_num_groups_on_segment(ctx->dNumGroupsTotal,
																   input_rows, path->locus));

	CdbPathLocus singleQE_locus;
	CdbPathLocus_MakeSingleQE(&singleQE_locus, getgpsegmentCount());
	path = cdbpath_create_motion_path(root,
									  path,
									  NIL,
									  false,
									  singleQE_locus);

	path = (Path *) create_agg_path(root,
									output_rel,
									path,
									info->final_target,
									split,
									AGGSPLIT_FINAL_DESERIAL | DEDUPLICATED_FLAG,
									false, /* streaming */
									root->parse->groupClause,
									ctx->havingQual,
									ctx->agg_final_costs,
									ctx->dNumGroupsTotal);

	add_path(output_rel, path);
}

/*
 * Get the common expressions in all grouping sets as a target list.
 *
 * In case of a simple GROUP BY, it's just all the grouping column. With
 * multiple grouping sets, identify the set of common entries, and return
 * a list of those. For example, if you do:
 *
 *   GROUP BY GROUPING SETS ((a, b, c), (b, c))
 *
 * the common cols are b and c.
 */
List *
get_common_group_tles(PathTarget *target,
					  List *groupClause,
					  List *rollups)
{
	List	   *tlist = make_tlist_from_pathtarget(target);
	List	   *group_tles;
	ListCell   *lc;
	Bitmapset  *common_groupcols = NULL;
	int			x;

	if (rollups)
	{
		ListCell   *lc;
		bool		first = true;

		foreach(lc, rollups)
		{
			RollupData *rollup = lfirst_node(RollupData, lc);
			ListCell   *lc2;

			foreach(lc2, rollup->gsets)
			{
				List	   *colidx_lists = (List *) lfirst(lc2);
				ListCell   *lc3;
				Bitmapset  *this_groupcols = NULL;

				foreach(lc3, colidx_lists)
				{
					int			colidx = lfirst_int(lc3);
					SortGroupClause *sc = list_nth(rollup->groupClause, colidx);

					this_groupcols = bms_add_member(this_groupcols, sc->tleSortGroupRef);
				}

				if (first)
					common_groupcols = this_groupcols;
				else
				{
					common_groupcols = bms_int_members(common_groupcols, this_groupcols);
					bms_free(this_groupcols);
				}
				first = false;
			}
		}
	}
	else
	{
		foreach(lc, groupClause)
		{
			SortGroupClause *sc = lfirst(lc);

			common_groupcols = bms_add_member(common_groupcols, sc->tleSortGroupRef);
		}
	}

	x = -1;
	group_tles = NIL;
	while ((x = bms_next_member(common_groupcols, x)) >= 0)
	{
		TargetEntry *tle = get_sortgroupref_tle(x, tlist);

		group_tles = lappend(group_tles, tle);
	}

	return group_tles;
}

/*
 * Choose a data distribution to perform the grouping.
 *
 * 'group_tles' is a target list that represents the grouping columns,
 * or all the common columns in all the grouping sets if there are
 * multple grouping sets. Use get_common_group_tles() to build that
 * list.
 */
CdbPathLocus
choose_grouping_locus(PlannerInfo *root, Path *path,
					  List *group_tles,
					  bool *need_redistribute_p)
{
	CdbPathLocus locus;
	bool		need_redistribute;

	/*
	 * If the input is already collected to a single segment, just perform the
	 * aggregation there. We could redistribute it, so that we could perform
	 * the aggregation in parallel, but Motions are pretty expensive so it's
	 * probably not worthwhile.
	 */
	if (CdbPathLocus_IsBottleneck(path->locus))
	{
		need_redistribute = false;
		CdbPathLocus_MakeNull(&locus);
	}
	/* If there are no GROUP BY columns, we have no choice but gather everything to a single node */
	else if (!group_tles)
	{
		need_redistribute = true;
		CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
	}
	/* If the input is already suitably distributed, no need to redistribute */
	else if (!CdbPathLocus_IsHashedOJ(path->locus) &&
			 cdbpathlocus_is_hashed_on_tlist(path->locus, group_tles, true))
	{
		need_redistribute = false;
		CdbPathLocus_MakeNull(&locus);
	}
	/*
	 * If the query's final result locus collocates with the GROUP BY, then
	 * redistribute directly to that locus and avoid a possible redistribute
	 * step later. (We might still need to redistribute the data for later
	 * windowing, LIMIT or similar, but this is a pretty good heuristic.)
	 */
	else if (CdbPathLocus_IsHashed(root->final_locus) &&
			 cdbpathlocus_is_hashed_on_tlist(root->final_locus, group_tles, true))
	{
		need_redistribute = true;
		locus = root->final_locus;
	}
	/*
	 * Construct a new locus from the GROUP BY columns. We greedily use as
	 * many columns as possible, to maximimize distribution. (It might be
	 * cheaper to pick only one or two columns, as long as they distribute
	 * the data evenly enough, but we're not that smart.)
	 */
	else
	{
		List	   *hash_exprs;
		List	   *hash_opfamilies;
		List	   *hash_sortrefs;
		ListCell   *lc;

		hash_exprs = NIL;
		hash_opfamilies = NIL;
		hash_sortrefs = NIL;
		foreach(lc, group_tles)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Oid			typeoid = exprType((Node *) tle->expr);
			Oid			opfamily;
			Oid			eqopoid;

			opfamily = cdb_default_distribution_opfamily_for_type(typeoid);
			if (!OidIsValid(opfamily))
				continue;

			/*
			 * If the datatype isn't mergejoinable, then we cannot represent
			 * the grouping in the locus. Skip such expressions.
			 */
			eqopoid = cdb_eqop_in_hash_opfamily(opfamily, typeoid);
			if (!op_mergejoinable(eqopoid, typeoid))
				continue;

			hash_exprs = lappend(hash_exprs, tle->expr);
			hash_opfamilies = lappend_oid(hash_opfamilies, opfamily);
			hash_sortrefs = lappend_int(hash_sortrefs, tle->ressortgroupref);
		}

		if (hash_exprs)
			locus = cdbpathlocus_from_exprs(root,
											path->parent,
											hash_exprs,
											hash_opfamilies,
											hash_sortrefs,
											getgpsegmentCount(),
											path->parallel_workers);
		else
			CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
		need_redistribute = true;
	}

	*need_redistribute_p = need_redistribute;
	return locus;
}

static DQAType
recognize_dqa_type(cdb_agg_planning_context *ctx)
{
	ListCell    *lc, *lcc;
	List        *dqaArgs = NIL;
	ctx->type = INVALID_DQA;

	foreach (lc, ctx->agg_costs->distinctAggrefs)
	{
		Aggref *aggref = (Aggref *) lfirst(lc);
		SortGroupClause *arg_sortcl;

		/* I can not give a case for a DQA have order by yet. */
		if (aggref->aggorder != NIL)
			return ctx->type;

		foreach (lcc, aggref->aggdistinct)
		{
			arg_sortcl = (SortGroupClause *) lfirst(lcc);
			if (!arg_sortcl->hashable)
			{
				/*
				 * XXX: I'm not sure if the hashable flag is always set correctly
				 * for DISTINCT args. DISTINCT aggs are never implemented with hashing
				 * in PostgreSQL.
				 */
				return ctx->type;
			}
		}

		/* get the first dqa arguments */
		if (dqaArgs == NIL)
		{
			dqaArgs = aggref->args;
			ctx->type = SINGLE_DQA;
		}
		/* if there is another dqa with different args, it's MULTI_DQAS */
		else if (!equal(dqaArgs, aggref->args))
		{
			ctx->type = MULTI_DQAS;
			break;
		}
	}

	if (ctx->type != INVALID_DQA)
	{
		/* Check that there are no non-DISTINCT aggregates mixed in. */
		List *varnos = pull_var_clause((Node *) ctx->target->exprs,
									   PVC_INCLUDE_AGGREGATES |
									   PVC_INCLUDE_WINDOWFUNCS |
									   PVC_INCLUDE_PLACEHOLDERS);
		foreach (lc, varnos)
		{
			Node	   *node = lfirst(lc);

			if (IsA(node, Aggref))
			{
				Aggref	   *aggref = (Aggref *) node;

				if (!aggref->aggdistinct)
				{
					/* mixing DISTINCT and non-DISTINCT aggs */
					if (ctx->type == SINGLE_DQA)
						ctx->type = SINGLE_DQA_WITHAGG;
					else
						ctx->type = MULTI_DQAS_WITHAGG;

					return ctx->type;
				}
			}
		}
	}

	return ctx->type;
}

/*
 * fetch_multi_dqas_info
 *
 * 1. fetch all dqas path required information as single dqa's function.
 *
 * 2. append an AggExprId into Pathtarget to indicate which DQA expr is
 * in the output tuple after TupleSplit.
 */
static void
fetch_multi_dqas_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_multi_dqas_info *info)
{
	ListCell    *lc;
	ListCell    *lcc;
	Index		maxRef = 0;
	PathTarget *proj_target;
	int			num_input_segments;
	double		num_total_input_rows;
	List	   *group_exprs;
	double		dNumDistinctGroups;

	if (CdbPathLocus_IsPartitioned(path->locus))
		num_input_segments = CdbPathLocus_NumSegments(path->locus);
	else
		num_input_segments = 1;
	num_total_input_rows = path->rows * num_input_segments;

	group_exprs = get_sortgrouplist_exprs(root->parse->groupClause,
										  make_tlist_from_pathtarget(path->pathtarget));

	proj_target = copy_pathtarget(path->pathtarget);
	if (proj_target->sortgrouprefs)
	{
		for (int idx = 0; idx < list_length(proj_target->exprs); idx++)
		{
			if (proj_target->sortgrouprefs[idx] > maxRef)
				maxRef = proj_target->sortgrouprefs[idx];
		}
	}
	else
		proj_target->sortgrouprefs = (Index *) palloc0(list_length(proj_target->exprs) * sizeof(Index));

	info->dqa_expr_lst = NIL;

	/*
	 * assign numDisDQAs and agg_args_id_bms
	 *
	 * find all DQAs with different args, count the number, store their args bitmapsets
	 */
	dNumDistinctGroups = 0;
	forboth(lc, ctx->agg_partial_costs->distinctAggrefs,
	        lcc, ctx->agg_final_costs->distinctAggrefs)
	{
		Aggref	        *aggref = (Aggref *) lfirst(lc);
		Aggref	        *aggref_final = (Aggref *) lfirst(lcc);
		SortGroupClause *arg_sortcl;
		TargetEntry     *arg_tle;
		ListCell        *lc2;
		Bitmapset       *bms = NULL;
		List		   *this_dqa_group_exprs;

		this_dqa_group_exprs = list_copy(group_exprs);

		foreach (lc2, aggref->aggdistinct)
		{
			arg_sortcl = (SortGroupClause *) lfirst(lc2);
			arg_tle = get_sortgroupclause_tle(arg_sortcl, aggref->args);
			ListCell    *lc3;
			int         dqa_idx = 0;

			foreach (lc3, proj_target->exprs)
			{
				Expr    *expr = lfirst(lc3);

				if (equal(arg_tle->expr, expr))
					break;

				dqa_idx++;
			}

			/*
			 * DQA expr is not in PathTarget
			 *
			 * SELECT DQA(a + b) from foo;
			 */
			if (dqa_idx == list_length(proj_target->exprs))
			{
				add_column_to_pathtarget(proj_target, arg_tle->expr, ++maxRef);

				SortGroupClause *sortcl;

				sortcl = copyObject(arg_sortcl);
				sortcl->tleSortGroupRef = maxRef;
				sortcl->hashable = true;	/* we verified earlier that it's hashable */

				info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);
				this_dqa_group_exprs = lappend(this_dqa_group_exprs, arg_tle->expr);

				bms = bms_add_member(bms, maxRef);
			}
			else if (proj_target->sortgrouprefs[dqa_idx] == 0)
			{
				/*
				 * DQA expr in PathTarget but no reference
				 *
				 * SELECT DQA(a) FROM foo ;
				 */
				proj_target->sortgrouprefs[dqa_idx] = ++maxRef;

				SortGroupClause *sortcl;

				sortcl = copyObject(arg_sortcl);
				sortcl->tleSortGroupRef = maxRef;
				sortcl->hashable = true;	/* we verified earlier that it's hashable */

				info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);
				this_dqa_group_exprs = lappend(this_dqa_group_exprs, arg_tle->expr);

				bms = bms_add_member(bms, maxRef);
			}
			else
			{
				/*
				 * DQA expr in PathTarget and referenced by GROUP BY clause
				 *
				 * SELECT DQA(a) FROM foo GROUP BY a;
				 */
				Index exprRef = proj_target->sortgrouprefs[dqa_idx];
				bms = bms_add_member(bms, exprRef);
			}
		}

		/*
		 * DQA(a, b) and DQA(b, a) and their filter is same, as well as, they
		 * do not contain volatile expression, then they can share one split
		 * tuple.
		 */
		Index agg_expr_id ;
		if (!contain_volatile_functions((Node *)aggref->aggfilter))
		{
			ListCell *lc_dqa;
			agg_expr_id = 1;
			foreach (lc_dqa, info->dqa_expr_lst)
			{
				DQAExpr *dqaExpr = (DQAExpr *)lfirst(lc_dqa);

				if (bms_equal(bms, dqaExpr->agg_args_id_bms)
					&& equal(aggref->aggfilter, dqaExpr->agg_filter))
					break;

				agg_expr_id++;
			}
		}
		else
		{
			agg_expr_id = list_length(info->dqa_expr_lst) + 1;
		}

		/* If DQA(expr1) FILTER (WHERE expr2) is different with previous, create new one */
		if ((agg_expr_id - 1) == list_length(info->dqa_expr_lst))
		{
			DQAExpr *dqaExpr= makeNode(DQAExpr);

			dqaExpr->agg_expr_id = agg_expr_id;
			dqaExpr->agg_args_id_bms = bms;
			dqaExpr->agg_filter = (Expr *)copyObject(aggref->aggfilter);
			info->dqa_expr_lst = lappend(info->dqa_expr_lst, dqaExpr);

			/*
			 * How many distinct combinations of GROUP BY columns and the
			 * DISTINCT arguments of this aggregate are there? Add it to the
			 * total.
			 */
			dNumDistinctGroups += estimate_num_groups(root,
			                                          this_dqa_group_exprs,
			                                          num_total_input_rows,
			                                          NULL);
		}

		/* assign an agg_expr_id value to aggref*/
		aggref->agg_expr_id = agg_expr_id;

		/* rid of filter in aggref */
		aggref->aggfilter = NULL;
		aggref_final->aggfilter = NULL;
	}
	info->dNumDistinctGroups = dNumDistinctGroups;

	info->input_proj_target = proj_target;
	info->tup_split_target = copy_pathtarget(proj_target);
	{
		AggExprId *a_expr_id = makeNode(AggExprId);
		add_column_to_pathtarget(info->tup_split_target, (Expr *)a_expr_id, ++maxRef);

		Oid eqop;
		bool hashable;
		SortGroupClause *sortcl;
		get_sort_group_operators(INT4OID, false, true, false, NULL, &eqop, NULL, &hashable);

		sortcl = makeNode(SortGroupClause);
		sortcl->tleSortGroupRef = maxRef;
		sortcl->hashable = hashable;
		sortcl->eqop = eqop;
		info->dqa_group_clause = lcons(sortcl, info->dqa_group_clause);
	}

	info->dqa_group_clause = list_concat(info->dqa_group_clause,
										 list_copy(root->parse->groupClause));

	info->partial_target= ctx->partial_grouping_target;
	info->final_target = ctx->target;
}

/*
 * fetch_single_dqa_info
 *
 * fetch single dqa path required information and store in cdb_multi_dqas_info
 *
 * info->input_target contains subpath target expr + all DISTINCT expr
 *
 * info->dqa_group_clause contains DISTINCT expr + GROUP BY expr
 */
static void
fetch_single_dqa_info(PlannerInfo *root,
					  Path *path,
					  cdb_agg_planning_context *ctx,
					  cdb_multi_dqas_info *info)
{
	Index		maxRef;
	List	   *dqa_group_exprs;

	/* Prepare a modifiable copy of the input path target */
	info->input_proj_target = copy_pathtarget(path->pathtarget);
	maxRef = 0;
	List *exprLst = info->input_proj_target->exprs;
	if (info->input_proj_target->sortgrouprefs)
	{
		for (int idx = 0; idx < list_length(exprLst); idx++)
		{
			if (info->input_proj_target->sortgrouprefs[idx] > maxRef)
				maxRef = info->input_proj_target->sortgrouprefs[idx];
		}
	}
	else
		info->input_proj_target->sortgrouprefs = (Index *) palloc0(list_length(exprLst) * sizeof(Index));

	dqa_group_exprs = get_sortgrouplist_exprs(root->parse->groupClause,
											  make_tlist_from_pathtarget(path->pathtarget));

	Aggref	   *aggref = list_nth(ctx->agg_costs->distinctAggrefs, 0);
	SortGroupClause *arg_sortcl;
	SortGroupClause *sortcl = NULL;
	TargetEntry *arg_tle;
	int			idx = 0;
	ListCell   *lc;
	ListCell   *lcc;

	foreach (lc, aggref->aggdistinct)
	{
		arg_sortcl = (SortGroupClause *) lfirst(lc);
		arg_tle = get_sortgroupref_tle(arg_sortcl->tleSortGroupRef, aggref->args);

		/* Now find this expression in the sub-path's target list */
		idx = 0;
		foreach(lcc, info->input_proj_target->exprs)
		{
			Expr		*expr = lfirst(lcc);

			if (equal(expr, arg_tle->expr))
				break;
			idx++;
		}

		if (idx == list_length(info->input_proj_target->exprs))
			add_column_to_pathtarget(info->input_proj_target, arg_tle->expr, ++maxRef);
		else if (info->input_proj_target->sortgrouprefs[idx] == 0)
			info->input_proj_target->sortgrouprefs[idx] = ++maxRef;

		sortcl = copyObject(arg_sortcl);
		sortcl->tleSortGroupRef = info->input_proj_target->sortgrouprefs[idx];
		sortcl->hashable = true;	/* we verified earlier that it's hashable */

		info->dqa_group_clause = lappend(info->dqa_group_clause, sortcl);
		dqa_group_exprs = lappend(dqa_group_exprs, arg_tle->expr);
	}

	info->dqa_group_clause = list_concat(list_copy(root->parse->groupClause),
										 info->dqa_group_clause);

	/*
	 * Estimate how many groups there are in DISTINCT + GROUP BY, per segment.
	 * For example in query:
	 *
	 * select count(distinct c) from t group by b;
	 *
	 * dNumDistinctGroups is the estimate of distinct combinations of b and c.
	 */
	double		num_total_input_rows;
	if (CdbPathLocus_IsPartitioned(path->locus))
		num_total_input_rows = path->rows * CdbPathLocus_NumSegments(path->locus);
	else
		num_total_input_rows = path->rows;
	info->dNumDistinctGroups = estimate_num_groups(root,
												   dqa_group_exprs,
												   num_total_input_rows,
												   NULL);
}

/*
 * Prepare the input path for sorted Agg node.
 *
 * The input to a (sorted) Agg node must be:
 *
 * 1. distributed so that rows belonging to the same group reside on the
 *    same segment, and
 *
 * 2. sorted according to the pathkeys.
 *
 * If the input is already suitably distributed, this is no different from
 * upstream, and we just add a Sort node if the input isn't already sorted.
 *
 * This also works for the degenerate case with no pathkeys, which means
 * simple aggregation without grouping. For that, all the rows must be
 * brought to a single node, but no sorting is needed.
 *
 * For non-sorted input, the logic is the same as in choose_grouping_locus()
 * (in fact this uses choose_grouping_locus()), except that if the input
 * is already sorted, we prefer to gather it to a single node to make
 * use of the pre-existing order, instead of redistributing and resorting
 * it.
 */
Path *
cdb_prepare_path_for_sorted_agg(PlannerInfo *root,
								bool is_sorted,
								/* args corresponding to create_sort_path */
								RelOptInfo *rel,
								Path *subpath,
								PathTarget *target,
								List *group_pathkeys,
								double limit_tuples,
								/* extra arguments */
								List *groupClause,
								List *rollups)
{
	CdbPathLocus locus;
	bool		need_redistribute;

	/*
	 * If the input is already collected to a single segment, just add a Sort
	 * node (if needed). We could redistribute it, so that we could perform the
	 * aggregation in parallel, but Motions are pretty expensive so it's
	 * probably not worthwhile.
	 */
	if (CdbPathLocus_IsBottleneck(subpath->locus))
	{
		need_redistribute = false;
	}
	else
	{
		List	   *group_tles;

		group_tles = get_common_group_tles(target,
										   groupClause,
										   rollups);

		locus = choose_grouping_locus(root, subpath, group_tles,
									  &need_redistribute);
	}
	if (!need_redistribute)
	{
		if (!is_sorted)
		{
			subpath = (Path *) create_sort_path(root,
												rel,
												subpath,
												group_pathkeys,
												-1.0);
		}
		return subpath;
	}

	if (is_sorted && group_pathkeys)
	{
		/*
		 * The input is already conveniently sorted. We could redistribute
		 * it by the grouping keys, but then we'd need to re-sort it. That
		 * doesn't seem like a good idea, so we prefer to gather it all, and
		 * take advantage of the sort order.
		 */
		CdbPathLocus_MakeSingleQE(&locus, getgpsegmentCount());
		subpath = cdbpath_create_motion_path(root,
											 subpath,
											 group_pathkeys,
											 false, locus);
	}
	else if (!is_sorted && group_pathkeys)
	{
		/*
		 * If we need to redistribute, it's usually best to redistribute
		 * the data first, and then sort in parallel on each segment.
		 *
		 * But if we don't have any expressions to redistribute on, i.e.
		 * if we are gathering all data to a single node to perform the
		 * aggregation, then it's better to sort all the data on the
		 * segments first, in parallel, and do a order-preserving motion
		 * to merge the inputs.
		 */
		if (CdbPathLocus_IsPartitioned(locus))
			subpath = cdbpath_create_motion_path(root, subpath, NIL,
												 false, locus);

		subpath = (Path *) create_sort_path(root,
											rel,
											subpath,
											group_pathkeys,
											-1.0);

		if (!CdbPathLocus_IsPartitioned(locus))
			subpath = cdbpath_create_motion_path(root, subpath,
												 group_pathkeys,
												 false, locus);
	}
	else
	{
		/*
		 * The grouping doesn't require any sorting, i.e. the GROUP BY
		 * consists entirely of (pseudo-)constants.
		 *
		 * The locus could be Hashed, which is a bit silly because with
		 * all-constant grouping keys, all the rows will end up on a
		 * single QE anyway. We could mark the locus as SingleQE here, so
		 * that in simple cases where the result needs to end up in the QD,
		 * the planner could Gather the result there directly. However, in
		 * other cases hashing the result to one QE node is more helpful
		 * for the plan above this.
		 */
		Assert(!group_pathkeys);
		subpath = cdbpath_create_motion_path(root,
											 subpath,
											 subpath->pathkeys,
											 false, locus);
	}

	return subpath;
}

/*
 * Prepare the input path for hashed Agg node.
 *
 * This is much simpler than the sorted case. We only need to care about
 * distribution, not sorting.
 */
Path *
cdb_prepare_path_for_hashed_agg(PlannerInfo *root,
								Path *subpath,
								PathTarget *target,
								/* extra arguments */
								List *groupClause,
								List *rollups)
{
	List	   *group_tles;
	CdbPathLocus locus;
	bool		need_redistribute;

	if (CdbPathLocus_IsBottleneck(subpath->locus))
		return subpath;

	group_tles = get_common_group_tles(target,
									   groupClause,
									   rollups);
	locus = choose_grouping_locus(root,
								  subpath,
								  group_tles,
								  &need_redistribute);

	/*
	 * Redistribute if needed.
	 *
	 * The hash agg doesn't care about input order, and it destroys any
	 * order there was, so don't bother with a order-preserving Motion even
	 * if we could.
	 */
	if (need_redistribute)
		subpath = cdbpath_create_motion_path(root,
											 subpath,
											 NIL /* pathkeys */,
											 false,
											 locus);

	return subpath;
}

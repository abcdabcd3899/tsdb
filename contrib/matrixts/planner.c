#include <postgres.h>
#include <access/tsmapi.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <optimizer/clauses.h>
#include <optimizer/planner.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/tlist.h>
#include <catalog/namespace.h>
#include <utils/elog.h>
#include <utils/guc.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <optimizer/restrictinfo.h>
#include <utils/lsyscache.h>
#include <executor/nodeAgg.h>
#include <utils/timestamp.h>
#include <utils/selfuncs.h>
#include <access/xact.h>

#include <optimizer/cost.h>
#include <tcop/tcopprot.h>
#include <optimizer/plancat.h>
#include <nodes/nodeFuncs.h>

#include <parser/analyze.h>

#include <catalog/pg_constraint.h>

#include "guc.h"
#include "plan_agg_bookend.h"
#include "gapfill/gapfill_custom_scan.h"

void _planner_init(void);
void _planner_fini(void);

extern void ts_sort_transform_optimization(PlannerInfo *root, RelOptInfo *rel);

static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook;
static create_upper_paths_hook_type prev_create_upper_paths_hook;

static void
apply_optimizations(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
    if (ts_guc_disable_optimizations)
        return;

    ts_sort_transform_optimization(root, rel);
}

static void
matrix_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
    /* Quick exit if this is a relation we're not interested in */
    if (!OidIsValid(rte->relid) || IS_DUMMY_REL(rel))
    {
        if (prev_set_rel_pathlist_hook != NULL)
            (*prev_set_rel_pathlist_hook)(root, rel, rti, rte);
        return;
    }

    apply_optimizations(root, rel, rte);
}

static void
mx_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
						   RelOptInfo *output_rel, void *extra)
{
	Query *parse = root->parse;

	if (prev_create_upper_paths_hook != NULL)
		prev_create_upper_paths_hook(root, stage, input_rel, output_rel, extra);

	if (ts_guc_disable_optimizations || input_rel == NULL || IS_DUMMY_REL(input_rel))
		return;

	if (stage == UPPERREL_GROUP_AGG && output_rel != NULL)
	{
		if (parse->hasAggs)
			ts_preprocess_first_last_aggregates(root, root->processed_tlist);
	}
}

void
_planner_init(void)
{
	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = matrix_set_rel_pathlist;

	prev_create_upper_paths_hook = create_upper_paths_hook;
	create_upper_paths_hook = mx_create_upper_paths_hook;

	gapfill_register_customscan_methods();
}

void
_planner_fini(void)
{
	set_rel_pathlist_hook = prev_set_rel_pathlist_hook;
}



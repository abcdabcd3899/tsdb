/*--------------------------------------------------------------------
 * mars_planner.h
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/mx_mars.h"
#include "catalog/namespace.h"
#include "nodes/extensible.h"
#include "nodes/pathnodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

#include "mars_planner.h"
#include "customnodes/modifytable.h"

static create_upper_paths_hook_type prev_upper_hook;
static planner_hook_type prev_planner_hook;

/*
 * TODO: we should create a catalog to store time series table info
 * instead of dynamic analyze in planner
 */
static bool
is_mars_table(PlannerInfo *root, Path *path)
{
	RangeVar *rv;
	Oid template_rel_oid;
	char template_relname[NAMEDATALEN];
	ModifyTablePath *mt_path = (ModifyTablePath *)path;

	RangeTblEntry *rte = planner_rt_fetch(mt_path->rootRelation, root);
	memset(template_relname, 0, NAMEDATALEN);
	sprintf(template_relname, "template_%d", rte->relid);

	rv = makeRangeVarFromNameList(list_make2(makeString("mars"), makeString(template_relname)));

	template_rel_oid = RangeVarGetRelidExtended(rv, AccessShareLock,
												RVR_MISSING_OK, NULL, NULL);

	if (template_rel_oid != InvalidOid)
		return true;

	return false;
}

static List *
replace_mars_modifytable_path(PlannerInfo *root, List *pathlist)
{
	ListCell *lc;
	List *new_pathlist = NIL;
	int i;

	foreach_with_count(lc, pathlist, i)
	{
		Path *path = (Path *)lfirst(lc);

		if (IsA(path, ModifyTablePath) && ((ModifyTablePath *)path)->operation == CMD_INSERT && (((ModifyTablePath *)path)->rootRelation != 0) /* It is a partition table */
			&& ((ModifyTablePath *)path)->onconflict && ((ModifyTablePath *)path)->onconflict->arbiterWhere == NULL							   /* can not handle filter in ON CONFLICT */
			&& is_mars_table(root, path))
		{
			path = create_mars_modifytable_path(root, (ModifyTablePath *)path, i);
		}

		new_pathlist = lappend(new_pathlist, path);
	}
	return new_pathlist;
}

static void
mars_create_upper_paths_hook(PlannerInfo *root,
							 UpperRelationKind stage,
							 RelOptInfo *input_rel,
							 RelOptInfo *output_rel,
							 void *extra)
{
	if (prev_upper_hook)
		prev_upper_hook(root, stage, input_rel, output_rel, extra);

	if (output_rel != NULL && output_rel->pathlist)
		output_rel->pathlist = replace_mars_modifytable_path(root, output_rel->pathlist);
}

static PlannedStmt *
mars_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	ListCell *lc;
	PlannedStmt *stmt;

	if (prev_planner_hook != NULL)
		stmt = (prev_planner_hook)(parse, cursorOptions, boundParams);

	stmt = standard_planner(parse, cursorOptions, boundParams);

	if (IsNormalProcessingMode())
	{
		mars_modifytable_tlist_fixup(stmt->planTree);

		foreach (lc, stmt->subplans)
		{
			Plan *subplan = (Plan *)lfirst(lc);
			mars_modifytable_tlist_fixup(subplan);
		}
	}

	return stmt;
}

void _mars_planner_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = mars_planner;

	prev_upper_hook = create_upper_paths_hook;
	create_upper_paths_hook = mars_create_upper_paths_hook;

	mars_modifytable_register();
}

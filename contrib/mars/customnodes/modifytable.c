/*-------------------------------------------------------------------------
 *
 * modifytable.c
 *	  MARS Custom ModifyTable node.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/customnodes/modifytable.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/mx_mars.h"
#include "catalog/pg_inherits.h"
#include "modifytable.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

static void
mars_mt_begin(CustomScanState *node,
			  EState *estate,
			  int eflags)
{
	MARS_ModifyTableState *state = (MARS_ModifyTableState *)node;
	ModifyTableState *mtstate;
	PlanState *ps;

	ps = ExecInitNode(&state->mt->plan, estate, eflags);
	node->custom_ps = list_make1(ps);
	mtstate = castNode(ModifyTableState, ps);

	/* mark on conflict attribute */
	mars_infer_elems = state->infer_elems;
}

static TupleTableSlot *
mars_mt_exec(CustomScanState *node)
{
	TupleTableSlot *slot;
	mx_partition_without_validation = true;
	slot = ExecProcNode(linitial(node->custom_ps));
	mx_partition_without_validation = false;
	return slot;
}

static void
mars_mt_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));

	/* reset on conflict attribute */
	mars_infer_elems = NIL;
}

static void
mars_mt_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
}

static void
mars_mt_explain(CustomScanState *node,
				List *ancestors,
				ExplainState *es)
{
	MARS_ModifyTableState *state = (MARS_ModifyTableState *)node;
	Index rti = state->mt->nominalRelation;
	RangeTblEntry *rte = rt_fetch(rti, es->rtable);
	const char *relname = get_rel_name(rte->relid);
	const char *namespace = get_namespace_name(get_rel_namespace(rte->relid));

	appendStringInfo(es->str, "InsertByMerge: ");
	appendStringInfo(es->str, "%s.%s\n",
					 quote_identifier(relname),
					 quote_identifier(namespace));
}

static CustomExecMethods mars_modifytable_state_methods = {
	.CustomName = "MARSModifyTable",
	.BeginCustomScan = mars_mt_begin,
	.EndCustomScan = mars_mt_end,
	.ExecCustomScan = mars_mt_exec,
	.ReScanCustomScan = mars_mt_rescan,
	.ExplainCustomScan = mars_mt_explain,
};

static Node *
create_mars_modifytable_state(CustomScan *cscan)
{
	MARS_ModifyTableState *state;

	state = (MARS_ModifyTableState *)newNode(sizeof(MARS_ModifyTableState), T_CustomScanState);
	state->mt = linitial(cscan->custom_plans);
	state->cscan_state.methods = &mars_modifytable_state_methods;
	state->infer_elems = (List *)linitial(cscan->custom_private);

	return (Node *)state;
}

static CustomScanMethods create_mars_modifytable_plan_methods = {
	.CustomName = "MarsModifyTable",
	.CreateCustomScanState = create_mars_modifytable_state,
};

void mars_modifytable_register()
{
	RegisterCustomScanMethods(&create_mars_modifytable_plan_methods);
}

static Plan *
create_mars_modifytable_plan(PlannerInfo *root, RelOptInfo *relopt, CustomPath *best_path,
							 List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	ModifyTable *mt = linitial(custom_plans);

	cscan->custom_plans = list_make1(mt);

	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;

	cscan->methods = &create_mars_modifytable_plan_methods;
	cscan->custom_scan_tlist = tlist;
	cscan->scan.plan.targetlist = tlist;

	Assert(tlist == NIL);

	cscan->scan.plan.targetlist = copyObject(root->processed_tlist);
	cscan->custom_scan_tlist = cscan->scan.plan.targetlist;

	cscan->custom_private = best_path->custom_private;

	return (Plan *)cscan;
}

static CustomPathMethods mars_modifytable_path_methods = {
	.CustomName = "TS_ModifyTablePath",
	.PlanCustomPath = create_mars_modifytable_plan,
};

void mars_modifytable_tlist_fixup(Plan *plan)
{
	if (IsA(plan, CustomScan))
	{
		CustomScan *cscan = (CustomScan *)plan;

		if (cscan->methods == &create_mars_modifytable_plan_methods)
		{
			ModifyTable *mt = linitial_node(ModifyTable, cscan->custom_plans);

			if (mt->plan.targetlist == NIL)
			{
				cscan->custom_scan_tlist = NIL;
				cscan->scan.plan.targetlist = NIL;
			}
			else
			{
				int resno;
				ListCell *lc;

				cscan->custom_scan_tlist = mt->plan.targetlist;
				cscan->scan.plan.targetlist = NIL;

				foreach_with_count(lc, mt->plan.targetlist, resno)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);
					Var *v = makeVarFromTargetEntry(INDEX_VAR, tle);

					v->varattno = AttrOffsetGetAttrNumber(resno);
					cscan->scan.plan.targetlist = lappend(cscan->scan.plan.targetlist,
														  makeTargetEntry(&v->xpr, tle->resno, tle->resname, false));
				}
			}
		}
	}
}

Path *
create_mars_modifytable_path(PlannerInfo *root, ModifyTablePath *path, int subpath_idx)
{
	ListCell *lc;
	MARS_ModifyTablePath *mars_m_path;

	/*
	 * Since MARS only support columns as its group key, custom mt fall back if
	 * it contain any element is not a simple var.
	 */
	foreach (lc, path->onconflict->arbiterElems)
	{
		if (!IsA(lfirst(lc), InferenceElem) ||
			!IsA(lfirst_node(InferenceElem, lc)->expr, Var))
			return (Path *)path;
	}

	mars_m_path = palloc0(sizeof(MARS_ModifyTablePath));
	memcpy(&mars_m_path->cpath.path, &path->path, sizeof(Path));
	mars_m_path->cpath.path.type = T_CustomPath;
	mars_m_path->cpath.path.pathtype = T_CustomScan;
	mars_m_path->cpath.custom_paths = list_make1(path);
	mars_m_path->cpath.methods = &mars_modifytable_path_methods;
	mars_m_path->cpath.custom_private = lappend(mars_m_path->cpath.custom_private,
												copyObject(path->onconflict->arbiterElems));

	return &mars_m_path->cpath.path;
}

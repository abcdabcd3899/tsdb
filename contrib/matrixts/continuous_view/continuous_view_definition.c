/*-------------------------------------------------------------------------
 *
 * continuous_view_definition.c
 *	  Generalized codes to define a continuous view
 *
 * Create a continuous view is almose the same with creating a normal view except
 * 1) To store partially aggregated results, we create an auxiliary table with
 * customized SORTHEAP AM.
 * 2) the view is mocked to query from the auxiliary table.
 *
 * For example:
 * 		 create view cv
 * 		 with (continuous)
 * 		 as select avg(c2), max(c2) from target group by c1 + 1;
 *
 * We rely on two stage group aggregate mechanism to continuous aggregate tuples
 * from target table. The two-state GroupAggregate plan looks like:
 *
 *  Finalize GroupAggregate
 *    Output: avg(c2), max(c2), ((c1 + 1))
 *    ->  Gather Motion 3:1  (slice1; segments: 3)
 *        Output: ((c1 + 1)), (PARTIAL avg(c2)), (PARTIAL max(c2))
 *        Merge Key: ((c1 + 1))
 *        ->  Partial GroupAggregate
 *            Output: ((c1 + 1)), PARTIAL avg(c2), PARTIAL max(c2)
 *            ->  Sort
 *                Output: ((c1 + 1)), c2
 *                Sort Key: ((target.c1 + 1))
 *                ->  Seq Scan on public.target
 *                    Output: (c1 + 1), c2
 *
 * The auxiliary table is defined as:
 * 		cv_internal_storage((c1 + 1), PARTIAL avg(c2), PARTIAL max(c2))
 * which is the targetlist of partial aggregate.
 *
 * The view of normal view is mocked to:
 *
 *  SELECT
 *  avg(cv_internal_storage.internal_column_2) AS avg,
 *  max(cv_internal_storage.internal_column_3) AS max,
 *  FROM cv_internal_storage
 *  GROUP BY cv_internal_storage.internal_column_1;
 *
 * 	where
 * 		cv_internal_storage.internal_column_2 equal to PARTIAL avg(c2)
 * 		cv_internal_storage.internal_column_3 equal to PARTIAL max(c2)
 * 		cv_internal_storage.internal_column_1 equal to (c1 + 1)
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/matrixts/continuous_view/continuous_view_definition.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "tcop/utility.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_aggregate.h"
#include "catalog/toasting.h"
#include "catalog/pg_inherits.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "executor/spi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "continuous_view.h"

#define AUXILIARY_TABLE_SUFFIX "internal_storage"

static ProcessUtility_hook_type prevProcessUtility_hook = NULL;

/*
 * Create a list of columndef from the targetlist, columdefs are used to define
 * a relation or an index
 */
static List *
create_attrlist_from_tList(List *tlist)
{
	List *attrList = NIL;
	ListCell *lc;
	int idx;

	foreach_with_count(lc, tlist, idx)
	{
		TargetEntry *tle = (TargetEntry *)lfirst(lc);

		if (!tle->resname)
		{
			tle->resname = (char *)palloc(NAMEDATALEN);
			sprintf(tle->resname, "internal_column_%d", idx + 1);
		}

		if (tle->resjunk)
			elog(ERROR, "continuous view: unexpected junk target entry");

		ColumnDef *def = makeColumnDef(tle->resname,
									   exprType((Node *)tle->expr),
									   exprTypmod((Node *)tle->expr),
									   exprCollation((Node *)tle->expr));

		/*
		 * It's possible that the column is of a collatable type but the
		 * collation could not be resolved, so double-check.
		 */
		if (!type_is_collatable(exprType((Node *)tle->expr)))
			def->collOid = InvalidOid;

		attrList = lappend(attrList, def);
	}

	return attrList;
}

/* Helper function to create a view or a auxiliary table */
static ObjectAddress
define_simple_relation(RangeVar *relation,
					   List *parent,
					   char relkind,
					   char *tablespacename,
					   List *attrList,
					   List *distkeys,
					   char *accessMethod)
{
	ListCell *lc;
	CreateStmt *createStmt;
	ObjectAddress address;
	DistributedBy *distby = NULL;

	if (relkind == RELKIND_RELATION)
	{
		distby = makeNode(DistributedBy);
		distby->ptype = POLICYTYPE_PARTITIONED;
		distby->numsegments = getgpsegmentCount();

		foreach (lc, distkeys)
		{
			DistributionKeyElem *dkelem;
			ColumnDef *coldef;

			coldef = list_nth(attrList, lfirst_int(lc) - 1);
			dkelem = makeNode(DistributionKeyElem);

			dkelem->name = coldef->colname;
			dkelem->opclass = NULL; /* or should we explicitly set the opclass we just looked up? */
			dkelem->location = -1;

			distby->keyCols = lappend(distby->keyCols, dkelem);
		}
	}

	/* Define the view */
	createStmt = makeNode(CreateStmt);

	createStmt->ownerid = GetUserId();
	createStmt->relation = relation;
	createStmt->tableElts = attrList;
	createStmt->inhRelations = parent;
	createStmt->constraints = NIL;
	createStmt->options = NULL;
	createStmt->oncommit = ONCOMMIT_NOOP;
	createStmt->tablespacename = tablespacename;
	createStmt->relKind = relkind;
	createStmt->if_not_exists = false;
	createStmt->distributedBy = distby;
	createStmt->accessMethod = accessMethod;

	address = DefineRelation(createStmt,
							 relkind,
							 InvalidOid,
							 NULL,
							 NULL,
							 false,
							 true,
							 NULL);

	/* Make the new view relation visible */
	CommandCounterIncrement();

	return address;
}

static void
find_bottomvar(Var *top, Plan *plan, Var **bottom)
{
	Assert(IsA(top, Var));

	if (top->varno == OUTER_VAR)
	{
		TargetEntry *tle;

		plan = outerPlan(plan);

		if (IsA(plan, Append))
		{
			Append *append = (Append *)plan;

			plan = (Plan *)linitial(append->appendplans);
		}

		tle = (TargetEntry *)list_nth(plan->targetlist,
									  top->varattno - 1);

		if (IsA(tle->expr, Var))
			find_bottomvar((Var *)tle->expr, plan, bottom);
	}
	else if (!IS_SPECIAL_VARNO(top->varno))
		*bottom = top;
}

/*
 * Create an auxiliary table for the continuous view, the table uses the
 * CVSORTHEAP access method and store the partially aggregated/partially sorted
 * results.
 *
 * The schema of the auxiliary table is same with the output of partial agg.
 * eg: the view is defined as
 *
 * the two-stage plan like:
 *
 *  Finalize GroupAggregate
 *    Output: avg(c2), max(c2), ((c1 + 1))
 *    ->  Gather Motion 3:1  (slice1; segments: 3)
 *        Output: ((c1 + 1)), (PARTIAL avg(c2)), (PARTIAL max(c2))
 *        Merge Key: ((c1 + 1))
 *        ->  Partial GroupAggregate
 *            Output: ((c1 + 1)), PARTIAL avg(c2), PARTIAL max(c2)
 *            ->  Sort
 *                Output: ((c1 + 1)), c2
 *                Sort Key: ((target.c1 + 1))
 *                ->  Seq Scan on public.target
 *                    Output: (c1 + 1), c2
 *
 * The schema of auxiliary table will be
 * 		((c1 + 1)), PARTIAL avg(c2), PARTIAL max(c2)
 * which is the targetlist of partial aggregate.
 */
static ObjectAddress
create_cv_auxtable(Oid srcoid, List *parent, char *viewname, Query *viewdef, Oid namespace)
{
	char name[NAMEDATALEN];
	List *attrList;
	RangeVar *rangeVar;
	List *distkeys = NIL;
	CvPlannedStmt *cvplan;

	/*
	 * We need an auxiliary table namGed VIEWNAME_AUXILIARY_TABLE_SUFFIX to
	 * store the partial aggregate result. Check the length of the name, we
	 * prefer to use 'viewoid_AUXILIARY_TABLE_SUFFIX' pattern but the view is not
	 * created for now.
	 */
	if (snprintf(name, NAMEDATALEN, "%s_%d_%s", viewname, srcoid,
				 AUXILIARY_TABLE_SUFFIX) >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("the name of continuous view is too long")));

	rangeVar = makeNode(RangeVar);
	rangeVar->schemaname = get_namespace_name(namespace);
	rangeVar->relname = name;
	rangeVar->relpersistence = RELPERSISTENCE_PERMANENT;

	/* Get the two stage group aggregate plan */
	cvplan = continuous_view_planner(viewdef);

	/*
	 * Decide the distributed key for the auxiliary table.
	 *
	 * 1) random policy for view with plain aggregate
	 * 2) keep the policy same with target table if its distributed key is same
	 * with the group key of view.
	 */
	if (viewdef->groupClause)
	{
		RangeTblEntry *rte;
		GpPolicy *policy;

		rte = (RangeTblEntry *)linitial(viewdef->rtable);
		policy = GpPolicyFetch(rte->relid);

		if (GpPolicyIsHashPartitioned(policy))
		{
			int i;
			Agg *pagg = cvplan->partial_agg;

			/* Check the group key of partial agg */
			for (i = 0; i < pagg->numCols; i++)
			{
				TargetEntry *tle =
					list_nth(pagg->plan.targetlist, pagg->grpColIdx[i] - 1);

				/*
				 * If group keys contain a non-var expr, use random policy caus
				 * most of distributed key are refer to columns directly.
				 */
				if (!IsA(tle->expr, Var))
				{
					distkeys = NIL;
					break;
				}
				else
				{
					Var *bottom = NULL;

					/*
					 * After planning, Var are mostly likely down-linked to the
					 * targetlist of subplan, so find down through and get the
					 * columns referred in the bottom scan node.
					 */
					find_bottomvar((Var *)tle->expr, (Plan *)pagg, &bottom);

					if (bottom && bottom->varattno == policy->attrs[i])
						distkeys = lappend_int(distkeys, pagg->grpColIdx[i]);
					else
					{
						distkeys = NIL;
						break;
					}
				}

				/*
				 * If first ith group keys are match with distributed keys, ok
				 * to stop and use same distributed keys.
				 */
				if (policy->nattrs == i + 1)
					break;
			}
		}
		else if (GpPolicyIsReplicated(policy) || GpPolicyIsEntry(policy))
			elog(ERROR, "continuous view don't support replicated or catalog table");
	}

	attrList = create_attrlist_from_tList(cvplan->partial_agg->plan.targetlist);

	return define_simple_relation(rangeVar,
								  parent,
								  RELKIND_RELATION,
								  NULL,
								  attrList,
								  distkeys,
								  CVAMNAME);
}

/*
 * Create an index on the auxiliary table, the index key keep the same with the
 * group key of the view
 */
static void
create_cv_auxtable_index(ViewStmt *stmt, Oid auxoid, Query *viewdef)
{
	/* define a special sortheap_btree index on the auxiliary table */
	int i;
	Agg *pagg;
	List *attrList;
	IndexStmt *index;
	CvPlannedStmt *cvplan;

	index = makeNode(IndexStmt);
	index->idxname = NULL;
	/* the relation built on */
	index->relation = makeRangeVar("matrixts_internal", get_rel_name(auxoid), -1);
	index->accessMethod = "sortheap_btree";

	cvplan = continuous_view_planner(viewdef);
	pagg = cvplan->partial_agg;

	attrList = create_attrlist_from_tList(pagg->plan.targetlist);
	for (i = 0; i < pagg->numCols; i++)
	{
		IndexElem *elem = makeNode(IndexElem);
		ColumnDef *column = (ColumnDef *)list_nth(attrList, pagg->grpColIdx[i] - 1);

		elem->name = column->colname;
		elem->expr = NULL;

		index->indexParams = lappend(index->indexParams, elem);
	}

	/*
	 * If it is a plain aggregate without group key, use a fake dummy const key
	 * for it.
	 */
	if (!index->indexParams)
	{
		IndexElem *elem = makeNode(IndexElem);

		elem->name = NULL;
		elem->expr = (Node *)makeConst(BOOLOID,
									   -1,
									   InvalidOid,
									   sizeof(bool),
									   BoolGetDatum(true),
									   false,
									   true);

		index->indexParams = lappend(index->indexParams, elem);
	}

	DefineIndex(auxoid,
				index,
				InvalidOid,
				InvalidOid,
				InvalidOid,
				false,
				false,
				false,
				true,
				true,
				false);
}

/*
 * Create a continuous view, it is actually a normal view, however, the view
 * definition is mocked:
 *
 * 1. the range table is changed to refer the auxiliary table.
 * 2. the targetlist is changed to refer the auxiliary table.
 * 3. the filter is removed.
 */
static ObjectAddress
create_continuous_view(ViewStmt *stmt, Query *original, Oid auxtable)
{
	Agg *fagg = NULL;
	List *attrList;
	Query *mockviewdef;
	ListCell *lc;
	ListCell *lc1;
	FromExpr *fromexpr;
	RangeTblEntry *rte;
	ObjectAddress address;
	CvPlannedStmt *cvplan;

	/* Make a copy to don't modify the original definition */
	mockviewdef = copyObject(original);

	/*
	 * Change the range table to use the auxiliary table
	 */
	Assert(list_length(mockviewdef->rtable) == 1);
	rte = linitial_node(RangeTblEntry, mockviewdef->rtable);
	rte->relid = auxtable;

	/*
	 * Remove the where clause
	 */
	fromexpr = mockviewdef->jointree;
	fromexpr->quals = NULL;

	/*
	 * Adjust the targetlist to refer the auxiliary table, we always do the
	 * continuous view planner on the original query
	 */
	cvplan = continuous_view_planner(original);
	fagg = cvplan->final_agg;

	/*
	 * the rtable is changed, we need to convert the vars in the targetlist of
	 * view definition, we can adjust it based on the targetlist of final aggnode
	 * because its vars are refering to the output of partial aggnode which is
	 * exactly the same schema of auxiliary table
	 */
	forboth(lc, mockviewdef->targetList, lc1, fagg->plan.targetlist)
	{
		TargetEntry *to = lfirst_node(TargetEntry, lc);
		TargetEntry *from = lfirst_node(TargetEntry, lc1);
		List *vars;
		ListCell *cell;
		Expr *old = to->expr;
		Var *var;

		/*
		 * Replace the tle expression of viewdef with the one of final aggnode,
		 * this makes the Vars has the right varattno.
		 */
		to->expr = from->expr;

		/*
		 * Restore the varnoold and varoattno
		 */
		if (IsA(to->expr, Var))
		{
			((Var *)to->expr)->varnoold = ((Var *)old)->varnoold;
			((Var *)to->expr)->varoattno = ((Var *)to->expr)->varattno;
		}

		/*
		 * Restore the aggsplit type because AGGSPLIT_INITIAL_SERIAL is not
		 * expected in the expression of parsetree before planning.
		 */
		if (IsA(to->expr, Aggref))
			((Aggref *)to->expr)->aggsplit = AGGSPLIT_SIMPLE;

		vars = pull_var_clause((Node *)to->expr, PVC_RECURSE_AGGREGATES);

		foreach (cell, vars)
		{
			var = lfirst_node(Var, cell);
			var->varno = 1;
		}
	}

	attrList = create_attrlist_from_tList(mockviewdef->targetList);

	address = define_simple_relation(stmt->view,
									 NIL,
									 RELKIND_VIEW,
									 NULL,
									 attrList,
									 NULL,
									 NULL);

	/* Store the query for the view */
	StoreViewQuery(address.objectId, mockviewdef, false);

	return address;
}

static void
continuous_view_sanitycheck(Query *select)
{
	Node *jtnode;
	ListCell *lc;
	RangeTblEntry *rte;

	if (select->commandType != CMD_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous view disallows non-select commands")));

	/* Bellow cases are too complex for fast path */
	if (select->scatterClause ||
		select->hasSubLinks ||
		select->hasModifyingCTE ||
		select->hasWindowFuncs ||
		select->groupingSets ||
		select->havingQual ||
		select->limitCount ||
		select->sortClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous view disallows orderby/sublink/with/scatter/having/window/limit/groupingsets")));

	if (!select->hasAggs &&
		!select->groupClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous view only supports continuous aggregation")));

	/* We only consider simple FROM RELATION case */
	if (list_length(select->jointree->fromlist) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous view disallows multiple target relations")));

	jtnode = linitial(select->jointree->fromlist);

	if (!IsA(jtnode, RangeTblRef))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous view disallows multiple target relations")));

	rte = rt_fetch(((RangeTblRef *)jtnode)->rtindex, select->rtable);

	if (rte->rtekind != RTE_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous view disallows non-relation target (eg: subquery)")));

	/* Check the combination of the sortClause and groupClause */
	foreach (lc, select->targetList)
	{
		TargetEntry *tle = (TargetEntry *)lfirst(lc);

		if (select->groupClause)
		{
			/*
			 * The TargetEntry must be the group key or an aggregate function,
			 * we only need to check that all target entry are not junk.
			 */
			if (tle->resjunk)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("continuous view must have group/sort key specified within targetlist")));
		}
		else
		{
			if (!select->hasAggs) /* must be Plain Agg */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("continuous view only supports continuous aggregation or continuous sorting")));
		}
	}

	/* Check the aggref has aggtransfn and aggcombinefn */
	if (select->hasAggs)
	{
		foreach (lc, select->targetList)
		{
			HeapTuple aggTuple;
			TargetEntry *tle;
			Form_pg_aggregate aggform;

			tle = (TargetEntry *)lfirst(lc);

			if (!IsA(tle->expr, Aggref))
				continue;

			aggTuple = SearchSysCache1(AGGFNOID,
									   ObjectIdGetDatum(((Aggref *)tle->expr)->aggfnoid));

			aggform = (Form_pg_aggregate)GETSTRUCT(aggTuple);

			if (!OidIsValid(aggform->aggcombinefn) ||
				((Aggref *)tle->expr)->aggorder != NIL ||
				((Aggref *)tle->expr)->aggfilter != NULL ||
				((Aggref *)tle->expr)->aggdistinct != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("continuous view only supports aggregation functions can be partially aggregated")));
			ReleaseSysCache(aggTuple);
		}
	}

	if (select->groupClause && !grouping_is_sortable(select->groupClause))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous view only supports sortable group keys")));
}

static Oid
continuous_view_target_relation(Query *select)
{
	Node *jtnode;
	RangeTblEntry *rte;

	jtnode = linitial(select->jointree->fromlist);

	rte = rt_fetch(((RangeTblRef *)jtnode)->rtindex, select->rtable);

	return rte->relid;
}

static void
define_continuous_view_internal(Oid srcoid,
								Oid viewoid,
								List *parent_auxname,
								ViewStmt *stmt,
								char *viewname,
								Query *viewdef,
								Oid *r_viewoid,
								Oid *r_auxoid)
{
	ObjectAddress auxaddr;
	ObjectAddress viewaddr;
	ObjectAddress srcaddr;

	/*
	 * viewname can be directly assigned or fetched from stmt.
	 * Anyway, the viewname must not be empty.
	 */
	if (stmt)
		viewname = stmt->view->relname;
	Assert(viewname != NULL);
	Assert(stmt != NULL || viewoid != InvalidOid);

	/*
	 * Create a auxiliaty table for continuous view, all tuples are actually
	 * stored on the auxiliary table
	 */
	auxaddr = create_cv_auxtable(srcoid, parent_auxname, viewname, viewdef,
								 get_namespace_oid("matrixts_internal", false));

	if (r_auxoid)
		*r_auxoid = auxaddr.objectId;

	/* Create a toast table for aux table */
	NewRelationCreateToastTable(auxaddr.objectId, (Datum)0);

	/* Only create continuous view for parent source table */
	if (viewoid == InvalidOid)
	{
		viewaddr = create_continuous_view(stmt, viewdef, auxaddr.objectId);

		/* Create a dependence between the view and auxiliary table */
		recordDependencyOn(&auxaddr, &viewaddr, DEPENDENCY_NORMAL);
		recordDependencyOn(&auxaddr, &viewaddr, DEPENDENCY_INTERNAL);

		/* Create a dependence between the view and source table */
		srcaddr.classId = RelationRelationId;
		srcaddr.objectId = srcoid;
		srcaddr.objectSubId = 0;

		recordDependencyOn(&viewaddr, &srcaddr, DEPENDENCY_NORMAL);

		if (r_viewoid)
			*r_viewoid = viewaddr.objectId;
	}
	else
	{
		viewaddr.classId = RelationRelationId;
		viewaddr.objectId = viewoid;
		viewaddr.objectSubId = 0;

		/* Create a dependence between the view and auxiliary table */
		recordDependencyOn(&auxaddr, &viewaddr, DEPENDENCY_NORMAL);
		recordDependencyOn(&auxaddr, &viewaddr, DEPENDENCY_INTERNAL);
	}
}

/*
 * A continuous view consists of three part:
 *
 * 1. A auxiliary table to store the real materialized partial aggregated/sorted data
 * 2. A normal view with the mocked view definition.
 * 3. A record in the matrixts_internal.continuous_views to identify the continuous view
 */
static void
define_continuous_views(ViewStmt *stmt, const char *queryString,
						int stmt_location, int stmt_len,
						bool is_populate)
{
	Oid srcoid;
	Oid auxoid;
	Oid parent_viewoid;
	char *targetName;
	char *targetNameSpace;
	const char *query_viewdef = debug_query_string ? debug_query_string : "unknown";
	bool create_trigger = false;
	List *all_auxoids = NIL;
	Query *viewdef;
	RawStmt *rawstmt;
	Relation srcrel;
	ViewStmt *dispatchStmt;
	StringInfoData str;
	List *saved_options = stmt->options;

	stmt->options = NULL;
	rawstmt = makeNode(RawStmt);
	rawstmt->stmt = (Node *)copyObject(stmt->query);
	rawstmt->stmt_location = stmt_location;
	rawstmt->stmt_len = stmt_len;
	viewdef = parse_analyze(rawstmt, queryString, NULL, 0, NULL);

	/* Do sanity check and ereport if fails */
	continuous_view_sanitycheck(viewdef);

	srcoid = continuous_view_target_relation(viewdef);
	targetName = get_rel_name(srcoid);

	srcrel = table_open(srcoid, ExclusiveLock);
	targetNameSpace = get_namespace_name(srcrel->rd_rel->relnamespace);
	/*
	 * For partition table, only one continuous view is defined, however, we
	 * create BEFORE ROW TRIGGER on each child table and create an auxiliary
	 * table for each child table, all auxiliary tables are inherited from
	 * a parent auxiliary table.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		initStringInfo(&str);
		SPI_connect();

		/* Check whether another continuous view already defined on the source table */
		/* Insert a record in the matrixts_internal.continuous_views */
		appendStringInfo(&str,
						 "SELECT * FROM matrixts_internal.continuous_views"
						 " WHERE srcoid = %d LIMIT 1",
						 srcoid);

		if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
			elog(ERROR, "Cannot exec query from continuous view");

		create_trigger = (SPI_processed == 0);
	}

	if (srcrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		List *parent_auxname;
		List *all_children_list;
		ListCell *lc;

		/* first define a parent auxiliary table and create the view  */
		define_continuous_view_internal(srcoid,
										InvalidOid,
										NIL,
										stmt,
										NULL,
										viewdef,
										&parent_viewoid,
										&auxoid);
		all_auxoids = lappend_oid(all_auxoids, auxoid);

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			resetStringInfo(&str);
			/* Insert a record in the matrixts_internal.continuous_views */
			appendStringInfo(&str,
							 "INSERT INTO matrixts_internal.continuous_views"
							 " VALUES (%d, %s, %d, %s, %d, %s, %s, %s, %s)",
							 srcoid, quote_literal_cstr(targetName),
							 parent_viewoid,
							 quote_literal_cstr(stmt->view->relname),
							 auxoid, quote_literal_cstr(get_rel_name(auxoid)),
							 quote_literal_cstr(nodeToString(viewdef)),
							 quote_literal_cstr(query_viewdef), "true");

			if (SPI_exec(str.data, 0) != SPI_OK_INSERT)
				elog(ERROR, "Cannot insert record for continuous view");
		}

		parent_auxname =
			list_make1(makeRangeVar("matrixts_internal", get_rel_name(auxoid), -1));

		all_children_list =
			find_all_inheritors(srcrel->rd_id, ExclusiveLock, NULL);

		foreach (lc, all_children_list)
		{
			Oid pkrelid = lfirst_oid(lc);

			/* skip intermediate partitions, we're only interested in leaves */
			if (get_rel_relkind(pkrelid) != RELKIND_RELATION)
				continue;

			targetName = get_rel_name(pkrelid);

			define_continuous_view_internal(pkrelid,
											parent_viewoid,
											parent_auxname,
											stmt,
											NULL,
											viewdef,
											NULL,
											&auxoid);

			if (Gp_role == GP_ROLE_DISPATCH)
			{
				all_auxoids = lappend_oid(all_auxoids, auxoid);

				/* Create a trigger on the targetOid table */
				if (create_trigger)
				{
					resetStringInfo(&str);
					appendStringInfo(&str,
									 "CREATE TRIGGER continuous_view_trigger"
									 " BEFORE INSERT"
									 " ON %s"
									 " FOR EACH ROW"
									 " EXECUTE PROCEDURE matrixts_internal.continuous_view_insert_trigger()",
									 quote_qualified_identifier(targetNameSpace, targetName));

					if (SPI_exec(str.data, 0) != SPI_OK_UTILITY)
						elog(ERROR, "Cannot create trigger for continuous view");
				}

				resetStringInfo(&str);

				appendStringInfo(&str,
								 "INSERT INTO matrixts_internal.continuous_views"
								 " VALUES (%d, %s, %d, %s, %d, %s, %s, %s, %s)",
								 pkrelid, quote_literal_cstr(targetName),
								 parent_viewoid,
								 quote_literal_cstr(stmt->view->relname),
								 auxoid, quote_literal_cstr(get_rel_name(auxoid)),
								 quote_literal_cstr(nodeToString(viewdef)),
								 quote_literal_cstr(query_viewdef), "false");

				if (SPI_exec(str.data, 0) != SPI_OK_INSERT)
					elog(ERROR, "Cannot insert record for continuous view");
			}
		}
	}
	else
	{
		define_continuous_view_internal(srcoid,
										InvalidOid,
										NIL,
										stmt,
										NULL,
										viewdef,
										&parent_viewoid,
										&auxoid);

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			all_auxoids = lappend_oid(all_auxoids, auxoid);

			/* Create a trigger on the targetOid table */
			if (create_trigger)
			{
				resetStringInfo(&str);
				appendStringInfo(&str,
								 "CREATE TRIGGER continuous_view_trigger"
								 " BEFORE INSERT"
								 " ON %s"
								 " FOR EACH ROW"
								 " EXECUTE PROCEDURE matrixts_internal.continuous_view_insert_trigger()",
								 quote_qualified_identifier(targetNameSpace, targetName));

				if (SPI_exec(str.data, 0) != SPI_OK_UTILITY)
					elog(ERROR, "Cannot create trigger for continuous view");
			}

			/* Insert a record in the matrixts_internal.continuous_views */
			resetStringInfo(&str);
			appendStringInfo(&str,
							 "INSERT INTO matrixts_internal.continuous_views"
							 " VALUES (%d, %s, %d, %s, %d, %s, %s, %s, %s)",
							 srcoid, quote_literal_cstr(targetName),
							 parent_viewoid,
							 quote_literal_cstr(stmt->view->relname),
							 auxoid, quote_literal_cstr(get_rel_name(auxoid)),
							 quote_literal_cstr(nodeToString(viewdef)),
							 quote_literal_cstr(query_viewdef), "true");

			if (SPI_exec(str.data, 0) != SPI_OK_INSERT)
				elog(ERROR, "Cannot insert record for continuous view");
		}
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		ListCell *lc;

		stmt->options = saved_options;
		dispatchStmt = (ViewStmt *)copyObject(stmt);
		CdbDispatchUtilityStatement((Node *)dispatchStmt,
									DF_CANCEL_ON_ERROR |
										DF_WITH_SNAPSHOT |
										DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);

		/*
		 * create index for the auxiliary table
		 *
		 * Caution: this must be called after the CreateView statement dispatched
		 * to the QEs.
		 */
		foreach (lc, all_auxoids)
			create_cv_auxtable_index(stmt, lfirst_oid(lc), viewdef);

		/*
		 * Call rebuild_continuous_view to populate the existing tuples.
		 */
		if (is_populate)
		{
			resetStringInfo(&str);
			appendStringInfo(&str,
							 "SELECT matrixts_internal.rebuild_continuous_view('%s')",
							 quote_qualified_identifier(stmt->view->schemaname,
														stmt->view->relname));

			if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
				elog(ERROR, "Cannot populate continuous view");
		}

		SPI_finish();
	}

	table_close(srcrel, NoLock);
}

/*
 * Inherit continuous views, need such steps:
 *
 * 1. select definition of continuous views that bind on parent partition.
 * 2. use this definition to create a new continuous view on the sub partition table.
 * 3. create aux table for the new sub partition table.
 * 4. create a trigger that redirects inserts data.
 * 5. update continuous view meta data.
 * 6. do populate, which means update raw data from sub-table data to its aux table.
 *
 */
static void
inherit_continuous_view(RawStmt *stmt, RangeVar *child_relname, RangeVar *parent_relname)
{
	Oid parent_relid;
	StringInfoData str;

	parent_relid = RangeVarGetRelid(parent_relname, AccessShareLock, true);

	SPI_connect();
	/*
	 * Select aux table oid which created on target table.
	 * Each table may have one or more continuous views.
	 */
	initStringInfo(&str);
	appendStringInfo(&str,
					 "SELECT viewoid, viewname, querytree, querytext, auxoid FROM matrixts_internal.continuous_views "
					 "WHERE srcoid = %d AND toplevel = true",
					 parent_relid);

	if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "Cannot exec query from continuous view");

	/* If parent table has continuous views.*/
	if (SPI_processed > 0)
	{
		int i;
		List *l_viewoids = NIL;
		List *l_viewnames = NIL;
		List *l_viewdefs = NIL;
		List *l_rawquery = NIL;
		List *l_auxoids = NIL;
		List *l_sub_auxoids = NIL;
		List *parent_auxname = NIL;
		ListCell *lcv;
		ListCell *lcn;
		ListCell *lcq;
		ListCell *lcd;
		ListCell *lca;
		Query *viewdef;
		Oid child_relid;
		Oid auxoid;
		Oid viewoid;
		Oid parent_auxoid;
		char *viewname;
		char *rawquery;
		char *targetNameSpace;
		Relation child_rel;

		for (i = 0; i < SPI_processed; i++)
		{
			TupleDesc tupdesc = SPI_tuptable->tupdesc;
			HeapTuple tuple = SPI_tuptable->vals[i];

			l_viewoids = lappend_oid(l_viewoids,
									 atoi(SPI_getvalue(tuple, tupdesc, 1)));
			l_viewnames = lappend(l_viewnames,
								  SPI_getvalue(tuple, tupdesc, 2));
			l_viewdefs = lappend(l_viewdefs,
								 stringToNode(SPI_getvalue(tuple, tupdesc, 3)));
			l_rawquery = lappend(l_rawquery,
								 SPI_getvalue(tuple, tupdesc, 4));
			l_auxoids = lappend_oid(l_auxoids,
									atoi(SPI_getvalue(tuple, tupdesc, 5)));
		}

		child_relid = RangeVarGetRelid(child_relname, AccessShareLock, true);
		child_rel = table_open(child_relid, ExclusiveLock);
		targetNameSpace = get_namespace_name(child_rel->rd_rel->relnamespace);
		table_close(child_rel, NoLock);

		/* Inherit and create continuous views on QD and QE. */
		forfive(lcv, l_viewoids, lcn, l_viewnames, lcd, l_viewdefs,
				lcq, l_rawquery, lca, l_auxoids)
		{
			viewoid = lfirst_oid(lcv);
			viewname = lfirst(lcn);
			viewdef = lfirst(lcd);
			rawquery = lfirst(lcq);
			parent_auxoid = lfirst_oid(lca);

			parent_auxname =
				list_make1(makeRangeVar("matrixts_internal",
										get_rel_name(parent_auxoid),
										-1));

			define_continuous_view_internal(child_relid,
											viewoid,
											parent_auxname,
											NULL,
											viewname,
											viewdef,
											NULL,
											&auxoid);
			l_sub_auxoids = lappend_oid(l_sub_auxoids, auxoid);
		}

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			RawStmt *dispatchStmt;
			forfive(lcv, l_viewoids, lcn, l_viewnames, lcd, l_viewdefs,
					lcq, l_rawquery, lca, l_sub_auxoids)
			{
				viewoid = lfirst_oid(lcv);
				viewname = lfirst(lcn);
				viewdef = lfirst(lcd);
				rawquery = lfirst(lcq);
				auxoid = lfirst_oid(lca);

				resetStringInfo(&str);

				/* Insert a record in the matrixts_internal.continuous_views */
				appendStringInfo(&str,
								 "INSERT INTO matrixts_internal.continuous_views"
								 " VALUES (%d, %s, %d, %s, %d, %s, %s, %s, %s)",
								 child_relid,
								 quote_literal_cstr(child_relname->relname),
								 viewoid,
								 quote_literal_cstr(viewname),
								 auxoid,
								 quote_literal_cstr(get_rel_name(auxoid)),
								 quote_literal_cstr(nodeToString(viewdef)),
								 quote_literal_cstr(rawquery),
								 "false");

				if (SPI_exec(str.data, 0) != SPI_OK_INSERT)
					elog(ERROR, "Cannot insert record for continuous view");
			}

			/* Create trigger on sub partition table. */
			resetStringInfo(&str);
			appendStringInfo(&str,
							 "CREATE TRIGGER continuous_view_trigger"
							 " BEFORE INSERT"
							 " ON %s"
							 " FOR EACH ROW"
							 " EXECUTE PROCEDURE matrixts_internal.continuous_view_insert_trigger()",
							 quote_qualified_identifier(targetNameSpace,
														child_relname->relname));

			if (SPI_exec(str.data, 0) != SPI_OK_UTILITY)
				elog(ERROR, "Cannot create trigger for continuous view");

			dispatchStmt = copyObject(stmt);

			CdbDispatchUtilityStatement((Node *)dispatchStmt,
										DF_CANCEL_ON_ERROR |
											DF_WITH_SNAPSHOT |
											DF_NEED_TWO_PHASE,
										GetAssignedOidsForDispatch(),
										NULL);

			forthree(lcv, l_viewoids, lcd, l_viewdefs, lca, l_sub_auxoids)
			{
				viewoid = lfirst_oid(lcv);
				viewdef = lfirst(lcd);
				auxoid = lfirst_oid(lca);
				create_cv_auxtable_index(NULL, auxoid, viewdef);

				/* Default to do populate. */
				resetStringInfo(&str);
				appendStringInfo(&str,
								 "SELECT matrixts_internal.rebuild_continuous_view(%d::regclass)",
								 viewoid);

				if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
					elog(ERROR, "Cannot populate continuous view");
			}
		}
	}

	SPI_finish();
}

/*
 * Cleanup specified records for the continuous view, we only drop trigger and
 * delete record in matrixts_internal.continuous_views here. For auxiliary
 * table, it will be dropped as an depend object of a normal view.
 */
static void
cleanup_continuous_view(Oid viewoid, Oid srcoid)
{
	bool drop_trigger = false;
	StringInfoData str;

	/* Delete Trigger and Records */
	SPI_connect();

	initStringInfo(&str);

	/* Check whether we are the last continuous view on the source table */
	appendStringInfo(&str,
					 "SELECT * FROM matrixts_internal.continuous_views "
					 "WHERE srcoid = %d and viewoid != %d LIMIT 1",
					 srcoid, viewoid);

	if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "Cannot drop trigger for continuous view");

	drop_trigger = (SPI_processed == 0);

	if (drop_trigger)
	{
		char *nspname;
		Relation srcrel;

		srcrel = table_open(srcoid, AccessShareLock);
		nspname = get_namespace_name(srcrel->rd_rel->relnamespace);

		resetStringInfo(&str);
		appendStringInfo(&str,
						 "DROP TRIGGER IF EXISTS continuous_view_trigger on %s",
						 quote_qualified_identifier(nspname, RelationGetRelationName(srcrel)));

		table_close(srcrel, AccessShareLock);
		if (SPI_exec(str.data, 0) != SPI_OK_UTILITY)
			elog(ERROR, "Cannot drop trigger for continuous view");
	}

	resetStringInfo(&str);
	appendStringInfo(&str,
					 "DELETE FROM matrixts_internal.continuous_views "
					 "WHERE viewoid = %d",
					 viewoid);

	if (SPI_exec(str.data, 0) != SPI_OK_DELETE)
		elog(ERROR, "Cannot delete record for continuous view");

	SPI_finish();
}

static bool
aux_table_exists(RangeVar *relation)
{
	bool exists = false;
	StringInfoData str;

	SPI_connect();

	initStringInfo(&str);

	/* Check if aux table exists. */
	appendStringInfo(&str,
					 "SELECT * FROM matrixts_internal.continuous_views "
					 "WHERE auxname = %s",
					 quote_literal_cstr(relation->relname));

	if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "Cannot exec query from continuous view");

	/* Aux table name is made of view name and table oid. So there should be at
	 * most one aux table. */
	Assert(SPI_processed <= 1);

	exists = (SPI_processed == 1);

	SPI_finish();

	return exists;
}

/* -------------------------------------------------------------------
 * Utility Process Hook to hijack the creation of continuous view.
 * -------------------------------------------------------------------
 */

/*
 * Hijack the DDL statements.
 *
 * Continuous View use the same creation syntax with normal View except that
 * continuous view has a 'continuous' option in WITH.
 *
 * If continuous is found in WITH, call customized creation routinue.
 * If it's not Continuous View statement
 * 1) call prevProcessUtility_hook if exists and quit.
 * 2) or call standard_ProcessUtility.
 */
static void
ContinuousViewProcessUtilityHook(PlannedStmt *pstmt,
								 const char *queryString,
								 ProcessUtilityContext context,
								 ParamListInfo params,
								 QueryEnvironment *queryEnv,
								 DestReceiver *dest, char *completionTag)
{
	Node *parsetree = pstmt->utilityStmt;

	/* Hijack Create View */
	if (IsA(parsetree, ViewStmt))
	{
		ViewStmt *vs = (ViewStmt *)parsetree;
		bool is_continuous_view = false;
		bool is_populate = true;
		DefElem *def;
		ListCell *cur_item;
		ListCell *prev_item;

		/* Check if options contain "continuous" */
		foreach (cur_item, vs->options)
		{
			def = (DefElem *)lfirst(cur_item);

			if (strcmp(def->defname, "continuous") == 0 &&
				defGetBoolean(def) == true)
			{
				is_continuous_view = true;
			}
			else if (strcmp(def->defname, "populate") == 0 &&
					 defGetBoolean(def) == false)
			{
				is_populate = false;
			}
		}

		if (is_continuous_view)
		{
			Oid npoid = LookupNamespaceNoError("matrixts_internal");

			if (npoid == InvalidOid)
				elog(ERROR, "You must create extension matrixts to use continuous view");

			define_continuous_views(vs,
									queryString,
									pstmt->stmt_location,
									pstmt->stmt_len,
									is_populate);
			return;
		}

		/*
		 * If not a continuous view, standard ProcessUtility will process the
		 * DDL, we need to remove continuous/population options, because they
		 * are not standard hard-coded options.
		 */
		prev_item = NULL;
		cur_item = list_head(vs->options);

		while (cur_item != NULL)
		{
			ListCell *next_item = lnext(cur_item);

			def = (DefElem *)lfirst(cur_item);

			if (strcmp(def->defname, "continuous") == 0)
			{
				vs->options = list_delete_cell(vs->options,
											   cur_item,
											   prev_item);
				cur_item = next_item;
				continue;
			}
			else if (strcmp(def->defname, "populate") == 0)
			{
				vs->options = list_delete_cell(vs->options,
											   cur_item,
											   prev_item);
				cur_item = next_item;
				continue;
			}

			prev_item = cur_item;
			cur_item = next_item;
		}
	}

	/* Hijack Drop View */
	if (IsA(parsetree, DropStmt) &&
		((DropStmt *)parsetree)->removeType == OBJECT_VIEW)
	{
		DropStmt *ds = (DropStmt *)parsetree;

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			ListCell *lc;
			int flags = DF_CANCEL_ON_ERROR | DF_NEED_TWO_PHASE | DF_WITH_SNAPSHOT;

			CdbDispatchUtilityStatement((Node *)ds,
										flags,
										NIL,
										NULL);

			foreach (lc, ds->objects)
			{
				int i;
				Oid npoid;
				Oid viewoid;
				List *l_srcoids = NIL;
				ListCell *srccell;
				RangeVar *viewrel;
				StringInfoData str;

				npoid = LookupExplicitNamespace("matrixts_internal", true);
				if (npoid == InvalidOid)
					continue;

				viewrel = makeRangeVarFromNameList((List *)lfirst(lc));
				viewoid = RangeVarGetRelid(viewrel, AccessShareLock, true);

				if (viewoid == InvalidOid)
					continue;

				SPI_connect();

				/*
				 * Judge whether the target materialized view is a continuous view by
				 * searching the matrixts_internal.continuous_view
				 */
				initStringInfo(&str);
				appendStringInfo(&str,
								 "SELECT srcoid, auxoid FROM matrixts_internal.continuous_views "
								 "WHERE viewoid = %i",
								 viewoid);

				if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
					elog(ERROR, "Cannot exec query from continuous view");

				for (i = 0; i < SPI_processed; i++)
				{
					TupleDesc tupdesc = SPI_tuptable->tupdesc;
					HeapTuple tuple = SPI_tuptable->vals[i];

					l_srcoids =
						lappend_oid(l_srcoids, atoi(SPI_getvalue(tuple, tupdesc, 1)));
				}

				foreach (srccell, l_srcoids)
					cleanup_continuous_view(viewoid, lfirst_oid(srccell));

				SPI_finish();
			}
		}

		RemoveRelations(ds);

		return;
	}

	/* Hijack Attach Partition and Alter Expand*/
	if (IsA(parsetree, AlterTableStmt))
	{
		Oid npoid;
		ListCell *lc;
		AlterTableStmt *ats;

		ats = (AlterTableStmt *)parsetree;

		/* If matrixts is not exist. */
		npoid = LookupNamespaceNoError("matrixts_internal");
		if (npoid != InvalidOid)
		{
			foreach (lc, ats->cmds)
			{
				AlterTableCmd *atc;

				/* only AlterTableCmd */
				if (!IsA(lfirst(lc), AlterTableCmd))
					continue;

				atc = (AlterTableCmd *)lfirst(lc);

				switch (atc->subtype)
				{
				case AT_AttachPartition:
				{
					RangeVar *child_relation;
					/* Judge whether cmd is ATTACH PARTITION. */
					if (!IsA(atc->def, PartitionCmd))
						break;

					child_relation = ((PartitionCmd *)atc->def)->name;

					if (Gp_role == GP_ROLE_DISPATCH)
					{
						standard_ProcessUtility(pstmt, queryString,
												context, params, queryEnv,
												dest, completionTag);
					}
					else if (Gp_role == GP_ROLE_EXECUTE)
					{
						/*
						 * Has no superclass means the relation is beging created but
						 * haven`t created yet.
						 */
						if (!has_superclass(
								RangeVarGetRelid(child_relation, AccessShareLock, true)))
						{
							standard_ProcessUtility(pstmt, queryString,
													context, params, queryEnv,
													dest, completionTag);

							/* Create relation done, go back inherit continuous views. */
							return;
						}
					}

					/*
					 * Inherit continuous views from parent table.
					 */
					inherit_continuous_view((RawStmt *)ats, child_relation, ats->relation);

					return;
				}
				case AT_ExpandTable:
				{
					if (aux_table_exists(ats->relation))
					{
						atc->subtype = AT_SetDistributedBy;

						DistributedBy *distributedBy;

						distributedBy = makeNode(DistributedBy);
						distributedBy->ptype = POLICYTYPE_PARTITIONED;
						distributedBy->keyCols = NIL;
						distributedBy->numsegments = GP_POLICY_DEFAULT_NUMSEGMENTS();
						atc->def = (Node *)list_make2(NULL, distributedBy);
					}

					break;
				}
				default:
					break;
				}
			}
		}
	}

	if (IsA(parsetree, CreateStmt))
	{
		/*
		 * Create table on QD and QE first,
		 * then create continuous view and aux table.
		 */
		CreateStmt *cs = (CreateStmt *)parsetree;
		Oid npoid;
		ListCell *lc;
		RangeVar *rv;

		/* If matrixts exists. */
		npoid = LookupNamespaceNoError("matrixts_internal");
		if (npoid != InvalidOid)
		{
			foreach (lc, cs->inhRelations)
			{
				if (!IsA(lfirst(lc), RangeVar))
					break;

				rv = (RangeVar *)lfirst(lc);

				if (rv->relpersistence != RELKIND_PARTITIONED_TABLE)
					continue;

				if (Gp_role == GP_ROLE_DISPATCH)
				{
					standard_ProcessUtility(pstmt, queryString,
											context, params, queryEnv,
											dest, completionTag);
				}
				else if (Gp_role == GP_ROLE_EXECUTE)
				{
					/* InvalidOid means relation haven`t been created. */
					if (RangeVarGetRelid(cs->relation, AccessShareLock, true) ==
						InvalidOid)
					{
						standard_ProcessUtility(pstmt, queryString,
												context, params, queryEnv,
												dest, completionTag);

						/*
						 * Create relation done, go back QD,
						 * and keep going to inherit continuous views.
						 */
						return;
					}
				}

				inherit_continuous_view((RawStmt *)parsetree, cs->relation, rv);

				return;
			}
		}
	}

	/* Not a continuous view, we must call one "ProcessUtility" routine */
	if (prevProcessUtility_hook)
		(*prevProcessUtility_hook)(pstmt, queryString,
								   context, params, queryEnv,
								   dest, completionTag);
	else
		standard_ProcessUtility(pstmt, queryString,
								context, params, queryEnv,
								dest, completionTag);
}

void init_cv_processutility_hook(void)
{
	prevProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = ContinuousViewProcessUtilityHook;
}

void fini_cv_processutility_hook(void)
{
	ProcessUtility_hook = prevProcessUtility_hook;
}

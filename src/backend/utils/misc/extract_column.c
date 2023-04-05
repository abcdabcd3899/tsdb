/*
 * ----------------------------------------------
 *
 * extract_column.c
 * 		walk through a targetlist or a qual node to find the columns
 * 		referred.
 *
 *  * Portions Copyright (c) 2009-2010, Greenplum Inc.
 *  * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 * ------------------------------------------------
 */
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "parser/parse_relation.h"
#include "utils/extract_column.h"

static bool
extractcolumns_walker(Node *node, struct ExtractcolumnContext *ecCtx)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;

		if (IS_SPECIAL_VARNO(var->varno))
			return false;

		if (var->varattno > 0 && var->varattno <= ecCtx->natts)
		{
			ecCtx->cols[var->varattno -1] = true;
			ecCtx->found = true;
		}
			/*
			 * If all attributes are included,
			 * set all entries in mask to true.
			 */
		else if (var->varattno == 0)
		{
			for (AttrNumber attno = 0; attno < ecCtx->natts; attno++)
				ecCtx->cols[attno] = true;
			ecCtx->found = true;

			return true;
		}

		return false;
	}
	else if (IsA(node, String))
	{
		/* Case for analyze t1 (c1, c2) */
		AttrNumber	attno;
		char		*col;

		col = strVal(node);
		attno = attnameAttNum(ecCtx->rel, col, false);
		Assert(attno > InvalidAttrNumber);

		ecCtx->cols[attno - 1] = true;
		ecCtx->found = true;

		return false;
	}

	return expression_tree_walker(node, extractcolumns_walker, (void *)ecCtx);
}

bool
extractcolumns_from_node(Relation rel, Node *expr, bool *cols, AttrNumber natts)
{
	struct ExtractcolumnContext	ecCtx;

	ecCtx.rel = rel;
	ecCtx.cols	= cols;
	ecCtx.natts = natts;
	ecCtx.found = false;

	extractcolumns_walker(expr, &ecCtx);

	return  ecCtx.found;
}

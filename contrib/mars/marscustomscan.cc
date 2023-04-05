/*-------------------------------------------------------------------------
 *
 * marscustomscan.cc
 * 		Some common codes can be used by custom aggscan and sortscan.
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "wrapper.hpp"

#include "marsam.h"
#include "marscustomscan.h"

extern "C"
{

	void
	explain_targetlist_exprs(PlanState *planstate,
							 List *l_resno,
							 const char *label,
							 List *ancestors,
							 ExplainState *es)
	{
		Plan *plan = planstate->plan;
		List *deparsing_context;
		List *result = NULL;
		bool useprefix;
		ListCell *lc;

		/* Set up deparsing context */
		deparsing_context =
			set_deparse_context_planstate(es->deparse_cxt,
										  (Node *)planstate,
										  ancestors);

		useprefix = list_length(es->rtable) > 1;

		/* Deparse each expression */
		foreach (lc, l_resno)
		{
			AttrNumber resno = lfirst_int(lc);
			TargetEntry *tle = get_tle_by_resno(plan->targetlist, resno);

			if (!tle)
				continue;

			result = lappend(result,
							 deparse_expression((Node *)tle->expr,
												deparsing_context,
												useprefix, false));
		}

		/* Print results */
		ExplainPropertyList(label, result, es);
	}

	void
	explain_qual_exprs(List *qual, const char *qlabel,
					   PlanState *planstate, List *ancestors,
					   bool useprefix, ExplainState *es)
	{
		Node *node;
		List *context;
		char *exprstr;

		/* No work if empty qual */
		if (qual == NULL)
			return;

		/* Convert AND list to explicit AND */
		node = (Node *)make_ands_explicit(qual);

		/* Set up deparsing context */
		context = set_deparse_context_planstate(es->deparse_cxt,
												(Node *)planstate,
												ancestors);

		/* Deparse the expression */
		exprstr = deparse_expression(node, context, useprefix, false);

		/* And add to es->str */
		ExplainPropertyText(qlabel, exprstr, es);
	}

} // extern "C"

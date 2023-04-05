/*-------------------------------------------------------------------------
 *
 * marscustomscan.h
 *	  The MARS CustomScan for Agg Pushdown.
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_MARSCUSTOMSCAN_H
#define MARS_MARSCUSTOMSCAN_H

#include "wrapper.hpp"

extern "C"
{

	typedef struct MarsExplainContext
	{
		ExplainState *es;
		List *ancestors;
	} MarsExplainContext;

	void mars_register_customaggscan_methods(void);
	void mars_register_customsortscan_methods(void);
	void mars_unregister_customaggscan_methods(void);
	void mars_unregister_customsortscan_methods(void);

	void explain_targetlist_exprs(PlanState *planstate,
								  List *l_resno,
								  const char *label,
								  List *ancestors,
								  ExplainState *es);

	void explain_qual_exprs(List *qual, const char *qlabel,
							PlanState *planstate, List *ancestors,
							bool useprefix, ExplainState *es);

	bool IsMarsCustomSortScan(Path *path);
	bool IsMarsCustomAggScan(Path *path);

} /* extern "C" finish */

#endif // MARS_MARCUSTOMSCAN_H

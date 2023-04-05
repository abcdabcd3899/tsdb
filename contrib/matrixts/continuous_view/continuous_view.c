/*-------------------------------------------------------------------------
 *
 * continuous_view.c
 *
 * Continuous view is new powerful view which derived some properties from both
 * the normal view and the materialized view. It materializes the result of
 * query in the view to the permanent storage and update the result continuously,
 * a select from the continuous view will always return the lastest result for
 * the query.
 *
 * Continuous view only support continuous aggregation on a single source table.
 * The continuous view is implemented by the row level TRIGGER on the INSERT
 * action, so only newly inserted rows (no populate ) of the source table will
 * be updated to the continuous view, the UPDATE/DELETE will not affect the
 * result of continuous view.
 *
 * Newly inserted rows are partially aggregated or partially sorted, then be
 * combined with previous processed rows and be output.
 *
 * The continuous view acting like a continuous materialized view, however, we
 * implement it as a normal view plus an extra auxiliary permanent table which
 * uses the customized SORTHEAP AM to store the partial aggregated results.
 *
 * We don't use materialized view because it is treat as a normal table
 * in the planner, the planner cannot recognize the aggregation/sorting info
 * hidden behind the materialized view and this will become a big problem to
 * continuously combine the partial results.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/matrixts/continuous_view/continuous_view.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "commands/defrem.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"

#include "continuous_view.h"

/* Get the view definition associated with the auxiliary table */
int get_continuous_view_def(AttrNumber anum, Oid key,
							Oid **ptr_viewoids, Oid **ptr_auxoids,
							Oid **ptr_srcoids, Query **ptr_query,
							bool missing_ok)
{
	int size = 8;
	int nrecords = 0;
	Oid continuous_views_oid;
	Oid namespace_id;
	Oid *viewoids = NULL;
	Oid *auxoids = NULL;
	Oid *srcoids = NULL;
	Query *query = NULL;
	bool isnull;
	Datum d;
	Relation continuous_views_rel;
	ScanKeyData scankey[1];
	TableScanDesc scandesc;
	TupleTableSlot *slot;

	if (ptr_viewoids)
		viewoids = (Oid *)palloc(size * sizeof(Oid));
	if (ptr_auxoids)
		auxoids = (Oid *)palloc(size * sizeof(Oid));
	if (ptr_srcoids)
		srcoids = (Oid *)palloc(size * sizeof(Oid));

	namespace_id = LookupExplicitNamespace("matrixts_internal", false);
	continuous_views_oid = get_relname_relid("continuous_views", namespace_id);

	continuous_views_rel = table_open(continuous_views_oid, AccessShareLock);

	ScanKeyInit(&scankey[0],
				anum,
				BTEqualStrategyNumber, F_OIDEQ,
				key);

	scandesc = table_beginscan(continuous_views_rel, GetActiveSnapshot(),
							   1, scankey);
	slot = table_slot_create(continuous_views_rel, NULL);

	while (table_scan_getnextslot(scandesc, ForwardScanDirection, slot))
	{
		if (nrecords > size - 1)
		{
			size = 2 * size;

			if (ptr_viewoids)
				viewoids = (Oid *)repalloc(viewoids, size * sizeof(Oid));
			if (ptr_auxoids)
				auxoids = (Oid *)repalloc(auxoids, size * sizeof(Oid));
			if (ptr_srcoids)
				srcoids = (Oid *)repalloc(srcoids, size * sizeof(Oid));
		}

		if (ptr_viewoids)
		{
			d = slot_getattr(slot, Anum_continuous_views_viewid, &isnull);
			viewoids[nrecords] = DatumGetObjectId(d);
		}

		if (ptr_auxoids)
		{
			d = slot_getattr(slot, Anum_continuous_views_auxtblid, &isnull);
			auxoids[nrecords] = DatumGetObjectId(d);
		}

		if (ptr_srcoids)
		{
			d = slot_getattr(slot, Anum_continuous_views_srcrelid, &isnull);
			srcoids[nrecords] = DatumGetObjectId(d);
		}

		if (ptr_query && query == NULL)
		{
			d = slot_getattr(slot, Anum_continuous_views_query, &isnull);
			query = stringToNode(TextDatumGetCString(d));
		}

		nrecords++;
	}

	if (nrecords == 0 && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("There is no continuous view defined")));

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scandesc);
	table_close(continuous_views_rel, AccessShareLock);

	if (ptr_viewoids)
		*ptr_viewoids = viewoids;

	if (ptr_auxoids)
		*ptr_auxoids = auxoids;

	if (ptr_srcoids)
		*ptr_srcoids = srcoids;

	if (ptr_query)
		*ptr_query = query;

	return nrecords;
}

/* ------------------------------------------------------------------------
 * _PG_init/_PG_fini callbacks, those functions are called when matrixts.so
 * is load/unload.
 * ------------------------------------------------------------------------
 */
void _continuous_view_init(void)
{
	init_cv_processutility_hook();
	init_cv_create_upper_paths_hook();
	register_continuous_view_methods();
}

void _continuous_view_fini(void)
{
	fini_cv_processutility_hook();
	fini_cv_create_upper_paths_hook();
}

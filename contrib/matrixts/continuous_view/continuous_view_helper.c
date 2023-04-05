#include "postgres.h"

#include "access/skey.h"
#include "access/table.h"

#include "access/relation.h"
#include "access/sortheapam.h"
#include "commands/vacuum.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "executor/spi.h"
#include "storage/smgr.h"
#include "storage/bufmgr.h"
#include "utils/elog.h"
#include "utils/relcache.h"
#include "utils/memutils.h"
#include "optimizer/optimizer.h"
#include "partitioning/partdesc.h"
#include "continuous_view.h"

PG_FUNCTION_INFO_V1(info_continuous_view);
PG_FUNCTION_INFO_V1(analyze_continuous_view);
PG_FUNCTION_INFO_V1(truncate_continuous_view);
PG_FUNCTION_INFO_V1(vacuum_continuous_view);
PG_FUNCTION_INFO_V1(rebuild_continuous_view);

/*
 * info_continuous_view
 *
 * Helper function to get the detail info about the internal sortheap storage
 */
Datum
info_continuous_view(PG_FUNCTION_ARGS)
{
	int 		i;
	int 		nrecords;
	Oid 		*auxtbls;
	Oid         viewid = PG_GETARG_OID(0);
	Relation 	rel;
	StringInfoData info;

	initStringInfo(&info);

	PushActiveSnapshot(GetTransactionSnapshot());

	nrecords =
		get_continuous_view_def(Anum_continuous_views_viewid, viewid,
								NULL, &auxtbls, NULL, NULL, false);

	for (i = 0; i < nrecords; i++)
	{
		rel = relation_open(auxtbls[i], AccessShareLock);
		sortheap_fetch_details(rel, &info);
		relation_close(rel, AccessShareLock);
	}

	PopActiveSnapshot();

	return CStringGetDatum(info.data);
}

/*
 * analyze_continuous_view
 *
 * Helper function to analyze the internal aheap storage
 */
Datum
analyze_continuous_view(PG_FUNCTION_ARGS)
{
	int 		i;
	Oid			auxoid;
	Oid         viewid = PG_GETARG_OID(0);
	char 		*auxname;
	RangeVar	*rv;
	VacuumParams params;
	StringInfoData str;

	params.index_cleanup = VACOPT_TERNARY_DEFAULT;
	params.truncate = VACOPT_TERNARY_DEFAULT;
	params.options = VACOPT_ANALYZE | VACOPT_SKIP_LOCKED;

	/*
	 * matrixts_internal.continuous_views is a replicated table, QD has no data
	 * of it, so we need SPI to query the auxiliary table oid
	 */
	SPI_connect();

	initStringInfo(&str);
	appendStringInfo(&str,
					 "SELECT auxoid, auxname "
					 "FROM matrixts_internal.continuous_views "
					 "WHERE viewoid = '%d'", viewid);

	if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "Cannot exec query from continuous view");

	for (i = 0; i < SPI_processed; i++)
	{
		auxoid = atoi(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
		auxname = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);

		rv = makeNode(RangeVar);
		rv->schemaname = "matrixts_internal";
		rv->relname = auxname;
		rv->relpersistence = RELPERSISTENCE_PERMANENT;

		analyze_rel(auxoid,
					rv,
					&params,
					NIL,
					false,
					GetAccessStrategy(BAS_VACUUM),
					NULL);
		pfree(rv);
	}

	SPI_finish();

	return BoolGetDatum(true);
}

/*
 * truncate_continuous_view
 *
 * Helper function to truncate the storage
 */
Datum
truncate_continuous_view(PG_FUNCTION_ARGS)
{
	int 		i;
	char 		*auxname;
	List		*l_auxnames = NIL;
	Oid         viewid = PG_GETARG_OID(0);
	ListCell	*lc;
	StringInfoData str;

	/*
	 * matrixts_internal.continuous_views is a replicated table, QD has no data
	 * of it, so we need SPI to query the auxiliary table oid
	 */
	SPI_connect();

	initStringInfo(&str);
	appendStringInfo(&str,
					 "SELECT auxname "
					 "FROM matrixts_internal.continuous_views "
					 "WHERE viewoid = '%d'", viewid);

	if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "Cannot exec query from continuous view");

	for (i = 0; i < SPI_processed; i++)
	{
		auxname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
		l_auxnames = lappend(l_auxnames, auxname);
	}

	/* Truncate all auxiliry tables */
	foreach(lc, l_auxnames)
	{
		auxname = (char *) lfirst(lc);
		resetStringInfo(&str);
		appendStringInfo(&str, "TRUNCATE matrixts_internal.%s", auxname);

		if (SPI_exec(str.data, 0) != SPI_OK_UTILITY)
			elog(ERROR, "Cannot query on continuous view: %s", str.data);
	}

	SPI_finish();

	return BoolGetDatum(true);
}

/*
 * vacuum_continuous_view
 *
 * Helper function to manually trigger a vacuum 
 */
Datum
vacuum_continuous_view(PG_FUNCTION_ARGS)
{
	StringInfoData str;
	LWLockMode 	mode;
	List	   *l_auxoids = NIL;
	ListCell   *lc;
	int 		i;
	Oid 		auxoid;
	Oid         viewid = PG_GETARG_OID(0);
	bool 		vacuum_full = PG_GETARG_BOOL(1);

	/*
	 * matrixts_internal.continuous_views is a replicated table, QD has no data
	 * of it, so we need SPI to query the auxiliary table oid
	 */
	SPI_connect();

	initStringInfo(&str);
	appendStringInfo(&str,
					 "SELECT auxoid "
					 "FROM matrixts_internal.continuous_views "
					 "WHERE viewoid = '%d'", viewid);

	if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "Cannot exec query from continuous view");

	for (i = 0; i < SPI_processed; i++)
	{
		auxoid = atoi(SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1));
		l_auxoids = lappend_oid(l_auxoids, auxoid);
	}

	mode = vacuum_full ? AccessExclusiveLock : ShareUpdateExclusiveLock;

	foreach(lc, l_auxoids)
	{
		auxoid = lfirst_oid(lc);

		LaunchSortHeapMergeWorker(auxoid, mode,
								  true, /* on qd */
								  vacuum_full,
								  true, /* force merge */
								  true /* wait */);
	}

	SPI_finish();
	return BoolGetDatum(true);
}

/*
 * rebuild_continuous_view
 *
 * Helper function to rebuild a continuous view
 */
Datum
rebuild_continuous_view(PG_FUNCTION_ARGS)
{
	Oid         viewid = PG_GETARG_OID(0);

	/* QD only truncate all auxiliary tables, QEs will rebuild the tuples */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		int 		i;
		char 		*auxname;
		List 		*l_auxnames = NIL;
		ListCell	*lc;
		StringInfoData str;

		/*
		 * matrixts_internal.continuous_views is a replicated table, QD has no data
		 * of it, so we need SPI to query the auxiliary table oid
		 */
		SPI_connect();

		initStringInfo(&str);
		appendStringInfo(&str,
						 "SELECT auxname "
						 "FROM matrixts_internal.continuous_views "
						 "WHERE viewoid = '%d'", viewid);

		if (SPI_exec(str.data, 0) != SPI_OK_SELECT)
			elog(ERROR, "Cannot exec query from continuous view");

		for (i = 0; i < SPI_processed; i++)
		{
			auxname = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);
			l_auxnames = lappend(l_auxnames, auxname);
		}

		/* Truncate all auxiliary tables */
		foreach (lc, l_auxnames)
		{
			auxname = (char *) lfirst(lc);

			resetStringInfo(&str);
			appendStringInfo(&str, "TRUNCATE matrixts_internal.%s", auxname);

			if (SPI_exec(str.data, 0) != SPI_OK_UTILITY)
				elog(ERROR, "Cannot query on continuous view: %s", str.data);
		}

		SPI_finish();

		resetStringInfo(&str);
		appendStringInfo(&str, "SELECT matrixts_internal.rebuild_continuous_view(%d)", viewid);

		CdbDispatchCommand(str.data, DF_WITH_SNAPSHOT, NULL);

		return BoolGetDatum(true);
	}
	else
	{
		int 		i;
		int 		nrecords;
		Oid 		*auxtbls;
		Oid 		*srctbls;
		Relation 	srcrel;
		Relation 	auxrel;
		Snapshot 	snapshot;
		TupleTableSlot *slot;
		TableScanDesc scan;

		snapshot = GetTransactionSnapshot();
		PushActiveSnapshot(snapshot);

		nrecords =
			get_continuous_view_def(Anum_continuous_views_viewid, viewid,
									NULL, &auxtbls, &srctbls, NULL, false);

		for (i = 0; i < nrecords; i++)
		{
			auxrel = table_open(auxtbls[i], RowExclusiveLock);
			srcrel = table_open(srctbls[i], ExclusiveLock);
			slot = table_slot_create(srcrel, NULL);

			if (srcrel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
			{
				scan = table_beginscan(srcrel, snapshot, 0, NULL);

				while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
					table_tuple_insert(auxrel, slot, 0, 0, NULL);

				/* Insert an empty slot to signal the end of insert */
				ExecClearTuple(slot);
				table_tuple_insert(auxrel, slot, 0, 0, NULL);

				table_endscan(scan);
			}

			ExecDropSingleTupleTableSlot(slot);
			table_close(srcrel, NoLock);
			table_close(auxrel, RowExclusiveLock);
		}

		PopActiveSnapshot();

		return BoolGetDatum(true);
	}
}

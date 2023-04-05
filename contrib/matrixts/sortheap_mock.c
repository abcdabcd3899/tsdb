#include "postgres.h"
#include "access/sortheapam.h"
#include "access/relation.h"

PG_FUNCTION_INFO_V1(sortheap_tableam_handler);
PG_FUNCTION_INFO_V1(sortheap_bthandler);
PG_FUNCTION_INFO_V1(info_sortheap);

Datum
sortheap_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&sortheapam_methods);
}

Datum
sortheap_bthandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	memcpy(amroutine, &sortheap_btmethods, sizeof(IndexAmRoutine));

	PG_RETURN_POINTER(amroutine);
}

/*
 * info_continuous_view
 *
 * Helper function to get the detail info about the internal sortheap storage
 */
Datum
info_sortheap(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Relation	rel;
	StringInfoData info;

	initStringInfo(&info);

	rel = relation_open(relid, AccessShareLock);

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		elog(ERROR, "Cannot info the parent partition table");

	sortheap_fetch_details(rel, &info);
	relation_close(rel, AccessShareLock);

	return CStringGetDatum(info.data);
}

/*-------------------------------------------------------------------------
 *
 * reorder.c
 *	  TODO file description
 *
 *
 * Copyright (c) 2020-Present TODO
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "access/multixact.h"
#include "commands/cluster.h"
#include "utils/snapmgr.h"


PG_FUNCTION_INFO_V1(__reorder_swap_relation_files);
Datum
__reorder_swap_relation_files(PG_FUNCTION_ARGS)
{
	Oid			r1 = PG_GETARG_OID(0);
	Oid			r2 = PG_GETARG_OID(1);

	swap_relation_files(r1						/* r1 */,
						r2						/* r2 */,
						false					/* target_is_pg_class */,
						false					/* swap_toast_by_content */,
						false					/* swap_stats */,
						false					/* is_internal */,
						RecentXmin				/* frozenXid */,
						ReadNextMultiXactId()	/* cutoffMulti */,
						NULL					/* mapped_tables */);

	PG_RETURN_VOID();
}

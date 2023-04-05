/*-------------------------------------------------------------------------
 *
 * toin.c
 *		Implementation of TOIN indexes for Postgres
 *
 * TOIN, Timeseries Ordered Index, is a combination of BRIN and BTREE indices:
 * - it has the same disk layout of BRIN, so it is small;
 * - it supports index scan like BTREE, so it is efficient;
 *
 * It requires the underlying HEAP data to be strictly ordered, so it is only
 * suitable for the cold data in timeseries scenarios.
 *
 * Copyright (c) 2020-Present TODO
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/brin.h"
#include "access/brin_internal.h"
#include "access/relscan.h"
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"

#include "toin.h"

PG_FUNCTION_INFO_V1(toinhandler);

/*
 * TOIN handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
Datum
toinhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = BRIN_LAST_OPTIONAL_PROCNUM;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = true;
	amroutine->amstorage = true;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = brinbuild;
	amroutine->ambuildempty = brinbuildempty;
	amroutine->aminsert = brininsert;
	amroutine->ambulkdelete = brinbulkdelete;
	amroutine->amvacuumcleanup = brinvacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = brinoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = brinvalidate;
	amroutine->ambeginscan = toinbeginscan;
	amroutine->amrescan = toin_rescan;
	amroutine->amgettuple = toin_gettuple;
	amroutine->amgetbitmap = bringetbitmap;
	amroutine->amendscan = toinendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * Initialize state for a TOIN index scan.
 *
 * We read the metapage here to determine the pages-per-range number that this
 * index was built with.  Note that since this cannot be changed while we're
 * holding lock on index, it's not necessary to recompute it during brinrescan.
 */
IndexScanDesc
toinbeginscan(Relation r, int nkeys, int norderbys)
{
	Toin	   *toin;
	IndexScanDesc scan;

	toin = palloc(sizeof(*toin));
	toin->counted = false;
	toin->firstBlock = InvalidBlockNumber;
	toin->lastBlock = InvalidBlockNumber;

	/* create the brin opaque */
	scan = brinbeginscan(r, nkeys, norderbys);
	toin->brin = scan->opaque;

	/* we need the scan, but only the content, not the pointer */
	toin->scan = *scan;
	pfree(scan);

	/* create the btscan opaque */
	toin_beginscan(&toin->scan, r, nkeys, norderbys);

	/*
	 * let brin be the default scan opaque, so we could reuse most of the brin
	 * functions.
	 */
	toin->scan.opaque = toin->brin;

	return &toin->scan;
}

/*
 * Close down a TOIN index scan
 */
void
toinendscan(IndexScanDesc scan)
{
	toin_endscan(scan);
	brinendscan(scan);

	/* the toin memory will be freed by the scan */
}

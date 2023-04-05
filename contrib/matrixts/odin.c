/*-------------------------------------------------------------------------
 *
 * odin.c
 *	  TODO file description
 *
 *
 * Copyright (c) 2020-Present TODO
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "catalog/pg_am.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "optimizer/cost.h"
#include "utils/index_selfuncs.h"
#include "utils/lsyscache.h"

#ifndef ENABLE_ODIN
# error ENABLE_ODIN is not defined
#endif  /* ENABLE_ODIN */

#define ODIN_HACK_BEGIN(index, feature) \
	Oid			relam = (index)->rd_rel->relam; \
	PG_TRY(); \
	{ \
		odin_indexoid = index->rd_id; \
		odin_features = ODIN_FEATURE_ ## feature; \
		index->rd_rel->relam = BTREE_AM_OID

#define ODIN_HACK_END(index) \
		odin_indexoid = InvalidOid; \
		odin_features = 0; \
		(index)->rd_rel->relam = relam; \
	} \
	PG_CATCH(); \
	{ \
		odin_indexoid = InvalidOid; \
		odin_features = 0; \
		index->rd_rel->relam = relam; \
		PG_RE_THROW(); \
	}; \
	PG_END_TRY() \

static relopt_kind	odin_relopt_kind = 0;
static relopt_parse_elt odin_relopt_tab[INDEX_MAX_KEYS + 1];

static void
odin_add_reloptions(void)
{
	if (odin_relopt_kind)
		return;

	odin_relopt_kind = add_reloption_kind();

	/* btree fillfactor is handled in _bt_pagestate() */
	odin_relopt_tab[0].optname = "fillfactor";
	odin_relopt_tab[0].opttype = RELOPT_TYPE_INT;
	odin_relopt_tab[0].offset = offsetof(OdinOptions, fillfactor);
	add_int_reloption(odin_relopt_kind,
					  odin_relopt_tab[0].optname,
					  "Packs table pages only to this percentage",
					  ODIN_DEFAULT_FILL_FACTOR,
					  ODIN_MIN_FILL_FACTOR,
					  ODIN_MAX_FILL_FACTOR);

	odin_relopt_tab[1].optname = "tuples_per_range";
	odin_relopt_tab[1].opttype = RELOPT_TYPE_INT;
	odin_relopt_tab[1].offset = offsetof(OdinOptions, tuples_per_range);
	add_int_reloption(odin_relopt_kind,
					  odin_relopt_tab[1].optname,
					  "Tuples per odin record, for AO/AOCS only",
					  ODIN_DEFAULT_TUPLES_PER_RANGE,
					  ODIN_MIN_TUPLES_PER_RANGE,
					  ODIN_MAX_TUPLES_PER_RANGE);

	odin_relopt_tab[2].optname = "bitmap_scan";
	odin_relopt_tab[2].opttype = RELOPT_TYPE_BOOL;
	odin_relopt_tab[2].offset = offsetof(OdinOptions, bitmap_scan);
	add_bool_reloption(odin_relopt_kind,
					  odin_relopt_tab[2].optname,
					  "Enable bitmap scan or not",
					  ODIN_DEFAULT_BITMAP_SCAN);
}

static void
odin_validate_index(Relation index)
{
	Form_pg_index rd_index = index->rd_index;

	if (index->rd_indexprs != NIL)
		elog(ERROR, "odin: expr keys are unsupported");

	if (rd_index->indnatts != rd_index->indnkeyatts * 2)
		elog(ERROR, "odin: index must be created as (key1, key2, ...) include (key1, key2, ...)");

	for (int i = 0; i < rd_index->indnkeyatts; i++)
	{
		if (rd_index->indkey.values[i] !=
			rd_index->indkey.values[rd_index->indnkeyatts + i])
			elog(ERROR, "odin: index must be created as (key1, key2, ...) include (key1, key2, ...)");

		if (index->rd_indoption[i] & INDOPTION_DESC)
			elog(ERROR, "odin: DESC keys are unsupported");
		if (index->rd_indoption[i] & INDOPTION_NULLS_FIRST)
			elog(ERROR, "odin: NULLS FIRST keys are unsupported");
	}
}

static ScanKey
odin_adjust_bitmapscan_keys(IndexScanDesc scan,
							ScanKey inkeys, int *nkeys)
{
	TupleDesc   itupdesc = RelationGetDescr(scan->indexRelation);
	ScanKey		outkeys;
	int			noutkeys;
	int			ninkeys = *nkeys;
	bool		hasEq = false;

	/* The first scan is to check whether EQ appears */
	for (int i = 0; i < ninkeys; i++)
	{
		if (inkeys[i].sk_strategy == BTEqualStrategyNumber)
		{
			hasEq = true;
			break;
		}
	}

	if (!hasEq)
		return inkeys;

	/*
	 * ODIN bitmapscan can not handle EQ directly, it must be replaced with
	 * a GE and a LE, so we need to double the array size.
	 */
	outkeys = palloc(sizeof(ScanKeyData) * ninkeys * 2);
	noutkeys = 0;

	/* The second scan does the actual translation */
	for (int i = 0; i < ninkeys; i++)
	{
		ScanKey		inkey = &inkeys[i];
		Oid			amoplefttype;
		Oid			amoprighttype;
		Oid			amopopr;
		RegProcedure proc;

		if (inkey->sk_strategy != BTEqualStrategyNumber)
		{
			outkeys[noutkeys++] = *inkey;
			continue;
		}

		amoplefttype = itupdesc->attrs[inkey->sk_attno - 1].atttypid;
		amoprighttype = inkey->sk_subtype;

		amopopr = get_comparison_operator(amoplefttype, amoprighttype, CmptLEq);
		proc = get_opcode(amopopr);
		ScanKeyEntryInitialize(&outkeys[noutkeys++],
							   inkey->sk_flags,
							   inkey->sk_attno,
							   BTLessEqualStrategyNumber,
							   inkey->sk_subtype,
							   inkey->sk_collation,
							   proc,
							   inkey->sk_argument);

		amopopr = get_comparison_operator(amoplefttype, amoprighttype, CmptGEq);
		proc = get_opcode(amopopr);
		ScanKeyEntryInitialize(&outkeys[noutkeys++],
							   inkey->sk_flags,
							   inkey->sk_attno,
							   BTGreaterEqualStrategyNumber,
							   inkey->sk_subtype,
							   inkey->sk_collation,
							   proc,
							   inkey->sk_argument);
	}

	/* TODO: raise an error if nkeys > INDEX_MAX_KEYS */

	*nkeys = noutkeys;
	return outkeys;
}

/*
 *	btbuild() -- build a new btree index.
 */
static IndexBuildResult *
odinbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;

	odin_validate_index(index);

	ODIN_HACK_BEGIN(index, ALL);
	{
		result = btbuild(heap, index, indexInfo);
	}
	ODIN_HACK_END(index);

	return result;
}

/*
 *	btinsert() -- insert an index tuple into a btree.
 *
 *		Descend the tree recursively, find the appropriate location for our
 *		new tuple, and put it there.
 */
static bool
odininsert(Relation rel, Datum *values, bool *isnull,
		   ItemPointer ht_ctid, Relation heapRel,
		   IndexUniqueCheck checkUnique,
		   IndexInfo *indexInfo)
{
	elog(ERROR, "INSERT not implemented for ODIN indices");
}

/*
 * Bulk deletion of all index entries pointing to a set of heap tuples.
 * The set of target tuples is specified via a callback routine that tells
 * whether any given heap tuple (identified by ItemPointer) is being deleted.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
static IndexBulkDeleteResult *
odinbulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
			   IndexBulkDeleteCallback callback, void *callback_state)
{
	elog(ERROR, "DELETE not implemented for ODIN indices");
}

/*
 * Post-VACUUM cleanup.
 *
 * Result: a palloc'd struct containing statistical info for VACUUM displays.
 */
static IndexBulkDeleteResult *
odinvacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	elog(LOG, "VACUUM is not needed for ODIN indices");

	return stats;
}

/*
 *	btcanreturn() -- Check whether btree indexes support index-only scans.
 *
 * ODIN never do, so this is trivial.
 */
static bool
odincanreturn(Relation index, int attno)
{
	return false;
}

static void
odincostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				 Cost *indexStartupCost, Cost *indexTotalCost,
				 Selectivity *indexSelectivity, double *indexCorrelation,
				 double *indexPages)
{
	/*
	 * TODO: customize the cost model.
	 *
	 * FIXME: we expect this could control whether to "disable" the bitmapscan
	 * for this path, but unfortunately this function is only used for index /
	 * indexonly scans, the bitmapscan cost is estimated in
	 * cost_bitmap_heap_scan().
	 */

	btcostestimate(root, path, loop_count, indexStartupCost, indexTotalCost,
				   indexSelectivity, indexCorrelation, indexPages);
}

static bytea *
odinoptions(Datum reloptions, bool validate)
{
	relopt_value *options;
	int			numoptions;
	OdinOptions *rdopts;

	/* Parse the user-given reloptions */
	options = parseRelOptions(reloptions, validate,
							  odin_relopt_kind, &numoptions);

	/* if none set, we're done */
	if (numoptions == 0)
		return NULL;

	rdopts = allocateReloptStruct(sizeof(OdinOptions), options, numoptions);
	fillRelOptions(rdopts, sizeof(OdinOptions), options, numoptions,
				   validate, odin_relopt_tab, lengthof(odin_relopt_tab));

	pfree(options);
	return (bytea *) rdopts;
}

/*
 *	btproperty() -- Check boolean properties of indexes.
 *
 * This is optional, but handling AMPROP_RETURNABLE here saves opening the rel
 * to call btcanreturn.
 */
static bool
odinproperty(Oid index_oid, int attno,
			 IndexAMProperty prop, const char *propname,
			 bool *res, bool *isnull)
{
	switch (prop)
	{
		case AMPROP_BITMAP_SCAN:
			{
				/*
				 * FIXME: We expect this could turn on / off the bitmapscan
				 * support dynamically, but unfortunately it does not work, the
				 * planner makes the decision only by checking
				 * ->amgetbitmap.
				 */

				Relation	indexrel = index_open(index_oid, AccessShareLock);
				bool		bitmap_scan;

				bitmap_scan = OdinGetBitmapScan(indexrel);
				index_close(indexrel, AccessShareLock);
				return bitmap_scan;
			}

		default:
			return btproperty(index_oid, attno, prop, propname, res, isnull);
	}
}

/*
 *	btbeginscan() -- start a scan on a btree index
 */
static IndexScanDesc
odinbeginscan(Relation rel, int nkeys, int norderbys)
{
	BTScanOpaque so;
	IndexScanDesc scan;

	ODIN_HACK_BEGIN(rel, ALL);
	{
		scan = btbeginscan(rel, nkeys, norderbys);
	}
	ODIN_HACK_END(rel);

	so = (BTScanOpaque) scan->opaque;
	so->odin_heapfetch = NULL;
	so->odin_slot = NULL;
	so->odin_counted = false;
	so->odin_advancing = false;
	so->odin_firstBlock = InvalidBlockNumber;
	so->odin_lastBlock = InvalidBlockNumber;
	so->odin_currentBlock = InvalidBlockNumber;
	so->odin_minoff = InvalidOffsetNumber;
	so->odin_maxoff = InvalidOffsetNumber;

	return scan;
}

/*
 *	btrescan() -- rescan an index relation
 */
static void
odinrescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
		   ScanKey orderbys, int norderbys)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	if (so->odin_heapfetch)
		table_index_fetch_reset(so->odin_heapfetch);

#if 0
	if (nscankeys > 0 && scankey)
	{
		scankey = odin_adjust_bitmapscan_keys(scan, scankey, &nscankeys);
		scan->numberOfKeys = nscankeys;
	}

	if (nscankeys > 0)
	{
		Assert(so->keyData);
		so->keyData = repalloc(so->keyData, sizeof(ScanKeyData) * nscankeys);
	}
#endif

	ODIN_HACK_BEGIN(scan->indexRelation, ALL);
	{
		if (so->odin_slot)
			ExecClearTuple(so->odin_slot);

		btrescan(scan, scankey, nscankeys, orderbys, norderbys);
	}
	ODIN_HACK_END(scan->indexRelation);
}

/*
 *	btgettuple() -- Get the next tuple in the scan.
 */
static bool
odingettuple(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	bool		result;

	/*
	 * We must fetch with our own heapfetch instead of scan->xs_heapfetch due
	 * to that:
	 *
	 * - the targetlist of scan->xs_heapfetch might not contain the scankeys;
	 * - the targetlist of scan->xs_heapfetch might contain too many columns,
	 *   while we only care about the scankeys;
	 */
	if (!so->odin_heapfetch)
	{
		Relation	heapRel = odin_get_heap_relation(scan);
		List	   *targetlist = odin_make_tlist_from_scankeys(scan);

		so->odin_heapfetch = table_index_fetch_begin(heapRel);
		table_index_fetch_extractcolumns(so->odin_heapfetch,
										 targetlist, NIL /* qual */);
	}

	ODIN_HACK_BEGIN(scan->indexRelation, INDEXSCAN);
	{
		result = btgettuple(scan, dir);
	}
	ODIN_HACK_END(scan->indexRelation);

	return result;
}

/*
 * btgetbitmap() -- construct a TIDBitmap.
 */
static Node *
odingetbitmap(IndexScanDesc scan, Node *n)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Node	   *result;
	ScanKey		keyData = scan->keyData;
	int			numberOfKeys = scan->numberOfKeys;

#if 1
	if (scan->numberOfKeys > 0 && scan->keyData)
		scan->keyData = odin_adjust_bitmapscan_keys(scan, scan->keyData,
													&scan->numberOfKeys);

	if (scan->numberOfKeys > 0)
	{
		Assert(so->keyData);
		so->keyData = repalloc(so->keyData,
							   sizeof(ScanKeyData) * scan->numberOfKeys);
	}
#endif

	ODIN_HACK_BEGIN(scan->indexRelation, BITMAPSCAN);
	{
		result = btgetbitmap(scan, n);
	}
	ODIN_HACK_END(scan->indexRelation);

	/* restore the old keys */
	scan->keyData = keyData;
	scan->numberOfKeys = numberOfKeys;

	return result;
}

/*
 *	btendscan() -- close down a scan
 */
static void
odinendscan(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	if (so->odin_heapfetch)
	{
		table_index_fetch_end(so->odin_heapfetch);
		so->odin_heapfetch = NULL;
	}

	if (BTScanPosIsValid(so->currPos))
	{
		_bt_relbuf(scan->indexRelation, so->currPos.buf);
		BTScanPosInvalidate(so->currPos);
	}

	if (so->odin_slot)
	{
		ExecDropSingleTupleTableSlot(so->odin_slot);
		so->odin_slot = NULL;
	}

	ODIN_HACK_BEGIN(scan->indexRelation, ALL);
	{
		btendscan(scan);
	}
	ODIN_HACK_END(scan->indexRelation);
}

/*
 * Derived from bthandler().
 *
 * Btree handler function: return IndexAmRoutine with access method parameters
 * and callbacks.
 */
PG_FUNCTION_INFO_V1(odinhandler);
Datum
odinhandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = true; /* TODO: we should disable UNIQUE */
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = true;
	amroutine->amsearchnulls = true; /* TODO: should we disable nulls? */
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = true;
	amroutine->amcaninclude = true; /* TODO: we should forbid INCLUDE clause,
									 * and generate include clause internally,
									 * it has special meaning in ODIN. */
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = odinbuild;
	amroutine->ambuildempty = btbuildempty;
	amroutine->aminsert = odininsert;
	amroutine->ambulkdelete = odinbulkdelete;
	amroutine->amvacuumcleanup = odinvacuumcleanup;
	amroutine->amcanreturn = odincanreturn;
	amroutine->amcostestimate = odincostestimate;
	amroutine->amoptions = odinoptions;
	amroutine->amproperty = odinproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->ambeginscan = odinbeginscan;
	amroutine->amrescan = odinrescan;
	amroutine->amgettuple = odingettuple;
	amroutine->amgetbitmap = NULL; // FIXME: odin bitmapscan crashes sometimes
	amroutine->amendscan = odinendscan;
	amroutine->ammarkpos = NULL; // btmarkpos;
	amroutine->amrestrpos = NULL; // btrestrpos;
	amroutine->amestimateparallelscan = btestimateparallelscan;
	amroutine->aminitparallelscan = btinitparallelscan;
	amroutine->amparallelrescan = btparallelrescan;

	odin_add_reloptions();

	PG_RETURN_POINTER(amroutine);
}

/*-------------------------------------------------------------------------
 *
 * nbtree.c
 *	  Implementation of Lehman and Yao's btree management algorithm for
 *	  Postgres.
 *
 * NOTES
 *	  This file contains only the public interface routines.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtree.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "access/nbtxlog.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/xlog.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/condition_variable.h"
#include "storage/indexfsm.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "utils/memutils.h"
#include "utils/guc.h"

#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"

#include "toin.h"


/*
 * Derived from btgettuple().
 *
 *	btgettuple() -- Get the next tuple in the scan.
 */
bool
toin_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;
	bool		res;

	/* btree indexes are never lossy */
	scan->xs_recheck = false;

	/*
	 * If we have any array keys, initialize them during first call for a
	 * scan.  We can't do this in btrescan because we don't know the scan
	 * direction at that time.
	 */
	if (so->numArrayKeys && !ToinScanPosIsValid(so->currPos))
	{
		/* punt if we have any unsatisfiable array keys */
		if (so->numArrayKeys < 0)
			return false;

		_toin_start_array_keys(scan, dir);
	}

	/* This loop handles advancing to the next array elements, if any */
	do
	{
		/*
		 * If we've already initialized this scan, we can just advance it in
		 * the appropriate direction.  If we haven't done so yet, we call
		 * _bt_first() to get the first item in the scan.
		 */
		if (!ToinScanPosIsValid(so->currPos))
			res = _toin_first(scan, dir);
		else
		{
			/*
			 * Now continue the scan.
			 */
			res = _toin_next(scan, dir);
		}

		/* If we have a tuple, return it ... */
		if (res)
			break;
		/* ... otherwise see if we have more array keys to deal with */
	} while (so->numArrayKeys && _toin_advance_array_keys(scan, dir));

	return res;
}

/*
 * Derived from btbeginscan().
 *
 *	btbeginscan() -- start a scan on a btree index
 */
void
toin_beginscan(IndexScanDesc scan, Relation rel, int nkeys, int norderbys)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so;

	/* no order by operators allowed */
	Assert(norderbys == 0);

	/* allocate private workspace */
	so = (ToinScanOpaque) palloc(sizeof(ToinScanOpaqueData));
	ToinScanPosInvalidate(so->currPos);
	if (scan->numberOfKeys > 0)
		so->keyData = (ScanKey) palloc(scan->numberOfKeys * sizeof(ScanKeyData));
	else
		so->keyData = NULL;

	so->arrayKeyData = NULL;	/* assume no array keys for now */
	so->numArrayKeys = 0;
	so->arrayKeys = NULL;
	so->arrayContext = NULL;
	so->slot = NULL;
	so->heapfetch = NULL;

	scan->xs_itupdesc = RelationGetDescr(rel);

	toin->so = so;
}

/*
 * Derived from btrescan().
 *
 *	btrescan() -- rescan an index relation
 */
void
toin_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			ScanKey orderbys, int norderbys)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;

	if (so->heapfetch)
		table_index_fetch_reset(so->heapfetch);

	if (so->slot)
	{
		ExecClearTuple(so->slot);
	}

	/* we aren't holding any read locks, but gotta drop the pins */
	if (ToinScanPosIsValid(so->currPos))
	{
		ToinScanPosInvalidate(so->currPos);
	}

	/*
	 * Reset the scan keys. Note that keys ordering stuff moved to _bt_first.
	 * - vadim 05/05/97
	 */
	if (scankey && scan->numberOfKeys > 0)
		memmove(scan->keyData,
				scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	so->numberOfKeys = 0;		/* until _bt_preprocess_keys sets it */

	/* If any keys are SK_SEARCHARRAY type, set up array-key info */
	_toin_preprocess_array_keys(scan);
}

/*
 * Derived from btendscan().
 *
 *	btendscan() -- close down a scan
 */
void
toin_endscan(IndexScanDesc scan)
{
	Toin	   *toin = (Toin *) scan;
	ToinScanOpaque so = toin->so;

	if (so->heapfetch)
		table_index_fetch_end(so->heapfetch);

	if (so->slot)
		ExecDropSingleTupleTableSlot(so->slot);

	/* No need to invalidate positions, the RAM is about to be freed. */

	/* Release storage */
	if (so->keyData != NULL)
		pfree(so->keyData);
	/* so->arrayKeyData and so->arrayKeys are in arrayContext */
	if (so->arrayContext != NULL)
		MemoryContextDelete(so->arrayContext);
	/* so->markTuples should not be pfree'd, see btrescan */
	pfree(so);
}

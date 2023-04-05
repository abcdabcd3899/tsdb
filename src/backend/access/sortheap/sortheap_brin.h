/*-------------------------------------------------------------------------
 *
 * sortheap_brin.h
 *	  POSTGRES Sort Heap brin index routines.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap_brin.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SORTHEAP_BRIN_H
#define SORTHEAP_BRIN_H

#include "sortheap.h"

/* Routines for tape brin-like min/max */
TupleDesc sortheap_brin_builddesc(TupleDesc btdesc);

void sortheap_brin_update_minmax(TupleDesc tupdesc, int nkeys, SortSupport sortKey,
								 Datum *datum_minmax, bool *nulls_minmax,
								 Datum *datum_new, bool *nulls_new, bool initminmax);

void sortheap_brin_flush_minmax(TapeInsertState *tapeinsertstate,
								Datum *datum_minmax, bool *nulls_minmax,
								bool runlevel);

int sortheap_brin_get_next_range(Relation relation,
								 TapeScanDesc tapescandesc,
								 ItemPointer rangestart);
#endif /* //SORTHEAP_BRIN_H */

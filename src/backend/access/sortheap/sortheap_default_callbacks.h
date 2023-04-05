/*----------------------------------------------------------------------------
 *
 * sortheap_default_callbacks.h
 *		Customized callbacks for external sort framework.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap_default_callbacks.h
 *
 *----------------------------------------------------------------------------
 */
#ifndef SORTHEAPAM_DEFAULT_CALLBACKS_H
#define SORTHEAPAM_DEFAULT_CALLBACKS_H

bool default_fetchtup(SortHeapState *shstate, TapeScanDesc tapescandesc,
					  SortTuple *stup, TupleTableSlot *slot, bool ismerge);

bool default_readtup(SortHeapState *shstate, TapeScanDesc tapescandesc,
					 SortTuple *stup, bool ismerge);

void default_dumptups(SortHeapState *shstate, SortTuple *memtuples, int memtupcount,
					  int64 *availmem, bool lastdump);

void default_mergetup(SortHeapState *shstate, SortTuple *stup);

void default_writetup(SortHeapState *shstate, SortTuple *stup);

bool default_copytup(SortHeapState *shstate, SortTuple *stup, TupleTableSlot *slot,
					 int64 *availmem);
int default_comparetup(SortTuple *a, SortTuple *b, SortHeapState *shstate);

#endif

/*----------------------------------------------------------------------------
 *
 * sortheap_external_sort.h
 *		Customized callbacks for external sort framework.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap_external_sort.h
 *
 *---------------------------------------------------------------------------
 */
#ifndef SORTHEAP_TAPESETS_EXTERNAL_SORT_H
#define SORTHEAP_TAPESETS_EXTERNAL_SORT_H

#include "sortheap_common.h"

/* Routines for runtime external merge-able sorted aggregation */
void select_newtape(TapeSetsMeta *metad);
void init_tapes(TapeSetsState *tsstate);
bool merge_runs(TapeSetsState *tsstate);
void vacuum_runs(TapeSetsState *tsstate);
bool dump_tuples(TapeSetsState *tsstate, TransactionId xid,
				 CommandId cid, bool lastdump);

#endif /* //SORTHEAP_TAPESETS_EXTERNAL_SORT_H */

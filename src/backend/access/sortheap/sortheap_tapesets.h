/* ----------------------------------------------------------------
 * sortheap_tapesets.h
 * 		Generic routines for a logical tape
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap_tapesets.h
 * ----------------------------------------------------------------
 */
#ifndef SORTHEAP_TAPESETS_H
#define SORTHEAP_TAPESETS_H

#include "postgres.h"
#include "access/heapam.h"
#include "executor/execdesc.h"
#include "sortheap_common.h"
#include "sortheap_external_sort.h"
#include "sortheap_default_callbacks.h"
#include "sortheap_tape.h"

#define MAX_TAPES 150
#define MIN_TAPES 6

/*
 * Possible states of a logical tapesets in a sort heap. These denote the states
 * that persist between calls of sort heap routines.
 */
typedef enum
{
	TS_UNKNOWN,
	TS_INITIAL,		   /* Initial status, only used on meta page is
						* initialized */
	TS_BUILDRUNS,	   /* Loading tuples; writing to tape */
	TS_MERGE_SORTED,   /* Sort completed, final run is on tape, but
						* not vacuumed */
	TS_MERGE_VACUUMED, /* Sort completed, final run is on tape, and
						* vacuumed */
} TapeSetsStatus;

#define TAPESETSSTATE(state, i) ((TapeSetsState *)state->tapesetsstates[i])

/*
 * The metadata for a merge runs, we have at most two run sets within a file.
 * The reason we have two sets is that we cannot do MERGE/INSERT in the same
 * merge runs at the same time.
 */
typedef struct TapeSetsMeta
{
	/*
	 * The current status of the tapesets.
	 *
	 * CAUTION: don't use the status field directly, always use
	 * tapesets_getstatus() instead which will consider the status with the
	 * visibility of lastMergeXid.
	 */
	TapeSetsStatus status;
	TransactionId lastMergeXid;
	TransactionId lastVacuumXid;
	TransactionId lastInsertXid;

	/*
	 * While building initial runs, this is the current output run number.
	 * Afterwards, it is the number of initial runs we made.
	 */
	int currentRun;
	int level;		/* Knuth's l */
	int destTape;	/* current logical output tape (Knuth's j,
					 * less * 1) */
	int maxTapes;	/* number of tapes (Knuth's T), also the size
					 * of tapes[] */
	int tapeRange;	/* maxTapes-2 (Knuth's P) */
	int resultTape; /* for AHS_MERGE_SORTED only, store sorted
					 * tuples */
	SortHeapLogicalTape tapes[MAX_TAPES];
} TapeSetsMeta;

typedef struct TapeSetsState
{
	Index tapesetsno;
	Relation relation;
	SortHeapState *parent;

	/* the cached status to avoid duplicated check on lastMergeXid */
	TapeSetsStatus status_forread;
	TapeSetsStatus status_forwrite;

	/* Snapshot used to scan the underlying files */
	Snapshot snapshot;

	/* the operation performing */
	SortHeapOP op;

	/* fast access to the TapeSetsMeta */
	BlockNumber metablk;
	Buffer metabuf;

	/* snapshot of the TapeSetsMeta when the state is created */
	TapeSetsMeta metad_snap;

	/* current tape we are reading in this tapesets */
	int cur_readtape;
	int end_readtape;
	int cur_readrun;
	TapeReadState **readstates;
	TapeInsertState *insertstate;

	/*
	 * This array holds the tuples now in sort memory. For SELECT/MERGE
	 * operation, the array are used to implement a "heap" order per Algorithm
	 * H. For INSERT operation, the array is used to sort the tuples in batch.
	 */
	int64 availMem;		  /* remaining memory available, in bytes */
	int64 allowedMem;	  /* total memory allowed, in bytes */
	SortTuple *memtuples; /* array of SortTuple structs */
	int memtupcount;	  /* number of tuples currently present */
	int memtupsize;		  /* allocated length of memtuples array */
	bool growmemtuples;	  /* memtuples' growth still underway? */

	/* used to keep_order if status is AHS_BUILD_RUNS */
	bool sortdone;
	SortTuple *lastReturnedTuple;
	TupleTableSlot *lastReturnedSlot;

	/*
	 * External sort callbacks including
	 * copytup/dumptups/default_comparetup/mergetup
	 */
	const ExternalSortMethods *extsort_methods;
} TapeSetsState;

/* Routines for the tapesets */
TupleTableSlot *tapesets_getnexttuple(TapeSetsState *tsstate, ScanDirection direction,
									  SortTuple *sorttuple, TupleTableSlot *slottuple,
									  SortHeapScanDesc tablescandesc);
void tapesets_performvacuum(TapeSetsState *tss);
bool tapesets_performmerge(TapeSetsState *tss);
bool tapesets_try_lock(TapeSetsState *tss, LOCKMODE mode);
void tapesets_unlock(TapeSetsState *tss, LOCKMODE mode);
int tapesets_num_insert(TapeSetsState *tsstate);
void tapesets_insert(TapeSetsState *tsstate,
					 TupleTableSlot *slot,
					 CommandId cid,
					 int options,
					 TransactionId xid);
bool tapesets_insert_finish(TapeSetsState *tsstate);
bool tapesets_merge_validate_xid(TransactionId xid);
bool tapesets_vacuum_validate_mergexid(TapeSetsState *tss);

TapeSetsMeta *tapesets_getmeta_disk(TapeSetsState *state);
TapeSetsMeta *tapesets_getmeta_snapshot(TapeSetsState *state);

TapeSetsStatus tapesets_getstatus_forread(TapeSetsState *state);
TapeSetsStatus tapesets_getstatus_forwrite(TapeSetsState *tsstate, bool refresh);

void tapesets_initialize_state(TapeSetsState *tsstate);
void tapesets_destroy_state(TapeSetsState *tsstate);

char *tapesets_statusstr(TapeSetsStatus status);

#endif /* //SORTHEAP_TAPESETS_H */

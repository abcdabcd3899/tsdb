/*-------------------------------------------------------------------------
 *
 * sortheapam.h
 *	  Sort Heap access method routines.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/include/access/sortheapam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SORTHEAPAM_HANDLER_H
#define SORTHEAPAM_HANDLER_H

#include "access/tableam.h"
#include "access/amapi.h"
#include "catalog/index.h"
#include "utils/sortsupport.h"

typedef struct TapeSetsState TapeSetsState;
typedef struct TapeScanDescData *TapeScanDesc;
typedef struct SortHeapScanDescData *SortHeapScanDesc;
typedef struct SortHeapState SortHeapState;

#define SORTHEAPAMNAME "sortheap"
#define SORTHEAPBTREEAMNAME "sortheap_btree"

#define MAX_TAPESETS 2
#define SORTHEAP_BRIN_RANGE 128

#define COMPARETUP(state, a, b) \
	((*(state)->extsort_methods->comparetup)(a, b, state))
#define COPYTUP(state, stup, slot, availmem) \
	((*(state)->extsort_methods->copytup)(state, stup, slot, availmem))
#define FETCHTUP(state, tapescandesc, stup, slot, ismerge) \
	((*(state)->extsort_methods->fetchtup)(state,          \
										   tapescandesc,   \
										   stup,           \
										   slot,           \
										   ismerge))
#define MERGETUP(state, stup) \
	((*(state)->extsort_methods->mergetup)(state, stup))
#define DUMPTUPS(state, memtuples, memtupcount, availmem, lastdump) \
	((*(state)->extsort_methods->dumptups)(state,                   \
										   memtuples,               \
										   memtupcount,             \
										   availmem,                \
										   lastdump))
#define READTUP(state, tapescandesc, stup, ismerge) \
	((*(state)->extsort_methods->readtup)(state, tapescandesc, stup, ismerge))
#define WRITETUP(state, stup) \
	((*(state)->extsort_methods->writetup)(state, stup))

/*  the operation we are performing */
typedef enum
{
	SHOP_INSERT,
	SHOP_SEQSCAN,
	SHOP_INDEXSCAN,
	SHOP_MERGE,
} SortHeapOP;

/*
 * The objects we actually sort are SortTuple structs.  These contain
 * a pointer to the tuple proper, which is a separate palloc chunk --- we
 * assume it is just one chunk and can be freed by a simple pfree().
 * SortTuples also contain the tuple's first key column in Datum/nullflag
 * format, and an index integer.
 *
 * Storing the first key column lets us save heap_getattr or index_getattr
 * calls during tuple comparisons.  We could extract and save all the key
 * columns not just the first, but this would increase code complexity and
 * overhead, and wouldn't actually save any comparison cycles in the common
 * case where the first key determines the comparison result.  Note that
 * for a pass-by-reference datatype, datum1 points into the "tuple" storage.
 *
 * tapeno holds the input tape number that each tuple in the heap was read
 * runno  holds the input tape number that each tuple in the heap was read
 *
 */
#define MAXKEYCACHE 3
typedef struct SortTuple
{
	HeapTuple tuple;		   /* values in tuple form */
	uint16 tapeno;			   /* see notes above */
	uint16 runno;			   /* see notes above */
	bool cached[MAXKEYCACHE];  /* number of sort key values cached */
	Datum datums[MAXKEYCACHE]; /* value of key column */
	bool isnulls[MAXKEYCACHE]; /* is key column NULL? */
} SortTuple;

typedef struct ExternalSortMethods
{
	/*
	 * These function pointers decouple the routines that must know what kind
	 * of tuple we are sorting from the routines that don't need to know it.
	 * They are set up by the tuplesort_begin_xxx routines.
	 *
	 * Function to compare two tuples; result is per qsort() convention, ie:
	 * <0, 0, >0 according as a<b, a=b, a>b.  The API must match
	 * qsort_arg_comparator.
	 */
	int (*comparetup)(SortTuple *a, SortTuple *b, SortHeapState *shstate);

	/*
	 * Function to copy a supplied input tuple into palloc'd space and set up
	 * its SortTuple representation (ie, set tuple/datum1/isnull1).  Also,
	 * availmem must be decreased by the amount of space used for the tuple
	 * copy (note the SortTuple struct itself is not counted).
	 */
	bool (*copytup)(SortHeapState *shstate, SortTuple *stup,
					TupleTableSlot *slot, int64 *availmem);

	/*
	 * Function to merge a stored tuple onto tape. The representation of the
	 * tuple in the stup is always a HeapTuple. Don't need to free the stup
	 * because a slab tuple is used.
	 */
	void (*mergetup)(SortHeapState *shstate, SortTuple *stup);

	/*
	 * Function to read a stored tuple from tape back into memory.
	 */
	bool (*fetchtup)(SortHeapState *shstate, TapeScanDesc tapescandesc,
					 SortTuple *stup, TupleTableSlot *slot, bool ismerge);

	/*
	 * Function to dump sorted tuples into the dest logical tape, dumptups
	 * should pfree the tuples and increase availmem by the amount of memory
	 * space thereby released.
	 */
	void (*dumptups)(SortHeapState *shstate, SortTuple *memtuples,
					 int memtupcount, int64 *availmem, bool lastdump);

	/* Bellow functions should only be called by other callbacks above */
	bool (*readtup)(SortHeapState *shstate, TapeScanDesc tapescandesc,
					SortTuple *stup, bool ismerge);
	void (*writetup)(SortHeapState *shstate, SortTuple *stup);
} ExternalSortMethods;

typedef struct SortHeapState SortHeapState;
struct SortHeapState
{
	/*
	 * This should always be the first field if SortHeapState is used to be
	 * the argument of resource owner cleanup callback
	 */
	ResourceOwner cleanupresowner;

	int nkeys;					/* number of columns in sort key */
	MemoryContext sortcontext;	/* memory context holding most sort data */
	MemoryContext tuplecontext; /* sub-context of sortcontext for tuple data */

	/*
	 * external sort callbacks including
	 * copytup/dumptups/default_comparetup/mergetup
	 */
	const ExternalSortMethods *extsort_methods;

	SortSupport sortkeys; /* array of length nkeys */

	/* What operation this sort state is serving for */
	SortHeapOP op;

	/* metabuf which is pinned in the lifetime of SortHeapState */
	Buffer metabuf;

	/* fields for the execution of sorting/aggregation */
	/* the sort heap relation this state for */
	Relation relation;

	/* used to build internal btree index */
	Relation indexrel; /* the internal btree index */
	bool closeindexrel;
	IndexInfo *indexinfo; /* info about index being used for reference */
	EState *estate;		  /* for evaluating index expressions */

	/*
	 * We use resource owner to cleanup the sort state, we need to make sure
	 * the sort state is destroyed even an error occurs.
	 */
	ResourceOwner resowner;

	/*
	 * Fields associated with tapesets
	 */
	Index cur_readtapesets;
	Index cur_inserttapesets;
	TapeSetsState *tapesetsstates[MAX_TAPESETS];

	/* For select */
	int minheapcount;
	SortTuple *minheap;
	SortTuple *lastReturnedTuple;

	/* Private State for callbacks */
	void *args;
	void *private;
};

/* --------------------------------------
 * Access Methods
 */
extern const ExternalSortMethods externalsort_default_methods;
extern const ExternalSortMethods externalsort_compress_methods;
extern TableAmRoutine sortheapam_methods;
extern IndexAmRoutine sortheap_btmethods;

/* ---------------------------------------
 * PG_INIT callbacks
 * --------------------------------------
 */
extern void _sortheapam_customscan_init(void);
extern void _sortheapam_customscan_fini(void);

/* ---------------------------------------
 * helper functions
 * --------------------------------------
 */
extern void sortheap_fetch_details(Relation relation, StringInfoData *info);
extern bool external_amcheck(Oid relid, char *amname);
extern bool sortheap_performmerge(Relation relation, bool force);
extern void LaunchSortHeapMergeWorker(Oid relid, LOCKMODE lockmode, bool on_qd,
									  bool vacuum_full, bool forcemerge,
									  bool wait);

/* Routines for sortheap merge worker */
extern void SortHeapQDMergeWorkerMain(Datum main_arg);
extern void SortHeapQEMergeWorkerMain(Datum main_arg);

#endif /* //SORTHEAPAM_HANDLER_H */

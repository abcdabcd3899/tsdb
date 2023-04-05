/*--------------------------------------------------------------------------
 *
 * sortheap_tape.h
 * 		Generic routines for a logical tape
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * src/backend/access/sortheap/sortheap_tape.h
 *
 *--------------------------------------------------------------------------
 */
#ifndef SORTHEAP_TAPE_H
#define SORTHEAP_TAPE_H

#include "sortheap_common.h"

#define ENDRUNMARKER_LEN MAXALIGN(offsetof(HeapTupleHeaderData, t_bits))

#define ALLOCATEDBLOCKPERAUXBLOCK ((BLCKSZ - PAGE_RESERVED_SPACE) / sizeof(BlockNumber))

#define TAPEINSERTSTATE(shstate) \
	(shstate->tapesetsstates[shstate->cur_inserttapesets]->insertstate)
#define TAPEREADSTATE(shstate, tapesetsno, tapeno) \
	(shstate->tapesetsstates[tapesetsno]->readstates ? shstate->tapesetsstates[tapesetsno]->readstates[tapeno] : NULL)

/*
 * Logic Tape, consisting of heap blocks
 */
typedef struct SortHeapLogicalTape
{
	/* Fields for Knuth's Algorithm 5.4.2D */
	int fib;
	int runs;
	int dummy;

	int currun;
	int nextrun;
	bool mergeactive;

	/* Field for blocks management in this tape */
	BlockNumber header; /* The header block number of this tape,
						 * containing physical information of the
						 * blocks */
} SortHeapLogicalTape;

typedef struct TapeRunsMetaEntry
{
	ItemPointerData firsttuple; /* The first tuple in this run */
	ItemPointerData lasttuple;	/* The last tuple in this run */
	BlockNumber btroot;			/* The root page of the btree of the run */

	uint32 nranges;						/* The number of ranges (128 blocks in the
										 * run) */
	ItemPointerData range_minmax_tuple; /* The first tuple contains the
										 * min/max of the range (128 blocks),
										 * min/max tuples for other ranges
										 * should follow the first tuple */
	ItemPointerData run_minmax_tuple;	/* The tuple contains the min/max of a
										 * run */
} TapeRunsMetaEntry;

typedef struct TapeRunsMeta
{
	TapeRunsMetaEntry entries[1];
} TapeRunsMeta;

#define NUMRUNMETA_PERPAGE ((BLCKSZ - sizeof(TapeRunsMeta) - PAGE_RESERVED_SPACE) / sizeof(TapeRunsMetaEntry))
#define NUMPAGES_RUNSMETA 128

#define InvalidTapeNum -1
#define MIN_PREALLOC_SIZE 8
#define MAX_PREALLOC_SIZE 128

typedef struct HeapTapeMeta
{
	uint32 nruns;			   /* number of runs in this tape */
	BlockNumber lastdatablock; /* the last data block which is the tail of
								* data blocks of this tape */

	/* Additional blocks to record the blocks allocated in this tape */
	BlockNumber firstauxblock;	 /* the first auxiliary block to record the
								  * blocks already allocated in this tape */
	BlockNumber currentauxblock; /* current auxiliary block to record the
								  * blocks allocated in this tape */
	long numblocks;				 /* number of blocks allocated in this tape */

	/* The blocks that store the index info of runs */
	BlockNumber runsmetad[NUMPAGES_RUNSMETA];

	/* The first block */

	/* Block prealloc logic, to make the blocks in same tape adjacent */
	int prealloc_size;
	int nprealloc;
	BlockNumber prealloc[MAX_PREALLOC_SIZE];
} HeapTapeMeta;

typedef struct BTPageState BTPageState;

struct TapeScanDescData
{
	TapeReadState *parent;

	bool empty;

	SortHeapScanDesc tablescandesc;

	/* Underlying heap scan descriptor */
	HeapScanDesc hdesc;

	/* Underlying index scan descriptor */
	IndexScanDesc idesc;

	/* Slot to hold scanned tuple */
	TupleTableSlot *scanslot;

	/* slab tuple to avoid frequent palloc/pfree */
	HeapTuple slab_tuple; /* for merge stage */
	uint32 slab_size;

	/* metad info about the run */
	Buffer runsmetabuf;
	TapeRunsMetaEntry runmetad;
	TupleDesc brindesc;
	uint32 runno;
	uint32 range; /* current range are reading from */
	Buffer brinbuf;
	ItemPointerData brinindex;
	ItemPointerData firsttuple;
	ItemPointerData lasttuple;
	FmgrInfo **strategy_procinfos;

	/* ---------------------------------------------
	 * Fields to read a column stored tuple
	 */
	MemoryContext columnchunkcontext;
	TupleDesc columnstore_desc;
	TupleDesc *columnchunk_descs;
	HeapTuple columnstore_tuple;
	bool cached;
	Datum **datum_cache;
	bool **isnull_cache;
	int cur_cache;
	int size_cache;
	SortTuple stup_cache;
};

typedef struct TapeScanDescData *TapeScanDesc;

typedef struct TapeReadState
{
	/* Pointer to the tapesets state */
	TapeSetsState *parent;

	/* Cache of header blocknum and header buffer */
	int nruns;
	int tapeno;
	BlockNumber header;
	Buffer headerbuf;
	Snapshot snapshot;

	ScanDirection dir;

	/* Scan descriptor for all runs */
	TapeScanDesc *runs_scandescs;

	HeapTuple dummy;
} TapeReadState;

typedef struct TapeInsertState
{
	/* Pointer to the tapesets state */
	TapeSetsState *parent;
	Relation relation;

	/* Insertion transaction id and command id */
	TransactionId xid;
	CommandId cid;

	/* Bulk insert state */
	BulkInsertState bistate;

	/* Cache of header blocknum and header buffer */
	BlockNumber header;
	Buffer headerbuf;
	Buffer currentauxbuf;

	/* -------------------------------
	 *  Fields for building index
	 * -------------------------------
	 */
	bool buildindex;

	Buffer runsmetabuf;
	TapeRunsMetaEntry *runmetad;

	/* Fields for the brin like min/max index */
	Buffer brinbuf;

	TupleDesc brindesc;

	BlockNumber range_firstblock; /* The first block in the range, used to
								   * seek the start of the range */
	BlockNumber range_lastblock;  /* The last block in the range, used to
								   * count the number of blocks in range */
	int range_nblocks;			  /* Number of blocks in the range */
	int nranges;				  /* Number of ranges in the run */
	int nextrange;

	/* Min/Max of the range (128 blocks) */
	Datum *range_minmax;
	bool *range_minmax_nulls;

	/* Min/Max of the run */
	Datum *run_minmax;
	bool *run_minmax_nulls;

	/* fields for internal btree index */
	Relation indexrel;
	BTPageState *btstate;
	BTScanInsert inskey;
	uint32 curRun;
	int btlevel;

	/* Inddex Datums and isnulls to form a Index Tuple */
	Datum indexdatums[INDEX_MAX_KEYS];
	bool indexnulls[INDEX_MAX_KEYS];

	/* ----------------------------------
	 * TupleDesc for the compressed column store tuple
	 */
	TupleDesc columnstore_desc;
	TupleDesc *columnchunk_descs;

	/* Memory Tuples for the merging */
	MemoryContext mergecachecontext;
	SortTuple *mergecached;
	int mergecache_size;
	int mergecache_cur;
} TapeInsertState;

/* Tape read/write interface */
TapeInsertState *tape_create_insertstate(TapeSetsState *tsstate,
										 TransactionId xid,
										 CommandId cid,
										 BlockNumber tapeheader,
										 int curRun,
										 bool buildIndex);
TapeReadState *tape_create_readstate(TapeSetsState *tsstate, Snapshot snap,
									 BlockNumber header, int tapenum,
									 int nruns, ScanDirection dir);
void tape_destroy_insertstate(TapeInsertState *insertstate);
void tape_destroy_readstate(TapeReadState *readstate);

ItemPointerData tape_writetup(TapeInsertState *insertstate, HeapTuple tuple);
HeapTuple tape_readtup(TapeScanDesc tapescandesc);
HeapTuple tape_readtup_pagemode(TapeScanDesc tapescandesc);

/* Routines for physical tape level access */
BlockNumber tape_getheader(TapeSetsState *tsstate, int tapenum,
						   bool physical, bool createit);
bool tape_initialize_scandesc(TapeReadState *tapereadstate, int runno,
							  TapeRunsMetaEntry *runmetad,
							  SortHeapScanDesc tablescandesc);
TapeScanDesc tape_get_scandesc(TapeSetsState *tsstate, int tapeno, int runno,
							   SortHeapScanDesc tablescandesc);

Buffer tape_get_new_bt_buffer(TapeInsertState *tapeinsertstate,
							  int level);
BlockNumber tape_extend(TapeInsertState *tapeinsertstate, Buffer curbuf,
						SortHeapPageType pagetype, int level);

#endif /* //SORTHEAP_TAPE_H */

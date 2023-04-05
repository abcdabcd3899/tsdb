/*-------------------------------------------------------------------------
 *
 * gapfill/custom_scan.h
 *	  The GAPFILL CustomScan for fill gaps of time bucket.
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef GAPFILL_CUSTOM_SCAN_H
#define GAPFILL_CUSTOM_SCAN_H

#include "access/amapi.h"
#include "access/attnum.h"
#include "access/genam.h"
#include "access/relation.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/skey.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_appendonly_fn.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_namespace_d.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "catalog/toasting.h"
#include "cdb/cdbgroupingpaths.h"
#include "cdb/cdbpath.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "executor/nodeIndexscan.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "funcapi.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "port.h"
#include "storage/backendid.h"
#include "storage/lockdefs.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

#define MATRIXTS_GAPFILL_UDF_NAME "time_bucket_gapfill"
#define MATRIXTS_GAPFILL_INTERPOLATE_UDF_NAME "interpolate"
#define MATRIXTS_GAPFILL_LOCF_UDF_NAME "locf"

typedef struct GapfillExplainContext
{
	ExplainState *es;
	List *ancestors;

} GapfillExplainContext;

typedef struct GapfillScanPath
{
	CustomPath cpath;
	FuncExpr *func;
} GapfillScanPath;

/*
 * Fetch and account Gapfill-UDFs.
 */
typedef struct
	GapfillUDFCount
{
	int gapfill_count;
	int deep_interpoate_count;
} GapfillUDFCount;

/*
 * The main logic of time_bucket_gapfill is to find and fill the gap which is
 * composed of a left boundary and a right boundary.
 *
 * GapBoundaryState records what kind of boundary is missed.
 */
typedef enum GapBoundaryState
{
	MISS_LEFT_BOUNDARY,
	MISS_RIGHT_BOUNDARY,
	NO_BOUNDARY_MISS,
} GapBoundaryState;

/*
 * The state for judging whether the gap filling is over.
 */
typedef enum GapfillState
{
	FILLING,
	FINISHED,
} GapfillState;

typedef struct GapfillBoundInfo
{
	int64 time;
	bool is_from_param; /* true: if it is set by udf */
} GapfillBoundInfo;

typedef struct GapfillScanState
{
	CustomScanState ccstate;

	/*
	 * Three TupleTableSlot for fetching, generating, move and output gapfill
	 * tuple slots.
	 *
	 * {Output_fill_slot} is output tuples at the current round. Each round
	 * will be released at begin, and then keep a new output tuple
	 * from {Left_bound_slot}.
	 */
	TupleTableSlot *output_fill_slot;
	/*
	 * {Left_bound_slot} is the preliminary output, that is, the output tuple of
	 * the next round. When the window moves, {Left*} is also responsible for
	 * saving the tuple value of the right boundary of the window.
	 * When the gapfill is all over, it will be released.
	 */
	TupleTableSlot *left_bound_slot;
	/*
	 * {Right_bound_slot} saves the tuple of the right boundary. When the window
	 * moves, {Right*} will copy its own content to left and release the space
	 * at the same time to receive the newly captured tuple value.
	 */
	TupleTableSlot *right_bound_slot;

	/*
	 * Datum of each column in left and right bound.
	 * Length of the array is equal to attrs number of tuple.
	 */
	Datum *left_bound_value_list;
	Datum *right_bound_value_list;

	Bitmapset *interpolate_attr_bm; /* store all interpolate column attrs */
	Bitmapset *groupby_attr_bm;		/* store all 'GROUP BY' except time_bucket attrs */

	/*
	 *  The data types of period and cursor_time both follow 'function type' of
	 *  time_bucket_gapfill().
	 *  But no matter what type time_bucket_gapfill() is, type int64 can be
	 *  used to store data and just use it for intermediate calculations.
	 *  The final type will be converted again in Tuple.
	 */
	int64 period;	   /* Interval of every two time bucket. */
	int64 cursor_time; /* Record of the time of current output slot. */

	AttrNumber time_bucket_attr; /* position of time_bucket_gapfill column */

	GapfillBoundInfo left_bound;  /* Third param in time_bucket_gapfill(). */
	GapfillBoundInfo right_bound; /* Fourth param in time_bucket_gapfill(). */

	GapBoundaryState gap_miss;
	GapfillState fill_gap_state;
} GapfillScanState;

/* CustomScan Plan */
extern void gapfill_register_customscan_methods(void);
extern void gapfill_unregister_customscan_methods(void);

/* Callbacks for planner */
extern Plan *create_gapfill_cscan_plan(PlannerInfo *root, RelOptInfo *rel,
									   CustomPath *best_path, List *tlist,
									   List *clauses, List *custom_plans);

/* CustomScan Exec */
extern Node *create_gapfill_cscan_state(CustomScan *cscan);
/* Callbacks for execution of gapfill custom scan */
extern void begin_gapfill_cscan(CustomScanState *node, EState *estate,
								int eflags);
extern TupleTableSlot *exec_gapfill_cscan(CustomScanState *node);
extern void end_gapfill_cscan(CustomScanState *node);
extern void rescan_gapfill_cscan(CustomScanState *node);
/* parallel plan */
/* TODO so far, gapfill do not support the parallel plan. */
extern Size estimate_dsm_gapfill_cscan(CustomScanState *node,
									   ParallelContext *pcxt);
extern void initialize_dsm_gapfill_cscan(CustomScanState *node,
										 ParallelContext *pcxt,
										 void *coordinate);
extern void reinitialize_dsm_gapfill_cscan(CustomScanState *node,
										   ParallelContext *pcxt,
										   void *coordinate);
extern void initialize_worker_gapfill_cscan(CustomScanState *node, shm_toc *toc,
											void *coordinate);
extern void gapfill_create_upper_paths(PlannerInfo *root,
									   UpperRelationKind stage,
									   RelOptInfo *input_rel,
									   RelOptInfo *output_rel,
									   void *extra);

#endif /* GAPFILL_CUSTOM_SCAN_H */

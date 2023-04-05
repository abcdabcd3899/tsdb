#ifndef CONTINUOUS_VIEW_H
#define CONTINUOUS_VIEW_H

#include "postgres.h"
#include "access/sortheapam.h"
#include "executor/execdesc.h"
#include "nodes/pathnodes.h"
#include "storage/shm_mq.h"

#define CVAMNAME		"cvsortheap"

#define Anum_continuous_views_srcrelid 		1
#define Anum_continuous_views_srcrelname 	2
#define Anum_continuous_views_viewid 		3
#define Anum_continuous_views_auxtblid 		5
#define Anum_continuous_views_query 		7

int		 get_continuous_view_def(AttrNumber anum, Oid key,
									Oid **ptr_viewoids, Oid **ptr_auxoids,
									Oid **ptr_srcoid, Query **ptr_query,
									bool missing_ok);

/* ------------------------------------
 * Executor state / plan state related
 * ------------------------------------
 */
typedef struct CvPlannedStmt
{
	PlannedStmt *plannedStmt;
	Agg *partial_agg;
	Agg *final_agg;
} CvPlannedStmt;

typedef struct CvExecutorState
{
	QueryDesc	*querydesc;
	AggState	*aggstate;
	ScanState	*scanstate;
	TupleTableSlot *scanslot;
} CvExecutorState;

CvExecutorState *continuous_view_executor_start(SortHeapState *shstate, Oid srcrelid);
void continuous_view_executor_end(CvExecutorState *cvestate);
CvPlannedStmt	*continuous_view_planner(Query *query);


/* -------------------------------
 * Functions to initialize hooks
 * -------------------------------
 */
void		_continuous_view_init(void);
void		_continuous_view_fini(void);
void 		init_cv_create_upper_paths_hook(void);
void 		fini_cv_create_upper_paths_hook(void);
void		init_cv_processutility_hook(void);
void		fini_cv_processutility_hook(void);
void		register_continuous_view_methods(void);
void		ContinuousViewInsertWorkerMain(Datum main_arg);
#endif

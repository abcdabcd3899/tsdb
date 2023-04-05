/*-------------------------------------------------------------------------
 *
 * parallel.h
 *	  Infrastructure for launching parallel workers
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/parallel.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARALLEL_H
#define PARALLEL_H

#include "access/xlogdefs.h"
#include "lib/ilist.h"
#include "postmaster/bgworker.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/predicate.h"

typedef void (*parallel_worker_main_type) (dsm_segment *seg, shm_toc *toc);

typedef struct ParallelWorkerInfo
{
	BackgroundWorkerHandle *bgwhandle;
	shm_mq_handle *error_mqh;
	int32		pid;
} ParallelWorkerInfo;

typedef struct ParallelContext
{
	dlist_node	node;
	SubTransactionId subid;
	int			nworkers;
	int			nworkers_launched;
	char	   *library_name;
	char	   *function_name;
	ErrorContextCallback *error_context_stack;
	shm_toc_estimator estimator;
	dsm_segment *seg;
	void	   *private_memory;
	shm_toc    *toc;
	ParallelWorkerInfo *worker;
	int			nknown_attached_workers;
	bool	   *known_attached_workers;
} ParallelContext;

typedef struct ParallelWorkerContext
{
	dsm_segment *seg;
	shm_toc    *toc;
} ParallelWorkerContext;

#ifdef MATRIXDB_VERSION
/*
 * MatrixDB: moved from src/backend/access/transam/parallel.c, must keep in sync with it.
 */
/* Fixed-size parallel state. */
typedef struct FixedParallelState
{
	/* Fixed-size state that workers must restore. */
	Oid			database_id;
	Oid			authenticated_user_id;
	Oid			current_user_id;
	Oid			outer_user_id;
	Oid			temp_namespace_id;
	Oid			temp_toast_namespace_id;
	int			sec_context;
	bool		is_superuser;
	PGPROC	   *parallel_master_pgproc;
	pid_t		parallel_master_pid;
	BackendId	parallel_master_backend_id;
	TimestampTz xact_ts;
	TimestampTz stmt_ts;
	SerializableXactHandle serializable_xact_handle;

	/* Mutex protects remaining fields. */
	slock_t		mutex;

	/* Maximum XactLastRecEnd of any worker. */
	XLogRecPtr	last_xlog_end;

	/* GPDB: fields need to sync to workers */
	int		session_id;
	int		dtxcontext_len;
	int		numsegmentsFromQD;
} FixedParallelState;

/* Pointer to our fixed parallel state. */
extern FixedParallelState *MyFixedParallelState;
#endif /* MATRIXDB_VERSION */

extern volatile bool ParallelMessagePending;
extern PGDLLIMPORT int ParallelWorkerNumber;
extern PGDLLIMPORT bool InitializingParallelWorker;

#define		IsParallelWorker()		(ParallelWorkerNumber >= 0)

extern ParallelContext *CreateParallelContext(const char *library_name,
											  const char *function_name, int nworkers);
extern void InitializeParallelDSM(ParallelContext *pcxt);
extern void InitializeParallelDSM_short(ParallelContext *pcxt);
extern void ReinitializeParallelDSM(ParallelContext *pcxt);
extern void LaunchParallelWorkers(ParallelContext *pcxt);
extern void WaitForParallelWorkersToAttach(ParallelContext *pcxt);
extern void WaitForParallelWorkersToFinish(ParallelContext *pcxt);
extern void DestroyParallelContext(ParallelContext *pcxt);
extern bool ParallelContextActive(void);

extern void HandleParallelMessageInterrupt(void);
extern void HandleParallelMessages(void);
extern void AtEOXact_Parallel(bool isCommit);
extern void AtEOSubXact_Parallel(bool isCommit, SubTransactionId mySubId);
extern void ParallelWorkerReportLastRecEnd(XLogRecPtr last_xlog_end);

extern void ParallelWorkerMain(Datum main_arg);

shm_toc * shm_toc_attach_parallel(dsm_segment *seg);
#endif							/* PARALLEL_H */

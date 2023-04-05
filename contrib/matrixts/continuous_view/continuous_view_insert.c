/*-------------------------------------------------------------------------
 *
 * continuous_view_insert.c
 *
 * This file contains the top level routines for recording the newly inserted
 * tuples from the source table to the auxiliary table.
 *
 * The new tuples are captured by an insertion trigger procedure and then be
 * inserted into the auxiliary table in two modes:
 * 1. if the local buffer is fully filled in the middle of insertions, single
 * or multiple background worker will be launched to insert the tuples.
 * 2. if the local buffer is not fully filled in the end of insertions, current
 * process will insert the tuples in the local buffer.
 *
 * The size of the local buffer is set to BLCKSIZE for two reasons:
 * 1) it is proper size to balance the send frequency of the shared memory
 *    messages.
 * 2) it is proper size for the performance of shared memory messages receiving.
 *
 * We use resource owner mechanism to clean up the background workers and shared
 * parallel memories.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/matrixts/continuous_view/continuous_view_insert_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"

#include "tcop/tcopprot.h"
#include "access/table.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "libpq/pqmq.h"
#include "optimizer/cost.h"
#include "storage/shm_toc.h"
#include "storage/shm_mq.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/sharedsnapshot.h"
#include "continuous_view.h"


#define CV_SHM_MAGIC	0x20200615
#define CONTINUOUS_VIEW_SEND_BUFFER_SIZE		BLCKSZ
#define CONTINUOUS_VIEW_TUPLE_QUEUE_SIZE		CONTINUOUS_VIEW_SEND_BUFFER_SIZE * 200
#define CONTINUOUS_VIEW_ERROR_QUEUE_SIZE  		16384	/* same as
														 * PARALLEL_ERROR_QUEUE_SIZE */
#define CONTINUOUS_VIEW_TUPLE_QUEUE_KEY		0xE000000000000001
#define CONTINUOUS_VIEW_ERROR_QUEUE_KEY		0xE000000000000002
#define CONTINUOUS_VIEW_FIXED_KEY			0xE000000000000003
#define CONTINUOUS_VIEW_DTX_KEY				0xE000000000000004
#define CONTINUOUS_VIEW_TRANSACTION_STATE	0xE000000000000005

static  char CVMSG_SINGLE = 's';
static  char CVMSG_MULTI = 'm';

typedef struct FixedContinuousViewWorkerArgs
{
	FixedParallelState fps;
	Oid			relid;
	int			sortheap_sort_mem;
} FixedContinuousViewWorkerArgs;

typedef struct ContinuousViewInsertState
{
	/*
	 * This should always be the first field if SortHeapState is used to be
	 * the argument of resource owner cleanup callback
	 */
	ResourceOwner cleanupresowner;

	Oid			srcrelid;
	ParallelContext *pcxt;
	char		send_buf[CONTINUOUS_VIEW_SEND_BUFFER_SIZE];
	int			buf_len;
	int			current_worker;
	shm_mq_handle **tqueue;
} ContinuousViewInsertState;

/* Insert trigger for continuous view */
PG_FUNCTION_INFO_V1(continuous_view_insert_trigger);

static void cv_finish_parallel_insert(ResourceReleasePhase phase, bool isCommit,
									  bool isTopLevel, void *arg);
static void
cv_send_to_insert_workers(ContinuousViewInsertState *insertstate,
						  shm_mq_iovec *iov, int iovcnt);

/*
 * Wait the background workers to finish and cleanup the parallel context.
 */
static void
cv_teardown_insert_workers(void *context, bool iscommit)
{
	ContinuousViewInsertState *insertstate = (ContinuousViewInsertState *) context;
	ParallelContext *pcxt = insertstate->pcxt;
	ParallelWorkerInfo *worker;

	/* Make sure the local cached tuples are flushed to background worker */
	if (iscommit && insertstate->buf_len > 0)
	{
		worker = &insertstate->pcxt->worker[insertstate->current_worker];

		/*
		 * If we haven't start any worker yet, current process can insert the
		 * tuples to the continuous view by itself
		 */
		if (!worker->bgwhandle)
		{
			int 		i;
			int 		nrecords;
			Oid 		*auxoids;
			Relation	rel;
			TupleTableSlot *slot;
			HeapTupleData htup;

			PushActiveSnapshot(GetTransactionSnapshot());

			rel = RelationIdGetRelation(insertstate->srcrelid);
			slot = MakeSingleTupleTableSlot(rel->rd_att, &TTSOpsHeapTuple);
			RelationClose(rel);

			nrecords =
				get_continuous_view_def(Anum_continuous_views_srcrelid,
										insertstate->srcrelid,
										NULL, &auxoids,
										NULL, NULL,
										false);

			for (i = 0; i < nrecords; i++)
			{
				int			pos = 0;

				rel = table_open(auxoids[i], RowExclusiveLock);

				while (pos < insertstate->buf_len)
				{
					htup.t_tableOid = InvalidOid;
					htup.t_len = *(uint32 *) (insertstate->send_buf + pos);
					pos += sizeof(htup.t_len);
					htup.t_data = (HeapTupleHeader) (insertstate->send_buf + pos);
					pos += htup.t_len;

					ExecClearTuple(slot);
					ExecStoreHeapTuple(&htup, slot, false);
					/* Insert into materialized view */
					table_tuple_insert(rel, slot, GetCurrentCommandId(true), 0, NULL);
				}

				/* Insert an empty slot to signal the end of insertion */
				ExecClearTuple(slot);
				table_tuple_insert(rel, slot, GetCurrentCommandId(true), 0, NULL);

				table_close(rel, NoLock);
			}

			ExecDropSingleTupleTableSlot(slot);

			PopActiveSnapshot();
		}
		else
		{
			shm_mq_iovec	iov[2];

			iov[0].data = &CVMSG_MULTI;
			iov[0].len = 1;
			iov[1].data = insertstate->send_buf;
			iov[1].len = insertstate->buf_len;

			cv_send_to_insert_workers(insertstate, iov, 2);
		}

		insertstate->buf_len = 0;
	}

	if (iscommit)
	{
		/* Detach all shm mq firstly */
		for (int i = 0; i < pcxt->nworkers; i++)
		{
			worker = &pcxt->worker[i];

			if (insertstate->tqueue[i])
			{
				shm_mq_detach(insertstate->tqueue[i]);
				insertstate->tqueue[i] = NULL;
			}
		}

		for (int i = 0; i < pcxt->nworkers; i++)
		{
			worker = &pcxt->worker[i];

			/* check whether the worker has error or not */
			if (ParallelMessagePending)
				HandleParallelMessages();

			/* Wait the worker to finish */
			if (worker->bgwhandle)
			{
				BgwHandleStatus status;

				status = WaitForBackgroundWorkerShutdown(worker->bgwhandle);
				if (status == BGWH_POSTMASTER_DIED)
					ereport(FATAL,
							(errcode(ERRCODE_ADMIN_SHUTDOWN),
							 errmsg("postmaster exited during a parallel transaction")));
				worker->bgwhandle = NULL;
			}

			if (pcxt->worker[i].error_mqh)
			{
				shm_mq_detach(pcxt->worker[i].error_mqh);
				pcxt->worker[i].error_mqh = NULL;
			}

			CHECK_FOR_INTERRUPTS();
		}

		if (pcxt->toc != NULL)
		{
			FixedParallelState *fps;

			fps = shm_toc_lookup(pcxt->toc, CONTINUOUS_VIEW_FIXED_KEY, false);
			if (fps->last_xlog_end > XactLastRecEnd)
				XactLastRecEnd = fps->last_xlog_end;
		}

		/* reset the insertstate */
		DestroyParallelContext(insertstate->pcxt);
	}
	else
	{
		/* Kill any existing parallel workers */
		DestroyParallelContext(insertstate->pcxt);
	}

	pfree(insertstate->tqueue);
	pfree(insertstate);

	/* Unregister the res cleanup callback */
	UnregisterResourceReleaseCallback(cv_finish_parallel_insert, insertstate);
}

static void
cv_finish_parallel_insert(ResourceReleasePhase phase,
						  bool isCommit,
						  bool isTopLevel,
						  void *arg)
{
	ContinuousViewInsertState	*insertstate = (ContinuousViewInsertState *) arg;

	if (!insertstate || CurrentResourceOwner != insertstate->cleanupresowner ||
		phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	cv_teardown_insert_workers(arg, isCommit);
}

static ContinuousViewInsertState *
cv_init_parallel_insert_workers(Oid srcrelid, int nworkers)
{
	ContinuousViewInsertState *state;
	ParallelContext *pcxt;
	int			i;
	int 		tstatelen;
	Size		segsize;
	char	   *tuplequeuespace;
	char	   *errorqueuespace;
	char	   *fixedargsspace;
	char	   *dtxcontextspace;
	char	   *tstatespace;
	FixedContinuousViewWorkerArgs *fixedargs;
	MemoryContext oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	Assert(CurrentMemoryContext == TopTransactionContext);

	state = (ContinuousViewInsertState *) palloc0(sizeof(ContinuousViewInsertState));
	state->tqueue = (shm_mq_handle **) palloc0(sizeof(shm_mq_handle *) * nworkers);

	/* FIXME: enter parallel mode to pass the check in CreateParallelContext */
	pcxt = CreateParallelContext("matrixts", "ContinuousViewInsertWorkerMain",
								 nworkers);

	pcxt->worker = palloc0(sizeof(ParallelWorkerInfo) * pcxt->nworkers);

	/*
	 * We need to make sure a transaction id is assigned and update the
	 * SharedLocalSnapshot to let the worker reuse the same transaction
	 * id.
	 */
	GetCurrentTransactionId();
	LWLockAcquire(SharedLocalSnapshotSlot->slotLock, LW_EXCLUSIVE);
	SetSharedTransactionId_writer(DistributedTransactionContext);
	LWLockRelease(SharedLocalSnapshotSlot->slotLock);

	/* Initialize a DSM and reserve proper shared memory for shm_mq */
	shm_toc_initialize_estimator(&pcxt->estimator);
	/* Estimate space for tuple queues. */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(CONTINUOUS_VIEW_TUPLE_QUEUE_SIZE, nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Estimate the space for error queues */
	shm_toc_estimate_chunk(&pcxt->estimator,
						   mul_size(CONTINUOUS_VIEW_ERROR_QUEUE_SIZE, nworkers));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Estimate the space for distributed transaction context */
	shm_toc_estimate_chunk(&pcxt->estimator, serializedDtxContextInfolen);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Estimate the space for transaction context */
	tstatelen = EstimateTransactionStateSpace();
	shm_toc_estimate_chunk(&pcxt->estimator, tstatelen);
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Estimate space for fixed args */
	shm_toc_estimate_chunk(&pcxt->estimator, sizeof(FixedContinuousViewWorkerArgs));
	shm_toc_estimate_keys(&pcxt->estimator, 1);

	/* Create DSM and initialize with new table of contents */
	segsize = shm_toc_estimate(&pcxt->estimator);
	pcxt->seg = dsm_create(segsize, DSM_CREATE_NULL_IF_MAXSEGMENTS);
	if (!pcxt->seg)
		elog(ERROR, "cannot alloc DSM for continuous view insert worker");

	pcxt->toc = shm_toc_create(CV_SHM_MAGIC, dsm_segment_address(pcxt->seg),
							   segsize);

	/* Allocate the space for fixed args */
	fixedargsspace = shm_toc_allocate(pcxt->toc, sizeof(FixedContinuousViewWorkerArgs));
	fixedargs = (FixedContinuousViewWorkerArgs *) fixedargsspace;
	fixedargs->relid = srcrelid;
	fixedargs->sortheap_sort_mem = sortheap_sort_mem;
	fixedargs->fps.database_id = MyDatabaseId;
	fixedargs->fps.current_user_id = GetAuthenticatedUserId();
	fixedargs->fps.parallel_master_backend_id = MyBackendId;
	fixedargs->fps.parallel_master_pid = MyProcPid;
	fixedargs->fps.numsegmentsFromQD= numsegmentsFromQD;
	fixedargs->fps.dtxcontext_len = serializedDtxContextInfolen;
	fixedargs->fps.session_id= gp_session_id;
	SpinLockInit(&fixedargs->fps.mutex);
	shm_toc_insert(pcxt->toc, CONTINUOUS_VIEW_FIXED_KEY, fixedargsspace);

	/* Allocate the space for tuple message queue and set me as the sender */
	tuplequeuespace = shm_toc_allocate(pcxt->toc,
									   mul_size(CONTINUOUS_VIEW_TUPLE_QUEUE_SIZE, nworkers));
	for (i = 0; i < nworkers; i++)
	{
		char	   *start;
		shm_mq	   *mq;

		start = tuplequeuespace + i * CONTINUOUS_VIEW_TUPLE_QUEUE_SIZE;
		mq = shm_mq_create(start, (Size) CONTINUOUS_VIEW_TUPLE_QUEUE_SIZE);
		shm_mq_set_sender(mq, MyProc);
		state->tqueue[i] = shm_mq_attach(mq, pcxt->seg, NULL);
	}
	shm_toc_insert(pcxt->toc, CONTINUOUS_VIEW_TUPLE_QUEUE_KEY, tuplequeuespace);

	/* Allocate the space for error message queue and set me as the receiver */
	errorqueuespace = shm_toc_allocate(pcxt->toc,
									   mul_size(CONTINUOUS_VIEW_ERROR_QUEUE_SIZE, nworkers));
	for (i = 0; i < nworkers; i++)
	{
		char	   *start;
		shm_mq	   *mq;

		start = errorqueuespace + i * CONTINUOUS_VIEW_ERROR_QUEUE_SIZE;
		mq = shm_mq_create(start, (Size) CONTINUOUS_VIEW_ERROR_QUEUE_SIZE);
		shm_mq_set_receiver(mq, MyProc);
		pcxt->worker[i].error_mqh = shm_mq_attach(mq, pcxt->seg, NULL);
	}
	shm_toc_insert(pcxt->toc, CONTINUOUS_VIEW_ERROR_QUEUE_KEY, errorqueuespace);

	/* Allocate the space for distributed transaction context. */
	dtxcontextspace = shm_toc_allocate(pcxt->toc, serializedDtxContextInfolen);
	memcpy(dtxcontextspace, serializedDtxContextInfo, serializedDtxContextInfolen);
	shm_toc_insert(pcxt->toc, CONTINUOUS_VIEW_DTX_KEY, dtxcontextspace);

	/* Allocate the space for transaction context */
	tstatespace = shm_toc_allocate(pcxt->toc, tstatelen);
	SerializeTransactionState(tstatelen, tstatespace);
	shm_toc_insert(pcxt->toc, CONTINUOUS_VIEW_TRANSACTION_STATE, tstatespace);

	state->pcxt = pcxt;
	state->srcrelid = srcrelid;

	MemoryContextSwitchTo(oldcontext);

	/*
	 * We use the resource owner callback mechanism to cleanup the background
	 * workers and parallel context.
	 *
	 * CAUTION: the callback is called at the very end of the cleanup of the
	 * resource owner, the dynamic shared memory will be freed automatically
	 * which is still needed by the background workers.
	 *
	 * We need a way to stop the background workers firstly then cleanup the
	 * dynamic shared memory, to do this, we allocated dynamic shared memory
	 * in current resource owner and register the cleanup callback in the
	 * child resource owner. In this way, callback will be called before the
	 * dsm is freed.
	 */
	state->cleanupresowner = ResourceOwnerCreate(CurrentResourceOwner, "ContinuousViewResOwner");
	RegisterResourceReleaseCallback(cv_finish_parallel_insert, (void *) state);

	return state;
}

static bool 
cv_launch_insert_worker(ParallelContext *pcxt, int worker_identifier)
{
	BackgroundWorker bgw;
	MemoryContext oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	/* Startup a background worker */
	bgw.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_ConsistentState;
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(bgw.bgw_library_name, "matrixts");
	sprintf(bgw.bgw_function_name, "ContinuousViewInsertWorkerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "Continuous View Insert Worker");
	bgw.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(pcxt->seg));
	bgw.bgw_notify_pid = MyProcPid;
	memcpy(bgw.bgw_extra, &worker_identifier, sizeof(int));

	/* Start workers */
	if (!RegisterDynamicBackgroundWorker(&bgw, &pcxt->worker[worker_identifier].bgwhandle))
	{
		return false;
	}

	shm_mq_set_handle(pcxt->worker[worker_identifier].error_mqh,
					  pcxt->worker[worker_identifier].bgwhandle);
	pcxt->nworkers_launched++;

	MemoryContextSwitchTo(oldcontext);

	return true;
}


/* --------------------------------------------------------------------------
 * The main entry of the background worker for insertion.
 * ---------------------------
 * the logical is very simple:
 * 1. open the auxiliary table
 * 2. read tuples from the shared memory message queue and insert into the
 *    auxiliary table
 * 3. close and flush the auxiliary table.
 */
void
ContinuousViewInsertWorkerMain(Datum main_arg)
{
	int			workerIdentifier;
	char	   *errorQueueSpace;
	char	   *tupleQueueSpace;
	char	   *dtxcontextSpace;
	char	   *tstateSpace;
	shm_toc    *toc;
	shm_mq	   *mq;
	dsm_segment *seg;
	shm_mq_handle *tupleMqh;
	shm_mq_handle *errMqh;
	TupleTableSlot *slot = NULL;
	DtxContextInfo dtxcontext;
	FixedContinuousViewWorkerArgs *fixedArgs;

	/* Establish signal handlers. */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	memcpy(&workerIdentifier, MyBgworkerEntry->bgw_extra, sizeof(int));

	seg = dsm_attach(DatumGetUInt32(main_arg));
	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not map dynamic shared memory segment")));
	toc = shm_toc_attach(CV_SHM_MAGIC, dsm_segment_address(seg));

	fixedArgs = shm_toc_lookup(toc, CONTINUOUS_VIEW_FIXED_KEY, false);
	MyFixedParallelState = (FixedParallelState *) fixedArgs;

	tupleQueueSpace = shm_toc_lookup(toc, CONTINUOUS_VIEW_TUPLE_QUEUE_KEY, false);
	mq = (shm_mq *) (tupleQueueSpace +
					 workerIdentifier * CONTINUOUS_VIEW_TUPLE_QUEUE_SIZE);
	shm_mq_set_receiver(mq, MyProc);
	tupleMqh = shm_mq_attach(mq, seg, NULL);

	errorQueueSpace = shm_toc_lookup(toc, CONTINUOUS_VIEW_ERROR_QUEUE_KEY, false);
	mq = (shm_mq *) (errorQueueSpace +
					 workerIdentifier * CONTINUOUS_VIEW_ERROR_QUEUE_SIZE);
	shm_mq_set_sender(mq, MyProc);
	errMqh = shm_mq_attach(mq, seg, NULL);
	pq_redirect_to_shm_mq(seg, errMqh);
	pq_set_parallel_master(fixedArgs->fps.parallel_master_pid,
						   fixedArgs->fps.parallel_master_backend_id);

	/* Set myself to reader so we can get sync the shared snapshot */
	Gp_is_writer = false;
#ifdef MATRIXDB_VERSION
	Mx_require_motion = false;
#endif
	gp_session_id = fixedArgs->fps.session_id;
	/* Sync the work_mem */
	sortheap_sort_mem = fixedArgs->sortheap_sort_mem;
	numsegmentsFromQD = fixedArgs->fps.numsegmentsFromQD;

#if 0
	int		count = 10;

	while (count--)
		pg_usleep(1000 * 1000);
#endif

	BackgroundWorkerInitializeConnectionByOid(fixedArgs->fps.database_id,
											  fixedArgs->fps.current_user_id,
											  0);

	/* Setup the distributed transaction context */
	dtxcontextSpace = shm_toc_lookup(toc, CONTINUOUS_VIEW_DTX_KEY, false);
	memset(&dtxcontext, 0, sizeof(dtxcontext));
	DtxContextInfo_Deserialize(dtxcontextSpace, fixedArgs->fps.dtxcontext_len,  &dtxcontext);
	setupQEDtxContext(&dtxcontext);

	/* Crank up a transaction state appropriate to a parallel worker. */
	tstateSpace = shm_toc_lookup(toc, CONTINUOUS_VIEW_TRANSACTION_STATE, false);
	StartParallelWorkerTransaction(tstateSpace);
	/* FIXME: should we restore the COMCID */
	PushActiveSnapshot(GetTransactionSnapshot());

	PG_TRY();
	{
		int		i;
		int		nrecords;
		Oid 	*auxoids;
		Relation	*auxrels = NULL;
		Relation	srcrel = NULL;

		nrecords =
			get_continuous_view_def(Anum_continuous_views_srcrelid,
									fixedArgs->relid,
									NULL,
									&auxoids,
									NULL,
									NULL,
									false);

		srcrel = table_open(fixedArgs->relid, AccessShareLock);
		slot = MakeSingleTupleTableSlot(RelationGetDescr(srcrel), &TTSOpsHeapTuple);
		table_close(srcrel, AccessShareLock);

		auxrels = (Relation *) palloc(sizeof(Relation) * nrecords);

		for (i = 0; i < nrecords; i++)
			auxrels[i] = table_open(auxoids[i], RowExclusiveLock);

		/*
		 * Enter the main loop which receives the tuples and inserts them to
		 * according materialized view.
		 */
		while (true)
		{
			Size		nbytes;
			void	   *data;
			char		msgtype = CVMSG_MULTI;
			int			pos = 0;
			HeapTupleData htup;
			shm_mq_result result;

			/* Wait a new tuple available or the sender detached the mq. */
			result = shm_mq_receive(tupleMqh, &nbytes, &data, false);

			if (result == SHM_MQ_DETACHED)
				break;

			Assert(result == SHM_MQ_SUCCESS);

			/* Read the type */
			msgtype = *(char *) ((char *) data + pos);
			pos++;

			/* Parse the data */
			while (pos < nbytes)
			{
				if (msgtype == CVMSG_MULTI)
				{
					htup.t_tableOid = InvalidOid;
					htup.t_len = *(uint32 *) ((char *) data + pos);
					pos += sizeof(htup.t_len);
					htup.t_data = (HeapTupleHeader)((char *) data + pos);
					pos += htup.t_len;
				}
				else
				{
					Assert(msgtype == CVMSG_SINGLE);
					Assert(pos == 1);

					htup.t_tableOid = InvalidOid;
					htup.t_data = (HeapTupleHeader) ((char *) data + 1);
					htup.t_len = nbytes - 1;
					pos += htup.t_len;
				}

				/* Insert the tuples to all auxiliary table */
				for (i = 0; i < nrecords; i++)
				{
					ExecClearTuple(slot);
					ExecStoreHeapTuple(&htup, slot, false);
					/* Insert into materialized view */
					table_tuple_insert(auxrels[i], slot, GetCurrentCommandId(true), 0, NULL);
				}

				if (msgtype == CVMSG_MULTI)
					continue;
			}
		}

		for (i = 0; i < nrecords; i++)
		{
			/* Insert an empty slot to signal the end of insertion */
			ExecClearTuple(slot);
			table_tuple_insert(auxrels[i], slot, GetCurrentCommandId(true), 0, NULL);
			table_close(auxrels[i], NoLock);
		}

		ExecDropSingleTupleTableSlot(slot);

		/*
		 * Reset the Gp_is_writer, otherwise the worker will remove the shared
		 * snapshot slot from the shared memory
		 */
		Gp_is_writer = false;
	}
	PG_CATCH();
	{
		Gp_is_writer = false;
		PG_RE_THROW();
	}
	PG_END_TRY();

	PopActiveSnapshot();
	EndParallelWorkerTransaction();
}

/* --------------------------------------------------------------------
 * The trigger procedure to record a tuple to a continuous view.
 *
 * All continuous views share the same trigger procedure.
 * -------------------------------------------------------
 */
static void
cv_send_to_insert_workers(ContinuousViewInsertState *insertstate,
						  shm_mq_iovec *iov, int iovcnt)
{
	int			retry = 0;
	shm_mq_result result;
	ParallelWorkerInfo *worker;

	/*
	 * Try to send to the current worker in non-blocking mode, if we
	 * failed to send in 5 times, we would advance to secondary worker
	 */
	while (true)
	{
		worker = &insertstate->pcxt->worker[insertstate->current_worker];

		if (!worker->bgwhandle)
		{
			bool		launched;

			launched = cv_launch_insert_worker(insertstate->pcxt,
											   insertstate->current_worker);

			if (!launched)
			{
				/* Cannot allocate a new parallel worker, switch to prev worker */
				if (insertstate->current_worker > 0)
					insertstate->current_worker--;

				CHECK_FOR_INTERRUPTS();
				continue;
			}
		}

		result = shm_mq_sendv(insertstate->tqueue[insertstate->current_worker],
							  iov, iovcnt, true);

		if (result == SHM_MQ_SUCCESS)
		{
			/* If current worker seems to be busy, switch to next */
			if (retry > 2)
				insertstate->current_worker =
					(insertstate->current_worker + 1) % max_parallel_workers_per_gather;

			retry = 0;
			break;
		}
		else if (result == SHM_MQ_WOULD_BLOCK)
		{
			retry++;
			CHECK_FOR_INTERRUPTS();
		}
		else if (result == SHM_MQ_DETACHED)
		{
			/* Check the error message from worker first */
			CHECK_FOR_INTERRUPTS();
			ereport(ERROR,
					(errcode(ERRCODE_SYSTEM_ERROR),
						errmsg("Continuous View worker quit unexpectedly")));
		}
	}
}

Datum
continuous_view_insert_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Trigger 	*trigger = trigdata->tg_trigger;
	Relation	relation = trigdata->tg_relation;
	int 		sendlen;
	HeapTuple	htup = trigdata->tg_trigtuple;
	ContinuousViewInsertState *insertstate;

	/* make sure it's called as a trigger */
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("continuous_view_insert: must be called as trigger")));

	/* and that it's called on update */
	if (!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("continuous_view_insert: must be called on insert")));

	/* and that it's called for each row */
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("continuous_view_insert: must be called for each row")));

	/* find the target materialized view */
	if (!trigger->tgcleanup)
	{
		trigger->tgcleanup = (TriggerCleanupData *)
			MemoryContextAllocZero(TopTransactionContext, sizeof(TriggerCleanupData));
		trigger->tgcleanup->context = (void *)
			cv_init_parallel_insert_workers(relation->rd_id, max_parallel_workers_per_gather);
		trigger->tgcleanup->callback = cv_teardown_insert_workers;
	}

	insertstate = (ContinuousViewInsertState *) trigger->tgcleanup->context;

	sendlen = htup->t_len + sizeof(htup->t_len);

	/*
	 * If the tuple is larger than the half of CONTINUOUS_VIEW_SEND_BUFFER
	 * _SIZE, copy it to the local buffer seems to a waste, send it to the
	 * shared message queue directly
	 */
	if (sendlen >= CONTINUOUS_VIEW_SEND_BUFFER_SIZE / 2)
	{
		shm_mq_iovec	iov[2];

		iov[0].data = &CVMSG_SINGLE;
		iov[0].len = 1;
		iov[1].data = (char *) htup->t_data;
		iov[1].len = htup->t_len;

		cv_send_to_insert_workers(insertstate, iov, 2);
	}
	else
	{
		/*
		 * Not enough space left in the local send buffer, flush the local
		 * send buffer first
		 * */
		if (sendlen + insertstate->buf_len > CONTINUOUS_VIEW_SEND_BUFFER_SIZE)
		{
			shm_mq_iovec	iov[2];

			iov[0].data = &CVMSG_MULTI;
			iov[0].len = 1;
			iov[1].data = insertstate->send_buf;
			iov[1].len = insertstate->buf_len;

			cv_send_to_insert_workers(insertstate, iov, 2);

			insertstate->buf_len = 0;
		}

		/* copy the tuple length first */
		memcpy(insertstate->send_buf + insertstate->buf_len, &htup->t_len, sizeof(htup->t_len));
		insertstate->buf_len += sizeof(htup->t_len);
		/* copy the tuple data */
		memcpy(insertstate->send_buf + insertstate->buf_len, htup->t_data, htup->t_len);
		insertstate->buf_len += htup->t_len;
	}

	return PointerGetDatum(trigdata->tg_trigtuple);
}

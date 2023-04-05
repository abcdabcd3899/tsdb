/*---------------------------------------------------------------------------
 *
 * sortheap_merge_worker.c
 * 		Background worker to merge sort heap table
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 * 		src/backend/access/sortheap/sortheap_freepages.c
 *
 *---------------------------------------------------------------------------
 */
#include "sortheap.h"
#include "access/xact.h"
#include "nodes/makefuncs.h"

void LaunchSortHeapMergeWorker(Oid relid,
							   LOCKMODE lockmode,
							   bool on_qd,
							   bool vacuum_full,
							   bool forcemerge,
							   bool wait)
{
	Oid userid;
	Size size = 0;
	BgwHandleStatus status;
	BackgroundWorker bgw;
	BackgroundWorkerHandle *handle;

	MemoryContext oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	Assert(CurrentMemoryContext == TopTransactionContext);

	if (vacuum_full && !on_qd)
		elog(ERROR, "SortHeap QE merge worker cannot perform VACUUM FULL");

	/* Startup a background worker */
	bgw.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_ConsistentState;
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(bgw.bgw_library_name, "postgres");

	if (on_qd)
	{
		sprintf(bgw.bgw_function_name, "SortHeapQDMergeWorkerMain");
		snprintf(bgw.bgw_name, BGW_MAXLEN, "SortHeap QD Merge Worker");
	}
	else
	{
		sprintf(bgw.bgw_function_name, "SortHeapQEMergeWorkerMain");
		snprintf(bgw.bgw_name, BGW_MAXLEN, "SortHeap QE Merge Worker");
	}

	bgw.bgw_main_arg = relid;
	bgw.bgw_notify_pid = MyProcPid;

	/* Pass database id/user id through bgw_extra */
	memcpy(bgw.bgw_extra, &MyDatabaseId, sizeof(Oid));
	size += sizeof(Oid);
	userid = GetAuthenticatedUserId();
	memcpy(bgw.bgw_extra + size, &userid, sizeof(Oid));
	size += sizeof(Oid);
	memcpy(bgw.bgw_extra + size, &numsegmentsFromQD, sizeof(numsegmentsFromQD));
	size += sizeof(numsegmentsFromQD);
	memcpy(bgw.bgw_extra + size, &lockmode, sizeof(LOCKMODE));
	size += sizeof(LOCKMODE);
	memcpy(bgw.bgw_extra + size, &forcemerge, sizeof(bool));
	size += sizeof(bool);
	memcpy(bgw.bgw_extra + size, &vacuum_full, sizeof(bool));

	/* Start workers */
	if (!RegisterDynamicBackgroundWorker(&bgw, &handle) && wait)
		elog(ERROR, "Cannot start SortHeap merge worker");

	MemoryContextSwitchTo(oldcontext);

	if (wait)
	{
		/* Wait the worker to finish */
		status = WaitForBackgroundWorkerShutdown(handle);

		if (status == BGWH_POSTMASTER_DIED)
			ereport(FATAL,
					(errcode(ERRCODE_ADMIN_SHUTDOWN),
					 errmsg("postmaster exited during a parallel transaction")));
	}
}

void SortHeapQDMergeWorkerMain(Datum main_arg)
{
	Oid relid;
	Oid dbid;
	Oid userid;
	Size size = 0;
	bool force;
	bool vacuum_full;
	LOCKMODE lockmode;
	VacuumRelation *relation;
	VacuumStmt *vacuum_stmt;
	ParseState *pstate;

	/* Establish signal handlers. */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * Set myself as the writer because we will do the insert, the merge
	 * worker can assign its own transaction id and it doesn't need the
	 * distributed transaction, logically each segment can merge its data
	 * independently.
	 */
	Gp_is_writer = true;
#ifdef MATRIXDB_VERSION
	Mx_require_motion = false;
#endif
	gp_session_id = MyProc->mppLocalProcessSerial;

	/* retrieve args from bgw_extra */
	relid = DatumGetObjectId(main_arg);
	memcpy(&dbid, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	size += sizeof(Oid);
	memcpy(&userid, MyBgworkerEntry->bgw_extra + size, sizeof(Oid));
	size += sizeof(Oid);
	memcpy(&numsegmentsFromQD, MyBgworkerEntry->bgw_extra + size,
		   sizeof(numsegmentsFromQD));
	size += sizeof(numsegmentsFromQD);
	memcpy(&lockmode, MyBgworkerEntry->bgw_extra + size,
		   sizeof(LOCKMODE));
	size += sizeof(LOCKMODE);
	memcpy(&force, MyBgworkerEntry->bgw_extra + size, sizeof(bool));
	size += sizeof(bool);
	memcpy(&vacuum_full, MyBgworkerEntry->bgw_extra + size, sizeof(bool));

	BackgroundWorkerInitializeConnectionByOid(dbid,
											  userid,
											  0);

	relation = makeVacuumRelation(NULL, relid, NIL);

	/* Set up an ANALYZE command */
	vacuum_stmt = makeNode(VacuumStmt);
	if (vacuum_full)
		vacuum_stmt->options = list_make1(makeDefElem("full", NULL, -1));
	else
		vacuum_stmt->options = NIL;
	vacuum_stmt->rels = list_make1(relation);
	vacuum_stmt->is_vacuumcmd = true;

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = NULL;

	PortalContext = AllocSetContextCreate(TopMemoryContext,
										  "Sortheap Vacuum Portal",
										  ALLOCSET_DEFAULT_SIZES);
	StartTransactionCommand();
	ExecVacuum(pstate, vacuum_stmt, true);
	CommitTransactionCommand();

	free_parsestate(pstate);
	pfree(vacuum_stmt);
}

void SortHeapQEMergeWorkerMain(Datum main_arg)
{
	Relation rel;
	Oid relid;
	Oid dbid;
	Oid userid;
	Size size = 0;
	int nretry;
	bool force;
	LOCKMODE lockmode;

	/* Establish signal handlers. */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * Set myself as the writer because we will do the insert, the merge
	 * worker can assign its own transaction id and it doesn't need the
	 * distributed transaction, logically each segment can merge its data
	 * independently.
	 */
	Gp_is_writer = true;
#ifdef MATRIXDB_VERSION
	Mx_require_motion = false;
#endif
	gp_session_id = MyProc->mppLocalProcessSerial;

	/* retrieve args from bgw_extra */
	relid = DatumGetObjectId(main_arg);
	memcpy(&dbid, MyBgworkerEntry->bgw_extra, sizeof(Oid));
	size += sizeof(Oid);
	memcpy(&userid, MyBgworkerEntry->bgw_extra + size, sizeof(Oid));
	size += sizeof(Oid);
	memcpy(&numsegmentsFromQD, MyBgworkerEntry->bgw_extra + size,
		   sizeof(numsegmentsFromQD));
	size += sizeof(numsegmentsFromQD);
	memcpy(&lockmode, MyBgworkerEntry->bgw_extra + size,
		   sizeof(LOCKMODE));
	size += sizeof(LOCKMODE);
	memcpy(&force, MyBgworkerEntry->bgw_extra + size, sizeof(bool));

	BackgroundWorkerInitializeConnectionByOid(dbid,
											  userid,
											  0);

	/* retry 2 seconds */
	nretry = 40;
	for (int i = 0; i < nretry; i++)
	{
		bool retry;

		CHECK_FOR_INTERRUPTS();

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());
		rel = try_relation_open(relid, lockmode, true);

		/* Someone has dropped the view or the view is not visible */
		if (!rel)
		{
			/* Sleep 50 ms and retry */
			pg_usleep(50 * 1000);

			PopActiveSnapshot();
			CommitTransactionCommand();
			continue;
		}

		retry = sortheap_performmerge(rel, force);

		relation_close(rel, lockmode);
		PopActiveSnapshot();
		CommitTransactionCommand();

		if (retry == false)
			break;

		/* sleep 50 ms and retry */
		pg_usleep(50 * 1000);
	}
}

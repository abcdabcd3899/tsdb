/*-------------------------------------------------------------------------
 *
 * warden.c
 *	  A background worker that kills MatrixGate connection on PG death.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/matrixgate/warden.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbvars.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/backend_cancel.h"

#include "matrixgate.h"

static volatile sig_atomic_t got_sigterm = false;

static void
handle_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
kill_mxgate_backends(void)
{
	int i;
	int act_max;
	PgBackendStatus *beentry;

	/* Clear local data of activity snapshot */
	pgstat_clear_snapshot();

	/* Take a snapshot of current stat_activity data */
	act_max = pgstat_fetch_stat_numbackends();

	for (i = 0; i < act_max; i++)
	{
		/* Get information of single backend */
		beentry = pgstat_fetch_stat_beentry(i + 1); /* 1-based index */

		Assert(beentry);

		if (!beentry->st_appname[0])
			continue;

		/* Check if it is a connection from MatrixGate */
		if (strcmp(beentry->st_appname, "matrixgate") == 0)
		{
			char *msg = "cleanup MatrixGate connection as database shutdown";

			/* Terminate the backend immediately */
			elog(DEBUG1,
				 "warden kill MatrixGate connection %d",
				 beentry->st_procpid);
			SetBackendCancelMessage(beentry->st_procpid, msg);
			kill(beentry->st_procpid, SIGTERM);
		}
	}
}

/*
 * mxgate_start_warden() kills all mxgate sessions on postmaster stopping
 */
void
mxgate_start_warden(void)
{
	BackgroundWorker worker;

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 5; /* in seconds */
	strcpy(worker.bgw_library_name, "matrixgate");
	strcpy(worker.bgw_function_name, "mxgate_warden_main");
	strcpy(worker.bgw_name, "matrixgate warden");
	strcpy(worker.bgw_type, "matrixgate warden");

	RegisterBackgroundWorker(&worker);
}

void
mxgate_warden_main(Datum main_arg)
{
	int rc;

	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	/* Main loop of the warden - do nothing until SIGTERM */
	while (true)
	{
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   1000L,
					   PG_WAIT_TIMEOUT);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (got_sigterm)
		{
			elog(LOG, "Got sigterm, killing MatrixGate sessions.");
			kill_mxgate_backends();
			proc_exit(0);
		}
	}
}

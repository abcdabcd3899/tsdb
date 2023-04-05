#include "postgres.h"

#include "pgstat.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"
#include "utils/builtins.h"
#include "funcapi.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"
#include "postmaster/bgworker.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "telemetry.h"
#include "guc.h"

static volatile sig_atomic_t got_SIGHUP = false;

/* SIGHUP: set flag to reload config file */
static void
sigHupHandler(SIGNAL_ARGS)
{
	got_SIGHUP = true;

	if(MyProc)
		SetLatch(&MyProc->procLatch);
}

void
telemetry_start(void)
{
	BackgroundWorker worker;
	//TODO decide telemetry log names
	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = 5;	/* in seconds */
	strcpy(worker.bgw_library_name, "telemetry");
	strcpy(worker.bgw_function_name, "telemetry_bgworker_main");
	strcpy(worker.bgw_name, "monitor");
	strcpy(worker.bgw_type, "monitor");

	RegisterBackgroundWorker(&worker);
}

void
telemetry_bgworker_main(Datum main_arg)
{
	pqsignal(SIGHUP, sigHupHandler);

	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(DB_FOR_COMMON_ACCESS, NULL, 0);

	SetConfigOption("log_statement", "none", PGC_SUSET, PGC_S_SESSION);

	telemetry_monitor_wrapper();
}

void
telemetry_monitor_wrapper(void)
{
	MemoryContext	mcxt;
	MemoryContext	oldcxt;
	int				rc;
	int latchtimeout = 5 * 60 * 1000L;

	mcxt = AllocSetContextCreate(TopMemoryContext,
								 "monitor context",
								 ALLOCSET_DEFAULT_MINSIZE,
								 ALLOCSET_DEFAULT_INITSIZE,
								 ALLOCSET_DEFAULT_MAXSIZE);
	while (true)
	{
		/*dynamic update guc var->telemetry_guc_disable_http*/
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * bgworker context loop and switch
		 * When the first time starts DB, it will wait ${latchtimeout}(5 minutes) to send HTTP-post.
		 * Then every ${MONITOR_TIME_GAP}(12hours) send HTTP-post.
		 * */
		oldcxt = MemoryContextSwitchTo(mcxt);
		rc = WaitLatch(MyLatch,
					   WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   latchtimeout,
					   PG_WAIT_TIMEOUT);
		ResetLatch(MyLatch);
		telemetry_main_wrapper();
		latchtimeout = MONITOR_TIME_GAP;
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);


		MemoryContextSwitchTo(oldcxt);
		MemoryContextReset(mcxt);
	}
}

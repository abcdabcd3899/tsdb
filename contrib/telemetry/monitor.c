#include "postgres.h"

#include "fmgr.h"
#include <stdio.h>

#include "telemetry.h"
#include "utils/builtins.h"
#include "funcapi.h"

PG_FUNCTION_INFO_V1(telemetry_monitor);
PG_FUNCTION_INFO_V1(load_uuid_local);
PG_FUNCTION_INFO_V1(telemery_version);

Datum
telemery_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(TELEMETRY_VERSION));
}

Datum
load_uuid_local(PG_FUNCTION_ARGS)
{
	FILE	   *file;
	StringInfo	uuid = makeStringInfo();
	char read_buf[100] = "";

	//size_t read_size;

	file = fopen(UUID_FILE, "r");
	if (file == NULL)
	{
		PG_RETURN_TEXT_P(cstring_to_text(""));
	}

	while (true){
		if (fgets( read_buf, (size_t) UUID_SIZE, file ) == NULL)
			break;
		appendStringInfo(uuid, "%s", read_buf);
	}
	fclose(file);

	PG_RETURN_TEXT_P(cstring_to_text(uuid->data));
}

Datum
telemetry_monitor(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext	oldcontext;
		TupleDesc		tupdesc;
		int				nattr = 2;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(nattr);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "key", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "value", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->max_calls = 2;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		/* for each row */
		Datum		values[2];
		bool		nulls[2];
		HeapTuple	tuple;

		nulls[0] = false;
		nulls[1] = false;
		switch (funcctx->call_cntr)
		{
			case 0:
				values[0] = CStringGetTextDatum("PG_VERSION");
				values[1] = CStringGetTextDatum(PG_VERSION);
				break;

			case 1:
				values[0] = CStringGetTextDatum("GP_VERSION");
				values[1] = CStringGetTextDatum(GP_VERSION);
				break;
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

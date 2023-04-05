#include <postgres.h>
#include <utils/guc.h>
#include <miscadmin.h>

#include "guc.h"

bool telemetry_guc_disable_http = false;

void
_telemetry_guc_init(void)
{
	DefineCustomBoolVariable("matrix.disable_telemetry_http",
							 "Disable telemetry send monitor info",
							 NULL,
							 &telemetry_guc_disable_http,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

void
_telemetry_guc_fini(void)
{
}

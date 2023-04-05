#include <postgres.h>
#include <utils/guc.h>
#include <miscadmin.h>
#include <limits.h>

#include "guc.h"

bool ts_guc_disable_optimizations = false;
bool ts_guc_apm_allow_dml = false;
bool ts_guc_apm_allow_ddl = false;
int ts_guc_continuous_view_workmem = false;

void
_guc_init(void)
{
	DefineCustomBoolVariable("matrix.disable_optimizations",
							 "Disable all matrixts query optimizations",
							 NULL,
							 &ts_guc_disable_optimizations,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("matrix.ts_guc_apm_allow_dml",
							 "Allow direct insert/update on APM catalog tables",
							 NULL,
							 &ts_guc_apm_allow_dml,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("matrix.ts_guc_apm_allow_ddl",
							 "Allow DDL to APM catalog tables",
							 NULL,
							 &ts_guc_apm_allow_ddl,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	/* Define custom GUC variables */
	DefineCustomIntVariable("matrix.continuous_view_workmem",
							"The workmem for sorting array",
							NULL,
							&ts_guc_continuous_view_workmem,
							128000,
							64, MAX_KILOBYTES,
							PGC_USERSET,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);
}

void
_guc_fini(void)
{
}

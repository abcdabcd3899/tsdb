#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "access/sortheapam.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbvars.h"
#include "utils/lsyscache.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "guc.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern void _planner_init(void);
extern void _planner_fini(void);

extern void _continuous_view_init(void);
extern void _continuous_view_fini(void);

extern void _PG_init(void);
extern void _PG_fini(void);

void
_PG_init(void)
{
	_planner_init();
	_continuous_view_init();
	_sortheapam_customscan_init();
	_guc_init();
}

void
_PG_fini(void)
{
	_guc_fini();
	_continuous_view_fini();
	_sortheapam_customscan_fini();
	_planner_fini();
}

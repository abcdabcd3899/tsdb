/*-------------------------------------------------------------------------
 *
 * apm.h
 *	  Automated Partition Management functions.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef __MATRIXTS_APM_H__
#define __MATRIXTS_APM_H__

#include "postgres.h"

#include "fmgr.h"

#include "partitioning/partdefs.h"

typedef enum APMBoundPosition
{
	APM_BOUND_UNKNOWN = 0,
	APM_BOUND_ANCIENT,
	APM_BOUND_CURRENT,
	APM_BOUND_FUTURE,
} APMBoundPosition;

extern Datum apm_eval_partbound(PG_FUNCTION_ARGS);
extern Datum apm_metadata_protect(PG_FUNCTION_ARGS);
extern Datum apm_schema_protect(PG_FUNCTION_ARGS);
extern Datum apm_partition_boundary(PG_FUNCTION_ARGS);
extern PartitionBoundSpec *relid_to_boundspec(Oid relid);

extern Datum apm_max_timestamp(PG_FUNCTION_ARGS);
extern Datum apm_max_timestamptz(PG_FUNCTION_ARGS);
extern Datum apm_min_timestamp(PG_FUNCTION_ARGS);
extern Datum apm_min_timestamptz(PG_FUNCTION_ARGS);

extern Datum apm_is_valid_partition_period(PG_FUNCTION_ARGS);
extern Datum apm_date_trunc(PG_FUNCTION_ARGS);
extern Datum apm_time_trunc_ts(PG_FUNCTION_ARGS);
extern Datum apm_time_trunc_tz(PG_FUNCTION_ARGS);
extern Datum apm_time_trunc_date(PG_FUNCTION_ARGS);

#endif /* __MATRIXTS_APM_H__ */

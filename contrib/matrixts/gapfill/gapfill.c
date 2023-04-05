#include <postgres.h>

#include "fmgr.h"
#include "utils/lsyscache.h"

#include "time_bucket.h"
#include "gapfill/gapfill.h"

PG_FUNCTION_INFO_V1(gapfill_interpolate);
PG_FUNCTION_INFO_V1(gapfill_bucket_int16);
PG_FUNCTION_INFO_V1(gapfill_bucket_int32);
PG_FUNCTION_INFO_V1(gapfill_bucket_int64);
PG_FUNCTION_INFO_V1(gapfill_bucket_date);
PG_FUNCTION_INFO_V1(gapfill_bucket_timestamp);
PG_FUNCTION_INFO_V1(gapfill_bucket_timestamptz);

/*
 * Args:
 *  - smallint/int/bigint/float/real
 */
Datum
gapfill_interpolate(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

/*
 * Args:
 *  bucket_width SMALLINT,
 *  ts SMALLINT,
 *  start SMALLINT=NULL,
 *  finish SMALLINT=NULL
 */
Datum
gapfill_bucket_int16(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	return DirectFunctionCall2(ts_int16_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

/*
 * Args:
 *  bucket_width INT,
 *  ts INT,
 *  start INT=NULL,
 *  finish INT=NULL
 */
Datum
gapfill_bucket_int32(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	return DirectFunctionCall2(ts_int32_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

/*
 * Args:
 *  bucket_width BIGINT,
 *  ts BIGINT,
 *  start BIGINT=NULL,
 *  finish BIGINT=NULL
 */
Datum
gapfill_bucket_int64(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	return DirectFunctionCall2(ts_int64_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

/*
 * Args:
 *  bucket_width INTERVAL,
 *  ts DATE,
 *  start DATE=NULL,
 *  finish DATE=NULL
 */
Datum
gapfill_bucket_date(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	return DirectFunctionCall2(ts_date_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

/*
 * Args:
 *  bucket_width INTERVAL,
 *  ts TIMESTAMP,
 *  start TIMESTAMP=NULL,
 *  finish TIMESTAMP=NULL
 */
Datum
gapfill_bucket_timestamp(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	return DirectFunctionCall2(ts_timestamp_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

/*
 * Args:
 *  bucket_width INTERVAL,
 *  ts TIMESTAMPTZ,
 *  start TIMESTAMPTZ=NULL,
 *  finish TIMESTAMPTZ=NULL
 */
Datum
gapfill_bucket_timestamptz(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	return DirectFunctionCall2(ts_timestamptz_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1));
}

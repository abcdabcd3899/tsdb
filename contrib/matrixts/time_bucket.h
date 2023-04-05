#ifndef __MATRIXTS_TIME_BUCKET_H__
#define __MATRIXTS_TIME_BUCKET_H__ 

#include <postgres.h>
#include <fmgr.h>

extern Datum ts_int16_bucket(PG_FUNCTION_ARGS);
extern Datum ts_int32_bucket(PG_FUNCTION_ARGS);
extern Datum ts_int64_bucket(PG_FUNCTION_ARGS);
extern Datum ts_date_bucket(PG_FUNCTION_ARGS);
extern Datum ts_timestamp_bucket(PG_FUNCTION_ARGS);
extern Datum ts_timestamptz_bucket(PG_FUNCTION_ARGS);

extern int64 ts_time_bucket_by_type(int64 interval, int64 timestamp, Oid type);

#endif /* __MATRIXTS_TIME_BUCKET_H__ */

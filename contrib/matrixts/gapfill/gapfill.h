#ifndef MATRIXDB_MATRIXTS_GAPFILL_H
#define MATRIXDB_MATRIXTS_GAPFILL_H

#include <postgres.h>

#include <fmgr.h>

extern Datum gapfill_interpolate(PG_FUNCTION_ARGS);
extern Datum gapfill_bucket_int16(PG_FUNCTION_ARGS);
extern Datum gapfill_bucket_int32(PG_FUNCTION_ARGS);
extern Datum gapfill_bucket_int64(PG_FUNCTION_ARGS);
extern Datum gapfill_bucket_timestamp(PG_FUNCTION_ARGS);
extern Datum gapfill_bucket_timestamptz(PG_FUNCTION_ARGS);
extern Datum gapfill_bucket_date(PG_FUNCTION_ARGS);

#endif /* MATRIXDB_MATRIXTS_GAPFILL_H */

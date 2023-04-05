/*-------------------------------------------------------------------------
 *
 * ts_func.h
 *	  time_bucket udf.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/ts_func.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_TS_FUNC_H
#define MATRIXDB_TS_FUNC_H
#include "wrapper.hpp"

static inline int64
get_interval_period_timestamp_units(const Interval *interval)
{
	if (interval->month != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval defined in terms of month, year, century etc. not supported")));
	}
#ifdef HAVE_INT64_TIMESTAMP
	return interval->time + (interval->day * USECS_PER_DAY);
#else
	if (interval->time != trunc(interval->time))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval must not have sub-second precision")));
	}
	return interval->time + (interval->day * SECS_PER_DAY);
#endif
}

#ifdef HAVE_INT64_TIMESTAMP
#define JAN_3_2000 (2 * USECS_PER_DAY)
#else
#define JAN_3_2000 (2 * SECS_PER_DAY)
#endif
#define DEFAULT_ORIGIN (JAN_3_2000)
#define TIME_BUCKET_TS(period, timestamp, result, shift)                          \
	do                                                                            \
	{                                                                             \
		if (period <= 0)                                                          \
			ereport(ERROR,                                                        \
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),                    \
					 errmsg("period must be greater than 0")));                   \
		/* shift = shift % period, but use TMODULO */                             \
		TMODULO(shift, result, period);                                           \
                                                                                  \
		if ((shift > 0 && timestamp < DT_NOBEGIN + shift) ||                      \
			(shift < 0 && timestamp > DT_NOEND + shift))                          \
			ereport(ERROR,                                                        \
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),                 \
					 errmsg("timestamp out of range")));                          \
		timestamp -= shift;                                                       \
                                                                                  \
		/* result = (timestamp / period) * period */                              \
		TMODULO(timestamp, result, period);                                       \
		if (timestamp < 0)                                                        \
		{                                                                         \
			/*                                                                    \
			 * need to subtract another period if remainder < 0 this only happens \
			 * if timestamp is negative to begin with and there is a remainder    \
			 * after division. Need to subtract another period since division     \
			 * truncates toward 0 in C99.                                         \
			 */                                                                   \
			result = (result * period) - period;                                  \
		}                                                                         \
		else                                                                      \
			result *= period;                                                     \
                                                                                  \
		result += shift;                                                          \
	} while (0)

static inline Datum
ts_timestamp_bucket(const ::Interval *interval, Timestamp timestamp)
{

	Timestamp origin = DEFAULT_ORIGIN;
	Timestamp result;
	int64 period = get_interval_period_timestamp_units(interval);

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	TIME_BUCKET_TS(period, timestamp, result, origin);

	PG_RETURN_TIMESTAMP(result);
}

static inline Datum
ts_timestamp_bucket(int64 period, Timestamp timestamp)
{
	Timestamp origin = DEFAULT_ORIGIN;
	Timestamp result;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	TIME_BUCKET_TS(period, timestamp, result, origin);

	PG_RETURN_TIMESTAMP(result);
}

#endif // MATRIXDB_TS_FUNC_H

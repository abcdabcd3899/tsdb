/*-------------------------------------------------------------------------
 *
 * apm.c
 *	  Automated Partition Management functions.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "apm.h"
#include "guc.h"
#include "time_bucket.h"

PG_FUNCTION_INFO_V1(apm_eval_partbound);
PG_FUNCTION_INFO_V1(apm_metadata_protect);
PG_FUNCTION_INFO_V1(apm_schema_protect);
PG_FUNCTION_INFO_V1(apm_partition_boundary);
PG_FUNCTION_INFO_V1(apm_max_timestamp);
PG_FUNCTION_INFO_V1(apm_max_timestamptz);
PG_FUNCTION_INFO_V1(apm_min_timestamp);
PG_FUNCTION_INFO_V1(apm_min_timestamptz);

PG_FUNCTION_INFO_V1(apm_is_valid_partition_period);
PG_FUNCTION_INFO_V1(apm_date_trunc);
PG_FUNCTION_INFO_V1(apm_time_trunc_ts);
PG_FUNCTION_INFO_V1(apm_time_trunc_tz);
PG_FUNCTION_INFO_V1(apm_time_trunc_date);

static Datum time_trunc_internal(Interval *interval,
								 Datum ts,
								 Datum (*bucket_func)(PG_FUNCTION_ARGS),
								 Datum (*trunc_func)(PG_FUNCTION_ARGS));

/*
 * Given a partition table oid and an interval, calculate the relative
 * position between [now() - interval] and the partition boundary.
 * Parameters:
 *  [0] oid of partition table
 *  [1] interval of after now()
 *  [2] the "now()" value, if NULL then use now()
 * Returns
 *  0 APM_BOUND_UNKNOWN: rel->relpartbound and interval is not comparable
 *  1 APM_BOUND_ANCIENT: [rel->relpartbound) < [now() - interval]
 *  2 APM_BOUND_CURRENT: [now() - interval] overlaps [rel->relpartbound)
 *  3 APM_BOUND_FUTURE : [now() - interval] < [rel->relpartbound)
 */
Datum apm_eval_partbound(PG_FUNCTION_ARGS)
{
	Oid relid;
	Interval *interval;
	TimestampTz now;
	Datum sub;
	int64 pin;
	ListCell *cell;
	int64 lower_val = PG_INT64_MIN;
	int64 upper_val = PG_INT64_MAX;
	PartitionBoundSpec *boundspec = NULL;

	relid = PG_GETARG_OID(0);
	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid OID")));

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid interval")));

	interval = PG_GETARG_INTERVAL_P(1);
	boundspec = relid_to_boundspec(relid);

	/*
	 * If the PartitionBoundSpec says this is the default partition, return 0
	 * (not comparable)
	 */
	if (boundspec->is_default)
		PG_RETURN_INT32(APM_BOUND_UNKNOWN);

	/* Only range partition can evaluate bound, others returns unknown */
	if (boundspec->strategy != PARTITION_STRATEGY_RANGE)
		PG_RETURN_INT32(APM_BOUND_UNKNOWN);

	if (PG_NARGS() < 3 || PG_ARGISNULL(2))
		now = GetCurrentTransactionStartTimestamp();
	else
		now = PG_GETARG_TIMESTAMPTZ(2);

	sub = DirectFunctionCall2(timestamptz_mi_interval,
							  TimestampTzGetDatum(now),
							  IntervalPGetDatum(interval));
	pin = DatumGetInt64(sub);

	/*
	 * FIXME: we don't know why lowerdatums/upperdatums are list here we
	 * assume leaf partition only has one lowerdatums/ upperdatums pair.
	 */
	Assert(list_length(boundspec->lowerdatums) == 1);
	Assert(list_length(boundspec->upperdatums) == 1);

	foreach (cell, boundspec->upperdatums)
	{
		Const *upper;
		PartitionRangeDatum *upper_bound;

		upper_bound = castNode(PartitionRangeDatum, lfirst(cell));

		if (upper_bound->kind == PARTITION_RANGE_DATUM_MAXVALUE)
			upper_val = DT_NOEND;
		else if (upper_bound->kind == PARTITION_RANGE_DATUM_MINVALUE)
			upper_val = DT_NOBEGIN;
		else
		{
			upper = castNode(Const, upper_bound->value);
			switch (upper->consttype)
			{
			case DATEOID:
				upper_val = DatumGetInt64(DirectFunctionCall1(date_timestamptz,
															  upper->constvalue));
				break;
			case TIMESTAMPOID:
				upper_val = DatumGetInt64(DirectFunctionCall1(timestamp_timestamptz,
															  upper->constvalue));
				break;
			case TIMESTAMPTZOID:
				upper_val = DatumGetInt64(upper->constvalue);
				break;

			case INT2OID:
			case INT4OID:
			case INT8OID:
			case TIMEOID:
			case TIMETZOID:
			case INTERVALOID:
			default:
				/* Partition range types not comparable. */
				PG_RETURN_INT32(APM_BOUND_UNKNOWN);
			}
		}
	}

	foreach (cell, boundspec->lowerdatums)
	{
		Const *lower;
		PartitionRangeDatum *lower_bound;

		lower_bound = castNode(PartitionRangeDatum, lfirst(cell));

		if (lower_bound->kind == PARTITION_RANGE_DATUM_MINVALUE)
			lower_val = DT_NOBEGIN;
		else if (lower_bound->kind == PARTITION_RANGE_DATUM_MAXVALUE)
			lower_val = DT_NOEND;
		else
		{
			lower = castNode(Const, lower_bound->value);
			switch (lower->consttype)
			{
			case DATEOID:
				lower_val = DatumGetInt64(DirectFunctionCall1(date_timestamptz,
															  lower->constvalue));
				break;
			case TIMESTAMPOID:
				lower_val = DatumGetInt64(DirectFunctionCall1(timestamp_timestamptz,
															  lower->constvalue));
				break;
			case TIMESTAMPTZOID:
				lower_val = DatumGetInt64(lower->constvalue);
				break;

			case INT2OID:
			case INT4OID:
			case INT8OID:
			case TIMEOID:
			case TIMETZOID:
			case INTERVALOID:
			default:
				/* Partition range types not comparable. */
				PG_RETURN_INT32(APM_BOUND_UNKNOWN);
			}
		}
	}

	if (pin >= upper_val)
		PG_RETURN_INT32(APM_BOUND_ANCIENT);
	if (pin < lower_val)
		PG_RETURN_INT32(APM_BOUND_FUTURE);
	PG_RETURN_INT32(APM_BOUND_CURRENT);
}

/*
 * Get partition's boundary in timestamp with time zone
 * Parameters:
 *  [0] oid of partition table
 * Returns
 *  NULL when partition boundary cannot be converted to timestamp
 *  [0]: lower timestamp with time zone
 *  [1]: upper timestamp with time zone
 */
Datum apm_partition_boundary(PG_FUNCTION_ARGS)
{
	ListCell *cell;
	Oid relid;
	Datum values[2];
	TupleDesc tupdesc;
	bool nulls[2];
	Datum result;
	HeapTuple tuple;
	PartitionBoundSpec *boundspec = NULL;

	relid = PG_GETARG_OID(0);
	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid OID")));

	boundspec = relid_to_boundspec(relid);

	/*
	 * If the PartitionBoundSpec says this is the default partition, return 0
	 * (not comparable)
	 */
	if (boundspec->is_default)
		PG_RETURN_NULL();

	/* Only range partition can evaluate bound, others returns unknown */
	if (boundspec->strategy != PARTITION_STRATEGY_RANGE)
		PG_RETURN_NULL();

	foreach (cell, boundspec->lowerdatums)
	{
		Const *lower;
		PartitionRangeDatum *lower_bound;

		lower_bound = castNode(PartitionRangeDatum, lfirst(cell));
		if (lower_bound->kind == PARTITION_RANGE_DATUM_MINVALUE)
			values[0] = DirectFunctionCall1(timestamp_timestamptz, DT_NOBEGIN);
		else if (lower_bound->kind == PARTITION_RANGE_DATUM_MAXVALUE)

			/*
			 * It is impossible that lower bound can be MAXVALUE, here is just
			 * a safe guard before touching actual value in the lower bound.
			 */
			PG_RETURN_NULL();
		else
		{
			lower = castNode(Const, lower_bound->value);
			switch (lower->consttype)
			{
			case DATEOID:
				values[0] = DirectFunctionCall1(date_timestamptz,
												lower->constvalue);
				break;
			case TIMESTAMPOID:
				values[0] = DirectFunctionCall1(timestamp_timestamptz,
												lower->constvalue);
				break;
			case TIMESTAMPTZOID:
				values[0] = lower->constvalue;
				break;

			case INT2OID:
			case INT4OID:
			case INT8OID:
			case TIMEOID:
			case TIMETZOID:
			case INTERVALOID:
			default:
				/* Partition range types not comparable. */
				PG_RETURN_NULL();
			}
		}
		break;
	}

	foreach (cell, boundspec->upperdatums)
	{
		Const *upper;
		PartitionRangeDatum *upper_bound;

		upper_bound = castNode(PartitionRangeDatum, lfirst(cell));
		if (upper_bound->kind == PARTITION_RANGE_DATUM_MAXVALUE)
			values[1] = DirectFunctionCall1(timestamp_timestamptz, DT_NOEND);
		else if (upper_bound->kind == PARTITION_RANGE_DATUM_MINVALUE)

			/*
			 * It is impossible that upper bound can be MINVALUE, here is just
			 * a safe guard before touching actual value in the upper bound.
			 */
			PG_RETURN_NULL();
		else
		{
			upper = castNode(Const, upper_bound->value);
			switch (upper->consttype)
			{
			case DATEOID:
				values[1] = DirectFunctionCall1(date_timestamptz,
												upper->constvalue);
				break;
			case TIMESTAMPOID:
				values[1] = DirectFunctionCall1(timestamp_timestamptz,
												upper->constvalue);
				break;
			case TIMESTAMPTZOID:
				values[1] = upper->constvalue;
				break;

			case INT2OID:
			case INT4OID:
			case INT8OID:
			case TIMEOID:
			case TIMETZOID:
			case INTERVALOID:
			default:
				/* Partition range types not comparable. */
				PG_RETURN_NULL();
			}
		}
		break;
	}

	tupdesc = CreateTemplateTupleDesc(2);
	TupleDescInitEntry(tupdesc, (AttrNumber)1, "lower", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber)2, "upper", TIMESTAMPTZOID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);
	memset(nulls, 0, sizeof(nulls));
	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

/*
 * Trigger function to intercept user CURD on APM_* objects
 */
Datum apm_metadata_protect(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *)fcinfo->context;
	HeapTuple rettuple = NULL;

	if (!ts_guc_apm_allow_dml)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("MatrixTS: change APM metadata is not allowed")));

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		rettuple = trigdata->tg_trigtuple;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		rettuple = trigdata->tg_newtuple;
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		rettuple = trigdata->tg_trigtuple;
	else
		elog(ERROR, "invalid trigger event");

	return PointerGetDatum(rettuple);
}

/*
 * Event trigger to intercept user DDL on APM_* objects
 */
Datum apm_schema_protect(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata;

	/*
	 * Disallow direct call to this UDF on master. This function should only
	 * be called from event trigger.
	 */
	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	/* When the guc is on, allow user modification. */
	if (ts_guc_apm_allow_ddl)
		PG_RETURN_NULL();

	trigdata = (EventTriggerData *)fcinfo->context;

	/* Only works on ddl_command_start event. */
	if (strcmp("ddl_command_start", trigdata->event) != 0)
		PG_RETURN_NULL();

	if (IsA(trigdata->parsetree, AlterTableStmt))
	{
		AlterTableStmt *atstmt = (AlterTableStmt *)trigdata->parsetree;
		Oid relid;
		Oid nspid;
		char *nsp_name;
		char *rel_name;
		ListCell *lcmd;

		foreach (lcmd, atstmt->cmds)
		{
			AlterTableCmd *cmd = (AlterTableCmd *)lfirst(lcmd);

			switch (cmd->subtype)
			{
			case AT_ExpandTable:
			case AT_SetTableSpace:
			case AT_CheckNotNull:
			case AT_SetStatistics:
			case AT_ValidateConstraint:
			case AT_ValidateConstraintRecurse:
				/* commands allowed without ts_guc_apm_allow_ddl */
				PG_RETURN_NULL();
				break;

			default:
				/* continue checking */
				break;
			}
		}

		relid = RangeVarGetRelid(atstmt->relation, NoLock, true);
		if (!OidIsValid(relid))
			PG_RETURN_NULL();

		nspid = get_rel_namespace(relid);
		if (!OidIsValid(nspid))
			PG_RETURN_NULL();

		nsp_name = get_namespace_name(nspid);
		if (nsp_name == NULL)
			PG_RETURN_NULL();

		if (strncmp("matrixts_internal", nsp_name, NAMEDATALEN) != 0)
			PG_RETURN_NULL();

		rel_name = get_rel_name(relid);
		if (rel_name == NULL)
			PG_RETURN_NULL();

		if (strncmp("apm_", rel_name, 4) != 0)
			PG_RETURN_NULL();

		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("MatrixTS: cannot alter metatable %s", rel_name),
				 errhint("SET matrixts.ts_guc_apm_allow_ddl TO on to allow this operation.")));
	}

	PG_RETURN_NULL();
}

/*
 * Get PartitionBoundSpec by relid
 */
PartitionBoundSpec *
relid_to_boundspec(Oid relid)
{
	HeapTuple tuple;
	Datum datum;
	bool isnull;

	PartitionBoundSpec *boundspec = NULL;

	if (!OidIsValid(relid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid OID")));

	/* Try fetching the tuple from the catcache. */
	tuple = SearchSysCache1(RELOID, relid);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("could not open relation with OID %u", relid)));

	/* Get relpartbound of input relid. */
	datum = SysCacheGetAttr(RELOID, tuple,
							Anum_pg_class_relpartbound,
							&isnull);
	if (!isnull)
		boundspec = stringToNode(TextDatumGetCString(datum));
	ReleaseSysCache(tuple);

	/*
	 * The system cache may be out of date; if so, we may find no pg_class
	 * tuple or an old one where relpartbound is NULL.  In that case, try the
	 * table directly.
	 *
	 * Note: following logic was borrowed from RelationBuildPartitionDesc().
	 * It pointed out that concurrent ATTACH PARTITION operation may lead to
	 * invalidate system cache so we have to scan the raw catalog table.
	 */
	if (boundspec == NULL)
	{
		Relation pg_class;
		SysScanDesc scan;
		ScanKeyData key[1];
		Datum datum;
		bool isnull;

		pg_class = table_open(RelationRelationId, AccessShareLock);
		ScanKeyInit(&key[0],
					Anum_pg_class_oid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(relid));
		scan = systable_beginscan(pg_class, ClassOidIndexId, true,
								  NULL, 1, key);
		tuple = systable_getnext(scan);

		if (!tuple)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("could not find pg_class entry for OID %u", relid)));
		datum = heap_getattr(tuple, Anum_pg_class_relpartbound,
							 RelationGetDescr(pg_class), &isnull);
		if (!isnull)
			boundspec = stringToNode(TextDatumGetCString(datum));

		systable_endscan(scan);
		table_close(pg_class, AccessShareLock);
	}

	/* Sanity checks. */
	if (!boundspec)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("missing relpartbound for relation %u", relid)));
	if (!IsA(boundspec, PartitionBoundSpec))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("invalid relpartbound for relation %u", relid)));

	return boundspec;
}

Datum apm_max_timestamp(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMP(DT_NOEND);
}
Datum apm_max_timestamptz(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(DT_NOEND);
}
Datum apm_min_timestamp(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMP(DT_NOBEGIN);
}
Datum apm_min_timestamptz(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(DT_NOBEGIN);
}

/*
 * Given an interval, check if it is okay to be used as partition period.
 * time_bucket cannot support month, year, century.
 */
Datum apm_is_valid_partition_period(PG_FUNCTION_ARGS)
{
	Interval *interval;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	interval = PG_GETARG_INTERVAL_P(0);

	if (interval->month > 0)
	{
		if (interval->day > 0 || interval->time > 0)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("MatrixTS: cannot use month with additional day/time specification as partition period."),
					 errdetail("Such as \"1 month 1 day\", \"1 year 1 second\", nobody can do.")));
			PG_RETURN_BOOL(false);
		}

		/*
		 * for "1 month" or "1 year", although time_bucket() does not support,
		 * we can fallback to date_trunc() to align borders
		 */
		if (interval->month == 1)
			/* date_trunc by month */
			PG_RETURN_BOOL(true);
		if (interval->month == 3)
			/* date_trunc by quarter */
			PG_RETURN_BOOL(true);
		if (interval->month == 12)
			/* date_trunc by year */
			PG_RETURN_BOOL(true);

		ereport(NOTICE,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("MatrixTS: cannot use %d month as period specification.", interval->month),
				 errhint("Only 1/3/12 months is supported.")));
		PG_RETURN_BOOL(false);
	}
	else if ((interval->month < 0) ||
			 (interval->time + (interval->day * USECS_PER_DAY) <= 0))
	{
		ereport(NOTICE,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("MatrixTS: period must be greater than 0.")));
		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(true);
}

/*
 * time_trunc_internal is an extended version of time_bucket, it can handle
 * 1 month, 1 quarter and 1 year which common for partition boundary but not
 * implemented by time_bucket.
 */
static Datum
time_trunc_internal(Interval *interval,
					Datum ts,
					Datum (*bucket_func)(PG_FUNCTION_ARGS),
					Datum (*trunc_func)(PG_FUNCTION_ARGS))
{
	if (interval->month == 0)

		/*
		 * When month is not given, this interval is okay for time_bucket
		 * direct call time_bucket to truncate the input time
		 */
		return DirectFunctionCall2(bucket_func,
								   IntervalPGetDatum(interval),
								   ts);
	else
	{
		/*
		 * When month is given, time_bucket function no more works, we
		 * fallback to date_trunc for 1 month/quarter/year, but other inputs
		 * leads to error.
		 */
		if (interval->day > 0 || interval->time > 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("MatrixTS: cannot use month with time spec as partition period")));

		switch (interval->month)
		{
		case 1:
			return DirectFunctionCall2(trunc_func,
									   CStringGetTextDatum("month"),
									   ts);

		case 3:
			return DirectFunctionCall2(trunc_func,
									   CStringGetTextDatum("quarter"),
									   ts);

		case 12:
			return DirectFunctionCall2(trunc_func,
									   CStringGetTextDatum("year"),
									   ts);

		default:
			ereport(ERROR,
					(errcode(ERRCODE_DATA_EXCEPTION),
					 errmsg("MatrixTS: unsupported interval"),
					 errhint("please use '1 month/quarter/year' as period")));
		}
	}
}

Datum apm_time_trunc_ts(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	return time_trunc_internal(PG_GETARG_INTERVAL_P(0),
							   PG_GETARG_DATUM(1),
							   ts_timestamp_bucket,
							   timestamp_trunc);
}

Datum apm_time_trunc_tz(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	return time_trunc_internal(PG_GETARG_INTERVAL_P(0),
							   PG_GETARG_DATUM(1),
							   ts_timestamptz_bucket,
							   timestamptz_trunc);
}

Datum apm_date_trunc(PG_FUNCTION_ARGS)
{
	Datum trunced;

	trunced = DirectFunctionCall2(timestamp_trunc,
								  PG_GETARG_DATUM(0),
								  DirectFunctionCall1(date_timestamp,
													  PG_GETARG_DATUM(1)));

	return DirectFunctionCall1(timestamp_date, trunced);
}

Datum apm_time_trunc_date(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	return time_trunc_internal(PG_GETARG_INTERVAL_P(0),
							   PG_GETARG_DATUM(1),
							   ts_date_bucket,
							   apm_date_trunc);
}

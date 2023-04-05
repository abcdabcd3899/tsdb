/*-------------------------------------------------------------------------
 *
 * mx_mars.c
 *	  internal specifications of the MARS relation storage.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/catalog/mx_mars.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/gp_fastsequence.h"
#include "catalog/indexing.h"
#include "catalog/mx_mars.h"
#include "catalog/mx_mars_d.h"
#include "catalog/namespace.h"
#include "catalog/pg_appendonly_fn.h"
#include "catalog/pg_am.h"
#include "commands/defrem.h"
#include "cdb/cdbvars.h"
#include "optimizer/mx_paths.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

/* TODO: create a MARS table ModifyTableNode to store it. */
List *mars_infer_elems = NIL;

List *mx_monotonic_procs = NIL;

static Datum
option_get_attrnum_array(TupleDesc desc, relopt_value *options, int num_options,
						 const char *opt_name, List **attrList);

static void
option_get_attr_param_array(TupleDesc desc, relopt_value *options, int num_options,
							const char *opt_name, Datum *attrArray, Datum *paramArray, List **attrList);

static List *
optArrayToList(ArrayType *optArray, ArrayType *paramArray)
{
	int nElems;
	Datum *elems;
	int i;
	List *ret = NIL;
	if (optArray == NULL)
		return ret;

	deconstruct_array(optArray, INT2OID, 2, true, 's', &elems, NULL, &nElems);

	for (i = 0; i < nElems; i++)
	{
		MarsDimensionAttr *attr = makeNode(MarsDimensionAttr);
		attr->type = T_MarsDimensionAttr;
		attr->dimensiontype = MARS_DIM_ATTR_TYPE_DEFAULT;
		attr->attnum = elems[i];
		ret = lappend(ret, attr);
	}

	if (paramArray != NULL)
	{
		ListCell *lc;
		deconstruct_array(paramArray, INT8OID, 8, true, 'd', &elems, NULL, &nElems);

		foreach_with_count(lc, ret, i)
		{
			((MarsDimensionAttr *)lfirst(lc))->dimensiontype = MARS_DIM_ATTR_TYPE_WITH_PARAM;
			((MarsDimensionAttr *)lfirst(lc))->param = elems[i];
		}
	}

	return ret;
}

static inline int64
get_interval_period_timestamp_units(const Interval *interval)
{
	if (interval->month != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("mars: interval defined in terms of month, year, century etc. not supported")));
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

static Interval *
transAndValidateInterval(const char *str, TupleDesc desc, AttrNumber timeAttr)
{
	Form_pg_attribute attr = &desc->attrs[timeAttr - 1];
	Interval *interval = NULL;

	if (attr->atttypid == TIMESTAMPOID ||
		attr->atttypid == TIMESTAMPTZOID)
	{
		PG_TRY();
		{
			interval = DatumGetIntervalP(DirectFunctionCall3(interval_in,
															 CStringGetDatum(str),
															 ObjectIdGetDatum(InvalidOid),
															 Int32GetDatum(-1)));
		}
		PG_CATCH();
		{
			/* errors are reported outside the try-catch block. */
		}
		PG_END_TRY();
	}
	else if (attr->atttypid == INT2OID ||
			 attr->atttypid == INT4OID ||
			 attr->atttypid == INT8OID)
	{
		PG_TRY();
		{
			interval = palloc0(sizeof(Interval));
			interval->time = DirectFunctionCall1(int8in, CStringGetDatum(str));
		}
		PG_CATCH();
		{
			/* errors are reported outside the try-catch block. */
		}
		PG_END_TRY();
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("\"%s\" does not support param.", NameStr(attr->attname))));

	if (!interval)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("\"%s\" param \"%s\" is not legal Interval expression.",
						NameStr(attr->attname), str)));

	return interval;
}

/*
 * translate option text array to period array
 */
static void
option_get_attr_param_array(TupleDesc desc, relopt_value *options, int num_options,
							const char *opt_name, Datum *attrArray, Datum *paramArray, List **attrList)
{
	Datum attr_array = 0;
	Datum param_array = 0;
	relopt_value *v = get_option_set(options, num_options, opt_name);
	if (v)
	{
		Datum optArray = 0;
		Datum *textElems;
		Datum *attrElems;
		Datum *paramElems;
		int nElems;
		Oid typinput,
			typioparam;
		AttrNumber attr;

		getTypeInputInfo(TEXTARRAYOID, &typinput, &typioparam);
		optArray = OidInputFunctionCall(typinput, v->values.string_val, typioparam, -1);
		deconstruct_array(DatumGetArrayTypeP(optArray), TEXTOID, -1, false, 'i',
						  &textElems, NULL, &nElems);

		if (nElems > 0)
		{
			attrElems = palloc0(sizeof(Datum) * nElems);
			paramElems = palloc0(sizeof(Datum) * nElems);
			for (int i = 0; i < nElems; i++)
			{
				/*
				 * Parser MARS global_param_col option. Each option format is
				 * `A in B`.
				 * `A` reference to a relation column, the type should be int
				 * or timestamp.
				 * B type is an int or interval.
				 * The delimiter is `in`.
				 */
				static const char *delimiter = " in ";
				static const int del_len = 4;
				char *ptr = NULL;
				Interval *interval = NULL;

				char *col = TextDatumGetCString(textElems[i]);
				if ((ptr = strstr(col, delimiter)) != NULL)
				{
					*ptr = '\0';
					attr = GetAttrNumByName(desc, col);

					if (!AttrNumberIsForUserDefinedAttr(attr) ||
						attr > desc->natts)
						ereport(ERROR,
								(errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
								 errmsg("\"%s\" is not column name. Please re-check option.", col)));

					ptr += del_len;
					interval = transAndValidateInterval(ptr, desc, attr);
					ptr -= del_len;
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_NAME),
							 errmsg("\"%s\" does not contain delimiter \"in\"", col)));
				}

				attrElems[i] = attr;
				paramElems[i] = get_interval_period_timestamp_units(interval);

				if (attrList)
					*attrList = lappend_int(*attrList, attr);
			}
			attr_array = PointerGetDatum(construct_array(attrElems, nElems, INT2OID, 2, true, 's'));
			param_array = PointerGetDatum(construct_array(paramElems, nElems, INT8OID, 8, true, 'd'));
		}
	}
	*attrArray = attr_array;
	*paramArray = param_array;
}

/*
 * translate option text array to attr_num array
 */
Datum option_get_attrnum_array(TupleDesc desc, relopt_value *options, int num_options, const char *opt_name, List **attrList)
{
	Datum attr_array = 0;
	relopt_value *v = get_option_set(options, num_options, opt_name);
	if (v)
	{
		Datum optArray = 0;
		Datum *textElems;
		Datum *attrElems;
		int nElems;
		Oid typinput,
			typioparam;
		getTypeInputInfo(TEXTARRAYOID, &typinput, &typioparam);
		optArray = OidInputFunctionCall(typinput, v->values.string_val, typioparam, -1);
		deconstruct_array(DatumGetArrayTypeP(optArray), TEXTOID, -1, false, 'i',
						  &textElems, NULL, &nElems);

		if (nElems > 0)
		{
			attrElems = palloc0(sizeof(Datum) * nElems);

			for (int i = 0; i < nElems; i++)
			{
				Form_pg_attribute pAttr;
				char *col = TextDatumGetCString(textElems[i]);
				AttrNumber attr = GetAttrNumByName(desc, col);

				if (!AttrNumberIsForUserDefinedAttr(attr) ||
					attr > desc->natts)
					ereport(ERROR,
							(errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
							 errmsg("\"%s\" is not column name. Please re-check option.", col)));

				pAttr = &desc->attrs[AttrNumberGetAttrOffset(attr)];

				if ((pg_strcasecmp(opt_name, MX_MARS_GLOBAL_ORDER_COLUMN) == 0) ||
					(pg_strcasecmp(opt_name, MX_MARS_LOCAL_ORDER_COLUMN) == 0))
				{
					if (!InByvalTypeAllowedList(pAttr->atttypid) &&
						!InVarlenTypeAllowedList(pAttr->atttypid))
					{
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("%s column \"%s\"'s type request to be an INT, FLOAT, TIMESTAMP,\n"
										"VARCHAR, TEXT, NAME, INTERVAL or NUMERIC.",
										opt_name, col)));
					}
				}
				else if (pg_strcasecmp(opt_name, MX_MARS_GROUP_COLUMN) == 0)
				{
					if (!pAttr->attbyval &&
						!InVarlenTypeAllowedList(pAttr->atttypid))
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("%s column \"%s\"'s type is not unspported.",
										opt_name, col),
								 errhint("It requests to be a fixed size type or one of supported types: varchar, text, name, interval and numeric.")));
				}

				attrElems[i] = attr;
				if (attrList)
				{
					if (list_member_int(*attrList, (int)attr))
					{
						ereport(ERROR,
								(errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
								 errmsg("%s column \"%s\"'s is duplicated.",
										opt_name, col)));
					}

					*attrList = lappend_int(*attrList, attr);
				}
			}

			attr_array = PointerGetDatum(construct_array(attrElems, nElems, INT2OID, 2, true, 's'));
		}
	}

	return attr_array;
}

static void
addNotNullContraint(Datum value, bool is_null, TupleDesc tuple_desc)
{
	int elem_num;
	int i;

	Form_pg_attribute attr;
	Datum *elems;
	ArrayType *opt_array = (ArrayType *)DatumGetPointer(value);

	if (is_null || opt_array == NULL)
		return;

	deconstruct_array(opt_array, INT2OID, 2, true, 's', &elems, NULL, &elem_num);

	for (i = 0; i < elem_num; i++)
	{
		AttrNumber attr_num = elems[i];

		if (attr_num > tuple_desc->natts ||
			!AttrNumberIsForUserDefinedAttr(attr_num))
		{
			ereport(ERROR,
					(errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
					 errmsg("Column \"%d\" is beyond attr range. Please re-check option.",
							attr_num)));
		}

		attr = &tuple_desc->attrs[AttrNumberGetAttrOffset(attr_num)];
		attr->attnotnull = true;
	}
}

void InsertMARSEntryWithValidation(Oid relOid, Datum reloptions, TupleDesc tupleDesc)
{
	Relation mx_mars_rel;
	HeapTuple mx_mars_tuple = NULL;
	bool *nulls;
	Datum *values;
	int natts = 0;
	int num_options;
	relopt_value *options;
	relopt_value *tag_opt;
	relopt_value *time_opt;
	relopt_value *time_param_opt;

	options = parseRelOptions(reloptions, false, RELOPT_KIND_MARS, &num_options);
	tag_opt = get_option_set(options, num_options, MX_MARS_TAG_KEY);
	time_opt = get_option_set(options, num_options, MX_MARS_TIME_KEY);
	time_param_opt = get_option_set(options, num_options, MX_MARS_TIME_BUCKET);

	mx_mars_rel = table_open(MARSRelationId, RowExclusiveLock);

	natts = Natts_mx_mars;
	values = palloc0(sizeof(Datum) * natts);
	nulls = palloc0(sizeof(bool) * natts);
	values[Anum_mx_mars_relid - 1] = ObjectIdGetDatum(relOid);

	if (tag_opt || time_opt)
	{
		/* Parser simple grammar */
		List *tag_list = NIL;
		ListCell *lc;
		Interval *interval = NULL;
		AttrNumber time_attr = 0;
		int64 period = 0;
		Datum d[2];

		if (tag_opt)
		{
			/* try parse tag key using single key firstly */
			Form_pg_attribute pAttr;
			AttrNumber tag_attr = GetAttrNumByName(tupleDesc, tag_opt->values.string_val);

			if (AttrNumberIsForUserDefinedAttr(tag_attr) &&
				tag_attr <= tupleDesc->natts)
			{
				pAttr = &tupleDesc->attrs[AttrNumberGetAttrOffset(tag_attr)];
				if (!pAttr->attbyval && !InVarlenTypeAllowedList(pAttr->atttypid))
				{
					ereport(ERROR,
							(errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
							 errmsg("tagkey column \"%s\"'s type is not unspported.",
									tag_opt->values.string_val),
							 errhint("It requests to be a fixed size type or one of supported types: varchar, text, name, interval and numeric.")));
				}

				tag_list = lappend_int(tag_list, tag_attr);
			}
			else
			{
				/* multiple tagkeys, like tagkey='{tag1, tag2}', ....*/
				PG_TRY();
				{
					option_get_attrnum_array(tupleDesc, options, num_options,
											 MX_MARS_TAG_KEY, &tag_list);
				}
				PG_CATCH();
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
							 errmsg("TagKey specification error: \"%s\". Please re-check option.",
									tag_opt->values.string_val)));
				}
				PG_END_TRY();
			}
		}

		if (time_opt)
		{
			Form_pg_attribute pAttr;
			time_attr = GetAttrNumByName(tupleDesc, time_opt->values.string_val);

			if (!AttrNumberIsForUserDefinedAttr(time_attr) ||
				time_attr > tupleDesc->natts)
				ereport(ERROR,
						(errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
						 errmsg("\"%s\" is not column name. Please re-check option.",
								time_opt->values.string_val)));

			pAttr = &tupleDesc->attrs[AttrNumberGetAttrOffset(time_attr)];

			if (pAttr->atttypid != TIMESTAMPOID &&
				pAttr->atttypid != TIMESTAMPTZOID)
			{
				ereport(ERROR,
						(errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
						 errmsg("timekey column \"%s\"'s type request to be an TIMESTAMP.",
								time_opt->values.string_val)));
			}
		}

		if (list_member_int(tag_list, (int)time_attr))
			ereport(ERROR,
					(errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
					 errmsg("duplicate key value for tagkey and timekey.")));

		if (time_param_opt)
		{
			interval = transAndValidateInterval(time_param_opt->values.string_val,
												tupleDesc,
												time_attr);
			period = get_interval_period_timestamp_units(interval);
			values[Anum_mx_mars_grpParam - 1] =
				PointerGetDatum(construct_array(&period, 1, INT8OID, 8, true, 'd'));

			if (time_attr != 0)
			{
				d[0] = Int16GetDatum(time_attr);
				values[Anum_mx_mars_grpWithParamOpt - 1] =
					PointerGetDatum(construct_array(d, 1, INT2OID, 2, true, 's'));
			}

			if (tag_list != NIL)
			{
				int n = 0;
				Datum *groups = palloc(sizeof(Datum) * list_length(tag_list));

				foreach_with_count(lc, tag_list, n)
				{
					groups[n] = Int16GetDatum(lfirst_int(lc));
				}

				values[Anum_mx_mars_grpOpt - 1] =
					PointerGetDatum(construct_array(groups, n, INT2OID, 2, true, 's'));
				pfree(groups);
			}
		}
		else
		{
			int n = 0;

			/* if time_param_opt not specified, regard timekey as group key */
			Datum *groups = palloc(sizeof(Datum) * (list_length(tag_list) + 1));

			foreach_with_count(lc, tag_list, n)
			{
				groups[n] = Int16GetDatum(lfirst_int(lc));
			}

			if (time_attr != 0)
				groups[n++] = Int16GetDatum(time_attr);

			values[Anum_mx_mars_grpOpt - 1] =
				PointerGetDatum(construct_array(groups, n, INT2OID, 2, true, 's'));
			pfree(groups);
		}

		if (time_attr != 0)
		{
			d[0] = Int16GetDatum(time_attr);
			values[Anum_mx_mars_localOrder - 1] =
				PointerGetDatum(construct_array(d, 1, INT2OID, 2, true, 's'));
		}
	}
	else
	{
		List *orderList = NIL;
		List *optionList = NIL;
		ListCell *lc;
		Bitmapset *optAttrBm = NULL;
		/* Parser accurate grammar */
		values[Anum_mx_mars_grpOpt - 1] =
			option_get_attrnum_array(tupleDesc,
									 options,
									 num_options,
									 MX_MARS_GROUP_COLUMN,
									 &optionList);

		values[Anum_mx_mars_globalOrder - 1] =
			option_get_attrnum_array(tupleDesc,
									 options,
									 num_options,
									 MX_MARS_GLOBAL_ORDER_COLUMN,
									 &orderList);

		values[Anum_mx_mars_localOrder - 1] =
			option_get_attrnum_array(tupleDesc,
									 options,
									 num_options,
									 MX_MARS_LOCAL_ORDER_COLUMN,
									 NULL);

		option_get_attr_param_array(tupleDesc,
									options,
									num_options,
									MX_MARS_GROUP_PARAMETER_COLUMN,
									&values[Anum_mx_mars_grpWithParamOpt - 1],
									&values[Anum_mx_mars_grpParam - 1],
									&optionList);

		foreach (lc, optionList)
		{
			if (bms_is_member(lfirst_int(lc), optAttrBm))
				ereport(ERROR,
						(errcode(ERRCODE_WITH_CHECK_OPTION_VIOLATION),
						 errmsg("duplicate key value in %s, %s arrays.",
								MX_MARS_GROUP_COLUMN, MX_MARS_GLOBAL_ORDER_COLUMN)));

			optAttrBm = bms_add_member(optAttrBm, lfirst_int(lc));
		}

		// TODO::populate optionList and orderList
		{
			int i;
			int size = bms_num_members(optAttrBm);
			foreach_with_count(lc, orderList, i)
			{
				if (i >= size)
					break;

				if (!bms_is_member(lfirst_int(lc), optAttrBm))
				{
					ereport(ERROR,
							errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("column %s required in group_col_ or group_param_col_ array",
								   NameStr(tupleDesc->attrs[lfirst_int(lc) - 1].attname)));
				}
			}
		}
	}

	nulls[Anum_mx_mars_relid - 1] = false;
	nulls[Anum_mx_mars_grpOpt - 1] = values[Anum_mx_mars_grpOpt - 1] == 0;
	nulls[Anum_mx_mars_grpWithParamOpt - 1] = values[Anum_mx_mars_grpWithParamOpt - 1] == 0;
	nulls[Anum_mx_mars_grpParam - 1] = values[Anum_mx_mars_grpParam - 1] == 0;
	nulls[Anum_mx_mars_globalOrder - 1] = values[Anum_mx_mars_globalOrder - 1] == 0;
	nulls[Anum_mx_mars_localOrder - 1] = values[Anum_mx_mars_localOrder - 1] == 0;
	nulls[Anum_mx_mars_segrelid - 1] = true;

	/* Add constraint of "not null" for all tagkey */
	addNotNullContraint((Datum)values[Anum_mx_mars_grpOpt - 1],
						nulls[Anum_mx_mars_grpOpt - 1],
						tupleDesc);

	addNotNullContraint((Datum)values[Anum_mx_mars_grpWithParamOpt - 1],
						nulls[Anum_mx_mars_grpWithParamOpt - 1],
						tupleDesc);

	mx_mars_tuple = heap_form_tuple(RelationGetDescr(mx_mars_rel), values, nulls);

	CatalogTupleInsert(mx_mars_rel, mx_mars_tuple);

	table_close(mx_mars_rel, NoLock);

	free_options_deep(options, num_options);

	pfree(values);
	pfree(nulls);
}

static HeapTuple
GetMARSEntryForMove(Relation mx_mars_rel, TupleDesc mx_mars_dsc,
					Oid relId, Oid *segrelid)
{
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple tuple;
	Datum auxOid;
	bool isNull;

	ScanKeyInit(&key[0],
				Anum_mx_mars_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relId));

	scan = systable_beginscan(mx_mars_rel, MarsRelidIndexId, true,
							  NULL, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for relId %u", relId);

	auxOid = heap_getattr(tuple,
						  Anum_mx_mars_segrelid,
						  mx_mars_dsc,
						  &isNull);
	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("got invalid segrelid value: NULL")));

	*segrelid = DatumGetObjectId(auxOid);

	tuple = heap_copytuple(tuple);

	systable_endscan(scan);

	return tuple;
}

void GetMARSEntryOption(Oid relid, List **groupkeys, List **global_orderkeys, List **local_orderkeys)
{
	Relation mx_mars;
	TupleDesc tupDesc;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple tuple;
	bool isNull;
	Datum value;

	mx_mars = table_open(MARSRelationId, AccessShareLock);
	tupDesc = RelationGetDescr(mx_mars);

	ScanKeyInit(&key[0],
				Anum_mx_mars_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(mx_mars, MarsRelidIndexId, true,
							  NULL, 1, key);

	tuple = systable_getnext(scan);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing mx_mars entry for relation \"%s\"",
						get_rel_name(relid))));

	if (groupkeys != NULL)
	{
		ArrayType *array1 = NULL;
		ArrayType *array2 = NULL;
		*groupkeys = NULL;

		value = heap_getattr(tuple,
							 Anum_mx_mars_grpOpt,
							 tupDesc,
							 &isNull);
		if (!isNull)
			array1 = DatumGetArrayTypeP(value);

		*groupkeys = list_concat(*groupkeys, optArrayToList(array1, NULL));

		/* reset array1 */
		array1 = NULL;

		value = heap_getattr(tuple,
							 Anum_mx_mars_grpWithParamOpt,
							 tupDesc,
							 &isNull);
		if (!isNull)
			array1 = DatumGetArrayTypeP(value);

		value = heap_getattr(tuple,
							 Anum_mx_mars_grpParam,
							 tupDesc,
							 &isNull);
		if (!isNull)
			array2 = DatumGetArrayTypeP(value);

		*groupkeys = list_concat(*groupkeys, optArrayToList(array1, array2));
	}

	if (global_orderkeys != NULL)
	{
		value = heap_getattr(tuple,
							 Anum_mx_mars_globalOrder,
							 tupDesc,
							 &isNull);
		if (!isNull)
		{
			ArrayType *array = DatumGetArrayTypeP(value);
			*global_orderkeys = optArrayToList(array, NULL);
		}
		else
			*global_orderkeys = NULL;
	}

	if (local_orderkeys != NULL)
	{
		value = heap_getattr(tuple,
							 Anum_mx_mars_localOrder,
							 tupDesc,
							 &isNull);
		if (!isNull)
		{
			ArrayType *array = DatumGetArrayTypeP(value);
			*local_orderkeys = optArrayToList(array, NULL);
		}
		else
			*local_orderkeys = NULL;
	}

	systable_endscan(scan);
	table_close(mx_mars, AccessShareLock);
}

void GetMARSEntryAuxOids(Oid relid,
						 Snapshot metadataSnapshot,
						 Oid *segrelid)
{
	Relation mx_mars;
	TupleDesc tupDesc;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple tuple;
	Datum auxOid;
	bool isNull;

	mx_mars = table_open(MARSRelationId, AccessShareLock);
	tupDesc = RelationGetDescr(mx_mars);

	ScanKeyInit(&key[0],
				Anum_mx_mars_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(mx_mars, MarsRelidIndexId, true,
							  metadataSnapshot, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing mx_mars entry for relation \"%s\"",
						get_rel_name(relid))));

	if (segrelid != NULL)
	{
		auxOid = heap_getattr(tuple,
							  Anum_mx_mars_segrelid,
							  tupDesc,
							  &isNull);
		Assert(!isNull);
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("got invalid segrelid value: NULL")));

		*segrelid = DatumGetObjectId(auxOid);
	}

	systable_endscan(scan);
	table_close(mx_mars, AccessShareLock);
}

void UpdateMARSEntryAuxOids(Oid relid,
							Oid newSegrelid)
{
	Relation mx_mars;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple tuple, newTuple;
	Datum newValues[Natts_mx_mars];
	bool newNulls[Natts_mx_mars];
	bool replace[Natts_mx_mars];

	/*
	 * Check the mx_mars relation to be certain the ao table
	 * is there.
	 */
	mx_mars = table_open(MARSRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_mx_mars_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(mx_mars, MarsRelidIndexId, true,
							  NULL, 1, key);
	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing mx_mars entry for relation \"%s\"",
						get_rel_name(relid))));

	MemSet(newValues, 0, sizeof(newValues));
	MemSet(newNulls, false, sizeof(newNulls));
	MemSet(replace, false, sizeof(replace));

	if (OidIsValid(newSegrelid))
	{
		replace[Anum_mx_mars_segrelid - 1] = true;
		newValues[Anum_mx_mars_segrelid - 1] = newSegrelid;
	}

	newTuple = heap_modify_tuple(tuple, RelationGetDescr(mx_mars),
								 newValues, newNulls, replace);
	CatalogTupleUpdate(mx_mars, &newTuple->t_self, newTuple);

	heap_freetuple(newTuple);

	systable_endscan(scan);
	table_close(mx_mars, RowExclusiveLock);
}

void RemoveMARSEntry(Oid relid)
{
	Relation mx_mars_rel;
	ScanKeyData key[1];
	SysScanDesc scan;
	HeapTuple tuple;
	Oid segrelid = InvalidOid;

	mx_mars_rel = table_open(MARSRelationId, RowExclusiveLock);

	ScanKeyInit(&key[0],
				Anum_mx_mars_relid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(mx_mars_rel, MarsRelidIndexId, true,
							  NULL, 1, key);

	tuple = systable_getnext(scan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("mars table relid \"%d\" does not exist in "
						"mx_mars",
						relid)));

	{
		bool isNull;
		Datum datum = heap_getattr(tuple,
								   Anum_mx_mars_relid,
								   RelationGetDescr(mx_mars_rel),
								   &isNull);

		Assert(!isNull);
		segrelid = DatumGetObjectId(datum);
		Assert(OidIsValid(segrelid));
	}

	/* Piggyback here to remove gp_fastsequence entries */
	RemoveFastSequenceEntry(segrelid);

	/*
	 * Delete the mart table entry from the catalog (mx_mars).
	 */
	simple_heap_delete(mx_mars_rel, &tuple->t_self);

	/* Finish up scan and close mars catalog. */
	systable_endscan(scan);
	table_close(mx_mars_rel, NoLock);
}

bool AmIsMars(Oid amoid)
{
	Form_pg_am amform;
	HeapTuple tuple;
	bool ismars;

	/*
	 * Look up the amname with the amoid, it does the same thing as
	 * get_am_name(), we do not call that simply because we don't want to pay
	 * the cost of pstrdup() & pfree() of the amname.
	 */
	tuple = SearchSysCache1(AMOID, ObjectIdGetDatum(amoid));
	if (!HeapTupleIsValid(tuple))
		return false; /* No such AM */

	amform = (Form_pg_am)GETSTRUCT(tuple);
	ismars = strcmp(NameStr(amform->amname), "mars") == 0;
	ReleaseSysCache(tuple);

	return ismars;
}

bool IsMarsAM(Oid relid)
{
	Form_pg_class classform;
	HeapTuple tuple;
	Oid amoid;

	/* FIXME: do we have a better way to do this */

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return false; /* No such relation */

	classform = (Form_pg_class)GETSTRUCT(tuple);
	amoid = classform->relam;
	ReleaseSysCache(tuple);

	return AmIsMars(amoid);
}

bool RelationIsMars(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_RELATION)
		return false;

	/*
	 * FIXME: relam can be InvalidOid when rel is the parent of a partitioned
	 * table, does it matter?
	 */
	return AmIsMars(rel->rd_rel->relam);
}

bool mars_monotonic_function_catalog_init(bool *try_load)
{
	bool isnull;
	Oid mono_oid;
	Relation mono_rel;
	HeapTuple mono_tuple;
	TableScanDesc scan;
	TupleDesc tupdesc;
	MemoryContext oldctx;
	Datum d;
	List *names = NULL;
	*try_load = false;

	if (mx_monotonic_procs)
	{
		list_free_deep(mx_monotonic_procs);
		mx_monotonic_procs = NIL;
	}

	names = lcons(makeString("matrixts_internal"),
				  lcons(makeString("monotonic_func"), NULL));

	mono_oid = RangeVarGetRelidExtended(makeRangeVarFromNameList(names),
										AccessShareLock,
										RVR_MISSING_OK, NULL, NULL);
	if (!OidIsValid(mono_oid))
		return false;

	mono_rel = table_open(mono_oid, AccessShareLock);
	tupdesc = RelationGetDescr(mono_rel);
	scan = table_beginscan(mono_rel, GetTransactionSnapshot(), 0, NULL);

	oldctx = MemoryContextSwitchTo(TopMemoryContext);

	while ((mono_tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		mono_proc_info *info = (mono_proc_info *)palloc0(sizeof(mono_proc_info));

		d = heap_getattr(mono_tuple, 1, tupdesc, &isnull);

		/* has not null constraint */
		info->proc = DatumGetObjectId(d);

		d = heap_getattr(mono_tuple, 2, tupdesc, &isnull);
		if (!isnull)
			info->equivalence_arg = DatumGetInt16(d);

		d = heap_getattr(mono_tuple, 3, tupdesc, &isnull);
		if (!isnull)
		{
			ArrayType *array = DatumGetArrayTypeP(d);

			deconstruct_array(array, REGPROCEDUREOID, 8, true, 'i',
							  &info->equivalence_proc, NULL /* isnull */, &info->equivalence_proc_num);
		}
		mx_monotonic_procs = lappend(mx_monotonic_procs, info);
	}

	MemoryContextSwitchTo(oldctx);

	table_endscan(scan);

	table_close(mono_rel, AccessShareLock);

	return mx_monotonic_procs != NULL;
}

/*
 * derived from SwapAppendonlyEntries().
 * Swap mx_mars entries between tables.
 */
void SwapMarsAuxEntries(Oid datarelid1, Oid datarelid2)
{
	Relation mx_mars_rel;
	TupleDesc mx_mars_dec;
	HeapTuple tupleCopy1;
	HeapTuple tupleCopy2;
	Oid segrelid1;
	Oid segrelid2;
	Datum *newValues;
	bool *newNulls;
	bool *replace;

	if (!IsMarsAM(datarelid1) || !IsMarsAM(datarelid2))
	{
		if (IsMarsAM(datarelid1) || IsMarsAM(datarelid2))
			elog(ERROR, "swapping auxiliary table oid is not permitted for non-mars tables");
		return;
	}

	mx_mars_rel = table_open(MARSRelationId, RowExclusiveLock);
	mx_mars_dec = RelationGetDescr(mx_mars_rel);

	tupleCopy1 = GetMARSEntryForMove(mx_mars_rel, mx_mars_dec, datarelid1, &segrelid1);
	tupleCopy2 = GetMARSEntryForMove(mx_mars_rel, mx_mars_dec, datarelid2, &segrelid2);

	/* Since gp_fastsequence entry is referenced by aosegrelid, it rides along  */
	simple_heap_delete(mx_mars_rel, &tupleCopy1->t_self);
	simple_heap_delete(mx_mars_rel, &tupleCopy2->t_self);

	/*
	 * (Re)insert.
	 */
	newValues = palloc0(mx_mars_dec->natts * sizeof(Datum));
	newNulls = palloc0(mx_mars_dec->natts * sizeof(bool));
	replace = palloc0(mx_mars_dec->natts * sizeof(bool));

	replace[Anum_mx_mars_relid - 1] = true;
	newValues[Anum_mx_mars_relid - 1] = datarelid2;

	tupleCopy1 = heap_modify_tuple(tupleCopy1, mx_mars_dec,
								   newValues, newNulls, replace);

	CatalogTupleInsert(mx_mars_rel, tupleCopy1);

	heap_freetuple(tupleCopy1);

	newValues[Anum_mx_mars_relid - 1] = datarelid1;

	tupleCopy2 = heap_modify_tuple(tupleCopy2, mx_mars_dec,
								   newValues, newNulls, replace);

	CatalogTupleInsert(mx_mars_rel, tupleCopy2);

	heap_freetuple(tupleCopy2);

	table_close(mx_mars_rel, NoLock);

	pfree(newValues);
	pfree(newNulls);
	pfree(replace);

	MxTransferDependencyLink(datarelid2, segrelid1, "mars");
	MxTransferDependencyLink(datarelid1, segrelid2, "mars");
}

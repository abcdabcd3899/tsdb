#include "postgres.h"

#include "gapfill_custom_scan.h"
#include "time_bucket.h"
#include "utils/datum.h"
#include "utils/typcache.h"

/*
 * Swap any type pointers.
 */
#define swap_ptr(ptr1, ptr2)                                                   \
	do                                                                         \
	{                                                                          \
		static void *tmp;                                                      \
		tmp = ptr1;                                                            \
		ptr1 = ptr2;                                                           \
		ptr2 = tmp;                                                            \
	} while (0);

static int64
convert_time_value_from_datum(Oid type, Datum value)
{
	switch (type)
	{
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case INT8OID:
			return DatumGetInt64(value);
		case DATEOID:
		case INT4OID:
			return DatumGetInt32(value);
		case INT2OID:
			return DatumGetInt16(value);
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unknown datatype: %s", format_type_be(type))));
			pg_unreachable();
			break;
	}
}

static void
free_gapfill_state(GapfillScanState *gfss)
{
	bms_free(gfss->groupby_attr_bm);
	bms_free(gfss->interpolate_attr_bm);
	ExecClearTuple(gfss->left_bound_slot);
	ExecClearTuple(gfss->right_bound_slot);
	pfree(gfss->left_bound_value_list);
	pfree(gfss->right_bound_value_list);
}

static int64
convert_period(const Datum period, Oid conv_t)
{
	switch (conv_t)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return DatumGetInt64(period);
		case DATEOID:
			return (((Interval *) period)->month * DAYS_PER_MONTH *
						USECS_PER_DAY +
					((Interval *) period)->day * USECS_PER_DAY +
					((Interval *) period)->time) /
				   USECS_PER_DAY;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return ((Interval *) period)->month * DAYS_PER_MONTH *
					   USECS_PER_DAY +
				   ((Interval *) period)->day * USECS_PER_DAY +
				   ((Interval *) period)->time;
		default:
			/*
			 * UDF will detect wrong datatype first,
			 * so default case shouldn`t happen.
			 */
			break;
	}

	pg_unreachable();
	return 0;
}

/*
 * UDF time_bucket_gapfill params;
 * : two params (interval, timestamp)
 * : three params (interval, timestamp, start)
 * : four params (interval, timestamp, timestamp, timestamp)
 */
static void
parse_gapfill_bounds(GapfillScanState *state, FuncExpr *gapfill_func)
{
	size_t params_size;
	List  *args;
	Const *period,
		  *left_bound,
		  *right_bound;

	params_size = list_length(gapfill_func->args);
	args = gapfill_func->args;
	Assert(params_size == 4);

	period = linitial_node(Const, args);
	left_bound = lthird_node(Const, args);
	right_bound = lfourth_node(Const, args);

	state->period =
		convert_period(period->constvalue, gapfill_func->funcresulttype);

	if (!left_bound->constisnull)
	{
		state->left_bound.time =
			convert_time_value_from_datum(left_bound->consttype, left_bound->constvalue);
		state->left_bound.time =
			(int64) DirectFunctionCall2(ts_int64_bucket,
										state->period,
										state->left_bound.time);
		state->left_bound.is_from_param = true;
	}
	else
		state->left_bound.is_from_param = false;

	if (!right_bound->constisnull)
	{
		state->right_bound.time = convert_time_value_from_datum(right_bound->consttype,
													  right_bound->constvalue);
		state->right_bound.time =
			(int64) DirectFunctionCall2(ts_int64_bucket,
										state->period,
										state->right_bound.time);
		state->right_bound.is_from_param = true;
	}
	else
		state->right_bound.is_from_param = false;
}

static bool
if_contained_in_group_by(TargetEntry *targetEntry, List *group_clauses)
{
	ListCell *cell;
	if (targetEntry->ressortgroupref)
	{
		foreach (cell, group_clauses)
		{
			if (targetEntry->ressortgroupref ==
				((SortGroupClause *) lfirst(cell))->tleSortGroupRef)
				return true;
		}
	}
	return false;
}

/*
 * Allocate all gapfill state memory here.
 */
static void
init_gapfill_columns(TupleDesc tupledesc, GapfillScanState *gfss)
{
	TargetEntry *target_entry;
	CustomScan *cscan;
	List	   *groups;

	cscan = (CustomScan *) gfss->ccstate.ss.ps.plan;

	Assert(gfss->interpolate_attr_bm == NULL);
	Assert(gfss->groupby_attr_bm == NULL);
	groups = lsecond_node(List, cscan->custom_private);

	/* allocate memory for left, right and output slots.*/
	gfss->left_bound_slot = MakeTupleTableSlot(tupledesc, &TTSOpsVirtual);
	gfss->left_bound_value_list = palloc0(tupledesc->natts * sizeof(Datum));

	gfss->right_bound_slot = MakeTupleTableSlot(tupledesc, &TTSOpsVirtual);
	gfss->right_bound_value_list = palloc0(tupledesc->natts * sizeof(Datum));

	gfss->output_fill_slot = MakeTupleTableSlot(tupledesc, &TTSOpsVirtual);

	for (AttrNumber i = 0; i < tupledesc->natts; i++)
	{
		target_entry = list_nth(cscan->custom_scan_tlist, i);

		/* The function name is the only trigger of "gapfill" operators. */
		if (IsA(target_entry->expr, FuncExpr))
		{
			if (pg_strcasecmp(get_func_name(((FuncExpr *) target_entry->expr)->funcid),
			                  MATRIXTS_GAPFILL_UDF_NAME) == 0)
				gfss->time_bucket_attr = AttrOffsetGetAttrNumber(i);
			else if (pg_strcasecmp(get_func_name(((FuncExpr *) target_entry->expr)->funcid),
			                       MATRIXTS_GAPFILL_INTERPOLATE_UDF_NAME) == 0)
				gfss->interpolate_attr_bm =
					bms_add_member(gfss->interpolate_attr_bm,
								   AttrOffsetGetAttrNumber(i));
		}

		if (if_contained_in_group_by(target_entry, groups) &&
			gfss->time_bucket_attr != AttrOffsetGetAttrNumber(i))
			gfss->groupby_attr_bm = bms_add_member(gfss->groupby_attr_bm,
												   AttrOffsetGetAttrNumber(i));
	}
}

static inline TupleTableSlot *
fetch_slot(CustomScanState *state)
{
	return ExecProcNode(linitial(state->custom_ps));
}

static Datum
fetch_idx_value_in_slot(TupleTableSlot *slot, int index)
{
	Datum attr_value;
	bool  isnull;

	attr_value = slot_getattr(slot, index, &isnull);
	return attr_value;
}

static inline int64
calculate_interpolate_fill_gap(int64 x, int64 x0, int64 x1, Datum y0, Datum y1)
{
	float8 f_y0 = DatumGetFloat8(y0);
	float8 f_y1 = DatumGetFloat8(y1);
	int64 result = Float8GetDatum(
		((f_y0) * ((x1) - (x)) + (f_y1) * ((x) - (x0))) / ((x1) - (x0)));
	return result;
}

static void
move_to_fill_next_gap(GapfillScanState *gfss)
{
	int64 right_bound_time;

	switch (gfss->gap_miss)
	{
		case NO_BOUNDARY_MISS:
			right_bound_time = fetch_idx_value_in_slot(gfss->right_bound_slot,
													   gfss->time_bucket_attr);

			swap_ptr(gfss->left_bound_value_list, gfss->right_bound_value_list);
			swap_ptr(gfss->left_bound_slot, gfss->right_bound_slot);
			gfss->cursor_time = right_bound_time;

			gfss->gap_miss = MISS_RIGHT_BOUNDARY;
			break;
		case MISS_RIGHT_BOUNDARY:
		case MISS_LEFT_BOUNDARY:
			gfss->gap_miss = MISS_LEFT_BOUNDARY;
		default:
			break;
	}
}

static void
create_slot_fill_current_gap(GapfillScanState *gfss)
{
	/*
	 * Left Bound is always output in any turns.
	 * We just recalculate left_bound and output it.
	 */
	Datum	 y0,
			 y1;
	int64	 left_bound_time,
			 right_bound_time;
	TupleDesc tuple_desc;

	tuple_desc = ((PlanState *) gfss)->ps_ResultTupleDesc;

	left_bound_time = gfss->left_bound_value_list[AttrNumberGetAttrOffset(
		gfss->time_bucket_attr)];
	right_bound_time = gfss->right_bound_value_list[AttrNumberGetAttrOffset(
		gfss->time_bucket_attr)];

	for (AttrNumber i = 0; i < tuple_desc->natts; ++i)
	{
		if (bms_is_member(AttrOffsetGetAttrNumber(i),
						  gfss->interpolate_attr_bm))
		{
			y0 = gfss->left_bound_value_list[i];
			y1 = gfss->right_bound_value_list[i];
			gfss->left_bound_slot->tts_values[i] =
				calculate_interpolate_fill_gap(gfss->cursor_time,
											   left_bound_time,
											   right_bound_time,
											   y0,
											   y1);
		}
	}

	gfss->left_bound_slot
		->tts_values[AttrNumberGetAttrOffset(gfss->time_bucket_attr)] =
		gfss->cursor_time;
}

static void
reset_gaifll_output_slot(GapfillScanState *gfss)
{
	if (TupIsNull(gfss->output_fill_slot))
		ExecClearTuple(gfss->output_fill_slot);
}

static void
output_gapfill_slot(GapfillScanState *gfss)
{
	if (!TupIsNull(gfss->left_bound_slot))
		ExecCopySlot(gfss->output_fill_slot, gfss->left_bound_slot);
	else
		gfss->output_fill_slot = NULL;
}

/*
 * Left bound slot is always output.
 * Set output slot first and build next slot.
 */
static bool
try_to_fill_gaps(GapfillScanState *gfss)
{
	bool	if_desc;
	int64	left_bound_time,
			right_bound_time;

	output_gapfill_slot(gfss);

	left_bound_time =
		fetch_idx_value_in_slot(gfss->left_bound_slot, gfss->time_bucket_attr);
	right_bound_time =
		fetch_idx_value_in_slot(gfss->right_bound_slot, gfss->time_bucket_attr);

	if (left_bound_time < right_bound_time)
	{
		gfss->cursor_time += gfss->period;
		if_desc = false;
	}
	else
	{
		gfss->cursor_time -= gfss->period;
		if_desc = true;
	}
	/*
	 * If no gap between left bound and right bound,
	 * then move to next gap.
	 */
	if (gfss->cursor_time == right_bound_time)
	{
		move_to_fill_next_gap(gfss);
		return false;
	}
	/*
	 * When:
	 *      Case1: cursor_time less than right_bound_time and order is
	 *      ascending.
	 *      Case2: cursor_time larger than right_bound_time and order is
	 *      descending.
	 * Then Do:
	 *      Create a new tuple that copies from left_slot and Fine-tuning to
	 *      fill the gap.
	 * Otherwise:
	 *      Move to next gap. (Refresh left and right boundary)
	 */
	if ((gfss->cursor_time < right_bound_time) != if_desc)
	{
		create_slot_fill_current_gap(gfss);
		return true;
	}
	else
	{
		move_to_fill_next_gap(gfss);
		return false;
	}
}

static void
init_gap_bound(GapfillScanState *gfss, TupleTableSlot *slot)
{
	TupleDesc tuple_desc;

	tuple_desc = ((PlanState *) gfss)->ps_ResultTupleDesc;
	gfss->fill_gap_state = FILLING;
	switch (gfss->gap_miss)
	{
		case MISS_LEFT_BOUNDARY:
			ExecCopySlot(gfss->left_bound_slot, slot);
			for (int16 i = 0; i < tuple_desc->natts; ++i)
				gfss->left_bound_value_list[i] =
					fetch_idx_value_in_slot(slot, AttrOffsetGetAttrNumber(i));

			gfss->cursor_time = fetch_idx_value_in_slot(gfss->left_bound_slot,
														gfss->time_bucket_attr);
			gfss->gap_miss = MISS_RIGHT_BOUNDARY;
			break;
		case MISS_RIGHT_BOUNDARY:
			if (TupIsNull(gfss->right_bound_slot))
				ExecClearTuple(gfss->right_bound_slot);
			ExecCopySlot(gfss->right_bound_slot, slot);
			for (int16 i = 0; i < tuple_desc->natts; ++i)
				gfss->right_bound_value_list[i] =
					fetch_idx_value_in_slot(slot, AttrOffsetGetAttrNumber(i));

			gfss->gap_miss = NO_BOUNDARY_MISS;
			break;
		default:
			break;
	}
}

static bool
touch_next_group(GapfillScanState *gfss)
{
	TupleTableSlot *left_slot,
				   *right_slot;
	Datum		left_slot_value,
				right_slot_value;
	bool		isnull;
	TupleDesc	tuple_desc;

	left_slot = gfss->left_bound_slot;
	right_slot = gfss->right_bound_slot;
	tuple_desc = ((PlanState *) gfss)->ps_ResultTupleDesc;

	for (AttrNumber i = 0; i < tuple_desc->natts; ++i)
	{
		if (bms_is_member(AttrOffsetGetAttrNumber(i), gfss->groupby_attr_bm))
		{
			left_slot_value =
				slot_getattr(left_slot, AttrOffsetGetAttrNumber(i), &isnull);
			right_slot_value =
				slot_getattr(right_slot, AttrOffsetGetAttrNumber(i), &isnull);
			if (!datumIsEqual(left_slot_value,
							  right_slot_value,
							  tuple_desc->attrs[i].attbyval,
							  tuple_desc->attrs[i].attlen))
				return true;
		}
	}
	return false;
}

/*
 * true: populate success
 * false: no more tuple under sub state, goto finish state
 */
static bool
populate_start_finish(GapfillScanState *gfss)
{
	TupleTableSlot *slot;
	/*
	 * To complete the fill gap, at least need two slots.
	 * Check if the gap is FULL. If not full fetch slot from sub-ExecutorState.
	 */
	while (gfss->gap_miss != NO_BOUNDARY_MISS)
	{
		slot = fetch_slot((CustomScanState *) gfss);
		if (TupIsNull(slot))
			return false;

		init_gap_bound(gfss, slot);
	}

	/* When touch next group, move gap and refresh bounds. */
	if (touch_next_group(gfss))
	{
		output_gapfill_slot(gfss);
		move_to_fill_next_gap(gfss);
		return true;
	}
	/* TODO case: default start_at */
	/* TODO case: params start_at */

	/*
	 * true: stay at the current gap.
	 * false: move to next gap, reset bounds.
	 */
	try_to_fill_gaps(gfss);

	return true;
}

/*
 * Gapfill has two state: FILLING and FINISH.
 * The flow of state dgraph:
 * [BEGIN] => FILLING => FINISH => [END]
 *             /\  ||
 *             ||==||
 */
static void
do_gapfill(GapfillScanState *gfss)
{
	GapfillState fill_state;

	fill_state = gfss->fill_gap_state;
	switch (fill_state)
	{
		case FILLING:
			if (!populate_start_finish(gfss))
			{
				output_gapfill_slot(gfss);
				/* At finish state, just output rest of slots. */
				gfss->fill_gap_state = FINISHED;
			}
			break;
		case FINISHED:
			gfss->output_fill_slot = NULL;
			break;
		default:
			break;
	}
}

void
begin_gapfill_cscan(CustomScanState *node, EState *estate, int eflags)
{
	GapfillScanState *gfss;
	CustomScan	   *cs;
	Plan		   *child_plan;
	FuncExpr	   *gapfill_func;
	TupleDesc		tuple_desc;

	gfss = (GapfillScanState *) node;
	cs = (CustomScan *) node->ss.ps.plan;
	child_plan = (Plan *) linitial(cs->custom_plans);
	tuple_desc = node->ss.ps.ps_ResultTupleDesc;

	/*
	 * We don't actually execute the child planstate tree as usual, we have our
	 * own logic to schedule each planstate node.  Init the child planstate
	 * anyway and fill each node into gapfill customscanstate.
	 */

	node->custom_ps = list_make1(ExecInitNode(child_plan, estate, eflags));
	/* UDF expr and args saved when building plan. */
	gapfill_func = linitial_node(FuncExpr, cs->custom_private);

	gfss->cursor_time = INT64_MIN;
	gfss->gap_miss = MISS_LEFT_BOUNDARY;
	parse_gapfill_bounds(gfss, gapfill_func);
	init_gapfill_columns(tuple_desc, gfss);
	gfss->fill_gap_state = FILLING;
}

TupleTableSlot *
exec_gapfill_cscan(CustomScanState *node)
{
	GapfillScanState *gfss;

	gfss = (GapfillScanState *) node;
	reset_gaifll_output_slot(gfss);
	do_gapfill(gfss);
	return gfss->output_fill_slot;
}

void
end_gapfill_cscan(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	free_gapfill_state((GapfillScanState *) node);
	ExecEndNode(linitial(node->custom_ps));
}

void
rescan_gapfill_cscan(CustomScanState *node)
{
	elog(ERROR, "Gapfill doesn't support rescan");
}

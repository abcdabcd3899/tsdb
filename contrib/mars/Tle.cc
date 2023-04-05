/*-------------------------------------------------------------------------
 *
 * Tle.cc
 *	  The MARS TLE abstraction
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "Tle.h"
#include "BlockReader.h"
#include "Footer.h"
#include "ParquetReader.h"

#include <unordered_map>

namespace mars
{

	AggType
	AggregateTLE::GetAggType(const std::string name)
	{
		static std::unordered_map<std::string, AggType> aggs = {
			{"count", AggType::COUNT},
			{"first", AggType::FIRST},
			{"last", AggType::LAST},
			{"avg", AggType::AVG},
			{"sum", AggType::SUM},
			{"min", AggType::MIN},
			{"max", AggType::MAX}};

		auto iter = aggs.find(name);
		return iter != aggs.end() ? iter->second : AggType::UNKNOWN;
	}

	std::shared_ptr<TLE>
	AggregateTLE::Make(const TleContext &context)
	{
		auto resno = context.resno;
		auto attnum = context.attnum;
		auto tupdesc = context.tupdesc;
		// it is possible that there is not a valid attnum, for example count(*)
		auto attr = (AttrNumberIsForUserDefinedAttr(attnum) ? &tupdesc->attrs[attnum - 1] : nullptr);

		switch (context.aggtype)
		{
		case AggType::MIN:
			return std::make_shared<mars::AggregateMinTLE>(attr, resno, attnum);
		case AggType::MAX:
			return std::make_shared<mars::AggregateMaxTLE>(attr, resno, attnum);
		case AggType::COUNT:
			return std::make_shared<mars::AggregateRowCountTLE>(attr, resno, attnum);
		case AggType::FIRST:
		case AggType::LAST:
			// first() and last() share the same logic.
			return std::make_shared<mars::AggregateFirstLastTLE>(attr, context);
		case AggType::AVG:
			// need to further check the type
			switch (context.atttypid)
			{
			case INT4OID:
				return std::make_shared<mars::TypedAggregateAvgTLE<INT4OID>>(attr, resno, attnum);
			case INT8OID:
				// avg(int8) and sum(int8) share the same logic.
				return std::make_shared<mars::TypedAggregateSumTLE<INT8OID>>(attr, resno, attnum);
			case FLOAT4OID:
			case FLOAT8OID:
				// avg(float4) and avg(float8) share the same logic.
				return std::make_shared<mars::TypedAggregateAvgTLE<FLOAT4OID>>(attr, resno, attnum);
			default:
				elog(ERROR, "unsupported aggregate type");
			}
		case AggType::SUM:
			// need to further check the type
			switch (context.atttypid)
			{
			case INT4OID:
				return std::make_shared<mars::TypedAggregateSumTLE<INT4OID>>(attr, resno, attnum);
			case INT8OID:
				return std::make_shared<mars::TypedAggregateSumTLE<INT8OID>>(attr, resno, attnum);
			case FLOAT4OID:
				return std::make_shared<mars::TypedAggregateSumTLE<FLOAT4OID>>(attr, resno, attnum);
			case FLOAT8OID:
				return std::make_shared<mars::TypedAggregateSumTLE<FLOAT8OID>>(attr, resno, attnum);
			default:
				elog(ERROR, "unsupported aggregate type");
			}
		default:
			elog(ERROR, "unsupported aggregate function");
		}
	}

	AggregateFirstLastTLE::AggregateFirstLastTLE(const ::Form_pg_attribute attr,
												 const TleContext &context)
		: AggregateTLE(&context.tupdesc->attrs[GetArgAttnum(context, order_arg) - 1],
					   context.resno, GetArgAttnum(context, order_arg)),
		  context_(context), attnum_output_(GetArgAttnum(context, output_arg)), attnum_order_(GetArgAttnum(context, order_arg))
	{
		auto aggref = context_.aggref;

		// fill the type info for the trans state.
		state_.value.type_oid = linitial_oid(aggref->aggargtypes);
		state_.cmp.type_oid = lsecond_oid(aggref->aggargtypes);

		// Fill the serial function.
		//
		// TODO: the function oid and the flinfo is actually available in the
		// corresponding pertrans info, we'd better pass it to the TLE for
		// convenience.
		{
			auto tuple = ::SearchSysCache1(AGGFNOID,
										   ::ObjectIdGetDatum(aggref->aggfnoid));
			if (!HeapTupleIsValid(tuple))
				elog(ERROR, "cache lookup failed for aggregate %u",
					 aggref->aggfnoid);

			auto form = (::Form_pg_aggregate)GETSTRUCT(tuple);
			auto aggserialfn = form->aggserialfn;
			ReleaseSysCache(tuple);

			auto flinfo = (::FmgrInfo *)::palloc(sizeof(::FmgrInfo));
			fmgr_info(aggserialfn, flinfo);

			fcinfo_ = (::FunctionCallInfo)::palloc(SizeForFunctionCallInfo(1));
			InitFunctionCallInfoData(*fcinfo_,
									 flinfo,
									 1 /* nargs */,
									 InvalidOid /* fncollation */,
									 NULL /* context */,
									 NULL /* resultinfo */);

			fcinfo_->args[0].value = (::Datum)&state_;
			fcinfo_->args[0].isnull = false;
		}
	}

	::Datum
	AggregateFirstLastTLE::GetDatumValue(const mars::footer::Footer &footer) const
	{
		// first() / last() can only be optimized when the target values are on
		// the physical edges, so fallback to data scan if that is not the case.
		auto logical = footer.HasLogicalInfo();
		auto state = const_cast<InternalCmpAggStore *>(&state_);

		// fill the order value.
		if (context_.aggtype == AggType::FIRST)
		{
			state->cmp.datum = footer.GetMinDatum(attnum_order_ - 1, logical);
			state->cmp.is_null = false;
		}
		else
		{
			state->cmp.datum = footer.GetMaxDatum(attnum_order_ - 1, logical);
			state->cmp.is_null = false;
		}

		// get the min@ or max@ row, we have checked in Compatible() that it is on
		// the edge.
		if (context_.aggtype == AggType::FIRST)
		{
#ifdef USE_ASSERT_CHECKING
			auto row = footer.GetMinAt(attnum_order_ - 1, logical);
			Assert(footer.IsPhysicalFirst(attnum_output_ - 1, row, logical));
#endif // USE_ASSERT_CHECKING

			state->value.datum =
				footer.GetPhysicalFirstDatum(attnum_output_ - 1, logical);
			state->value.is_null = false;
		}
		else
		{
#ifdef USE_ASSERT_CHECKING
			auto row = footer.GetMaxAt(attnum_order_ - 1, logical);
			Assert(footer.IsPhysicalLast(attnum_output_ - 1, row, logical));
#endif // USE_ASSERT_CHECKING

			state->value.datum =
				footer.GetPhysicalLastDatum(attnum_output_ - 1, logical);
			state->value.is_null = false;
		}

		// serialize the trans state.
		auto fcinfo = const_cast<::FunctionCallInfo>(fcinfo_);
		return FunctionCallInvoke(fcinfo);
	}

	bool
	AggregateFirstLastTLE::HasResult(const mars::footer::Footer &footer) const
	{
		auto logical = footer.HasLogicalInfo();
		return footer.GetRowCount(attnum_order_ - 1, logical) > 0;
	}

	bool
	AggregateFirstLastTLE::Compatible(const mars::footer::Footer &footer) const
	{
		auto logical = footer.HasLogicalInfo();

		if (!HasResult(footer))
			// empty column is treated as compatible
			return true;

		// check the min@ or max@ edge.
		if (context_.aggtype == AggType::FIRST)
		{
			auto row = footer.GetMinAt(attnum_order_ - 1, logical);
			return footer.IsPhysicalFirst(attnum_output_ - 1, row, logical);
		}
		else
		{
			auto row = footer.GetMaxAt(attnum_order_ - 1, logical);
			return footer.IsPhysicalLast(attnum_output_ - 1, row, logical);
		}
	}

	::AttrNumber
	AggregateFirstLastTLE::GetArgAttnum(const TleContext &context, int nth)
	{
		// TODO: at the moment we do not allow the 3rd arg, the offset.
		Assert(::list_length(context.aggref->args) == 2);

		// TODO: at the moment we do not allow exprs in the first()/last() args.
		auto tle = list_nth_node(TargetEntry, context.aggref->args, nth);
		auto var = castNode(Var, tle->expr);

		while (IS_SPECIAL_VARNO(var->varno))
		{
			// FIXME: we need to use the PlanState of that var
			auto ps = outerPlanState(context.aggstate);
			auto tlist = ps->plan->targetlist;

			tle = list_nth_node(TargetEntry, tlist, var->varattno - 1);
			var = castNode(Var, tle->expr);
		}

		return var->varattno;
	}

} // namespace mars

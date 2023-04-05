/*-------------------------------------------------------------------------
 *
 * Tle.h
 *	  The MARS TLE abstraction
 *
 * XXX: All the preagg information are reported on the logical blocks when
 * logical information exists.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_TLE_H
#define MARS_TLE_H

#include "type_traits.hpp"

#include "Compare.h"
#include "Footer.h"
#include "ts_func.h"

namespace mars
{

	class ParquetReader;

	enum AggType
	{
		UNKNOWN = 0,
		COUNT,
		FIRST,
		LAST,
		AVG,
		SUM,
		MIN,
		MAX,
	};

	/*!
	 * The trans type of sum(int64).
	 *
	 * @note It is copied from src/backend/utils/adt/numeric.c, must keep in sync
	 *       with it.
	 */
	struct Int128AggState
	{
		bool calcSumX2; /* if true, calculate sumX2 */
		::int64 N;		/* count of processed numbers */
		::int128 sumX;	/* sum of processed numbers */
		::int128 sumX2; /* sum of squares of processed numbers */

		Int128AggState()
			: calcSumX2(false), N(0), sumX(0), sumX2(0)
		{
		}
	};

	/*!
	 * The trans type of avg(int2) and (int4).
	 *
	 * @note It is copied from src/backend/utils/adt/numeric.c, must keep in sync
	 *       with it.
	 */
	struct Int8TransTypeData
	{
		::int64 count;
		::int64 sum;
	};

	/*!
	 * The trans type of avg(float4).
	 *
	 * @see float4_accum().
	 */
	struct Float8TransTypeData
	{
		::float8 count;
		::float8 sum;
		::float8 sumX2;
	};

	/*!
	 * The Tle context.
	 *
	 * It contains all the information needed to detect and execute a Tle.
	 *
	 * @note This struct only contains pointers, the objects are all owned by the
	 *       caller, so do not free them in this struct.
	 */
	struct TleContext
	{
		TleContext(::AttrNumber _resno, ParquetReader *_reader,
				   ::AggState *_aggstate, ::ScanState *_scanstate)
			: resno(_resno), reader(_reader), aggstate(_aggstate), scanstate(_scanstate), tupdesc(RelationGetDescr(_scanstate->ss_currentRelation)), ps((PlanState *)_aggstate), attnum(0), interval(nullptr), aggtype(AggType::UNKNOWN), aggtypid(InvalidOid), atttypid(InvalidOid), aggref(nullptr)
		{
		}

		/* common information */

		::AttrNumber resno;

		ParquetReader *reader;

		::AggState *aggstate;
		::ScanState *scanstate;
		::TupleDesc tupdesc;

		::PlanState *ps;

		/* common properties for all kinds of TLE */

		::AttrNumber attnum;

		/* properties for TimeBucketTLE */

		::Interval *interval;

		/* properties for AggregateTLE */

		AggType aggtype; /*!< The AggTLE type */
		::Oid aggtypid;
		::Oid atttypid;
		::Aggref *aggref;
	};

	/*!
	 * pushdown: The common base of TLEs.
	 */
	class TLE
	{
	public:
		TLE(const ::Form_pg_attribute attr, ::AttrNumber resno, ::AttrNumber attnum)
			: resno_(resno), attnum_(attnum), comp_(attr)
		{
			AssertImply(attr, attr->attnum == attnum);
		}

		virtual ~TLE() = default;

		::AttrNumber GetResno() const
		{
			return resno_;
		}

		virtual ::Datum GetDatumValue(const mars::footer::Footer &footer) const = 0;

		virtual bool HasResult(const mars::footer::Footer &footer) const
		{
			auto logical = footer.HasLogicalInfo();
			return footer.GetRowCount(attnum_ - 1, logical) > 0;
		}

		virtual bool UniqueWithinFooter(const mars::footer::Footer &footer) const
		{
			if (AttrNumberIsForUserDefinedAttr(attnum_))
			{
				auto logical = footer.HasLogicalInfo();
				return footer.SingleValue(attnum_ - 1, comp_, logical);
			}
			else
			{
				// this happens in count(*)
				return true;
			}
		}

		/*!
		 * @return false if needs to fallback to data scan.
		 */
		virtual bool Compatible(const mars::footer::Footer &footer) const
		{
			return true;
		}

	protected:
		::AttrNumber resno_;
		::AttrNumber attnum_;
		const DatumCompare comp_;
	};

	/*!
	 * pushdown: A normal attribute TLE.
	 */
	class AttributeTLE : public TLE
	{
	public:
		AttributeTLE(const ::Form_pg_attribute attr,
					 ::AttrNumber resno, ::AttrNumber attnum)
			: TLE(attr, resno, attnum)
		{
		}

		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			return footer.GetMinDatum(attnum_ - 1, logical);
		}
	};

	/*!
	 * pushdown: A time_bucket() TLE.
	 */
	class TimeBucketTLE : public TLE
	{
	public:
		TimeBucketTLE(const ::Form_pg_attribute attr,
					  ::AttrNumber resno, ::AttrNumber attnum,
					  const ::Interval *interval)
			: TLE(attr, resno, attnum), interval_(*interval)
		{
		}

		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			auto value = footer.GetMinDatum(attnum_ - 1, logical);
			return ts_timestamp_bucket(&interval_, DatumGetTimestamp(value));
		}

		bool UniqueWithinFooter(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			auto min = footer.GetMinDatum(attnum_ - 1, logical);
			auto max = footer.GetMaxDatum(attnum_ - 1, logical);

			auto tb_min = DatumGetTimestamp(ts_timestamp_bucket(&interval_, DatumGetTimestamp(min)));
			auto tb_max = DatumGetTimestamp(ts_timestamp_bucket(&interval_, DatumGetTimestamp(max)));

			return comp_.equal(tb_min, tb_max);
		}

	private:
		::Interval interval_;
	};

	/*!
	 * pushdown: The common base of agg TLEs.
	 */
	class AggregateTLE : public TLE
	{
	public:
		static AggType GetAggType(const std::string name);

		static std::shared_ptr<TLE> Make(const TleContext &context);

		AggregateTLE(const ::Form_pg_attribute attr,
					 ::AttrNumber resno, ::AttrNumber attnum)
			: TLE(attr, resno, attnum)
		{
		}
	};

	class AggregateMinTLE : public AggregateTLE
	{
	public:
		AggregateMinTLE(const ::Form_pg_attribute attr,
						::AttrNumber resno, ::AttrNumber attnum)
			: AggregateTLE(attr, resno, attnum)
		{
		}

		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			return footer.GetMinDatum(attnum_ - 1, logical);
		}
	};

	class AggregateMaxTLE : public AggregateTLE
	{
	public:
		AggregateMaxTLE(const ::Form_pg_attribute attr,
						::AttrNumber resno, ::AttrNumber attnum)
			: AggregateTLE(attr, resno, attnum)
		{
		}

		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			return footer.GetMaxDatum(attnum_ - 1, logical);
		}
	};

	class AggregateRowCountTLE : public AggregateTLE
	{
	public:
		AggregateRowCountTLE(const ::Form_pg_attribute attr,
							 ::AttrNumber resno, ::AttrNumber attnum)
			: AggregateTLE(attr, resno, attnum)
		{
		}

		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			auto count = footer.GetRowCount(attnum_ - 1, logical);
			return Int64GetDatum(count);
		}

		bool HasResult(const mars::footer::Footer &footer) const override
		{
			// count() always returns a value.
			return true;
		}
	};

	class AggregateFirstLastTLE : public AggregateTLE
	{
	public:
		AggregateFirstLastTLE(const ::Form_pg_attribute attr,
							  const TleContext &context);

		::Datum GetDatumValue(const mars::footer::Footer &footer) const override;
		bool HasResult(const mars::footer::Footer &footer) const override;
		bool Compatible(const mars::footer::Footer &footer) const override;

	private:
		static constexpr int output_arg = 0; //<! the first arg is the output attr.
		static constexpr int order_arg = 1;	 //<! the second arg is the order attr.

		static ::AttrNumber GetArgAttnum(const TleContext &context, int nth);

	private:
		/*!
		 * A PolyDatum represents a polymorphic datum.
		 *
		 * @note Copied from contrib/matrixts/agg_bookend.c
		 */
		struct PolyDatum
		{
			::Oid type_oid;
			bool is_null;
			::Datum datum;
		};

		/*!
		 * Internal state for first() / last() aggregates.
		 *
		 * @note Copied from contrib/matrixts/agg_bookend.c
		 */
		struct InternalCmpAggStore
		{
			PolyDatum value;
			PolyDatum cmp; /* the comparison element. e.g. time */
		};

		TleContext context_;

		::AttrNumber attnum_output_;
		::AttrNumber attnum_order_;

		InternalCmpAggStore state_;
		// palloc'ed, so must not be freed in the c++ style.
		::FunctionCallInfoBaseData *fcinfo_;
	};

	//---------------------------------------------------------------------------
	// Agg avg()
	//---------------------------------------------------------------------------

	template <::Oid type_oid>
	class TypedAggregateAvgTLE : public AggregateTLE
	{
	};

	template <>
	class TypedAggregateAvgTLE<INT4OID> : public AggregateTLE
	{
	public:
		using T = pg_type_traits<INT4OID>;

		TypedAggregateAvgTLE(const ::Form_pg_attribute attr,
							 ::AttrNumber resno, ::AttrNumber attnum)
			: AggregateTLE(attr, resno, attnum)
		{
			// The trans state is an array of int64, the elems provide the initial
			// values, they are copied to the array.
			::Datum elems[2] = {0, 0};

			// The type information can be found by calling get_typlenbyvalalign(),
			// but as we believe they are not likely to change, so we hard code
			// them to save one syscache lookup.
			trans_array_ = ::construct_array(elems /* elems */,
											 2 /* nelems */,
											 INT8OID /* elmtype */,
											 sizeof(::int64) /* elmlen */,
											 true /* elmbyval */,
											 'd' /* elmalign */);
		}

		/*!
		 * @see int4_avg_accum().
		 */
		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			::int64 sum =
				footer.FooterGetSum<T::parquet_type>(attnum_ - 1, logical);

			auto array = const_cast<::ArrayType *>(trans_array_);
			auto transdata = (mars::Int8TransTypeData *)ARR_DATA_PTR(array);
			transdata->sum = sum;
			transdata->count = footer.GetRowCount(attnum_ - 1, logical);

			return PointerGetDatum(array);
		}

	private:
		// palloc'ed, so must not be freed in the c++ style.
		::ArrayType *trans_array_;
	};

	template <>
	class TypedAggregateAvgTLE<FLOAT4OID> : public AggregateTLE
	{
	public:
		using T = pg_type_traits<FLOAT4OID>;

		TypedAggregateAvgTLE(const ::Form_pg_attribute attr,
							 ::AttrNumber resno, ::AttrNumber attnum)
			: AggregateTLE(attr, resno, attnum)
		{
			// The trans state is an array of float8, the elems provide the initial
			// values, they are copied to the array.
			::Datum elems[3] = {0, 0, 0};

			// The type information can be found by calling get_typlenbyvalalign(),
			// but as we believe they are not likely to change, so we hard code
			// them to save one syscache lookup.
			trans_array_ = ::construct_array(elems /* elems */,
											 3 /* nelems */,
											 FLOAT8OID /* elmtype */,
											 sizeof(::float8) /* elmlen */,
											 true /* elmbyval */,
											 'd' /* elmalign */);
		}

		/*!
		 * @see float4_accum() and float8_accum().
		 */
		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			auto sum128 =
				footer.FooterGetSum<T::parquet_type>(attnum_ - 1, logical);
			::float8 sum = sum128;

			if (std::isfinite(sum128) && std::isinf(sum))
			{
				// the sum becomes Inf during the float128 -> float64 conversion.
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value out of range: overflow")));
			}

			auto array = const_cast<::ArrayType *>(trans_array_);
			auto transdata = (mars::Float8TransTypeData *)ARR_DATA_PTR(array);
			transdata->sum = sum;
			transdata->count = footer.GetRowCount(attnum_ - 1, logical);

			return PointerGetDatum(array);
		}

	private:
		// palloc'ed, so must not be freed in the c++ style.
		::ArrayType *trans_array_;
	};

	//---------------------------------------------------------------------------
	// Agg sum()
	//---------------------------------------------------------------------------

	template <::Oid type_oid>
	class TypedAggregateSumTLE : public AggregateTLE
	{
	};

	template <>
	class TypedAggregateSumTLE<INT4OID> : public AggregateTLE
	{
	public:
		using T = pg_type_traits<INT4OID>;

		TypedAggregateSumTLE(const ::Form_pg_attribute attr,
							 ::AttrNumber resno, ::AttrNumber attnum)
			: AggregateTLE(attr, resno, attnum)
		{
		}

		/*!
		 * @see int4_sum().
		 */
		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			::int64 sum =
				footer.FooterGetSum<T::parquet_type>(attnum_ - 1, logical);
			return ::Int64GetDatum(sum);
		}
	};

	template <>
	class TypedAggregateSumTLE<INT8OID> : public AggregateTLE
	{
	public:
		using T = pg_type_traits<INT8OID>;

		TypedAggregateSumTLE(const ::Form_pg_attribute attr,
							 ::AttrNumber resno, ::AttrNumber attnum)
			: AggregateTLE(attr, resno, attnum)
		{
			// We need a fake aggstate to fool int8_avg_serialize(), which
			// insists to be called in aggregate context only.
			aggstate_.ss.ps.type = T_AggState;

			// The struct FunctionCallInfoBaseData contains a var-length arg array,
			// in our case its size should be 1, there is no elegant way to define
			// such an object in C++, so we create it as a pointer allocated with
			// palloc().
			serialfn_fcinfo_ = (::FunctionCallInfo)::palloc(SizeForFunctionCallInfo(1));
			InitFunctionCallInfoData(*serialfn_fcinfo_,
									 (::FmgrInfo *)NULL,
									 1 /* nargs */,
									 InvalidOid /* fncollation */,
									 (::Node *)&aggstate_,
									 NULL /* resultinfo */);
			serialfn_fcinfo_->args[0].isnull = false;
		}

		/*!
		 * Get sum(float8) of the specified column of the block.
		 *
		 * It returns the same thing as int8_avg_accum() -> int8_avg_serialize().
		 */
		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			// sum(int8) returns a serialized PolyNumAggState pointer as trans
			// value, and when int128 is supported PolyNumAggState is represented
			// as Int128AggState.  In mars int128 is a mandatory requirement.
			//
			// A tricky part is that, we use pre-allocated trans state and fcinfo
			// to prevent re-initialize them every time, however it's not allowed
			// to modify them in a const(this) function, so we have to cast them to
			// non-const for the processing.
			auto logical = footer.HasLogicalInfo();
			auto state = const_cast<mars::Int128AggState *>(&trans_state_);
			state->sumX = footer.FooterGetSum<T::parquet_type>(attnum_ - 1, logical);
			state->N = footer.GetRowCount(attnum_ - 1, logical);

			auto fcinfo = const_cast<::FunctionCallInfo>(serialfn_fcinfo_);
			fcinfo->args[0].value = (::Datum)state;
			// We can't call FunctionCallInvoke() because we didn't fill ->flinfo.
			return int8_avg_serialize(fcinfo);
		}

	private:
		mars::Int128AggState trans_state_;
		::AggState aggstate_;
		// palloc'ed, so must not be freed in the c++ style.
		::FunctionCallInfoBaseData *serialfn_fcinfo_;
	};

	template <>
	class TypedAggregateSumTLE<FLOAT4OID> : public AggregateTLE
	{
	public:
		using T = pg_type_traits<FLOAT4OID>;

		TypedAggregateSumTLE(const ::Form_pg_attribute attr,
							 ::AttrNumber resno, ::AttrNumber attnum)
			: AggregateTLE(attr, resno, attnum)
		{
		}

		/*!
		 * sum(float4), returns float4.
		 *
		 * @see float4pl().
		 */
		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			auto sum128 =
				footer.FooterGetSum<T::parquet_type>(attnum_ - 1, logical);
			::float4 sum = sum128;

			if (std::isfinite(sum128) && std::isinf(sum))
			{
				// the sum becomes Inf during the float128 -> float32 conversion.
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value out of range: overflow")));
			}

			return Float4GetDatum(sum);
		}
	};

	template <>
	class TypedAggregateSumTLE<FLOAT8OID> : public AggregateTLE
	{
	public:
		using T = pg_type_traits<FLOAT8OID>;

		TypedAggregateSumTLE(const ::Form_pg_attribute attr,
							 ::AttrNumber resno, ::AttrNumber attnum)
			: AggregateTLE(attr, resno, attnum)
		{
		}

		/*!
		 * sum(float8), returns float8.
		 *
		 * @see float8pl().
		 */
		::Datum GetDatumValue(const mars::footer::Footer &footer) const override
		{
			auto logical = footer.HasLogicalInfo();
			auto sum128 =
				footer.FooterGetSum<T::parquet_type>(attnum_ - 1, logical);
			::float8 sum = sum128;

			if (std::isfinite(sum128) && std::isinf(sum))
			{
				// the sum becomes Inf during the float128 -> float64 conversion.
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("value out of range: overflow")));
			}

			return ::Float8GetDatum(sum);
		}
	};

} // namespace mars

#endif /* MARS_TLE_H */

/*-------------------------------------------------------------------------
 *
 * PreAgg.cc
 *	  MARS PreAgg collector.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/PreAgg.cc
 *
 *-------------------------------------------------------------------------
 */
#include "PreAgg.h"

namespace mars
{
	void
	PreAgg::Append(::Datum value, bool isnull)
	{
		if (isnull)
		{
			// fast path, other preagg statistics are not updated on null
			++null_count_;
			++total_count_;
			return;
		}

		++total_count_;

		if (IsFirstNonNullTuple())
		{
			first_ = value;
			last_ = value;
		}
		else
		{
			last_ = value;
		}

		if (bits_ & PreAggBits::SUM)
		{
			SumAppend(value);
		}

		if (bits_ & PreAggBits::COMPARE)
		{
			if (IsFirstNonNullTuple())
			{
				min_at_ = total_count_ - 1;
				min_ = copy_value(value, attr_);

				max_at_ = total_count_ - 1;
				max_ = copy_value(value, attr_);
			}
			else
			{
				if (cmp_->test(value, mars::OP::LT, min_))
				{
					min_at_ = total_count_ - 1;
					min_ = copy_value(value, attr_);
				}

				if (!cmp_->test(value, mars::OP::LT, max_))
				{
					max_at_ = total_count_ - 1;
					max_ = copy_value(value, attr_);
				}
			}
		}
	}

	void
	PreAgg::SumReset()
	{
		switch (attr_->atttypid)
		{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
			sum_ = (mars::Datum128)0;
			break;

		case INTERVALOID:
		case CASHOID:
		case NUMERICOID:
			// TODO: support sum of these types
			break;

		default:
			break;
		}
	}

	void
	PreAgg::SumAppend(::Datum datum)
	{
		switch (attr_->atttypid)
		{
		case INT2OID:
		{
			auto arg = ::DatumGetInt16(datum);
			auto sum = Datum128GetInt128(sum_);
			sum += arg;
			sum_ = Int128GetDatum128(sum);
			break;
		}

		case INT4OID:
		{
			auto arg = ::DatumGetInt32(datum);
			auto sum = Datum128GetInt128(sum_);
			sum += arg;
			sum_ = Int128GetDatum128(sum);
			break;
		}

		case INT8OID:
		{
			auto arg = ::DatumGetInt64(datum);
			auto sum = Datum128GetInt128(sum_);
			sum += arg;
			sum_ = Int128GetDatum128(sum);
			break;
		}

		case FLOAT4OID:
		{
			auto arg = ::DatumGetFloat4(datum);
			auto sum = Datum128GetFloat128(sum_);
			sum += arg;
			sum_ = Float128GetDatum128(sum);
			break;
		}

		case FLOAT8OID:
		{
			auto arg = ::DatumGetFloat8(datum);
			auto sum = Datum128GetFloat128(sum_);
			sum += arg;
			sum_ = Float128GetDatum128(sum);
			break;
		}

		case INTERVALOID:
		case CASHOID:
		case NUMERICOID:
			// TODO: support sum of these types
			break;

		default:
			break;
		}
	}

} // namespace mars

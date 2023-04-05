/*-------------------------------------------------------------------------
 *
 * PreAgg.h
 *	  MARS PreAgg collector.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/PreAgg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_PREAGG_H
#define MARS_PREAGG_H

#include "type_traits.hpp"

#include "Compare.h"
#include "utils.h"

namespace mars
{

	class PreAgg
	{
	protected:
		/*!
		 * Pre-Agg bits.
		 */
		enum PreAggBits
		{
			COMPARE = 1U << 0, //<! the type can be compared
			SUM = 1U << 1,	   //<! the type can be sumed
		};

	public:
		PreAgg(const ::Form_pg_attribute attr)
			: sum_(0), min_(0), max_(0), first_(0), last_(0), min_at_(0), max_at_(0), total_count_(0), null_count_(0), attr_(attr), bits_{0}, cmp_(nullptr)
		{
			if (attr->attbyval)
			{
				bits_ |= PreAggBits::SUM;

				copy_value = [](::Datum value, ::Form_pg_attribute attr)
				{
					return value;
				};
			}
			else
			{
				copy_value = byptr_value_dup;
			}

			// If there is a comparison operator that is compatible
			//   with the type of the current attribute,
			// we will count its pre-aggregated information.
			::Oid typid = attr->atttypid;
			::Oid opcode = CompareBase::GetCompareOpcode(mars::OP::LT, typid, typid, true);
			if (OidIsValid(opcode))
			{
				cmp_ = std::make_shared<DatumCompare>(typid,
													  typid,
													  attr->attcollation);

				bits_ |= PreAggBits::COMPARE;
			}

			Reset();
		}

		/*!
		 * Append a value to preagg.
		 *
		 * The value is not stored in preagg, only the statistics are updated.
		 */
		void Append(::Datum value, bool isnull);

		void Reset()
		{
			if (bits_ & PreAggBits::SUM)
			{
				SumReset();
			}

			if (bits_ & PreAggBits::COMPARE)
			{
				// these statistics are meaningless unless the first non-null value
				// arrives, they are actually initialized in Append(), but we still
				// reset them with some fake values to prevent leak historical
				// data.
				min_ = 0;
				max_ = 0;
				min_at_ = -1;
				max_at_ = -1;
			}

			first_ = 0;
			last_ = 0;

			total_count_ = 0;
			null_count_ = 0;
		}

	protected:
		/*!
		 * Reset the sum statistic.
		 *
		 * In postgres sum() is an aggregation, it is too heavy for us to maintain
		 * sum in such a way, so we do it manually.
		 *
		 * This method resets the sum statistic.
		 */
		void SumReset();

		/*!
		 * @see SumReset();
		 */
		void SumAppend(::Datum datum);

	private:
		inline bool IsFirstNonNullTuple() const
		{
			return total_count_ - null_count_ == 1;
		}

	public:
		mars::Datum128 sum_;
		::Datum min_;
		::Datum max_;

		::Datum first_; //<! the first value in physical order
		::Datum last_;	//<! the last value in physical order

		int32 min_at_;
		int32 max_at_;

		int32 total_count_;
		int32 null_count_;

	protected:
		const ::Form_pg_attribute attr_;

		uint32 bits_;						//<! or'ed bits of PreAggBits
		std::shared_ptr<DatumCompare> cmp_; //<! compare function

		::Datum (*copy_value)(::Datum, ::Form_pg_attribute);
	};

} // namespace mars

#endif // MARS_PREAGG_H

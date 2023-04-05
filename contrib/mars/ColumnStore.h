/*-------------------------------------------------------------------------
 *
 * ColumnStore.h
 *	  The MARS Column Store
 *
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_COLUMNREADER_H
#define MARS_COLUMNREADER_H

#include "type_traits.hpp"

#include "ScanKey.h"
#include "coder/Decoder.h"

#include "ts_func.h"

#include <parquet/column_reader.h>
#include <parquet/schema.h>

#include <list>
#include <memory>
#include <numeric>

namespace mars
{

	/*!
	 * Column Store.
	 *
	 * It stores data points of one column.
	 */
	class ColumnStore
	{
	public:
		struct FilterContext
		{
			const footer::Footer *footer;
			const mars::ScanKey *filter;
			std::vector<bool> *matches;
			int first;
			int last;
			bool has_mismatch;
		};

	public:
		/*!
		 * Make a TypedColumnStore according to the column type.
		 *
		 * @param column The parquet column, we know the column type with it.
		 *
		 * @return A new TypedColumnStore.
		 */
		static std::shared_ptr<ColumnStore> Make(const RelationCompare &comps,
												 int column);

		ColumnStore() = delete;

		ColumnStore(const ::Form_pg_attribute attr, const DatumCompare &comp)
			: datum_comp_(comp), values_(), isnull_(), attr_(attr)
		{
		}

		/*!
		 * Compare values[row1] and values[row2].
		 *
		 * The order is "ASC NULLS LAST".
		 *
		 * @param row1,row2 The row ids.
		 *
		 * @return (values[row1] < values[row2]).
		 */
		bool Compare(int row1, int row2) const
		{
			auto n1 = isnull_[row1];
			auto n2 = isnull_[row2];

			if (n1 && n2)
			{
				// both are NULL, equal
				return false;
			}
			else if (n1 || n2)
			{
				// only one of them is NULL, so row1 is smaller iff row2 is NULL.
				return n2;
			}

			// neither is NULL, compare by value
			auto v1 = values_[row1];
			auto v2 = values_[row2];

			return datum_comp_(v1, mars::OP::LT, v2);
		}

		/*!
		 * Compare time_bucket(values[row1]) and time_bucket(values[row2]).
		 *
		 * The order is "ASC NULLS LAST".
		 *
		 * @note Do not merge this and Compare(int, int) into one, this function is
		 * heavily called, we want to save some cpu cycles.
		 *
		 * @param interval The interval for time_bucket().
		 * @param row1,row2 The row ids.
		 *
		 * @return (time_bucket(interval, values[row1]) <
		 *          time_bucket(interval, values[row2])).
		 */
		bool Compare(::Interval *interval, int row1, int row2) const
		{
			auto n1 = isnull_[row1];
			auto n2 = isnull_[row2];

			if (n1 && n2)
			{
				// both are NULL, equal
				return false;
			}
			else if (n1 || n2)
			{
				// only one of them is NULL, so row1 is smaller iff row2 is NULL.
				return n2;
			}

			auto ts1 = DatumGetTimestamp(values_[row1]);
			auto ts2 = DatumGetTimestamp(values_[row2]);
			auto v1 = ts_timestamp_bucket(interval, ts1);
			auto v2 = ts_timestamp_bucket(interval, ts2);

			return datum_comp_(v1, mars::OP::LT, v2);
		}

		/*!
		 * Compare values[row1] and values[row2], in C style.
		 *
		 * The order is "ASC NULLS LAST".
		 *
		 * @param row1,row2 The row ids.
		 *
		 * @return -1 if (values[row1] <  values[row2]).
		 * @return  0 if (values[row1] == values[row2]).
		 * @return +1 if (values[row1] >  values[row2]).
		 */
		int CompareC(int row1, int row2) const
		{
			auto n1 = isnull_[row1];
			auto n2 = isnull_[row2];

			if (n1 && n2)
			{
				// both are NULL, equal
				return 0;
			}
			else if (n1 || n2)
			{
				// only one of them is NULL, so row1 is smaller iff row2 is NULL.
				return n2 ? -1 : 1;
			}

			auto v1 = values_[row1];
			auto v2 = values_[row2];

			return (datum_comp_(v1, mars::OP::LT, v2) ? -1 : datum_comp_(v1, mars::OP::GT, v2) ? +1
																							   : 0);
		}

		/*!
		 * Compare time_bucket(values[row1]) and time_bucket(values[row2]), in C
		 * style.
		 *
		 * The order is "ASC NULLS LAST".
		 *
		 * @note Do not merge this and CompareC(int, int) into one, this function
		 * is heavily called, we want to save some cpu cycles.
		 *
		 * @param interval The interval for time_bucket().
		 * @param row1,row2 The row ids.
		 *
		 * @return -1 if (time_bucket(interval, values[row1]) <
		 *                time_bucket(interval, values[row2])).
		 * @return  0 if (time_bucket(interval, values[row1]) ==
		 *                time_bucket(interval, values[row2])).
		 * @return +1 if (time_bucket(interval, values[row1]) >
		 *                time_bucket(interval, values[row2])).
		 */
		int CompareC(::Interval *interval, int row1, int row2) const
		{
			auto n1 = isnull_[row1];
			auto n2 = isnull_[row2];

			if (n1 && n2)
			{
				// both are NULL, equal
				return 0;
			}
			else if (n1 || n2)
			{
				// only one of them is NULL, so row1 is smaller iff row2 is NULL.
				return n2 ? -1 : 1;
			}

			auto ts1 = DatumGetTimestamp(values_[row1]);
			auto ts2 = DatumGetTimestamp(values_[row2]);
			auto v1 = ts_timestamp_bucket(interval, ts1);
			auto v2 = ts_timestamp_bucket(interval, ts2);

			return (datum_comp_(v1, mars::OP::LT, v2) ? -1 : datum_comp_(v1, mars::OP::GT, v2) ? +1
																							   : 0);
		}

		/*!
		 * Resize the column store.
		 *
		 * All the previously loaded rows are dropped.
		 *
		 * @param nrows The size.
		 */
		void Resize(int nrows);

		void Clear()
		{
			values_.clear();
			isnull_.clear();
		}

		/*!
		 * Return the capacity of the column store.
		 *
		 * @return The capacity.
		 */
		int Capacity() const
		{
			return values_.capacity();
		}

		/*!
		 * Return the count of loaded values.
		 *
		 * @return The count.
		 */
		int Count() const
		{
			return values_.size();
		}

		/*!
		 * Read some points from the parquet column reader.
		 *
		 * This will read at most \p nrows points and append them to the column
		 * store, there must be enough space reserved for the new rows.
		 *
		 * If the \p has_nulls is false, we can perform whole page reading,
		 * otherwise we have to read point by point, which is rather slow.
		 *
		 * @param version The format version.
		 * @param column_reader The parquet column reader.
		 * @param nrows The rows to read.
		 * @param has_nulls True if the column contains null values.
		 */
		virtual void ReadPoints(coder::Version version,
								const ::Form_pg_attribute attr,
								std::shared_ptr<parquet::ColumnReader> column_reader,
								unsigned int nrows, bool has_nulls) = 0;

		/*!
		 * Filter for all kinds of conditions.
		 *
		 * It is a wrapper for FilterValues() and FilterIsNull().
		 */
		bool FilterRows(FilterContext &ctx) const;

		/*!
		 * Get one point from the column store.
		 *
		 * This function return the point value as a datum, this is not type safe
		 * in c++, however it is efficient.
		 *
		 * @param remap The reordered map.  When spcified, values[remap[row]] is
		 *              returned, otherwise values[row] is returned.
		 * @param row The row id.
		 * @param values The datum.
		 * @param isnull The isnull flag.
		 */
		void GetPoint(const std::vector<int> *remap,
					  int row, ::Datum &values, bool &isnull) const;

		const std::vector<::Datum> &Values() const
		{
			return values_;
		}

		std::vector<::Datum> &Values()
		{
			return values_;
		}

		const std::vector<bool> &Nulls() const
		{
			return isnull_;
		}

		std::vector<bool> &Nulls()
		{
			return isnull_;
		}

		void Append(::Datum value, bool isnull)
		{
			if (!isnull && !attr_->attbyval)
			{
				value = byptr_value_dup(value, attr_);
			}
			values_.push_back(value);
			isnull_.push_back(isnull);
		}

		void Append(const ::TupleTableSlot *slot)
		{
			auto value = slot->tts_values[attr_->attnum - 1];
			auto isnull = slot->tts_isnull[attr_->attnum - 1];

			Append(value, isnull);
		}

		/*!
		 * @return true if a row is updated.
		 */
		bool Update(const ::ItemPointer otid, ::Datum value, bool isnull)
		{
			int row = ItemPointerGetOffsetNumber(otid) - FirstOffsetNumber;
			int nrows = Count();

			if (row < nrows)
			{
				if (values_[row] != value || isnull_[row] != isnull)
				{
					values_[row] = value;
					isnull_[row] = isnull;
					return true;
				}
			}

			return false;
		}

		bool Update(const ::ItemPointer otid, const ::TupleTableSlot *slot)
		{
			auto value = slot->tts_values[attr_->attnum - 1];
			auto isnull = slot->tts_isnull[attr_->attnum - 1];

			return Update(otid, value, isnull);
		}

		bool Upsert(int row, ::Datum value, bool isnull)
		{
			if (isnull)
			{
				return false;
			}
			else if (unlikely(attr_->atttypid == JSONBOID))
			{
				// special handling for jsonb values, merge new into old
				value = DirectFunctionCall2(::jsonb_concat, values_[row], value);
			}
			else if (!attr_->attbyval)
			{
				value = byptr_value_dup(value, attr_);
			}

			Assert((std::size_t)row < values_.size());
			values_[row] = value;
			isnull_[row] = isnull;
			return true;
		}

		bool UpsertLastRow(::Datum value, bool isnull)
		{
			Assert(!values_.empty());
			return Upsert(values_.size() - 1, value, isnull);
		}

		bool UpsertLastRow(const ::TupleTableSlot *slot)
		{
			auto value = slot->tts_values[attr_->attnum - 1];
			auto isnull = slot->tts_isnull[attr_->attnum - 1];

			return UpsertLastRow(value, isnull);
		}

		void DeleteRows(const std::list<int> &rows)
		{
			if (rows.empty())
				return;

			int dst = 0;
			int src = 0;
			for (auto row : rows)
			{
				if (src != dst)
				{
					auto values_begin = values_.begin();
					std::copy(values_begin + src, values_begin + row,
							  values_begin + dst);

					auto isnull_begin = isnull_.begin();
					std::copy(isnull_begin + src, isnull_begin + row,
							  isnull_begin + dst);
				}

				dst += row - src;
				src = row + 1;
			}

			Assert(src != dst);
			std::copy(values_.begin() + src, values_.end(),
					  values_.begin() + dst);
			std::copy(isnull_.begin() + src, isnull_.end(),
					  isnull_.begin() + dst);

			values_.resize(values_.size() - rows.size());
			isnull_.resize(isnull_.size() - rows.size());
		}

	protected:
		/*!
		 * Filter for non "IS NULL" conditions, only non-null values match.
		 *
		 * This base version is safe but slow, use the TypedColumnStore overrides
		 * whenever possible.
		 *
		 * @return false if no row matches.
		 */
		virtual bool FilterValues(FilterContext &ctx) const;

		/*!
		 * Filter for "IS NULL" condition, only null values match.
		 *
		 * @return false if no row matches.
		 */
		bool FilterIsNull(FilterContext &ctx) const;

	protected:
		/*!
		 * Helper functions to compare the points.
		 */
		const DatumCompare datum_comp_;

		// TODO: switch to parquet::ResizableBuffer
		std::vector<::Datum> values_;
		std::vector<bool> isnull_;

		const ::Form_pg_attribute attr_;
	};

	template <::Oid type_oid>
	class TypedColumnStore : public ColumnStore
	{
	public:
		using T = pg_type_traits<type_oid>;

		TypedColumnStore(const ::Form_pg_attribute attr, const DatumCompare &comp)
			: ColumnStore(attr, comp)
		{
		}

		virtual ~TypedColumnStore() = default;

		void ReadPoints(coder::Version version, const ::Form_pg_attribute attr,
						std::shared_ptr<parquet::ColumnReader> column_reader,
						unsigned int nrows, bool has_nulls) override;

	protected:
		/*!
		 * Filter for non "IS NULL" conditions, only non-null values match.
		 *
		 * These typed overrides are more efficient than the base implementation
		 * because they compare without calling pg opers when possible.
		 */
		bool FilterValues(FilterContext &ctx) const override;
	};

	using Int32ColumnStore = TypedColumnStore<INT4OID>;
	using Int64ColumnStore = TypedColumnStore<INT8OID>;
	using FloatColumnStore = TypedColumnStore<FLOAT4OID>;
	using DoubleColumnStore = TypedColumnStore<FLOAT8OID>;
	using BoolColumnStore = TypedColumnStore<BOOLOID>;
	using TextColumnStore = TypedColumnStore<TEXTOID>;

	class ColumnStoreSorter
	{
	public:
		ColumnStoreSorter(const std::vector<::AttrNumber> &order)
			: context_(order), compare_(context_)
		{
		}

		void BindData(const std::vector<std::shared_ptr<ColumnStore>> *cstores,
					  std::vector<int> *remap)
		{
			context_.cstores_ = cstores;
			context_.remap_ = remap;
		}

		void ResetReorderMap()
		{
			Assert(!context_.cstores_->empty());
			int nrows = context_.cstores_->at(0)->Count();

			context_.remap_->resize(nrows);
			std::iota(context_.remap_->begin(), context_.remap_->end(), 0);
		}

		bool IsSorted() const
		{
			if (context_.order_.empty())
			{
				return true;
			}

			return std::is_sorted(context_.remap_->cbegin(),
								  context_.remap_->cend(),
								  compare_);
		}

		void Sort()
		{
			if (context_.order_.empty())
			{
				return;
			}

			std::sort(context_.remap_->begin(), context_.remap_->end(), compare_);
		}

	protected:
		struct Context
		{
			Context(const std::vector<::AttrNumber> &order)
				: remap_(nullptr), cstores_(nullptr)
			{
				// convert AttrNumber vector to column index vector
				order_.resize(order.size());
				std::transform(order.begin(), order.end(), order_.begin(),
							   [](const ::AttrNumber &attrnum) -> int
							   {
								   return AttrNumberGetAttrOffset(attrnum);
							   });
			}

			std::vector<int> order_;
			std::vector<int> *remap_;
			const std::vector<std::shared_ptr<ColumnStore>> *cstores_;
		};

		struct Compare
		{
			Compare(const Context &context)
				: context_(context)
			{
			}

			Compare(const Compare &) = default;

			bool operator()(int a, int b) const
			{
				for (auto column : context_.order_)
				{
					const auto &cstore = *context_.cstores_->operator[](column);
					auto cmp = cstore.CompareC(a, b);

					if (cmp == 0)
						continue;
					return cmp < 0;
				}

				// all equal
				return false;
			}

			const Context &context_;
		};

	protected:
		Context context_;
		const Compare compare_;
	};

} // namespace mars

#endif /* MARS_COLUMNREADER_H */

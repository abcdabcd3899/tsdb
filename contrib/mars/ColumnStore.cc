/*-------------------------------------------------------------------------
 *
 * ColumnStore.cc
 *	  The MARS Column Store
 *
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "ColumnStore.h"
#include "DirectCompare.h"
#include "coder/Encoder.h"

#include <parquet/column_reader.h>
#include <parquet/types.h>

#include <memory>

using mars::OP;

namespace mars
{

	/*
	 * Filter nulls in non "IS NULL" conditions.
	 *
	 * This should only be called by FilterValues() or FilterTypedValues().
	 */
	static bool FilterNulls(ColumnStore::FilterContext &ctx,
							const std::vector<bool> &isnull)
	{
		auto column = AttrNumberGetAttrOffset(ctx.filter->scankey_.sk_attno);
		auto has_match = true;

		bool all_nulls = ctx.footer->GetRowCount(column) == 0;
		if (all_nulls)
		{
			// fastpath: no match is possible
			ctx.has_mismatch = true;
			return false;
		}

		bool has_nulls = ctx.footer->GetNullCount(column) > 0;
		if (has_nulls)
		{
			int n = ctx.matches->size();
			auto &matches = *ctx.matches;

			// to get better performance, do not use "if" in the loop.
			//
			// XXX: see FilterTypedValues() for compiler specific behaviors.
			has_match = false;
			for (int i = 0; i < n; ++i)
			{
				// first check null value
				bool match = !isnull[i];
				match = matches[i] && match;
				matches[i] = match;
				has_match = has_match || match;
				ctx.has_mismatch = ctx.has_mismatch || !match;
			}
		}

		return has_match;
	}

	template <::Oid type_oid, mars::OP op>
	static bool FilterTypedValues(ColumnStore::FilterContext &ctx,
								  const std::vector<::Datum> &values_,
								  const std::vector<bool> &isnull)
	{
		using T = pg_type_traits<type_oid>;

		Assert(values_.size() >= ctx.matches->size());

		auto has_match = FilterNulls(ctx, isnull);
		if (has_match)
		{
			int n = ctx.matches->size();
			auto &matches = *ctx.matches;
			auto values = values_.data();
			auto arg = T::FromDatum(ctx.filter->scankey_.sk_argument);

			// to get better performance, do not use "if" in the loop.
			//
			// XXX: we want the compiler to generate no-conditional-jump
			// instructions to be more cpu pipeline friendly, however gcc (at least
			// gcc-11.2) generates jumps for below loop body; clang (clang-12.0)
			// generates no-jump instructions as our expectation.
			has_match = false;
			for (int i = 0; i < n; ++i)
			{
				auto typed_value = T::FromDatum(values[i]);
				auto match = DirectCompare<type_oid, op>::match(typed_value, arg);
				match = matches[i] && match;
				matches[i] = match;
				has_match = has_match || match;
				ctx.has_mismatch = ctx.has_mismatch || !match;
			}
		}

		return has_match;
	}

	std::shared_ptr<mars::ColumnStore>
	mars::ColumnStore::Make(const RelationCompare &comps, int column)
	{
		const auto attr = comps.GetAttr(column);
		const auto &comp = comps.GetComparator(column);

		switch (attr->atttypid)
		{
		case INT4OID:
			return std::make_shared<mars::Int32ColumnStore>(attr, comp);

		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return std::make_shared<mars::Int64ColumnStore>(attr, comp);

		case FLOAT4OID:
			return std::make_shared<mars::FloatColumnStore>(attr, comp);

		case FLOAT8OID:
			return std::make_shared<mars::DoubleColumnStore>(attr, comp);

		case BOOLOID:
			return std::make_shared<mars::BoolColumnStore>(attr, comp);

		default:
			if (!attr->attbyval)
			{
				return std::make_shared<mars::TextColumnStore>(attr, comp);
			}
			else
			{
				elog(ERROR, "mars: type is not supported yet");
			}
		}
	}

	void
	ColumnStore::Resize(int nrows)
	{
		// clear all the previous values.
		values_.clear();
		isnull_.clear();

		// need to make sure that we only reserve exactly the required space
		values_.shrink_to_fit();
		isnull_.shrink_to_fit();

		values_.reserve(nrows);
		isnull_.reserve(nrows);
	}

	template <::Oid type_oid>
	void
	TypedColumnStore<type_oid>::ReadPoints(coder::Version version,
										   const ::Form_pg_attribute attr,
										   std::shared_ptr<parquet::ColumnReader> column_reader,
										   unsigned int nrows, bool has_nulls)
	{
		// TODO: reading all the rows at once is a waste for cases like LIMIT 1,
		// however reading rows one by one is inefficient for full table scanning,
		// so maybe a better balance is to read a small batch, say 20 rows, each
		// time.

		// TODO: now we store the datums in the column store, but the typed values
		// are stored on the disk, so we have to do the type conversion for every
		// point, any chance to avoid that?

		int64_t values_read = 0;
		int64_t rows_read = 0;
		int64_t offset = Count();

		Assert(offset + nrows <= Capacity());

		values_.resize(offset + nrows);
		isnull_.resize(offset + nrows);

		std::vector<int16_t> def_levels(nrows);

		if (version == coder::Version::V0_1)
		{
			auto typed_reader =
				std::static_pointer_cast<parquet::TypedColumnReader<parquet::PhysicalType<T::parquet_type>>>(column_reader);
			std::vector<typename T::reader_type> points(nrows);

			while (nrows > 0 && typed_reader->HasNext())
			{
				rows_read = typed_reader->ReadBatch(nrows,
													&def_levels[0],
													nullptr,
													&points[0],
													&values_read);
				// we have to convert the typed values to datums, also take the
				// chance to handle null values.
				for (int64_t i = 0, nvalues = 0; i < rows_read; ++i)
				{
					if (def_levels[i] > 0)
					{
						// not null
						Assert(isnull_[offset + i] == false);
						values_[offset + i] = T::ToDatum(points[nvalues++]);
					}
					else
					{
						// null
						Assert(values_[offset + i] == (::Datum)0);
						isnull_[offset + i] = true;
					}
				}

				offset += rows_read;
				nrows -= rows_read;
			}
		}
		else
		{
			// encoded data are always stored as parquet byte array
			auto typed_reader =
				std::static_pointer_cast<parquet::ByteArrayReader>(column_reader);

			if (nrows > 0)
			{
				Assert(typed_reader->HasNext());

				// encoded data are only stored at first row in each group.
				std::vector<parquet::ByteArray> points(1);

				rows_read = typed_reader->ReadBatch(1,
													&def_levels[0],
													nullptr,
													&points[0],
													&values_read);
				Assert(rows_read == 1 && def_levels[0] == 1);
				auto bytes = (const bytea *)points[0].ptr;
				rows_read = Decoder::Decode(version, attr, bytes,
											values_, isnull_, offset);

				offset += rows_read;
				nrows -= rows_read;
			}
		}

		Assert(nrows == 0);
	}

	template <::Oid type_oid>
	bool
	TypedColumnStore<type_oid>::FilterValues(FilterContext &ctx) const
	{
		if (ctx.filter->scankey_.sk_subtype != type_oid)
		{
			// fallback to the slow version when args have different types
			return ColumnStore::FilterValues(ctx);
		}

		if (ctx.filter->GetArrayKeyType() == mars::ScanKey::ARRAY_FILTER)
		{
			// fallback to the slow version on arraykeys
			return ColumnStore::FilterValues(ctx);
		}

		switch (ctx.filter->scankey_.sk_strategy)
		{
		case BTLessStrategyNumber:
			return FilterTypedValues<type_oid, OP::LT>(ctx, values_, isnull_);
		case BTLessEqualStrategyNumber:
			return FilterTypedValues<type_oid, OP::LE>(ctx, values_, isnull_);
		case BTEqualStrategyNumber:
			return FilterTypedValues<type_oid, OP::EQ>(ctx, values_, isnull_);
		case BTGreaterEqualStrategyNumber:
			return FilterTypedValues<type_oid, OP::GE>(ctx, values_, isnull_);
		case BTGreaterStrategyNumber:
			return FilterTypedValues<type_oid, OP::GT>(ctx, values_, isnull_);
		default:
			pg_unreachable();
		}
	}

	template <>
	bool
	TextColumnStore::FilterValues(FilterContext &ctx) const
	{
		return ColumnStore::FilterValues(ctx);
	}

	void
	ColumnStore::GetPoint(const std::vector<int> *remap,
						  int row, ::Datum &datum, bool &isnull) const
	{
		Assert(row < Count());

		if (remap)
			row = remap->at(row);

		isnull = isnull_[row];
		if (!isnull)
		{
			datum = values_[row];
		}
	}

	bool
	ColumnStore::FilterValues(FilterContext &ctx) const
	{
		Assert(values_.size() >= ctx.matches->size());

		auto has_match = FilterNulls(ctx, isnull_);
		if (has_match)
		{
			int n = ctx.matches->size();
			auto &matches = *ctx.matches;
			auto values = values_.data();

			has_match = false;
			for (int i = 0; i < n; ++i)
			{
				// value comparison is done with pg opers, they are slow, so only
				// do them when necessary.
				if (matches[i])
				{
					auto match = ctx.filter->MatchPoint(values[i]);
					match = matches[i] && match;
					matches[i] = match;
					has_match = has_match || match;
					ctx.has_mismatch = ctx.has_mismatch || !match;
				}
			}
		}

		return has_match;
	}

	bool
	ColumnStore::FilterIsNull(FilterContext &ctx) const
	{
		Assert(isnull_.size() >= ctx.matches->size());

		auto column = AttrNumberGetAttrOffset(ctx.filter->scankey_.sk_attno);

		bool has_nulls = ctx.footer->GetNullCount(column) > 0;
		if (!has_nulls)
		{
			// fastpath: no match is possible
			ctx.has_mismatch = true;
			return false;
		}

		bool all_nulls = ctx.footer->GetRowCount(column) == 0;
		if (all_nulls)
		{
			// fastpath: all rows match, the matching bitmap is unchanged
			return true;
		}

		int n = ctx.matches->size();
		auto &matches = *ctx.matches;

		// to get better performance, do not use "if" in the loop.
		//
		// XXX: see FilterTypedValues() for compiler specific behaviors.
		auto has_match = false;
		for (int i = 0; i < n; ++i)
		{
			bool match = isnull_[i];
			match = matches[i] && match;
			matches[i] = match;
			has_match = has_match || match;
			ctx.has_mismatch = ctx.has_mismatch || !match;
		}

		return has_match;
	}

	bool
	ColumnStore::FilterRows(FilterContext &ctx) const
	{
		auto sk_isnull = ctx.filter->scankey_.sk_flags & SK_ISNULL;

		if (sk_isnull)
		{
			return FilterIsNull(ctx);
		}
		else
		{
			return FilterValues(ctx);
		}
	}

} // namespace mars

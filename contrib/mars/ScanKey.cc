/*-------------------------------------------------------------------------
 *
 * ScanKey.cc
 *	  The MARS ScanKey
 *
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "ScanKey.h"
#include "Footer.h"
#include "ParquetReader.h"

using mars::OP;

namespace mars
{

	std::shared_ptr<mars::ScanKey>
	mars::ScanKey::Make(::Oid lefttype, ::Oid righttype, ::Oid collation)
	{
		if (::InByvalTypeAllowedList(righttype) ||
			::InVarlenTypeAllowedList(righttype))
			return std::make_shared<mars::ScanKey>(lefttype, righttype, collation);

		return nullptr;
	}

	mars::ScanKey::MergePolicy
	mars::ScanKey::Merge(const mars::ScanKey &scankey1,
						 const mars::ScanKey &scankey2)
	{
		const auto A = mars::ScanKey::MergePolicy::KEEP_A;
		const auto B = mars::ScanKey::MergePolicy::KEEP_B;
		const auto BOTH = mars::ScanKey::MergePolicy::KEEP_BOTH;
		const auto NONE = mars::ScanKey::MergePolicy::KEEP_NONE;
		const auto &a = scankey1;
		const auto &b = scankey2;

		Assert(a.scankey_.sk_attno == b.scankey_.sk_attno);

		switch (a.scankey_.sk_strategy)
		{
		case BTLessStrategyNumber:
			// a < 1:
			switch (b.scankey_.sk_strategy)
			{
			case BTLessStrategyNumber:
				// b < 0 : keep b
				// b < 1 : keep a, or b
				// b < 2 : keep a
				return a <= b ? A : B;
			case BTLessEqualStrategyNumber:
				// b <= 0 : keep b
				// b <= 1 : keep a
				// b <= 2 : keep a
				return a <= b ? A : B;
			case BTEqualStrategyNumber:
				// b = 0 : keep b
				// b = 1 : keep none
				// b = 2 : keep none
				return a <= b ? NONE : B;
			case BTGreaterEqualStrategyNumber:
				// b >= 0 : keep both
				// b >= 1 : keep none
				// b >= 2 : keep none
				return a <= b ? NONE : BOTH;
			case BTGreaterStrategyNumber:
				// b > 0 : keep both
				// b > 1 : keep none
				// b > 2 : keep none
				return a <= b ? NONE : BOTH;
			default:
				pg_unreachable();
			}
			break;
		case BTLessEqualStrategyNumber:
			// a <= 1
			switch (b.scankey_.sk_strategy)
			{
			case BTLessStrategyNumber:
				// b < 0 : keep b
				// b < 1 : keep b
				// b < 2 : keep a
				return a < b ? A : B;
			case BTLessEqualStrategyNumber:
				// b <= 0 : keep b
				// b <= 1 : keep b, or a
				// b <= 2 : keep a
				return a < b ? A : B;
			case BTEqualStrategyNumber:
				// b = 0 : keep b
				// b = 1 : keep b
				// b = 2 : keep none
				return a < b ? NONE : B;
			case BTGreaterEqualStrategyNumber:
				// b >= 0 : keep both
				// b >= 1 : keep both, or (a == 1)
				// b >= 2 : keep none
				return a < b ? NONE : BOTH;
			case BTGreaterStrategyNumber:
				// b > 0 : keep both
				// b > 1 : keep none
				// b > 2 : keep none
				return a <= b ? NONE : BOTH;
			default:
				pg_unreachable();
			}
			break;
		case BTEqualStrategyNumber:
			// a = 1:
			switch (b.scankey_.sk_strategy)
			{
			case BTLessStrategyNumber:
				// b < 0 : keep none
				// b < 1 : keep none
				// b < 2 : keep a
				return a < b ? A : NONE;
			case BTLessEqualStrategyNumber:
				// b <= 0 : keep none
				// b <= 1 : keep a
				// b <= 2 : keep a
				return a <= b ? A : NONE;
			case BTEqualStrategyNumber:
				// b = 0 : keep none
				// b = 1 : keep a, or b, TODO: keep arraykey
				// b = 2 : keep none
				return a == b ? A : NONE;
			case BTGreaterEqualStrategyNumber:
				// b >= 0 : keep a
				// b >= 1 : keep a
				// b >= 2 : keep none
				return a < b ? NONE : A;
			case BTGreaterStrategyNumber:
				// b > 0 : keep a
				// b > 1 : keep none
				// b > 2 : keep none
				return a <= b ? NONE : A;
			default:
				pg_unreachable();
			}
			break;
		case BTGreaterEqualStrategyNumber:
			// a >= 1
			switch (b.scankey_.sk_strategy)
			{
			case BTLessStrategyNumber:
				// b < 0 : keep none
				// b < 1 : keep none
				// b < 2 : keep both
				return a < b ? BOTH : NONE;
			case BTLessEqualStrategyNumber:
				// b <= 0 : keep none
				// b <= 1 : keep both, or (a == 1)
				// b <= 2 : keep both
				return a <= b ? BOTH : NONE;
			case BTEqualStrategyNumber:
				// b = 0 : keep none
				// b = 1 : keep b
				// b = 2 : keep b
				return a <= b ? B : NONE;
			case BTGreaterEqualStrategyNumber:
				// b >= 0 : keep a
				// b >= 1 : keep b, or a
				// b >= 2 : keep b
				return a <= b ? B : A;
			case BTGreaterStrategyNumber:
				// b > 0 : keep a
				// b > 1 : keep b
				// b > 2 : keep b
				return a <= b ? B : A;
			default:
				pg_unreachable();
			}
			break;
		case BTGreaterStrategyNumber:
			// a > 1
			switch (b.scankey_.sk_strategy)
			{
			case BTLessStrategyNumber:
				// b < 0 : keep none
				// b < 1 : keep none
				// b < 2 : keep both
				return a < b ? BOTH : NONE;
			case BTLessEqualStrategyNumber:
				// b <= 0 : keep none
				// b <= 1 : keep none
				// b <= 2 : keep both
				return a < b ? BOTH : NONE;
			case BTEqualStrategyNumber:
				// b = 0 : keep none
				// b = 1 : keep none
				// b = 2 : keep b
				return a < b ? B : NONE;
			case BTGreaterEqualStrategyNumber:
				// b >= 0 : keep a
				// b >= 1 : keep a
				// b >= 2 : keep b
				return a < b ? B : A;
			case BTGreaterStrategyNumber:
				// b > 0 : keep a
				// b > 1 : keep a, or b
				// b > 2 : keep b
				return a < b ? B : A;
			default:
				pg_unreachable();
			}
			break;
		default:
			pg_unreachable();
		}
	}

	bool
	mars::ScanKey::Reduce(const std::vector<std::shared_ptr<mars::ScanKey>> &original_scankeys,
						  std::vector<std::shared_ptr<mars::ScanKey>> &scankeys)
	{
		struct Bound
		{
			::AttrNumber attr;

			std::shared_ptr<mars::ScanKey> left;
			std::shared_ptr<mars::ScanKey> right;

			Bound(::AttrNumber _attr)
				: attr(_attr), left(nullptr), right(nullptr)
			{
			}
		};
		std::vector<Bound> bounds;

		scankeys.clear();

		for (const auto &iter : original_scankeys)
		{
			Assert(iter->GetArrayKeyType() != ArrayKeyType::ARRAY_FILTER);

			if (bounds.empty())
			{
				// the first key
				bounds.push_back({iter->scankey_.sk_attno});
			}

			if (bounds.back().attr != iter->scankey_.sk_attno)
			{
				// the new key is on a new attr
				bounds.push_back({iter->scankey_.sk_attno});
			}

			auto &bound = bounds.back();
			if (bound.left.get() == nullptr &&
				iter->scankey_.sk_strategy >= BTEqualStrategyNumber)
			{
				// "=", ">=", ">" are considered as left bound
				bound.left = iter;
			}
			else if (bound.right.get() == nullptr &&
					 iter->scankey_.sk_strategy < BTEqualStrategyNumber)
			{
				// "<", "<=" are considered as right bound
				bound.right = iter;
			}

			if (bound.left.get() && bound.left.get() != iter.get())
			{
				auto policy = mars::ScanKey::Merge(*(bound.left.get()), *iter);
				switch (policy)
				{
				case mars::ScanKey::MergePolicy::KEEP_A:
					break;
				case mars::ScanKey::MergePolicy::KEEP_B:
					bound.left = iter;
					break;
				case mars::ScanKey::MergePolicy::KEEP_BOTH:
					// it will be merged on the other edge
					Assert(mars::ScanKey::Merge(*(bound.right.get()), *iter) !=
						   mars::ScanKey::MergePolicy::KEEP_BOTH);
					break;
				case mars::ScanKey::MergePolicy::KEEP_NONE:
					// no match
					return false;
				}
			}

			if (bound.right.get() && bound.right.get() != iter.get())
			{
				auto policy = mars::ScanKey::Merge(*(bound.right.get()), *iter);
				switch (policy)
				{
				case mars::ScanKey::MergePolicy::KEEP_A:
					break;
				case mars::ScanKey::MergePolicy::KEEP_B:
					bound.right = iter;
					break;
				case mars::ScanKey::MergePolicy::KEEP_BOTH:
					// it will be merged on the other edge
					break;
				case mars::ScanKey::MergePolicy::KEEP_NONE:
					// no match
					return false;
				}
			}
		}

		if (bounds.empty())
		{
			return true;
		}

		// TODO: merge ">= 1" and "<= 1" into "= 1".

		for (auto &bound : bounds)
		{
			if (bound.left.get())
			{
				scankeys.push_back(bound.left);
			}

			if (bound.right.get() &&
				// "=" can appear on both sides, only keep one of them
				bound.right.get() != bound.left.get())
			{
				scankeys.push_back(bound.right);
			}
		}

		return true;
	}

	std::pair<::StrategyNumber, std::vector<std::shared_ptr<mars::ScanKey>>>
	mars::ScanKey::DecideStrategy(::ScanDirection scandir,
								  const std::vector<std::shared_ptr<mars::ScanKey>> &scankeys)
	{
		::StrategyNumber final_strategy;
		::StrategyNumber invalid_strategy;
		std::vector<std::shared_ptr<mars::ScanKey>> final_keys;

		if (ScanDirectionIsForward(scandir))
		{
			invalid_strategy = BTLessStrategyNumber - 1;
		}
		else
		{
			invalid_strategy = BTGreaterStrategyNumber + 1;
		}

		final_strategy = invalid_strategy;
		final_keys.reserve(scankeys.size());

		for (const auto &iter : scankeys)
		{
			auto strategy = iter->scankey_.sk_strategy;

			if (ScanDirectionIsForward(scandir))
			{
				final_strategy = std::max(final_strategy, strategy);

				if (strategy < BTEqualStrategyNumber)
				{
					break;
				}
			}
			else
			{
				final_strategy = std::min(final_strategy, strategy);

				if (strategy > BTEqualStrategyNumber)
				{
					break;
				}
			}

			// this key can be used to seek the first matching row.
			final_keys.push_back(iter);
		}

		if (final_strategy == invalid_strategy)
		{
			final_strategy = 0;
		}

		return std::make_pair(final_strategy, std::move(final_keys));
	}

	bool
	mars::ScanKey::ArrayKeyScanBegin(::ScanDirection scandir)
	{
		if (!arraykey_)
		{
			// no arraykey on this scankey at all, so just scan normally.
			return true;
		}

		Assert(arraykey_->num_elems > 0);
		// this also upgrades it from an array filter to an array key
		arraykey_->next_elem = 0;

		return ArrayKeyScanNext(scandir);
	}

	bool
	mars::ScanKey::ArrayKeyScanNext(::ScanDirection scandir)
	{
		if (!arraykey_)
		{
			// no arraykey on this scankey at all
			return false;
		}

		while (arraykey_->next_elem < arraykey_->num_elems &&
			   arraykey_->elem_nulls[arraykey_->next_elem])
		{
			// "x in (null)" is always null even if x is null, so there is no
			// match, move to next arraykey value.
			// FIXME: "IS NULL" and "IS NOT NULL".
			++(arraykey_->next_elem);
		}

		if (arraykey_->next_elem >= arraykey_->num_elems)
		{
			// end of current arraykey, it is important to reset next_elem to -1,
			// so if it is a block level arraykey, it can be seen as an array
			// filter at the table level.
			arraykey_->next_elem = -1;
			return false;
		}

		// the arraykeys are always sorted in ASC order, so we have to decide
		// which edge to begin from.
		int pos;
		if (ScanDirectionIsBackward(scandir))
		{
			pos = arraykey_->num_elems - arraykey_->next_elem - 1;
		}
		else
		{
			// NoMovementScanDirection is considered as ForwardScanDirection, too.
			pos = arraykey_->next_elem;
		}

		scankey_.sk_argument = arraykey_->elem_values[pos];
		++(arraykey_->next_elem);
		return true;
	}

	void
	mars::ScanKey::FirstMatchBlock(BlockSlice &blocks) const
	{
		int column = scankey_.sk_attno - 1;

		blocks.Shrink(scankey_.sk_strategy, column, scankey_.sk_argument,
					  lr_comp_.lefttype_, lr_comp_.righttype_,
					  scankey_.sk_collation);
	}

	mars::ScanKey::MatchType
	mars::ScanKey::MatchBlock(const mars::footer::Footer &footer,
							  int column, bool logical) const
	{
		bool all_nulls = footer.GetRowCount(column, logical) == 0;
		if (all_nulls)
		{
			// the block contains only NULL in the column, and we do not support
			// NULL in scankey or filter yet, so it must be a BLOCK_MISMATCH.
			return MatchType::BLOCK_MISMATCH;
		}

		// when the block contains NULL values, we have to report BLOCK_MAYBE_MATCH
		// instead of BLOCK_FULL_MATCH.
		// TODO: "IS NULL", "IS NOT NULL".
		auto has_nulls = footer.GetNullCount(column, logical) > 0;
		auto full_match = (has_nulls ? MatchType::BLOCK_MAYBE_MATCH : MatchType::BLOCK_FULL_MATCH);

		auto min = footer.GetMinDatum(column, logical);
		auto max = footer.GetMaxDatum(column, logical);

		if (GetArrayKeyType() == ArrayKeyType::ARRAY_FILTER)
		{
			Assert(scankey_.sk_strategy == BTEqualStrategyNumber);

			if (!ll_comp_.equal(min, max))
			{
				auto n = arraykey_->num_elems;
				auto ak_min = arraykey_->elem_values[0];
				auto ak_max = arraykey_->elem_values[n - 1];

				if (rr_comp_(ak_min, GT, ak_max))
				{
					// the elem_values have been sorted in scandir, in backward
					// scan we should swap the ak_{min,max}.
					std::swap(ak_min, ak_max);
				}

				return (lr_comp_(min, LE, ak_max) && lr_comp_(max, GE, ak_min) ? MatchType::BLOCK_MAYBE_MATCH : MatchType::BLOCK_MISMATCH);
			}
			else
			{
				// null values in the arraykey are stripped at the beginning, so we
				// can simply find the values directly.
				auto begin = arraykey_->elem_values;
				auto end = begin + arraykey_->num_elems;
				auto found = std::binary_search(begin, end, min, rl_comp_);

				return (found ? full_match : MatchType::BLOCK_MISMATCH);
			}
		}
		else
		{
			if (scankey_.sk_flags & SK_ISNULL)
			{
				// "x OP y", where OP is "<, <=, =, >=, >", is null when either x
				// or y is null.
				return MatchType::BLOCK_MISMATCH;
			}

			auto &comp = lr_comp_;
			auto arg = scankey_.sk_argument;

			// TODO: compare by calling scankey.sk_func
			switch (scankey_.sk_strategy)
			{
			case BTLessStrategyNumber:
				return (comp(max, LT, arg) ? full_match : comp(min, LT, arg) ? MatchType::BLOCK_MAYBE_MATCH
																			 : MatchType::BLOCK_MISMATCH);
			case BTLessEqualStrategyNumber:
				return (comp(max, LE, arg) ? full_match : comp(min, LE, arg) ? MatchType::BLOCK_MAYBE_MATCH
																			 : MatchType::BLOCK_MISMATCH);
			case BTEqualStrategyNumber:
				return ((comp.equal(min, arg) &&
						 comp.equal(max, arg))
							? full_match
						: (comp(min, LE, arg) &&
						   comp(max, GE, min))
							? MatchType::BLOCK_MAYBE_MATCH
							: MatchType::BLOCK_MISMATCH);
			case BTGreaterEqualStrategyNumber:
				return (comp(min, GE, arg) ? full_match : comp(max, GE, arg) ? MatchType::BLOCK_MAYBE_MATCH
																			 : MatchType::BLOCK_MISMATCH);
			case BTGreaterStrategyNumber:
				return (comp(min, GT, arg) ? full_match : comp(max, GT, arg) ? MatchType::BLOCK_MAYBE_MATCH
																			 : MatchType::BLOCK_MISMATCH);
			default:
				::pg_unreachable();
			}
		}
	}

	bool
	mars::ScanKey::MatchPoint(::Datum datum) const
	{
		if (GetArrayKeyType() == ArrayKeyType::ARRAY_FILTER)
		{
			// null values in the arraykey are stripped at the beginning, so we can
			// simply find the values directly.
			auto begin = arraykey_->elem_values;
			auto end = begin + arraykey_->num_elems;
			return std::binary_search(begin, end, datum, rl_comp_);
		}
		else
		{
			auto op = (mars::OP)scankey_.sk_strategy;
			return lr_comp_(datum, op, scankey_.sk_argument);
		}
	}

	void
	mars::ScanKey::PreprocessArrayKeys()
	{
		if (!arraykey_)
		{
			return;
		}

		// mark it as an array filter
		arraykey_->next_elem = -1;
	}

} // namespace mars

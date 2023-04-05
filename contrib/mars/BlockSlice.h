/*-------------------------------------------------------------------------
 *
 * BlockSlice.h
 *	  MARS block slicer.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_BLOCKSLICE_H
#define MARS_BLOCKSLICE_H

#include "Footer.h"
#include "Compare.h"

#include <vector>

namespace mars
{

	/*!
	 * A block slicer.
	 *
	 * It accesses the blocks in a specified range sequentially.  When the blocks
	 * are ordered, it also provides methods to fast seek or skip.
	 *
	 * A range is described as [begin, end), the begin is included, while the end
	 * is not.
	 *
	 * BlockSlice is similar to RowSlice, however BlockSlice also needs to
	 * distinguish the min or max edges when shrinking the range.
	 *
	 * @see RowSlice.
	 */
	class BlockSlice
	{
	public:
		using iterator = footer::Footer::footers_type::iterator;

		/*!
		 * The default ctor.
		 *
		 * This ctor does not make a valid range actually, make sure to
		 * re-initialize it by calling SetRange() before using it.
		 *
		 * @see SetRange().
		 */
		BlockSlice() = default;

		BlockSlice(::ScanDirection scandir,
				   const iterator &begin,
				   const iterator &end)
			: scandir_(scandir), begin_(begin), end_(end)
		{
		}

		/*!
		 * Set the range.
		 *
		 * @param scandir The scan direction.
		 * @param begin,end The range, [begin, end).
		 */
		void SetRange(::ScanDirection scandir,
					  const iterator &begin,
					  const iterator &end)
		{
			scandir_ = scandir;
			begin_ = begin;
			end_ = end;
		}

		/*!
		 * Get the current range according to the scan direction.
		 *
		 * @param[out] begin,end The range.
		 */
		void GetRange(iterator &begin, iterator &end) const
		{
			begin = begin_;
			end = end_;
		}

		/*!
		 * Get the current block according to the scandir.
		 *
		 * @return The current footer.
		 *
		 * @note Do not call this on an empty block range.
		 * @note This returns the footer instead of the footer iterator.
		 *
		 * @see GetPosition().
		 */
		const mars::footer::Footer &GetBlock() const
		{
			Assert(!IsEmpty());

			if (IsForward())
			{
				return begin_[0];
			}
			else
			{
				// end_ always points to the next one of the edge.
				return end_[-1];
			}
		}

		mars::footer::Footer TransferBlock() const
		{
			Assert(!IsEmpty());

			if (IsForward())
			{
				return begin_[0].Transfer();
			}
			else
			{
				// end_ always points to the next one of the edge.
				return std::move(end_[-1]);
			}
		}

		/*!
		 * Get the current block according to the scandir.
		 *
		 * @return The current footer iterator.
		 *
		 * @note Do not call this on an empty block range.
		 * @note This returns the footer iterator instead of the footer.
		 *
		 * @see GetBlock().
		 */
		const iterator &GetPosition() const
		{
			Assert(!IsEmpty());

			if (IsForward())
			{
				return begin_;
			}
			else
			{
				return end_;
			}
		}

		/*!
		 * Set the current block according to the scandir.
		 *
		 * @param pos The new position, it must be valid.
		 */
		void SetPosition(const iterator &pos)
		{
			if (IsForward())
			{
				begin_ = pos;
			}
			else
			{
				end_ = pos;
			}
		}

		/*!
		 * Clear the block range.
		 */
		void Clear()
		{
			begin_ = end_;
		}

		/*!
		 * Check whether the block range is empty.
		 *
		 * @return true if it is empty.
		 */
		bool IsEmpty() const
		{
			return begin_ == end_;
		}

		/*!
		 * Test whether it is a forward scan.
		 *
		 * @return true if it is forward.
		 */
		bool IsForward() const
		{
			// We treat NoMovementScanDirection as forward, too.
			return !IsBackward();
		}

		/*!
		 * Test whether it is a backward scan.
		 *
		 * @return true if it is backward.
		 */
		bool IsBackward() const
		{
			return ScanDirectionIsBackward(scandir_);
		}

		/*!
		 * Move to the next position in the range.
		 *
		 * This is done by shrink the range directly.
		 *
		 * @note Do not call this on an empty block range.
		 */
		void Next()
		{
			Assert(!IsEmpty());

			if (IsForward())
			{
				std::advance(begin_, 1);
			}
			else
			{
				std::advance(end_, -1);
			}
		}

	protected:
		/*!
		 * Seek the first pos where *pos >= target.
		 *
		 * The current range [begin, end) must be sorted in ASC order.
		 *
		 * @param target The target value to seek.
		 * @param compare The compare function or class.
		 *
		 * @return the pos.
		 */
		template <typename C>
		iterator SeekGE(::Datum target, const C &compare) const
		{
			auto iter = std::lower_bound(begin_, end_, target, compare);
			return iter;
		}

		/*!
		 * Seek the first pos where *pos > target.
		 *
		 * The current range [begin, end) must be sorted in ASC order.
		 *
		 * @param target The target value to seek.
		 * @param compare The compare function or class.
		 *
		 * @return the pos.
		 */
		template <typename C>
		iterator SeekGT(::Datum target, const C &compare) const
		{
			auto iter = std::upper_bound(begin_, end_, target, compare);
			return iter;
		}

		/*!
		 * Skip the all-null blocks on the right side.
		 *
		 * A all-null block is considered as greater than other kind of blocks.
		 *
		 * @note The column must be sorted in ASC NULLS LAST order.
		 */
		void SkipAllNull(int column)
		{
			if (IsEmpty())
			{
				return;
			}

			if (end_[-1].GetRowCount(column) != 0)
			{
				// The last block is not all-null, so there is no all-null blocks
				// at all.
				return;
			}

			mars::footer::ColumnCompareAllNull compare(column);
			// The 3rd arg, 0, is just a pesudo target value, it is ignored by the
			// ColumnCompareAllNull class.
			auto iter = std::lower_bound(begin_, end_, 0, compare);
			end_ = iter;
		}

	public:
		/*!
		 * Shrink the range to [begin, pos) where base[pos - 1] < target.
		 *
		 * - In a forward scan, we scan from begin to pos, so there is no real need
		 *   to seek the pos, we can reach pos eventually during the scan.
		 * - In a backward scan, we scan from pos to begin, so we have to actually
		 *   find out pos;
		 *
		 * @param target The target value to seek.
		 * @param compare The compare function or class.
		 *
		 * @note The current range [begin, end) must be sorted in ASC order.
		 */
		template <typename C>
		void ShrinkLT(::Datum target, const C &compare)
		{
			if (IsBackward())
			{
				// Seek the last pos where base[pos - 1] < target, it equals:
				// Seek the first pos where base[pos] >= target.
				end_ = SeekGE(target, compare);
			}
		}

		/*!
		 * Shrink the range to [begin, pos) where base[pos - 1] <= target.
		 *
		 * - In a forward scan, we scan from begin to pos, so there is no real need
		 *   to seek the pos, we can reach pos eventually during the scan.
		 * - In a backward scan, we scan from pos to begin, so we have to actually
		 *   find out pos;
		 *
		 * @param target The target value to seek.
		 * @param compare The compare function or class.
		 *
		 * @note The current range [begin, end) must be sorted in ASC order.
		 */
		template <typename C>
		void ShrinkLE(::Datum target, const C &compare)
		{
			if (IsBackward())
			{
				// Seek the last pos where base[pos - 1] <= target, it equals:
				// Seek the first pos where base[pos] > target.
				end_ = SeekGT(target, compare);
			}
		}

		/*!
		 * Shrink the range to [pos, end) where base[pos] >= target.
		 *
		 * - In a forward scan, we scan from pos to end, so we have to actually
		 *   find out pos;
		 * - In a backward scan, we scan from end to pos, so there is no real need
		 *   to seek the pos, we can reach pos eventually during the scan.
		 *
		 * @param target The target value to seek.
		 * @param compare The compare function or class.
		 *
		 * @note The current range [begin, end) must be sorted in ASC order.
		 */
		template <typename C>
		void ShrinkGE(::Datum target, const C &compare)
		{
			if (IsForward())
			{
				// Move begin to the first pos where base[pos] >= target.
				begin_ = SeekGE(target, compare);
			}
		}

		/*!
		 * Shrink the range to [pos, end) where base[pos] > target.
		 *
		 * - In a forward scan, we scan from pos to end, so we have to actually
		 *   find out pos;
		 * - In a backward scan, we scan from end to pos, so there is no real need
		 *   to seek the pos, we can reach pos eventually during the scan.
		 *
		 * @param target The target value to seek.
		 * @param compare The compare function or class.
		 *
		 * @note The current range [begin, end) must be sorted in ASC order.
		 */
		template <typename C>
		void ShrinkGT(::Datum target, const C &compare)
		{
			if (IsForward())
			{
				// Move begin to the first pos where base[pos] > target.
				begin_ = SeekGT(target, compare);
			}
		}

		/*!
		 * Shrink the range to [pos1, pos2) where the range matches the strategy.
		 *
		 * @param strategy The strategy.
		 * @param column The column number, counting from 0.
		 * @param target The target value to seek.
		 * @param lefttype,righttype The type oids of the left & right args.
		 * @param collation The collation.
		 *
		 * @note The current range [begin, end) must be sorted in ASC order.
		 */
		void Shrink(::StrategyNumber strategy, int column, ::Datum target,
					::Oid lefttype, ::Oid righttype, ::Oid collation)
		{
			// These compare objects are used after skipping the all-null blocks,
			// so no need to consider them again.
			CustomCompare<mars::footer::Footer>
				comp_min(lefttype, righttype, collation,
						 [column](const mars::footer::Footer &footer) -> ::Datum
						 {
							 return footer.GetMinDatum(column);
						 });

			CustomCompare<mars::footer::Footer>
				comp_max(lefttype, righttype, collation,
						 [column](const mars::footer::Footer &footer) -> ::Datum
						 {
							 return footer.GetMaxDatum(column);
						 });

			// Skip the all-null blocks on the right edge.
			SkipAllNull(column);

			// in a forward scan, there is no need to seek for "<" or "<=", we can
			// simply scan from the beginning.
			//
			// similarly, in a backward scan, there is no need to seek for ">" or
			// ">=", we can simply scan from the end.
			switch (strategy)
			{
			case BTLessStrategyNumber:
				ShrinkLT(target, comp_min);
				break;

			case BTLessEqualStrategyNumber:
				ShrinkLE(target, comp_min);
				break;

			case BTEqualStrategyNumber:
				if (IsForward())
				{
					// "==" is treated the same as ">=" in forward scan.
					ShrinkGE(target, comp_max);
				}
				else
				{
					// "==" is treated the same as "<=" in backward scan.
					ShrinkLE(target, comp_min);
				}
				break;

			case BTGreaterEqualStrategyNumber:
				ShrinkGE(target, comp_max);
				break;

			case BTGreaterStrategyNumber:
				ShrinkGT(target, comp_max);
				break;
			}
		}

	protected:
		::ScanDirection scandir_; //<! The scan direction.

		iterator begin_; //<! The begin of the range.
		iterator end_;	 //<! The end of the range.
	};

} // namespace mars

#endif /* MARS_BLOCKSLICE_H */

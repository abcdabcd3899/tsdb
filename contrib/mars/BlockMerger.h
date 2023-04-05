/*-------------------------------------------------------------------------
 *
 * contrib/mars/BlockMerger.h
 *	  The MARS Block Merger.
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_BLOCKMERGER_H
#define MARS_BLOCKMERGER_H

#include "wrapper.hpp"

#include "Footer.h"

#include <set>
#include <vector>

namespace mars
{

	class Scanner;
	class ColumnStore;

	struct MergeContext
	{
		MergeContext(Scanner &_scanner) noexcept
			: scanner(_scanner), mergekey_columns()
		{
		}

		MergeContext(const MergeContext &) noexcept = delete;
		MergeContext(MergeContext &&) noexcept = delete;

		void BuildMergeKeys();

		Scanner &scanner;
		std::vector<int> mergekey_columns;
	};

	struct RowData
	{
		RowData()
			: values(nullptr), isnull(nullptr)
		{
		}

		~RowData()
		{
			if (values)
				::pfree(values);
			if (isnull)
				::pfree(isnull);
		}

		void Init(int ncols)
		{
			Assert(ncols > 0);
			Assert(values == nullptr);
			Assert(isnull == nullptr);

			values = static_cast<::Datum *>(::palloc0(sizeof(::Datum) * ncols));
			isnull = static_cast<bool *>(::palloc0(sizeof(bool) * ncols));
		}

		::Datum *values;
		bool *isnull;
	};

	class ColumnData
	{
	public:
		ColumnData(std::vector<::Datum> &values, std::vector<bool> &isnull)
			: values_(), isnull_()
		{
			values_.swap(values);
			isnull_.swap(isnull);
		}

		const std::vector<::Datum> &Values() const
		{
			return values_;
		}

		const std::vector<bool> &Nulls() const
		{
			return isnull_;
		}

	protected:
		std::vector<::Datum> values_;
		std::vector<bool> isnull_;
	};

	struct BlockData
	{
	public:
		BlockData(MergeContext &context, ::BlockNumber blockno);
		BlockData(MergeContext &context, ::BlockNumber blockno,
				  std::vector<std::shared_ptr<ColumnStore>> &cstores,
				  std::vector<int> &remap);

		bool Empty() const
		{
			return cursor_ == end_;
		}

		const RowData &TopRow() const
		{
			return top_;
		}

		void PopRow(::TupleTableSlot *slot)
		{
			Assert(!Empty());
			StoreTop(slot);
			SetCursor(cursor_ + step_);
		}

		void PopMergeRow(::TupleTableSlot *slot)
		{
			Assert(!Empty());
			MergeTop(slot);
			SetCursor(cursor_ + step_);
		}

		const ColumnData &GetColumn(int col) const
		{
			return columns_[col];
		}

		::BlockNumber GetBlockNumber() const
		{
			return blockno_;
		}

	protected:
		void SetCursor(int cursor);
		void StoreTop(::TupleTableSlot *slot);
		void MergeTop(::TupleTableSlot *slot) const;

	protected:
		MergeContext &context_;
		std::vector<ColumnData> columns_;
		std::vector<int> remap_;

		RowData top_;

		// TODO: use RowStore for fast column-to-row conversion
		const ::BlockNumber blockno_;

		std::size_t ncols_; //<! effective # of columns, cover all projected ones
		std::size_t cursor_;
		std::size_t end_;
		std::size_t step_;
	};

	class BlockMerger
	{
	public:
		BlockMerger(Scanner &scanner)
			: context_(scanner), block_comp_(context_), blocks_()
		{
		}

		BlockMerger(const BlockMerger &) = delete;
		BlockMerger(BlockMerger &&) noexcept = delete;

		void Begin();
		void End();
		void Rescan();

		bool Empty() const
		{
			return blocks_.empty();
		}

		void PushBlock(const footer::Footer &footer, bool overlapped);
		void PushBlock(std::vector<std::shared_ptr<ColumnStore>> &cstores,
					   std::vector<int> &remap, ::BlockNumber blockno);
		bool PopSingleRow(::TupleTableSlot *slot);
		bool PopMergedRow(::TupleTableSlot *slot);

	protected:
		bool PopRow(::TupleTableSlot *slot, bool merge);

		struct BlockCompare
		{
			BlockCompare(const MergeContext &context)
				: context_(context), op_(mars::OP::EQ) // the initialize value is meaningless
			{
			}

			/*!
			 * @return (a > b) for forward scan, or (a < b) for backward scan.
			 */
			bool operator()(const std::shared_ptr<BlockData> &a,
							const std::shared_ptr<BlockData> &b) const;

			bool Mergeable(const BlockData &block,
						   const ::TupleTableSlot *slot) const;

			void SetScanDir(::ScanDirection scandir)
			{
				// - build a min-heap for forward scan with ">";
				// - build a max-heap for backward scan with "<";
				op_ = ScanDirectionIsBackward(scandir) ? OP::LT : OP::GT;
			}

			const MergeContext &context_;
			mars::OP op_;
		};

	protected:
		MergeContext context_;
		BlockCompare block_comp_;
		std::vector<std::shared_ptr<BlockData>> blocks_;
	};

} // namespace mars

#endif /* MARS_BLOCKMERGER_H */

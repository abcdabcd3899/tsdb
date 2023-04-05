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
#include "BlockMerger.h"

#include "BlockReader.h"
#include "ParquetReader.h"
#include "Scanner.h"

#include <algorithm>
#include <memory>
#include <unordered_set>

namespace mars
{

	void
	MergeContext::BuildMergeKeys()
	{
		if (!mergekey_columns.empty())
			// fastpath: already initialized
			return;

		// we use the groupkeys as the mergekeys, the scanner should ensure that
		// all the groupkeys are projected, this is important to produce the
		// correct merged output.
		const auto &groupkeys = scanner.GetGroupKeys();
		if (groupkeys.empty())
			return;

		mergekey_columns.clear();
		mergekey_columns.reserve(groupkeys.size());

		for (const auto &groupkey : groupkeys)
		{
			auto column = AttrNumberGetAttrOffset(groupkey.attnum);
			mergekey_columns.push_back(column);
		}
	}

	BlockData::BlockData(MergeContext &context, ::BlockNumber blockno,
						 std::vector<std::shared_ptr<ColumnStore>> &cstores,
						 std::vector<int> &remap)
		: context_(context), columns_(), remap_(), blockno_(blockno)
	{
		remap_.swap(remap);

		const auto &projcols = context_.scanner.GetProjectedColumns();
		ncols_ = projcols.back() + 1;

		columns_.reserve(ncols_);
		for (std::size_t i = 0; i < ncols_; ++i)
		{
			auto &cstore = *cstores[i];
			auto &values = cstore.Values();
			auto &isnull = cstore.Nulls();
			columns_.emplace_back(values, isnull);
		}

		auto nrows = remap_.size();
		auto cursor = 0;

		if (ScanDirectionIsBackward(context.scanner.GetScanDir()))
		{
			cursor = nrows - 1;
			end_ = -1;
			step_ = -1;
		}
		else
		{
			cursor = 0;
			end_ = nrows;
			step_ = 1;
		}

		top_.Init(ncols_);
		SetCursor(cursor);
	}

	void
	BlockData::SetCursor(int cursor)
	{
		cursor_ = cursor;
		if (Empty())
			return;

		auto row = remap_[cursor_];
		const auto &projcols = context_.scanner.GetProjectedColumns();
		for (auto column : projcols)
		{
			top_.values[column] = columns_[column].Values()[row];
			top_.isnull[column] = columns_[column].Nulls()[row];
		}
	}

	void
	BlockData::StoreTop(::TupleTableSlot *slot)
	{
		// TODO: copy effective ncols, the max number of projected columns
		::memcpy(slot->tts_values, top_.values, sizeof(::Datum) * ncols_);
		::memcpy(slot->tts_isnull, top_.isnull, sizeof(bool) * ncols_);

		auto row = remap_[cursor_];
		ItemPointerSet(&slot->tts_tid, blockno_, row + FirstOffsetNumber);
	}

	void
	BlockData::MergeTop(::TupleTableSlot *slot) const
	{
		auto row = remap_[cursor_];

		// the current block is always newer than the previous, refer to
		// BlockCompare::operator() for details.
		Assert(ItemPointerGetBlockNumber(&slot->tts_tid) < blockno_);

		const auto tupdesc = slot->tts_tupleDescriptor;
		const auto &projcols = context_.scanner.GetProjectedColumns();
		for (auto i : projcols)
		{
			if (!columns_[i].Nulls()[row])
			{
				auto newvalue = columns_[i].Values()[row];
				auto oldvalue = slot->tts_values[i];

				if (!slot->tts_isnull[i])
				{
					// special handling for jsonb values, merge new into old
					if (tupdesc->attrs[i].atttypid == JSONBOID)
						newvalue = DirectFunctionCall2(::jsonb_concat,
													   oldvalue, newvalue);
				}

				slot->tts_values[i] = newvalue;
				slot->tts_isnull[i] = false;
			}
		}
	}

	void
	BlockMerger::Begin()
	{
		Assert(blocks_.empty());

		context_.BuildMergeKeys();
		block_comp_.SetScanDir(context_.scanner.GetScanDir());
	}

	void
	BlockMerger::End()
	{
		blocks_.clear();
	}

	void
	BlockMerger::Rescan()
	{
		End();
		Begin();
	}

	void
	BlockMerger::PushBlock(const footer::Footer &footer, bool overlapped)
	{
		auto block_reader = BlockReader::Get(context_.scanner, footer);
		if (!block_reader->ReadBatch(overlapped))
			// the block does not contain matching row, ignore it
			return;

		PushBlock(context_.scanner.GetColumnStores(),
				  context_.scanner.GetReorderMap(),
				  footer.impl_->batch_);
	}

	void
	BlockMerger::PushBlock(std::vector<std::shared_ptr<ColumnStore>> &cstores,
						   std::vector<int> &remap, ::BlockNumber blockno)
	{
		if (remap.empty())
			return;

		auto block = std::make_shared<BlockData>(context_, blockno, cstores, remap);
		Assert(!block->Empty());

		blocks_.push_back(std::move(block));
		std::push_heap(blocks_.begin(), blocks_.end(), block_comp_);
	}

	bool
	BlockMerger::PopRow(::TupleTableSlot *slot, bool merge)
	{
		if (blocks_.empty())
			return false;

		auto &block = *blocks_.front();
		if (merge)
			block.PopMergeRow(slot);
		else
			block.PopRow(slot);

		// the block might be swapped during pop_heap(), so we should not touch it
		// after that.
		auto block_empty = block.Empty();

		if (blocks_.size() == 1)
		{
			// fastpath: there is only one block, no need to maintain it as a heap

			if (block_empty)
				// the block is fully consumed, pop it entirely.
				blocks_.clear();
		}
		else
		{
			// pop and re-push the block to maintain a valid heap.
			std::pop_heap(blocks_.begin(), blocks_.end(), block_comp_);

			if (block_empty)
			{
				// the block is fully consumed, pop it entirely.
				blocks_.pop_back();
			}
			else
			{
				// re-push it to the heap, do not touch "block" after this.
				std::push_heap(blocks_.begin(), blocks_.end(), block_comp_);
			}
		}

		return true;
	}

	bool
	BlockMerger::PopSingleRow(::TupleTableSlot *slot)
	{
		return PopRow(slot, false /* merge */);
	}

	bool
	BlockMerger::PopMergedRow(::TupleTableSlot *slot)
	{
		// read the first row
		if (!PopSingleRow(slot))
			return false;

		if (context_.mergekey_columns.empty())
			// row merging is only possible with group (param) keys
			return true;

		// mergeable rows are always overlapped, and we believe we have loaded all
		// the overlapped blocks for the current row, so no need to load more
		// blocks during the merge proces.
		while (!Empty() && block_comp_.Mergeable(*blocks_.front(), slot))
		{
			auto ret pg_attribute_unused() = PopRow(slot, true /* merge */);
			Assert(ret);
		}

		return true;
	}

	bool
	BlockMerger::BlockCompare::operator()(const std::shared_ptr<BlockData> &a,
										  const std::shared_ptr<BlockData> &b) const
	{
		const auto &a_row = a->TopRow();
		const auto &b_row = b->TopRow();

		Assert(op_ == OP::LT || op_ == OP::GT);

		for (auto column : context_.mergekey_columns)
		{
			const auto &comp = context_.scanner.GetColumnComparator(column);
			auto a_value = a_row.values[column];
			auto b_value = b_row.values[column];

			if (comp.equal(a_value, b_value))
				continue; // equal, need to check the next column
			else
				return comp(a_value, op_, b_value);
		}

		// all equal, now compare the block numbers, they must not be equal, and we
		// want to access the old blocks from old to new in all the scandirs, so
		// always compare the block numbers with ">".
		return a->GetBlockNumber() > b->GetBlockNumber();
	}

	bool
	BlockMerger::BlockCompare::Mergeable(const BlockData &block,
										 const ::TupleTableSlot *slot) const
	{
		const auto &row = block.TopRow();

		for (auto column : context_.mergekey_columns)
		{
			const auto &comp = context_.scanner.GetColumnComparator(column);

			Assert(!slot->tts_isnull[column]);
			Assert(!row.isnull[column]);
			if (!comp.equal(row.values[column], slot->tts_values[column]))
				return false;
		}

		// all equal
		return true;
	}

} // namespace mars

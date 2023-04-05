/*-------------------------------------------------------------------------
 *
 * contrib/mars/RowStore.h
 *	  The MARS column-to-row converter.
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "RowStore.h"

namespace mars
{

	void
	RowStore::BindEmpty()
	{
		cstores_ = nullptr;
		remap_ = nullptr;

		offset_ = 0;
		count_ = 0;
		global_remain_ = 0;
	}

	void
	RowStore::Bind(const std::vector<std::shared_ptr<ColumnStore>> *cstores,
				   const std::vector<int> *projcols, ::ScanDirection scandir,
				   int nrows, const std::vector<int> *remap)
	{
		cstores_ = cstores;
		projcols_ = projcols;
		remap_ = remap;
		forward_ = !ScanDirectionIsBackward(scandir);
		global_remain_ = nrows;

		if (forward_)
		{
			global_delta_ = 1;
			global_offset_ = 0;
		}
		else
		{
			global_delta_ = -1;
			global_offset_ = global_remain_ - 1;
		}

		ncols_ = 0;
		for (auto column : *projcols_)
		{
			ncols_ = std::max(ncols_, column + 1);
		}

		// TODO: we could compare ncols_ and projcols_->size() to know the
		// ratio of projected columns, and decide whether should copy all of
		// the columns, or just the projected ones.
		Assert(ncols_ > 0);

		for (int i = 0; i < batchsize; ++i)
		{
			row_values_[i].resize(ncols_);
			row_isnull_[i].resize(ncols_);
		}

		// the data might still be warm, take the chance to convert the first
		// batch.
		if (Convert())
		{
			Assert(count_ <= batchsize);
			Assert(offset_ < count_);
		}
	}

	void
	RowStore::DoConvert(int count)
	{
		const auto &cstores = *cstores_;

		offset_ = 0;
		count_ = count;
		global_remain_ -= count;

		if (remap_ == nullptr)
		{
			for (auto column : *projcols_)
			{
				const auto &cstore = *cstores[column];
				const auto &values = cstore.Values();
				const auto &isnull = cstore.Nulls();
				auto value_iter = std::next(values.cbegin(), global_offset_);
				auto isnull_iter = std::next(isnull.cbegin(), global_offset_);

				if (forward_)
				{
					for (int i = 0; i < count; ++i, ++value_iter, ++isnull_iter)
					{
						row_values_[i][column] = *value_iter;
						row_isnull_[i][column] = *isnull_iter;
					}
				}
				else
				{
					for (int i = 0; i < count; ++i, --value_iter, --isnull_iter)
					{
						row_values_[i][column] = *value_iter;
						row_isnull_[i][column] = *isnull_iter;
					}
				}
			}
		}
		else
		{
			for (auto column : *projcols_)
			{
				const auto &cstore = *cstores[column];
				const auto &values = cstore.Values();
				const auto &isnull = cstore.Nulls();
				auto remap_iter = std::next(remap_->cbegin(), global_offset_);

				if (forward_)
				{
					for (int i = 0; i < count; ++i, ++remap_iter)
					{
						auto row = *remap_iter;
						row_values_[i][column] = values[row];
						row_isnull_[i][column] = isnull[row];
					}
				}
				else
				{
					for (int i = 0; i < count; ++i, --remap_iter)
					{
						auto row = *remap_iter;
						row_values_[i][column] = values[row];
						row_isnull_[i][column] = isnull[row];
					}
				}
			}
		}
	}

	bool
	RowStore::Convert()
	{
		Assert(global_remain_ >= 0);
		if (global_remain_ == 0)
		{
			offset_ = 0;
			count_ = 0;
			return false;
		}

		if (global_remain_ >= batchsize)
		{
			DoConvert(batchsize);
		}
		else
		{
			DoConvert(global_remain_);
		}

		return true;
	}

} // namespace mars

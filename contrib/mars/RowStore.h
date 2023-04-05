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
#ifndef MARS_ROWSTORE_H
#define MARS_ROWSTORE_H

#include "wrapper.hpp"

#include "ColumnStore.h"

namespace mars
{

	class RowStore
	{
	public:
		RowStore() = default;
		RowStore(const RowStore &) = delete;
		RowStore(const RowStore &&) = delete;
		~RowStore() = default;

		/*!
		 * Bind an empty result set.
		 */
		void BindEmpty();

		/*!
		 * Bind a result set, which can be reordered.
		 */
		void Bind(const std::vector<std::shared_ptr<ColumnStore>> *cstores,
				  const std::vector<int> *projcols, ::ScanDirection scandir,
				  int nrows, const std::vector<int> *remap);

		/*!
		 * Read next row.
		 *
		 * @return false is no row is read.
		 */
		bool NextRow(::TupleTableSlot *slot, int &offset)
		{
			if (unlikely(offset_ == count_))
			{
				if (!Convert())
					return false;
			}

			// TODO: should we copy only the projected columns?
			Assert(offset_ < count_);
			::memcpy(slot->tts_values, row_values_[offset_].data(),
					 sizeof(::Datum) * ncols_);
			::memcpy(slot->tts_isnull, row_isnull_[offset_].data(),
					 sizeof(bool) * ncols_);
			++offset_;

			offset = remap_ ? remap_->at(global_offset_) : global_offset_;
			global_offset_ += global_delta_;

			return true;
		}

	private:
		/*!
		 * Do the real convertion.
		 */
		void DoConvert(int count);

		/*!
		 * Convert some rows if necessary.
		 *
		 * @return false is no row to convert.
		 */
		bool Convert();

	private:
		static constexpr int batchsize = 256 / sizeof(::Datum);

		const std::vector<std::shared_ptr<ColumnStore>> *cstores_;
		const std::vector<int> *projcols_;
		const std::vector<int> *remap_;

		bool forward_;
		int ncols_;

		int global_offset_; // global offset
		int global_remain_; // global remain
		int global_delta_;	// global delta

		int offset_; // offset in current batch
		int count_;	 // count in current batch

		std::vector<::Datum> row_values_[batchsize];
		std::vector<char> row_isnull_[batchsize];
	};

} // namespace mars

#endif /* MARS_ROWSTORE_H */

/*-------------------------------------------------------------------------
 *
 * MarsRowGroupSerializer.cc
 *	  Parquet format data serializer
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/MarsRowGroupSerializer.cc
 *
 *-------------------------------------------------------------------------
 */

#include "MarsRowGroupSerializer.h"
void mars::MarsRowGroupSerializer::Close()
{
	if (!closed_)
	{
		closed_ = true;
		CheckRowsWritten();

		for (size_t i = 0; i < column_writers_.size(); i++)
		{
			if (column_writers_[i])
			{
				total_bytes_written_ += column_writers_[i]->Close();
				column_writers_[i].reset();
			}
		}

		column_writers_.clear();

		// Ensures all columns have been written
		metadata_->set_num_rows(num_rows_);
		metadata_->Finish(total_bytes_written_, row_group_ordinal_);
	}
}

parquet::ColumnWriter *
mars::MarsRowGroupSerializer::NextColumn()
{
	if (buffered_row_group_)
	{
		throw parquet::ParquetException(
			"NextColumn() is not supported when a RowGroup is written by size");
	}

	if (column_writers_[0])
	{
		CheckRowsWritten();
	}

	// Throws an error if more columns are being written
	auto col_meta = metadata_->NextColumnChunk();

	if (column_writers_[0])
	{
		total_bytes_written_ += column_writers_[0]->Close();
	}

	++next_column_index_;

	const auto &path = col_meta->descr()->path();
	std::unique_ptr<parquet::PageWriter> pager = parquet::PageWriter::Open(
		sink_, properties_->compression(path), properties_->compression_level(path),
		col_meta, row_group_ordinal_, static_cast<int16_t>(next_column_index_ - 1),
		properties_->memory_pool(), false,
		nullptr /* meta_encryptor */, nullptr /* data_encryptor */);
	column_writers_[0] = ColumnWriter::Make(col_meta, std::move(pager), properties_);
	return column_writers_[0].get();
}

parquet::ColumnWriter *
mars::MarsRowGroupSerializer::column(int i)
{
	if (!buffered_row_group_)
	{
		throw parquet::ParquetException(
			"column() is only supported when a BufferedRowGroup is being written");
	}

	if (i >= 0 && i < static_cast<int>(column_writers_.size()))
	{
		return column_writers_[i].get();
	}
	return nullptr;
}

int64_t
mars::MarsRowGroupSerializer::total_compressed_bytes() const
{
	int64_t total_compressed_bytes = 0;
	for (size_t i = 0; i < column_writers_.size(); i++)
	{
		if (column_writers_[i])
		{
			total_compressed_bytes += column_writers_[i]->total_compressed_bytes();
		}
	}
	return total_compressed_bytes;
}

int64_t
mars::MarsRowGroupSerializer::total_bytes_written() const
{
	int64_t total_bytes_written = 0;
	for (size_t i = 0; i < column_writers_.size(); i++)
	{
		if (column_writers_[i])
		{
			total_bytes_written += column_writers_[i]->total_bytes_written();
		}
	}
	return total_bytes_written;
}
void mars::MarsRowGroupSerializer::CheckRowsWritten() const
{
	// verify when only one column is written at a time
	if (!buffered_row_group_ && column_writers_.size() > 0 && column_writers_[0])
	{
		int64_t current_col_rows = column_writers_[0]->rows_written();
		if (num_rows_ == 0)
		{
			num_rows_ = current_col_rows;
		}
		else if (num_rows_ != current_col_rows)
		{
			std::stringstream ss;
			ss << "Column " << next_column_index_ << " had " << current_col_rows << " while previous column had " << num_rows_;
			throw parquet::ParquetException(ss.str());
		}
	}
	else if (buffered_row_group_ &&
			 column_writers_.size() > 0)
	{ // when buffered_row_group = true
		int64_t current_col_rows = column_writers_[0]->rows_written();
		for (int i = 1; i < static_cast<int>(column_writers_.size()); i++)
		{
			int64_t current_col_rows_i = column_writers_[i]->rows_written();
			if (current_col_rows != current_col_rows_i)
			{
				std::stringstream ss;
				ss << "Column " << i << " had " << current_col_rows_i << " while previous column had " << current_col_rows;
				throw parquet::ParquetException(ss.str());
			}
		}
		num_rows_ = current_col_rows;
	}
}
void mars::MarsRowGroupSerializer::InitColumns()
{
	for (int i = 0; i < num_columns(); i++)
	{
		auto col_meta = metadata_->NextColumnChunk();
		const auto &path = col_meta->descr()->path();
		std::unique_ptr<parquet::PageWriter> pager = parquet::PageWriter::Open(
			sink_, properties_->compression(path), properties_->compression_level(path),
			col_meta, static_cast<int16_t>(row_group_ordinal_),
			static_cast<int16_t>(next_column_index_++), properties_->memory_pool(),
			buffered_row_group_, nullptr, nullptr);
		column_writers_.push_back(
			parquet::ColumnWriter::Make(col_meta, std::move(pager), properties_));
	}
}

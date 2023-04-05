/*-------------------------------------------------------------------------
 *
 * BlockReader.cc
 *	  The MARS Block Reader
 *
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "BlockReader.h"
#include "Footer.h"
#include "coder/Decoder.h"

#include <arrow/filesystem/localfs.h>

#include <memory>
#include <utility>

namespace mars
{

	std::map<std::string, std::shared_ptr<arrow::io::RandomAccessFile>> BlockReader::filetab_;
	std::shared_ptr<arrow::fs::FileSystem> BlockReader::fs_;

	std::shared_ptr<arrow::io::RandomAccessFile>
	BlockReader::GetFile(const std::string &filepath)
	{
		auto &key = filepath;
		auto iter = filetab_.find(key);

		auto file = iter != filetab_.end() ? iter->second : nullptr;

		if (file == nullptr)
		{
			if (!fs_.get())
			{
				auto options = arrow::fs::LocalFileSystemOptions::Defaults();
				fs_ = std::make_shared<arrow::fs::LocalFileSystem>(options);
			}

			auto result = fs_->OpenInputFile(filepath);
			file = *result;
			filetab_.insert(std::make_pair(key, file));
		}

		return file;
	}

	BlockReader::BlockReader(mars::Scanner &scanner,
							 const mars::footer::Footer &footer,
							 const std::shared_ptr<parquet::FileMetaData> metadata)
		: footer_(footer), scanner_(scanner)
	{
		auto filepath = scanner.GetDataFilepath(footer_);
		auto file = GetFile(filepath);
		auto options = parquet::ReaderProperties(mars::MemoryPool::GetDefault());
		file_reader_ = parquet::ParquetFileReader::Open(file, options, metadata);

		// get the file format
		version_ = coder::version_from_string(metadata->created_by());

#ifdef USE_ASSERT_CHECKING
		metadata_ = file_reader_->metadata();
		Assert(metadata_->num_row_groups() == 1);

		// ensure the column stores are correctly initialized.
		auto &column_stores = scanner_.GetColumnStores();
		Assert(column_stores.size() == (size_t)metadata_->num_columns());
#endif // USE_ASSERT_CHECKING
	}

	std::shared_ptr<BlockReader>
	BlockReader::Get(mars::Scanner &scanner, const mars::footer::Footer &footer)
	{
		uint32 size;
		auto content = footer.GetFooterContent(&size);
		auto metadata = parquet::FileMetaData::Make(content, &size);

		return std::make_shared<BlockReader>(scanner, footer, std::move(metadata));
	}

	bool
	BlockReader::ReadNext(::TupleTableSlot *slot, bool overlapped)
	{
		if (scanner_.table_never_match_)
		{
			return false;
		}

		if (unlikely(row_group_reader_.get() == nullptr))
		{
			LoadData(scanner_.GetProjectedColumns());
			scanner_.OptimizeBlockScanKeys();
			if (!scanner_.FilterRows(footer_, nrows_, overlapped) ||
				!scanner_.CollectRowIds(nrows_))
				return false;
			scanner_.ReorderBlock(footer_);
			scanner_.BeginRow(nrows_);
		}

		if (!slot)
		{
			// batch mode, done.
			return true;
		}

		int offset;
		if (scanner_.NextRow(slot, offset))
		{
			ItemPointerSet(&slot->tts_tid,
						   footer_.impl_->batch_,
						   offset + FirstOffsetNumber);
			return true;
		}
		else
		{
			return false;
		}
	}

	void
	BlockReader::ReadAt(int row, int column, ::Datum &datum, bool &isnull)
	{
		// ignore the scanner_.table_never_match_, we allow reading a row even if
		// we believe it does not match the scankeys.

		auto &column_stores = scanner_.GetColumnStores();

		// open the row group if not yet.
		if (row_group_reader_.get() == nullptr)
		{
			row_group_reader_ = file_reader_->RowGroup(0);

			pgstat_count_buffer_read(scanner_.rel_);

			Assert(metadata_->num_columns() ==
				   row_group_reader_->metadata()->num_columns());
			Assert(metadata_->num_rows() ==
				   row_group_reader_->metadata()->num_rows());

			nrows_ = footer_.GetRowCount();

			// ReadAt() allows reading any column, even those unprojected ones.
			Assert((int)column_stores.size() == metadata_->num_columns());
			for (auto &column_store : column_stores)
			{
				// do not pre-reserve any space.
				column_store->Resize(0);
			}
		}

		Assert(row < nrows_);
		Assert(column < (int)column_stores.size());

		auto &column_store = column_stores.at(column);

		// load the row if not yet.
		if (row >= column_store->Count())
		{
			// reserve space for the column if not yet.
			if (column_store->Capacity() == 0)
			{
				column_store->Resize(nrows_);
			}

			// TODO: is it cheap enough to get a column reader every time?
			auto column_reader = row_group_reader_->Column(column);
			// only read to the target row.
			auto nrows = row - column_store->Count() + 1;
			// the reading can be faster when no null value is stored.
			bool has_nulls = footer_.GetNullCount(column) > 0;
			auto tupdesc = RelationGetDescr(scanner_.rel_);

			column_store->ReadPoints(version_, &tupdesc->attrs[column],
									 std::move(column_reader), nrows, has_nulls);
		}

		// read the row.
		Assert(row < column_store->Count());
		column_store->GetPoint(nullptr /* remap */, row, datum, isnull);
	}

	void
	BlockReader::LoadData(const std::vector<int> &projected_columns)
	{
		pgstat_count_buffer_read(scanner_.rel_);

		row_group_reader_ = file_reader_->RowGroup(0);

		Assert(metadata_->num_columns() ==
			   row_group_reader_->metadata()->num_columns());
		Assert(metadata_->num_rows() ==
			   row_group_reader_->metadata()->num_rows());

		// when there is a primary order key, we need to skip the nulls in that
		// column, the block is sorted as NULLS LAST, and we know the count of null
		// values in the column, so we could seek the first null directly.
		nrows_ = scanner_.GetEffectiveRowCount(footer_);

		const auto tupdesc = RelationGetDescr(scanner_.rel_);
		auto &column_stores = scanner_.GetColumnStores();

		// load all the data in the row group
		for (auto column : projected_columns)
		{
			// TODO: catch ParquetException
			auto column_reader = row_group_reader_->Column(column);
			auto &column_store = column_stores[column];
			bool has_nulls = footer_.GetNullCount(column) > 0;
			column_store->Resize(nrows_);
			column_store->ReadPoints(version_, &tupdesc->attrs[column],
									 std::move(column_reader),
									 nrows_, has_nulls);
		}
	}

} // namespace mars

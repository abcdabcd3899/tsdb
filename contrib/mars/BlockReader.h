/*-------------------------------------------------------------------------
 *
 * BlockReader.h
 *	  The MARS Block Reader
 *
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_BLOCKREADER_H
#define MARS_BLOCKREADER_H

#include "ColumnStore.h"
#include "Footer.h"
#include "Scanner.h"
#include "coder/Coder.h"

#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <parquet/arrow/reader.h>

#include <string>

namespace mars
{

	struct MetaTableKey
	{
		int64_t segno_;
		int64_t batch_;

		bool operator==(const MetaTableKey &rhs) const
		{
			return segno_ == rhs.segno_ &&
				   batch_ == rhs.batch_;
		}
	};

	class MetaHashFunction
	{
	public:
		size_t operator()(const MetaTableKey &k) const
		{
			return ((size_t)k.segno_ << 32) | (0xFFFFFFFF & k.batch_);
		}
	};

	struct MetaTable
	{
		::Oid relFileOid;
		std::unordered_map<MetaTableKey, std::shared_ptr<parquet::FileMetaData>, MetaHashFunction> table_;
	};

	/*!
	 * The MARS Block Reader.
	 *
	 * A MARS block is a parquet logical file with one single row group at present,
	 * and we usually call it a footer in the source code.
	 */
	class BlockReader
	{
	public:
		/*!
		 * The ctor.
		 *
		 * This is rather slow because we have to parse the metadata, use
		 * BlockReader::Get() whenever possible, which is the cached ctor.
		 *
		 * @param scanner An ref to the scanner, should be the same one used by the
		 *                #ParquetReader.
		 * @param footer The footer of the parquet logical file.
		 * @param metadata The parquet metadata.  When setting to nullptr, a new
		 *                 one will be created internally.
		 *
		 * @see Get()
		 */
		BlockReader(mars::Scanner &scanner,
					const mars::footer::Footer &footer,
					const std::shared_ptr<parquet::FileMetaData> metadata = nullptr);
		virtual ~BlockReader() = default;

		/*!
		 * Get the cached BlockReader.
		 *
		 * Footers are all immutable, so the same footer always produce the same
		 * BlockReader, so instead of constructing a new BlockReader every time, we
		 * cache all the constructed ones internally, and reuse them later.
		 *
		 * @param scanner An ref to the scanner, should be the same one used by the
		 *                #ParquetReader.
		 * @param footer The footer of the parquet logical file.
		 */
		static std::shared_ptr<BlockReader> Get(mars::Scanner &scanner,
												const mars::footer::Footer &footer);

		/*!
		 * Read next tuple.
		 *
		 * Only the tuples that match the #scanner_ scankeys will be returned.
		 *
		 * @param slot The slot to store the result, nullptr is allowed, the caller
		 *             can get the results from the scanner column stores directly,
		 *             this is called the batch mode.
		 * @param overlapped True if the block overlaps with others.
		 *
		 * @return true if one tuple is returned.
		 * @return false if no tuple is returned, which means end of the block.
		 *
		 * @note The batch mode should only be used without scankeys, otherwise
		 *       incomplete result set might be returned when there are arraykeys.
		 */
		bool ReadNext(::TupleTableSlot *slot, bool overlapped);

		/*!
		 * Read the specific row.
		 *
		 * Seek to the specific row id, and read the row.
		 *
		 * The difference with ReadNext():
		 * - ReadNext() preloads all the rows, and hornors the scan order and scan
		 *   dir specified by the scanner;
		 * - ReadAt() only loads the specified row (but due to the limitation of
		 *   parquet, all the rows before it are also loaded), and ignores all the
		 *   scan information, such as the projection columns, the store order, the
		 *   scan order, the scan dir, and the scankeys, etc.;
		 *
		 * The loaded rows are cached, so re-reading can be very fast.
		 *
		 * @param row The row id to read at, counting from 0.
		 * @param column The column to read at, counting from 0.
		 * @param[out] datum,isnull The point.
		 *
		 * @note It is not recommended to mix the use of ReadNext() and ReadAt(),
		 *       they have different underlying logic by the nature, and may cause
		 *       undefined behavior to the other.
		 */
		void ReadAt(int row, int column, ::Datum &datum, bool &isnull);

		bool ReadBatch(bool overlapped)
		{
			return ReadNext(nullptr /* slot */, overlapped);
		}

	protected:
		/*!
		 * Get the cached parquet file.
		 *
		 * @param filepath The path of data file, such as "base/16384/21000".
		 *
		 * @return The parquet file.
		 */
		static std::shared_ptr<arrow::io::RandomAccessFile> GetFile(const std::string &filepath);

		void LoadData(const std::vector<int> &projected_columns);

	private:
		const mars::footer::Footer &footer_; //!< Ref to the footer
		mars::Scanner &scanner_;			 //!< Ref to the scanner

		static std::map<std::string, std::shared_ptr<arrow::io::RandomAccessFile>> filetab_;
		//!< The file cache
		static std::shared_ptr<arrow::fs::FileSystem> fs_;
		//!< The parquet file system

#ifdef USE_ASSERT_CHECKING
		std::shared_ptr<parquet::FileMetaData> metadata_;
		//!< The metadata
#endif // USE_ASSERT_CHECKING
		std::unique_ptr<parquet::ParquetFileReader> file_reader_;
		//!< The parquet file reader
		std::shared_ptr<parquet::RowGroupReader> row_group_reader_;
		//!< The parquet row group reader
		coder::Version version_; //<! The coder version

		int nrows_; //!< The # of rows
	};

} // namespace mars

#endif /* MARS_BLOCKREADER_H */

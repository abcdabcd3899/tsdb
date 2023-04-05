/*-------------------------------------------------------------------------
 *
 * MarsRowGroupSerializer.h
 *	  Parquet format data serializer
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/MarsRowGroupSerializer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_MARSROWGROUPSERIALIZER_H
#define MATRIXDB_MARSROWGROUPSERIALIZER_H
#include <parquet/api/writer.h>
namespace mars
{

	using arrow::KeyValueMetadata;
	using parquet::ColumnWriter;
	using parquet::FileMetaDataBuilder;
	using parquet::ParquetFileWriter;
	using parquet::RowGroupMetaDataBuilder;
	using parquet::RowGroupWriter;
	using parquet::WriterProperties;
	using parquet::schema::GroupNode;

	using ArrowOutputStream = ::arrow::io::OutputStream;

	class MarsRowGroupSerializer : public RowGroupWriter::Contents
	{
	public:
		MarsRowGroupSerializer(std::shared_ptr<ArrowOutputStream> sink,
							   RowGroupMetaDataBuilder *metadata, int16_t row_group_ordinal,
							   const WriterProperties *properties, bool buffered_row_group = false)
			: sink_(std::move(sink)),
			  metadata_(metadata),
			  properties_(properties),
			  total_bytes_written_(0),
			  closed_(false),
			  row_group_ordinal_(row_group_ordinal),
			  next_column_index_(0),
			  num_rows_(0),
			  buffered_row_group_(buffered_row_group)
		{
			if (buffered_row_group)
			{
				InitColumns();
			}
			else
			{
				column_writers_.push_back(nullptr);
			}
		}

		int num_columns() const override { return metadata_->num_columns(); }

		int64_t num_rows() const override
		{
			CheckRowsWritten();
			// CheckRowsWritten ensures num_rows_ is set correctly
			return num_rows_;
		}

		ColumnWriter *NextColumn() override;

		ColumnWriter *column(int i) override;

		int current_column() const override { return metadata_->current_column(); }

		int64_t total_compressed_bytes() const override;

		int64_t total_bytes_written() const override;

		void Close() override;

	private:
		std::shared_ptr<ArrowOutputStream> sink_;
		mutable RowGroupMetaDataBuilder *metadata_;
		const WriterProperties *properties_;
		int64_t total_bytes_written_;
		bool closed_;
		int16_t row_group_ordinal_;
		int next_column_index_;
		mutable int64_t num_rows_;
		bool buffered_row_group_;

		void CheckRowsWritten() const;

		void InitColumns();

		std::vector<std::shared_ptr<ColumnWriter>> column_writers_;
	};

} // namespace mars

#endif // MATRIXDB_MARSROWGROUPSERIALIZER_H

#ifndef MATRIXDB_MARSFILESERIALIZER_HPP
#define MATRIXDB_MARSFILESERIALIZER_HPP

#include "wrapper.hpp"

#include <parquet/file_writer.h>
#include <parquet/column_writer.h>
#include "MarsFile.h"

namespace mars {

using parquet::ParquetFileWriter;
using parquet::schema::GroupNode;
using arrow::KeyValueMetadata;
using parquet::WriterProperties;
using parquet::FileMetaDataBuilder;
using parquet::RowGroupWriter;
using parquet::RowGroupMetaDataBuilder;
using parquet::ColumnWriter;


class MarsFileSerializer : public ParquetFileWriter::Contents {
public:
	static std::unique_ptr<ParquetFileWriter::Contents> Open(
		std::shared_ptr<MarsFile> sink, std::shared_ptr<GroupNode> schema,
		std::shared_ptr<WriterProperties> properties,
		std::shared_ptr<const KeyValueMetadata> key_value_metadata) {
		std::unique_ptr<ParquetFileWriter::Contents> result(
			new MarsFileSerializer(std::move(sink), std::move(schema), std::move(properties),
			                       std::move(key_value_metadata)));

		return result;
	}
	MarsFileSerializer() = delete;
	void Close() override;

	int num_columns() const override { return schema_.num_columns(); }

	int num_row_groups() const override { return num_row_groups_; }

	int64_t num_rows() const override { return num_rows_; }

	const std::shared_ptr<WriterProperties>& properties() const override {
		return properties_;
	}

	RowGroupWriter* AppendRowGroup(bool buffered_row_group);

	RowGroupWriter* AppendRowGroup() override { return AppendRowGroup(false); }

	RowGroupWriter* AppendBufferedRowGroup() override { return AppendRowGroup(true); }

	~MarsFileSerializer() override {
		try {
			Close();
		} catch (...) {
		}
	}

private:
	MarsFileSerializer(std::shared_ptr<MarsFile> sink,
	                   std::shared_ptr<GroupNode> schema,
	                   std::shared_ptr<WriterProperties> properties,
	                   std::shared_ptr<const KeyValueMetadata> key_value_metadata);

	void StartFile();

private:
	std::shared_ptr<MarsFile> sink_;
	bool is_open_;
	const std::shared_ptr<WriterProperties> properties_;
	int num_row_groups_;
	int64_t num_rows_;
	std::unique_ptr<FileMetaDataBuilder> metadata_;
	// Only one of the row group writers is active at a time
	std::unique_ptr<RowGroupWriter> row_group_writer_;
};

} // namespace mars

#endif //MATRIXDB_MARSFILESERIALIZER_HPP

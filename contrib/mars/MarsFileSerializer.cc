#include "MarsFileSerializer.h"
#include "MarsRowGroupSerializer.h"

namespace mars {

MarsFileSerializer::MarsFileSerializer(std::shared_ptr<MarsFile> sink,
                                       std::shared_ptr<GroupNode> schema,
                                       std::shared_ptr<WriterProperties> properties,
                                       std::shared_ptr<const KeyValueMetadata> key_value_metadata)
	: ParquetFileWriter::Contents(std::move(schema), std::move(key_value_metadata)),
	  sink_(std::move(sink)), is_open_(true), properties_(std::move(properties)),
	  num_row_groups_(0), num_rows_(0),
	  metadata_(FileMetaDataBuilder::Make(&schema_, properties_, key_value_metadata_)) {
	StartFile();
}

void
MarsFileSerializer::StartFile()
{
	auto file_encryption_properties = properties_->file_encryption_properties();
	if (file_encryption_properties == nullptr) {
		// Unencrypted parquet files always start with PAR1
		PARQUET_THROW_NOT_OK(sink_->Write(parquet::kParquetMagic, 4));
	} else {
		throw parquet::ParquetException("Mars Encryption is not supported yet.");
	}
}

void
MarsFileSerializer::Close()
{
	if (is_open_) {
		// If any functions here raise an exception, we set is_open_ to be false
		// so that this does not get called again (possibly causing segfault)
		is_open_ = false;
		if (row_group_writer_) {
			num_rows_ += row_group_writer_->num_rows();
			row_group_writer_->Close();
		}
		row_group_writer_.reset();

		// Write magic bytes and metadata
		file_metadata_ = metadata_->Finish();

#ifdef SINGLE_FILE_DEBUG
		WriteFileMetaData(*file_metadata_, sink_.get());
#endif
	}
}

RowGroupWriter *
MarsFileSerializer::AppendRowGroup(bool buffered_row_group)
{
	if (row_group_writer_) {
		row_group_writer_->Close();
	}
	num_row_groups_++;
	auto rg_metadata = metadata_->AppendRowGroup();
	std::unique_ptr<RowGroupWriter::Contents> contents(new MarsRowGroupSerializer(
		sink_, rg_metadata, static_cast<int16_t>(num_row_groups_ - 1), properties_.get(),
		buffered_row_group));
	row_group_writer_.reset(new RowGroupWriter(std::move(contents)));
	return row_group_writer_.get();
}

} // namespace mars

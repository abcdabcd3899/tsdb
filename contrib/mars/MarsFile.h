/*-------------------------------------------------------------------------
 *
 * MarsFile.h
 *	  IO interface
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/MarsFile.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_MARSFILE_H
#define MATRIXDB_MARSFILE_H

#include "wrapper.hpp"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/io/type_fwd.h>
#include <arrow/status.h>
#include <arrow/util/io_util.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/properties.h>

namespace mars
{

	using OutputStream = ::arrow::io::OutputStream;
	using Seekable = ::arrow::io::Seekable;
	using Status = ::arrow::Status;
	using FileMode = ::arrow::io::FileMode;

	class OSFile
	{
	public:
		OSFile();

		~OSFile() = default;

		// Note: only one of the Open* methods below may be called on a given instance

		Status OpenWritable(const std::string &path, bool truncate, bool append,
							bool write_only);

		// This is different from OpenWritable(string, ...) in that it doesn't
		// truncate nor mandate a seekable file
		Status OpenWritable(int fd);

		Status OpenReadable(const std::string &path);

		Status OpenReadable(int fd);

		Status CheckClosed() const;

		Status Close();

		arrow::Result<int64_t> Read(int64_t nbytes, void *out);

		arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void *out);

		Status Seek(int64_t pos);

		arrow::Result<int64_t> Tell() const;

		Status Write(const void *data, int64_t length);

		inline int fd() const { return fd_; }

		inline bool is_open() const { return is_open_; }

		inline int64_t size() const { return size_; }

		inline FileMode::type mode() const { return mode_; }

		// std::mutex& lock() { return lock_; }

	protected:
		Status SetFileName(const std::string &file_name);

		Status SetFileName(int fd);

		Status CheckPositioned();

		::arrow::internal::PlatformFilename file_name_;

		// In MatrixDB, it seem never meet the requirement.
		/* std::mutex lock_; */

		// File descriptor
		int fd_;

		FileMode::type mode_;

		bool is_open_;
		int64_t size_;
		// Whether ReadAt made the file position non-deterministic.
		std::atomic<bool> need_seeking_;
	};

	class MarsFile : virtual public OutputStream, public Seekable
	{

	public:
		~MarsFile() override;

		static arrow::Result<std::shared_ptr<MarsFile>> Open(const ::RelFileNode &rd_node,
															 bool append,
															 bool needWAL);

		// static arrow::Result<std::shared_ptr<MarsFile>> Open(int fd);

		// OutputStream interface
		Status Close() override;
		bool closed() const override;
		arrow::Result<int64_t> Tell() const override { return os_file_.Tell(); };

		// Write bytes to the stream. Thread-unsafe
		Status Write(const void *data, int64_t nbytes) override;
		using Writable::Write;

		Status Seek(int64_t position) override;
		using Seekable::Seek;

		int file_descriptor() const { return os_file_.fd(); };

	private:
		MarsFile(const ::RelFileNode &rd_node, bool needWAL)
			: os_file_(), rd_node_(rd_node), need_wal_(needWAL){};

		OSFile os_file_;
		::RelFileNode rd_node_;
		const bool need_wal_;
	};

} // namespace mars

#endif // MATRIXDB_MARSFILE_H

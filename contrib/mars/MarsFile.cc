/*-------------------------------------------------------------------------
 *
 * MarsFile.cc
 *	  IO interface
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/MarsFile.h
 *
 *-------------------------------------------------------------------------
 */
#include "wrapper.hpp"
#include "MarsFile.h"
#include "FileManager.h"

namespace mars
{

	using ::arrow::Result;
	using ::arrow::Status;
	using ::arrow::io::FileMode;

	OSFile::OSFile() : fd_(-1), is_open_(false), size_(-1), need_seeking_(false) {}

	// Note: only one of the Open* methods below may be called on a given instance
	Status OSFile::OpenWritable(const std::string &path, bool truncate, bool append,
								bool write_only)
	{
		RETURN_NOT_OK(SetFileName(path));

		ARROW_ASSIGN_OR_RAISE(fd_, ::arrow::internal::FileOpenWritable(file_name_, write_only,
																	   truncate, append));
		is_open_ = true;
		mode_ = write_only ? FileMode::WRITE : FileMode::READWRITE;

		if (!truncate)
		{
			ARROW_ASSIGN_OR_RAISE(size_, ::arrow::internal::FileGetSize(fd_));
		}
		else
		{
			size_ = 0;
		}
		return Status::OK();
	}

	// This is different from OpenWritable(string, ...) in that it doesn't
	// truncate nor mandate a seekable file
	Status OSFile::OpenWritable(int fd)
	{
		auto result = ::arrow::internal::FileGetSize(fd);
		if (result.ok())
		{
			size_ = *result;
		}
		else
		{
			// Non-seekable file
			size_ = -1;
		}
		RETURN_NOT_OK(SetFileName(fd));
		is_open_ = true;
		mode_ = FileMode::WRITE;
		fd_ = fd;
		return Status::OK();
	}

	Status OSFile::OpenReadable(const std::string &path)
	{
		RETURN_NOT_OK(SetFileName(path));

		ARROW_ASSIGN_OR_RAISE(fd_, ::arrow::internal::FileOpenReadable(file_name_));
		ARROW_ASSIGN_OR_RAISE(size_, ::arrow::internal::FileGetSize(fd_));

		is_open_ = true;
		mode_ = FileMode::READ;
		return Status::OK();
	}

	Status OSFile::OpenReadable(int fd)
	{
		ARROW_ASSIGN_OR_RAISE(size_, ::arrow::internal::FileGetSize(fd));
		RETURN_NOT_OK(SetFileName(fd));
		is_open_ = true;
		mode_ = FileMode::READ;
		fd_ = fd;
		return Status::OK();
	}

	Status OSFile::CheckClosed() const
	{
		if (!is_open_)
		{
			return Status::Invalid("Invalid operation on closed file");
		}
		return Status::OK();
	}

	Status OSFile::Close()
	{
		if (is_open_)
		{
			// Even if closing fails, the fd will likely be closed (perhaps it's
			// already closed).
			is_open_ = false;
			int fd = fd_;
			fd_ = -1;
			RETURN_NOT_OK(::arrow::internal::FileClose(fd));
		}
		return Status::OK();
	}

	Result<int64_t> OSFile::Read(int64_t nbytes, void *out)
	{
		RETURN_NOT_OK(CheckClosed());
		RETURN_NOT_OK(CheckPositioned());
		return ::arrow::internal::FileRead(fd_, reinterpret_cast<uint8_t *>(out), nbytes);
	}

	Result<int64_t> OSFile::ReadAt(int64_t position, int64_t nbytes, void *out)
	{
		RETURN_NOT_OK(CheckClosed());

		if (position < 0 || nbytes < 0)
		{
			return Status::Invalid("Invalid IO range (offset = ", position, ", size = ", nbytes, ")");
		}
		// ReadAt() leaves the file position undefined, so require that we seek
		// before calling Read() or Write().
		need_seeking_.store(true);
		return ::arrow::internal::FileReadAt(fd_, reinterpret_cast<uint8_t *>(out), position,
											 nbytes);
	}

	Status OSFile::Seek(int64_t pos)
	{
		RETURN_NOT_OK(CheckClosed());
		if (pos < 0)
		{
			return Status::Invalid("Invalid position");
		}
		Status st = ::arrow::internal::FileSeek(fd_, pos);
		if (st.ok())
		{
			need_seeking_.store(false);
		}
		return st;
	}

	Result<int64_t> OSFile::Tell() const
	{
		RETURN_NOT_OK(CheckClosed());
		return ::arrow::internal::FileTell(fd_);
	}

	Status OSFile::Write(const void *data, int64_t length)
	{
		RETURN_NOT_OK(CheckClosed());

		// std::lock_guard<std::mutex> guard(lock_);
		RETURN_NOT_OK(CheckPositioned());
		if (length < 0)
		{
			return Status::IOError("Length must be non-negative");
		}
		return ::arrow::internal::FileWrite(fd_, reinterpret_cast<const uint8_t *>(data),
											length);
	}

	Status OSFile::SetFileName(const std::string &file_name)
	{
		return ::arrow::internal::PlatformFilename::FromString(file_name).Value(&file_name_);
	}

	Status OSFile::SetFileName(int fd)
	{
		std::stringstream ss;
		ss << "<fd " << fd << ">";
		return SetFileName(ss.str());
	}

	Status OSFile::CheckPositioned()
	{
		if (need_seeking_.load())
		{
			return Status::Invalid(
				"Need seeking after ReadAt() before "
				"calling implicitly-positioned operation");
		}
		return Status::OK();
	}
	// OSFile implementation finish

	MarsFile::~MarsFile()
	{

		arrow::Status st = this->Close();
		if (!st.ok())
		{
			// elog(ERROR, "File destroying failed in MarsFile dctor.");
		}
	}

	Status
	MarsFile::Close()
	{
		return os_file_.Close();
	}

	bool
	MarsFile::closed() const
	{
		return !os_file_.is_open();
	}

	arrow::Result<std::shared_ptr<MarsFile>>
	MarsFile::Open(const ::RelFileNode &rd_node, bool append, bool needWAL)
	{
		const bool truncate = !append;

		const std::string path = FetchIdlePath(rd_node);
		auto stream = std::shared_ptr<MarsFile>(new MarsFile(rd_node, needWAL));
		RETURN_NOT_OK(stream->os_file_.OpenWritable(path, truncate, append, true /* write_only */));

		return stream;
	}

#if 0
arrow::Result<std::shared_ptr<MarsFile>>
MarsFile::Open(int fd)
{
	auto stream = std::shared_ptr<MarsFile>(new MarsFile());
	RETURN_NOT_OK(stream->Open(fd));
	return stream;
}
#endif

	Status
	MarsFile::Write(const void *data, int64_t nbytes)
	{

		if (need_wal_)
			::xlog_mars_insert(rd_node_,
							   0,
							   os_file_.Tell().ValueOrDie(),
							   const_cast<void *>(data),
							   nbytes);

		return os_file_.Write(data, nbytes);
	}

	Status
	MarsFile::Seek(int64_t position)
	{
		return os_file_.Seek(position);
	}

} // namespace mars

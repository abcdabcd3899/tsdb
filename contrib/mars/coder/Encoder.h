/*-------------------------------------------------------------------------
 *
 * contrib/mars/coder/Encoder.cc
 *	  The MARS column encoder.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_ENCODER_H
#define MARS_ENCODER_H

#include "Compare.h"

#include "coder/Coder.h"
#include "guc.h"

#include <vector>

namespace mars
{

	class Encoder;

	struct EncodeResult
	{
		bool should_free;
		uint32 destlen;
		uint8 *dest;
	};

	class EncoderImpl
	{
	protected:
		EncoderImpl() = default;

	public:
		/*!
		 * Encode values.
		 *
		 * @return false if the encoding has no effect, the encoded size >=
		 * original size.
		 */
		virtual bool Encode(const ::Form_pg_attribute attr,
							const coder::MemoryHeader &header,
							const ::StringInfo src,
							::StringInfo dst) const = 0;
		virtual uint32 EstimateEncodeBufLen(const ::Form_pg_attribute attr,
											const coder::MemoryHeader &header,
											const ::StringInfo data) const = 0;
	};

	class Gorilla8EncoderImpl : public EncoderImpl
	{
	public:
		virtual ~Gorilla8EncoderImpl() = default;

		Gorilla8EncoderImpl() = default;

		bool Encode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					::StringInfo dst) const override;
		uint32 EstimateEncodeBufLen(const ::Form_pg_attribute attr,
									const coder::MemoryHeader &header,
									const ::StringInfo data) const override;
	};

	class DeltaDeltaEncoderImpl : public EncoderImpl
	{
	public:
		virtual ~DeltaDeltaEncoderImpl() = default;

		DeltaDeltaEncoderImpl() = default;

		bool Encode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					::StringInfo dst) const override;
		uint32 EstimateEncodeBufLen(const ::Form_pg_attribute attr,
									const coder::MemoryHeader &header,
									const ::StringInfo data) const override;
	};

	class Lz4EncoderImpl : public EncoderImpl
	{
	public:
		virtual ~Lz4EncoderImpl() = default;

		Lz4EncoderImpl() = default;

		bool Encode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					::StringInfo dst) const override;
		uint32 EstimateEncodeBufLen(const ::Form_pg_attribute attr,
									const coder::MemoryHeader &header,
									const ::StringInfo data) const override;
	};

	class ZstdEncoderImpl : public EncoderImpl
	{
	public:
		virtual ~ZstdEncoderImpl() = default;

		ZstdEncoderImpl() = default;

		bool Encode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					::StringInfo dst) const override;
		uint32 EstimateEncodeBufLen(const ::Form_pg_attribute attr,
									const coder::MemoryHeader &header,
									const ::StringInfo data) const override;
	};

	class NoneEncoderImpl : public EncoderImpl
	{
	public:
		virtual ~NoneEncoderImpl() = default;
		NoneEncoderImpl() = default;

		bool Encode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					::StringInfo dst) const override;
		uint32 EstimateEncodeBufLen(const ::Form_pg_attribute attr,
									const coder::MemoryHeader &header,
									const ::StringInfo data) const override;
	};

	class Encoder
	{
	public:
		Encoder(const ::Form_pg_attribute attr, coder::Version version,
				bool enable_column_encode, const std::vector<coder::Encoding> &compressions)
			: attr_(attr), version_(version), encodings_{}
		{
			if (mars_enable_default_encoder)
			{
				encodings_.reserve(1 + compressions.size());
				encodings_.push_back(DecideEncoding(attr, enable_column_encode));
			}
			else
			{
				encodings_.reserve(compressions.size());
			}

			for (coder::Encoding encoding : compressions)
			{
				if (encoding != coder::Encoding::NONE)
				{
					encodings_.push_back(encoding);
				}
			}

			Reset();
		}

		~Encoder()
		{
			Clear();
		}

		/*!
		 * Just store values, not compress here.
		 */
		void Append(::Datum value, bool isnull);

		void Append(const ::TupleTableSlot *slot)
		{
			int column = attr_->attnum - 1;
			Datum value = slot->tts_values[column];
			bool isnull = slot->tts_isnull[column];

			Append(value, isnull);
		}

		void AppendAll(const std::vector<::Datum> &values,
					   const std::vector<bool> &isnull,
					   const std::vector<int> &remap);

		bytea *Encode();
		bool Encode(const std::vector<coder::Encoding> &encodings,
					const coder::MemoryHeader &header,
					const ::StringInfo src, ::StringInfo dst);

		void Reset()
		{
			Clear();
		}

	public:
		const std::vector<::Datum> &Values() const
		{
			return values_;
		}

		const std::vector<bool> &Nulls() const
		{
			return isnull_;
		}

		std::size_t TotalCount() const
		{
			return isnull_.size();
		}

		std::size_t NullCount() const
		{
			return isnull_.size() - values_.size();
		}

	protected:
		void Clear()
		{
			if (!attr_->attbyval)
			{
				// for non-byval values we hold their copies instead of the
				// original pointers, see Append().  must free them in-time.
				for (auto datum : values_)
				{
					::pfree(DatumGetPointer(datum));
				}
			}

			values_.clear();
			isnull_.clear();
		}

		/*!
		 * This function checking if catalog.pg_compression has target compress
		 * function.
		 * After change Gorilla as Mars internal function, this function has been
		 * retired.
		 * However, we still keep it just in case reuse pg_compression functions in
		 * the future.
		 *
		 * @param name, compression function's name
		 * @return if found, return ture.
		 */
		static bool HasCompressionMethod(const char *name);
		static const std::shared_ptr<EncoderImpl> GetEncoderImpl(coder::Encoding encoding);
		static coder::Encoding DecideEncoding(const ::Form_pg_attribute attr,
											  bool enable_column_encode) noexcept;

	protected:
		const ::Form_pg_attribute attr_;
		const coder::Version version_;
		std::vector<coder::Encoding> encodings_;

		std::vector<::Datum> values_;
		std::vector<bool> isnull_;

	protected:
		static std::vector<std::shared_ptr<EncoderImpl>> impls_;
	};

} // namespace mars

#endif // MARS_ENCODER_H

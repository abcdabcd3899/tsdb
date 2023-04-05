/*-------------------------------------------------------------------------
 *
 * coder.h
 *	  MARS data coder.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_CODER_H
#define MARS_CODER_H

#include "wrapper.hpp"

#include <algorithm>
#include <string>
#include <vector>

namespace mars
{
	namespace coder
	{

		enum class Version : uint8
		{
			V0_1,
			V1_0,
		};

		static inline const char *pg_attribute_unused()
			version_to_string(Version version)
		{
			static constexpr const char *table[] = {
				"MARS 0.1",
				"MARS 1.0",
			};

			return table[(uint8)version];
		}

		static inline Version pg_attribute_unused()
			version_from_string(const std::string &str)
		{
			if (str == "MatrixDB")
			{
				return coder::Version::V0_1;
			}
			else if (str == "MARS 0.1")
			{
				return coder::Version::V0_1;
			}
			else if (str == "MARS 1.0")
			{
				return coder::Version::V1_0;
			}
			else
			{
				elog(ERROR, "unsupported mars file format: \"%s\"", str.c_str());
			}
		}

		enum class Encoding : uint8
		{
			NONE = 0, //<! not encoded
			GORILLA8,
			DELTADELTA,
			LZ4,
			ZSTD,
		};

		static constexpr Version DEFAULT_VERSION = Version::V1_0;
		static constexpr size_t N_ENCODINGS = 5;

		extern Version default_version;

		namespace bitmap
		{

			static inline uint32 pg_attribute_unused()
				get_bitmap_nbytes(uint32 nbits)
			{
				return (nbits + 7) / 8;
			}

			static uint8 *pg_attribute_unused()
				bitmap_serialize(uint8 *bitmap, const std::vector<bool> &bitlist)
			{
				uint32 nbits = bitlist.size();
				auto iter = bitlist.begin();
				auto endpos = nbits / 8;

				for (uint32 pos = 0; pos < endpos; ++pos)
				{
					bitmap[pos] = ((((uint8)iter[0]) << 0) |
								   (((uint8)iter[1]) << 1) |
								   (((uint8)iter[2]) << 2) |
								   (((uint8)iter[3]) << 3) |
								   (((uint8)iter[4]) << 4) |
								   (((uint8)iter[5]) << 5) |
								   (((uint8)iter[6]) << 6) |
								   (((uint8)iter[7]) << 7));
					iter += 8;
				}

				if (nbits % 8 > 0)
				{
					bitmap[endpos] = 0;

					for (uint32 shift = 0; shift < nbits % 8; ++shift)
					{
						bitmap[endpos] |= *iter << shift;
						++iter;
					}
				}

				return bitmap + get_bitmap_nbytes(nbits);
			}

			static void pg_attribute_unused()
				bitmap_deserialize(const uint8 *bitmap, uint32 nbits,
								   std::vector<bool> &bitlist, uint32 offset)
			{
				Assert(bitlist.size() >= offset + nbits);

				auto iter = bitlist.begin() + offset;
				auto endpos = nbits / 8;

				for (uint32 pos = 0; pos < endpos; ++pos)
				{
					if (bitmap[pos] == 0x00 || bitmap[pos] == 0xff)
					{
						bool bit = bitmap[pos] != 0x00;

						std::fill_n(iter, 8, bit);
						iter += 8;
					}
					else
					{
						auto byte = bitmap[pos];

						for (int i = 0; i < 8; ++i)
						{
							*iter = byte & 0x01;
							++iter;
							byte >>= 1;
						}
					}
				}

				for (uint32 shift = 0; shift < nbits % 8; ++shift)
				{
					*iter = (bitmap[endpos] >> shift) & 0x01;
					++iter;
				}
			}

		} // namespace bitmap

		/*!
		 * The physical layout of a mars column chunk, v0.1 .
		 *
		 * Columns are not encoded by mars itself in v0.1, so it is a pesudo struct.
		 */
		struct pg_attribute_packed() PhysicalHeader_V0_1{};

		/*!
		 * The physical layout of a mars column chunk, v1.0 .
		 */
		struct pg_attribute_packed() PhysicalHeader_V1_0
		{
			int32 vl_len_; //<! the bytea header, always in 4B format

			uint8 has_nulls : 1;			//<! has null values or not
			uint8 has_values : 1;			//<! has non-null values or not
			uint8 encoded : 1;				//<! encoded or not
			uint8 uncompressed_datalen : 1; //<! whether the original data length is saved
			uint8 padding : 4;				//<! padding bits

#ifdef MARS_VARLEN
			varatt total_count; //<! total count, including nulls

			if (encoded)
			{
				varatt nencodings; //<! # of encodings, >= 1 when present
				varatt encoding;   //<! encoding method, can be NONE
			}

			/*----------------------------------------------------------------------
			 * end of "header" section
			 *----------------------------------------------------------------------
			 */

			/*!
			 * The "nulls" section, it is skipped in all-null or no-null case.
			 */
			if (has_nulls && has_values)
			{
				varatt null_count; //<! null count

				uint8 bitmap[(total_count + 7) / 8]; //<! null bitmap
			}

			/*----------------------------------------------------------------------
			 * end of "nulls" section
			 *----------------------------------------------------------------------
			 */

			/*!
			 * The "values" section, it is skipped in all-null case.
			 *
			 * The format and length are encoding specific.
			 */
			if (has_values)
			{
				if (uncompressed_datalen)
				{
					varatt datalen;
				}

				char encoded_values[];
			}

			/*----------------------------------------------------------------------
			 * end of "values" section
			 *----------------------------------------------------------------------
			 */
#endif // MARS_VARLEN
		};

		static inline bool
		Encoded(const std::vector<coder::Encoding> &encodings)
		{
			return encodings.size() != 0 &&
				   (encodings.size() > 1 || encodings[0] != Encoding::NONE);
		}

		struct MemoryHeader
		{
		public:
			Version version_;

		public:
			/*
			 * Below are for v1.0
			 */

			uint32 total_count_;
			uint32 null_count_;

			bool has_nulls_;
			bool has_values_;
			bool encoded_;
			bool uncompressed_datalen_;

			std::vector<coder::Encoding> encodings_;

		public:
			MemoryHeader(Version version, uint32 total_count, uint32 null_count,
						 const std::vector<coder::Encoding> &encodings, bool uncompressed_datalen)
				: version_(version), total_count_(total_count), null_count_(null_count), has_nulls_(null_count > 0), has_values_(total_count > null_count), encoded_(Encoded(encodings)), uncompressed_datalen_(uncompressed_datalen), encodings_(encodings)
			{
			}

			MemoryHeader(Version version)
				: MemoryHeader(version, 0, 0, {}, false)
			{
			}

		public:
			static void
			WriterVarUInt32(::StringInfo bin, uint32 len)
			{
				if (len <= VARATT_SHORT_MAX)
				{
					SET_VARSIZE_SHORT(bin->data + bin->len, len);
					bin->len += VARHDRSZ_SHORT;
				}
				else
				{
					SET_VARSIZE(bin->data + bin->len, len);
					bin->len += VARHDRSZ;
				}
			}

			void
			WriteUncompressedSize(::StringInfo bin, int length) const
			{
				if (HasUncompressedDataLength())
				{
					WriterVarUInt32(bin, length);
				}
			}

			void SerializeHeader(::StringInfo bin) const
			{
				switch (version_)
				{
				case Version::V0_1:
					// v0.1 is serialized by the caller directly
					pg_unreachable();

				case Version::V1_0:
				{
					PhysicalHeader_V1_0 *phys;

					::enlargeStringInfo(bin, GetHeaderSize());

					phys = (PhysicalHeader_V1_0 *)(bin->data + bin->len);
					bin->len += sizeof(*phys);

					// the vl_len_ is unknown yet
					phys->has_nulls = has_nulls_;
					phys->has_values = has_values_;
					phys->encoded = encoded_;
					phys->uncompressed_datalen = uncompressed_datalen_;
					// it is important to pad with 0 bits for forward
					// compatibility.
					phys->padding = 0;

					// the total_count
					WriterVarUInt32(bin, total_count_);

					if (IsEncoded())
					{
						WriterVarUInt32(bin, encodings_.size()); // nencodings
						for (Encoding c : encodings_)
						{
							WriterVarUInt32(bin, (uint32)c);
						}
					}
				}
				break;
				}
			}

			void SerializeNulls(::StringInfo bin,
								const std::vector<bool> &isnull) const
			{
				switch (version_)
				{
				case Version::V0_1:
					// v0.1 is serialized by the caller directly
					pg_unreachable();

				case Version::V1_0:
					if (HasBitmap())
					{
						auto bitmaplen = bitmap::get_bitmap_nbytes(total_count_);

						::enlargeStringInfo(bin, VARHDRSZ + bitmaplen);

						// serialize null count
						WriterVarUInt32(bin, null_count_);

						auto bitmap = (uint8 *)(bin->data + bin->len);
						bin->len += bitmaplen;

						// serialize null bitmap
						bitmap::bitmap_serialize(bitmap, isnull);
					}
					break;
				}
			}

		public:
			static const char *
			ReadData(::StringInfo bin, int datalen)
			{
				if (datalen > bin->maxlen - bin->cursor)
				{
					elog(ERROR, "fail to decode data");
				}

				const auto data = bin->data + bin->cursor;
				bin->cursor += datalen;

				return data;
			}

			static uint8
			ReadUInt8(::StringInfo bin)
			{
				auto data = (const uint8 *)ReadData(bin, sizeof(uint8));
				return *data;
			}

			static uint32
			ReadUInt32(::StringInfo bin)
			{
				auto data = (const uint32 *)ReadData(bin, sizeof(uint32));
				return *data;
			}

			static uint32
			ReadVarUInt32(::StringInfo bin)
			{
				if (VARATT_IS_SHORT(bin->data + bin->cursor))
				{
					uint32 val = VARSIZE_SHORT(bin->data + bin->cursor);
					bin->cursor += VARHDRSZ_SHORT;
					return val;
				}
				else
				{
					uint32 val = VARSIZE(bin->data + bin->cursor);
					bin->cursor += VARHDRSZ;
					return val;
				}
			}

			/*!
			 * @return 0 means no uncompressed size is stored
			 */
			uint32
			ReadUncompressedSize(const ::StringInfo bin) const
			{
				if (HasUncompressedDataLength())
				{
					return ReadVarUInt32(bin);
				}

				return 0;
			}

			uint32
			GetFinalUncompressedSize() const
			{
				return (total_count_ - null_count_) * sizeof(Datum);
			}

			void DeserializeHeader(::StringInfo bin)
			{
				switch (version_)
				{
				case Version::V0_1:
					// v0.1 is deserialized by the caller directly
					pg_unreachable();

				case Version::V1_0:
				{
					const PhysicalHeader_V1_0 *phys;

					phys = (const PhysicalHeader_V1_0 *)
						ReadData(bin, sizeof(*phys));

					// do not care about vl_len_
					has_nulls_ = phys->has_nulls;
					has_values_ = phys->has_values;
					encoded_ = phys->encoded;
					uncompressed_datalen_ = phys->uncompressed_datalen;

					total_count_ = ReadVarUInt32(bin);

					// do not use IsEncoded(), which checks encodings_, which is
					// not set yet.
					if (encoded_)
					{
						auto nencodings = ReadVarUInt32(bin);
						Assert(nencodings > 0);
						(void)nencodings;

						encodings_.reserve(nencodings);
						for (size_t i = 0; i < nencodings; i++)
						{
							encodings_.push_back((Encoding)ReadVarUInt32(bin));
						}
					}
					else
					{
						encodings_ = {Encoding::NONE};
					}
				}
				break;
				}
			}

			void DeserializeNulls(::StringInfo bin,
								  std::vector<bool> &isnull, uint32 offset)
			{
				switch (version_)
				{
				case Version::V0_1:
					// v0.1 is deserialized by the caller directly
					pg_unreachable();

				case Version::V1_0:
				{
					// decide how many rows to decode
					uint32 capacity = isnull.size() - offset;
					auto nvalues = std::min(total_count_, capacity);

					if (IsAllNull())
					{
						null_count_ = total_count_;
						std::fill_n(isnull.begin() + offset, nvalues, true);
					}
					else if (IsNoNull())
					{
						null_count_ = 0;
						// we assume that the isnull is already filled with
						// false.
#ifdef USE_ASSERT_CHECKING
						auto begin = isnull.begin() + offset;
						auto end = begin + nvalues;
						Assert(std::find(begin, end, true) == end);
#endif /* USE_ASSERT_CHECKING */
					}
					else
					{
						// read the null count
						null_count_ = ReadVarUInt32(bin);

						// load null bitmap
						auto bitmaplen = bitmap::get_bitmap_nbytes(total_count_);
						auto bitmap = (const uint8 *)ReadData(bin, bitmaplen);
						bitmap::bitmap_deserialize(bitmap, nvalues,
												   isnull, offset);
					}
				}
				break;
				}
			}

		public:
			uint32 GetHeaderSize() const
			{
				switch (version_)
				{
				case Version::V0_1:
					return sizeof(PhysicalHeader_V0_1);
				case Version::V1_0:
					return sizeof(PhysicalHeader_V1_0) // fixed fields
						   + VARHDRSZ				   // total count, suppose in 4B format
						   + VARHDRSZ				   // nencodings, suppose in 4B format
						   + VARHDRSZ				   // encoding, suppose in 4B format
						;
				default:
					pg_unreachable();
				}
			}

			/*!
			 * Whether there is a "values" section in the physical layout.
			 *
			 * A "values" section is stored when there are non-null values.
			 */
			bool inline HasValues() const
			{
				return has_values_;
			}

			/*!
			 * Whether there is a "null bitmap" section in the physical layout.
			 *
			 * A "null bitmap" section is stored when there are both nulls and non-null
			 * values.
			 */
			bool inline HasBitmap() const
			{
				return has_nulls_ && has_values_;
			}

			bool inline IsAllNull() const
			{
				return !has_values_;
			}

			bool inline IsNoNull() const
			{
				return !has_nulls_;
			}

			bool inline IsEncoded() const
			{
				return encoded_ && Encoded(encodings_);
			}

			bool inline HasUncompressedDataLength() const
			{
				return uncompressed_datalen_;
			}
		};

	} // namespace coder
} // namespace mars

#endif // MARS_CODER_H

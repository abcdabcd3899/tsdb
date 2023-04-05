/*-------------------------------------------------------------------------
 *
 * contrib/mars/coder/Decoder.cc
 *	  The MARS column decoder.
 *
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "type_traits.hpp"

#include "coder/Decoder.h"
#include "coder/Coder.h"

#include <memory>

namespace mars
{

	std::vector<std::shared_ptr<DecoderImpl>> Decoder::impls_(coder::N_ENCODINGS,
															  nullptr);

	const std::shared_ptr<DecoderImpl>
	Decoder::GetDecoderImpl(coder::Encoding encoding)
	{
		uint8 i = static_cast<uint8>(encoding);

		Assert(i < (uint8)impls_.size());
		if (!impls_[i])
		{
			switch (encoding)
			{
			case coder::Encoding::NONE:
				impls_[i] = std::make_shared<NoneDecoderImpl>();
				break;
			case coder::Encoding::GORILLA8:
				impls_[i] = std::make_shared<Gorilla8DecoderImpl>();
				break;
			case coder::Encoding::DELTADELTA:
				impls_[i] = std::make_shared<DeltaDeltaDecoderImpl>();
				break;
			case coder::Encoding::LZ4:
				impls_[i] = std::make_shared<Lz4DecoderImpl>();
				break;
			case coder::Encoding::ZSTD:
				impls_[i] = std::make_shared<ZstdDecoderImpl>();
				break;
			}

			if (!impls_[i])
			{
				// TODO: use ereport
				elog(ERROR, "fail to create a decoder for encoding %u",
					 (uint8)encoding);
			}
		}

		return impls_[i];
	}

	uint32
	Decoder::Decode(coder::Version version,
					const ::Form_pg_attribute attr,
					const bytea *src,
					std::vector<::Datum> &values,
					std::vector<bool> &isnull,
					uint32 offset)
	{
		::StringInfoData srcData;

#if 0
	// make a StringInfo wrapper for the source data
	srcData.data = (char *) src;
	srcData.maxlen = VARSIZE(src);
	srcData.len = srcData.maxlen;
	srcData.cursor = 0;
#else
		// we could make a StringInfo wrapper for the source data instead of
		// duplicating it, however in gorilla & delta decoders the bit reader used
		// by them might read one more byte over the bound, it will crash unless
		// the allocator allocated more bytes than required (for example for
		// alignment or pooling), we cannot rely on that, so we duplicate the
		// source data and put some redundant room for the over reading.
		::initStringInfoOfSize(&srcData, VARSIZE(src) + 32);
		::appendBinaryStringInfo(&srcData, (const char *)src, VARSIZE(src));
#endif

		// v0.1 is handled by the caller
		Assert(version != coder::Version::V0_1);

		coder::MemoryHeader header(version);

		// load physical header
		header.DeserializeHeader(&srcData);

		// load null bitmap
		header.DeserializeNulls(&srcData, isnull, offset);

		// decide how many rows to decode
		uint32 capacity = values.size() - offset;
		auto nvalues = std::min(header.total_count_, capacity);

		if (!header.HasValues())
		{
			return nvalues;
		}

		// decode values
		StringInfo dst = makeStringInfo();
		Decode(attr, header, &srcData, dst);

		// relocate values to each row
		if (attr->attbyval)
		{
			auto datums = (const ::Datum *)(dst->data + dst->cursor);

			if (header.IsNoNull())
			{
				// fastpath: no null, so there is no hole in the values
				std::copy_n(datums, nvalues, values.begin() + offset);
			}
			else
			{
				for (uint32 j = 0; j < nvalues; j++)
				{
					if (!isnull[offset + j])
					{
						values[offset + j] = *datums++;
					}
				}
			}
		}
		else
		{
			// non-byval values, such as texts, are stored flatten, we only report
			// pointers to them.
			uint8 *data = (uint8 *)(dst->data + dst->cursor);

			for (uint32 j = 0; j < nvalues; j++)
			{
				if (!isnull[offset + j])
				{
					int datalen = byptr_value_get_length(attr, PointerGetDatum(data));
					values[offset + j] = PointerGetDatum(data);
					data += datalen;
				}
			}
		}

		// TODO: dst might be leaked, its lifecycle can be longer than the decoder,
		// we need a way to release it.

		return nvalues;
	}

	void
	Decoder::Decode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src, ::StringInfo dst)
	{
		::StringInfo mid1;
		::StringInfo mid2;
		for (int i = header.encodings_.size() - 1; i >= 0; i--)
		{
			auto impl = GetDecoderImpl(header.encodings_[i]);
			if (i == int(header.encodings_.size() - 1))
			{
				mid1 = src;
				mid2 = makeStringInfo();
			}

			if (i == 0)
			{
				mid2 = dst;
			}

			impl->Decode(attr, header, mid1, mid2);

			if (i != 0)
			{
				if (i == int(header.encodings_.size() - 1))
				{
					mid1 = makeStringInfo();
				}

				std::swap(mid1, mid2);
				resetStringInfo(mid2);
			}
		}
	}

	void
	Gorilla8DecoderImpl::Decode(const ::Form_pg_attribute attr,
								const coder::MemoryHeader &header,
								const ::StringInfo src,
								const ::StringInfo dst) const
	{
		uint32 dstlen = header.ReadUncompressedSize(src);
		if (dstlen == 0)
		{
			dstlen = header.GetFinalUncompressedSize();
		}

		dstlen += 32; // put some more buffer, just for safe
		enlargeStringInfo(dst, dstlen);

		dst->len += gorilla_decompress_data(src->data + src->cursor,
											src->len - src->cursor,
											dst->data + dst->len);
	}

	void
	DeltaDeltaDecoderImpl::Decode(const ::Form_pg_attribute attr,
								  const coder::MemoryHeader &header,
								  const ::StringInfo src,
								  const ::StringInfo dst) const
	{
		uint32 dstlen = header.ReadUncompressedSize(src);
		if (dstlen == 0)
		{
			dstlen = header.GetFinalUncompressedSize();
		}

		dstlen += 32; // put some more buffer, just for safe
		enlargeStringInfo(dst, dstlen);

		dst->len += delta_delta_decompress_data(src->data + src->cursor,
												src->len - src->cursor,
												dst->data + dst->len);
	}

	void
	Lz4DecoderImpl::Decode(const ::Form_pg_attribute attr,
						   const coder::MemoryHeader &header,
						   const ::StringInfo src,
						   const ::StringInfo dst) const
	{
		uint32 dstlen = header.ReadUncompressedSize(src);
		// for lz4 compression, uncompressed size must be stored
		if (dstlen == 0)
		{
			pg_unreachable();
		}

		dstlen += 32; // put some more buffer, just for safe
		enlargeStringInfo(dst, dstlen);

		dst->len += lz4_decompress_data(src->data + src->cursor,
										src->len - src->cursor,
										dst->data + dst->len,
										dstlen);
	}

	void
	ZstdDecoderImpl::Decode(const ::Form_pg_attribute attr,
							const coder::MemoryHeader &header,
							const ::StringInfo src,
							const ::StringInfo dst) const
	{
		uint32 dstlen = header.ReadUncompressedSize(src);
		// for zstd compression, uncompressed size must be stored
		if (dstlen == 0)
		{
			pg_unreachable();
		}
		enlargeStringInfo(dst, dstlen);

		dst->len += zstd_decompress_data(src->data + src->cursor,
										 src->len - src->cursor,
										 dst->data + dst->len,
										 dstlen);
	}

	void
	NoneDecoderImpl::Decode(const ::Form_pg_attribute attr,
							const coder::MemoryHeader &header,
							const ::StringInfo src,
							const ::StringInfo dst) const
	{
		uint32 dstlen = header.ReadUncompressedSize(src);
		if (dstlen == 0)
		{
			dstlen = header.GetFinalUncompressedSize();
		}
		uint32 srclen = src->len - src->cursor;
		enlargeStringInfo(dst, std::max(srclen, dstlen));

		memcpy(dst->data + dst->len, src->data + src->cursor, srclen);
		dst->len += srclen;
	}

} // namespace mars

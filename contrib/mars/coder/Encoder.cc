/*-------------------------------------------------------------------------
 *
 * contrib/mars/coder/Encoder.h
 *	  The MARS column encoder.
 *
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "coder/Encoder.h"

namespace mars
{

	std::vector<std::shared_ptr<EncoderImpl>> Encoder::impls_(coder::N_ENCODINGS,
															  nullptr);

	/*
	 * Copied from pg_compression.c
	 */
	static ::NameData
	comptype_to_name(const char *comptype)
	{
		char *dct; /* down cased comptype */
		size_t len;
		::NameData compname;

		if (::strlen(comptype) >= NAMEDATALEN)
			elog(ERROR, "compression name \"%s\" exceeds maximum name length "
						"of %d bytes",
				 comptype, NAMEDATALEN - 1);

		len = ::strlen(comptype);
		dct = ::asc_tolower(comptype, len);
		len = ::strlen(dct);

		::memcpy(&(NameStr(compname)), dct, len);
		NameStr(compname)[len] = '\0';
		::pfree(dct);

		return compname;
	}

	bool
	Encoder::HasCompressionMethod(const char *name)
	{
		/* derived from GetCompressionImplementation() */
		::HeapTuple tuple;
		::NameData compname;
		::Relation comprel;
		::ScanKeyData scankey;
		::SysScanDesc scan;
		bool result;

		compname = comptype_to_name(name);

		comprel = table_open(CompressionRelationId, AccessShareLock);

		/* SELECT * FROM pg_compression WHERE compname = :1 */
		ScanKeyInit(&scankey,
					Anum_pg_compression_compname,
					BTEqualStrategyNumber, F_NAMEEQ,
					NameGetDatum(&compname));
		scan = systable_beginscan(comprel, CompressionCompnameIndexId, true,
								  NULL, 1, &scankey);
		tuple = systable_getnext(scan);

		result = HeapTupleIsValid(tuple);

		systable_endscan(scan);
		table_close(comprel, AccessShareLock);

		return result;
	}

	const std::shared_ptr<EncoderImpl>
	Encoder::GetEncoderImpl(coder::Encoding encoding)
	{
		uint8 i = static_cast<uint8>(encoding);

		Assert(i < (uint8)impls_.size());
		if (!impls_[i])
		{
			switch (encoding)
			{
			case coder::Encoding::NONE:
				impls_[i] = std::make_shared<NoneEncoderImpl>();
				break;
			case coder::Encoding::GORILLA8:
				impls_[i] = std::make_shared<Gorilla8EncoderImpl>();
				break;
			case coder::Encoding::DELTADELTA:
				impls_[i] = std::make_shared<DeltaDeltaEncoderImpl>();
				break;
			case coder::Encoding::LZ4:
				impls_[i] = std::make_shared<Lz4EncoderImpl>();
				break;
			case coder::Encoding::ZSTD:
				impls_[i] = std::make_shared<ZstdEncoderImpl>();
				break;
			}

			if (!impls_[i])
			{
				// TODO: use ereport
				elog(ERROR, "fail to create an encoder for encoding %u",
					 (uint8)encoding);
			}
		}

		return impls_[i];
	}

	coder::Encoding
	Encoder::DecideEncoding(const ::Form_pg_attribute attr,
							bool enable_column_encode) noexcept
	{
		if (!enable_column_encode)
		{
			return coder::Encoding::NONE;
		}

		switch (attr->atttypid)
		{
		case FLOAT4OID:
			// at the moment float4 are encoded via the gorilla8 encoder, too,
			// it is done by padding 32 0-bits.
			//
			// fallthrough.
		case FLOAT8OID:
			return coder::Encoding::GORILLA8;

		case INT4OID:
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			// TODO: if this column is {mars group} or {mars order}.
			return coder::Encoding::DELTADELTA;

		default:
			return coder::Encoding::NONE;
		}
	}

	void
	Encoder::Append(::Datum value, bool isnull)
	{
		// if slot is null, do not put in compress cache.
		if (isnull)
		{
			isnull_.push_back(true);
		}
		else
		{
			if (!attr_->attbyval)
			{
				if (attr_->attlen > 0)
				{
					value = ::datumCopy(value, attr_->attbyval, attr_->attlen);
				}
				else
				{
					value = PointerGetDatum(PG_DETOAST_DATUM_COPY(value));
				}
			}

			isnull_.push_back(false);
			values_.push_back(value);
		}
	}

	void
	Encoder::AppendAll(const std::vector<::Datum> &values,
					   const std::vector<bool> &isnull,
					   const std::vector<int> &remap)
	{
		// TODO: encode the spaced values directly
		Reset();

		auto nrows = remap.size();
		values_.reserve(nrows);
		isnull_.reserve(nrows);

		for (auto row : remap)
		{
			Append(values[row], isnull[row]);
		}
	}

	bytea *
	Encoder::Encode()
	{
		StringInfoData dst;
		auto encodings = encodings_;
		auto total_count = TotalCount();
		auto null_count = NullCount();

	retry:
		bool uncompressed_datalen = Encoded(encodings);
		coder::MemoryHeader header(version_, total_count, null_count, encodings, uncompressed_datalen);

		::StringInfoData src;

		// make a StringInfo wrapper for the source data
		src.data = (char *)Values().data();
		src.maxlen = Values().size() * sizeof(::Datum);
		;
		src.len = src.maxlen;
		src.cursor = 0;

		// estimate the total length of the encoded data
		{
			auto headerlen = header.GetHeaderSize();
			auto bitmaplen = coder::bitmap::get_bitmap_nbytes(total_count);
			uint32 dstlen = 0;

			dstlen += headerlen;	  // header
			dstlen += sizeof(uint32); // null_count
			dstlen += bitmaplen;	  // null bitmap length
			dstlen += sizeof(uint32); // original data length

			initStringInfoOfSize(&dst, dstlen);
		}

		// write physical header
		header.SerializeHeader(&dst);

		// write null bitmap
		header.SerializeNulls(&dst, isnull_);

		// write encoded values, it is skipped when all-null
		if (header.HasValues())
		{
			bool effective = Encode(encodings, header, &src, &dst);

			if (!effective)
			{
				// the encoded size is greater than the original size, fallback to
				// none encoding.
				// TODO: prevent re-serializing the null bitmap.
				Assert(Encoded(encodings));
				encodings = {coder::Encoding::NONE};
				pfree(dst.data);
				goto retry;
			}
		}

		// set the actual vl_len_
		SET_VARSIZE(dst.data, dst.len);

		return (bytea *)dst.data;
	}

	bool
	Encoder::Encode(const std::vector<coder::Encoding> &encodings,
					const coder::MemoryHeader &header, const ::StringInfo src,
					::StringInfo dst)
	{
		int len = dst->len;
		bool effective = true;
		::StringInfo mid1;
		::StringInfo mid2;
		for (size_t i = 0; i < encodings.size(); i++)
		{
			auto impl = GetEncoderImpl(encodings[i]);
			if (i == 0)
			{
				mid1 = src;
				mid2 = makeStringInfo();
			}

			if (i == encodings.size() - 1)
			{
				mid2 = dst;
			}

			effective &= impl->Encode(attr_, header, mid1, mid2);

			if (i != encodings.size() - 1)
			{
				if (i == 0)
				{
					mid1 = makeStringInfo();
				}

				std::swap(mid1, mid2);
				resetStringInfo(mid2);
			}
		}

		if (effective)
		{
			return effective;
		}

		return (dst->len - len) < (src->len - src->cursor);
	}

	bool
	NoneEncoderImpl::Encode(const ::Form_pg_attribute attr,
							const coder::MemoryHeader &header,
							const ::StringInfo src,
							::StringInfo dst) const
	{
		// must keep in sync with EstimateEncodeBufLen()
		size_t buflen = EstimateEncodeBufLen(attr, header, src);

		header.WriteUncompressedSize(dst, buflen);

		if (attr->attbyval)
		{
			appendBinaryStringInfo(dst, src->data + src->cursor, buflen);
		}
		else
		{
			enlargeStringInfo(dst, buflen);

			size_t count = header.total_count_ - header.null_count_;
			const Datum *datum = (const Datum *)(src->data + src->cursor);
			for (std::size_t i = 0; i < count; ++i)
			{
				auto dataptr = (const char *)DatumGetPointer(datum[i]);
				int datalen = byptr_value_get_length(attr, datum[i]);

				// store the flatten text
				appendBinaryStringInfo(dst, dataptr, datalen);
			}
		}

		// the none-encoding is always considered effective
		return true;
	}

	uint32
	NoneEncoderImpl::EstimateEncodeBufLen(const ::Form_pg_attribute attr,
										  const coder::MemoryHeader &header,
										  const ::StringInfo data) const
	{
		size_t buflen = 0;

		if (attr->attbyval)
		{
			buflen = data->len;
		}
		else
		{
			// make an accurate estimation by accumulating all the texts, this is
			// slow, however it helps to prevent the string growing during
			// Encode(), so it is a win.
			size_t count = header.total_count_ - header.null_count_;
			const Datum *datum = (const Datum *)(data->data + data->cursor);
			for (std::size_t i = 0; i < count; ++i)
			{
				buflen += byptr_value_get_length(attr, datum[i]);
			}
		}

		return buflen;
	}

	bool
	Gorilla8EncoderImpl::Encode(const ::Form_pg_attribute attr,
								const coder::MemoryHeader &header,
								const ::StringInfo src,
								const ::StringInfo dst) const
	{
		header.WriteUncompressedSize(dst, src->len - src->cursor);

		uint32 buflen = EstimateEncodeBufLen(attr, header, src);
		enlargeStringInfo(dst, buflen);

		int32 dstlen = gorilla_compress_data(src->data + src->cursor,
											 src->len - src->cursor,
											 dst->data + dst->len,
											 dst->maxlen - dst->len);

		if (dstlen < 0)
		{
			elog(ERROR, "fail to encode data using gorilla");
		}

		dst->len += dstlen;

		return dstlen < (int32)src->len - src->cursor;
	}

	uint32
	Gorilla8EncoderImpl::EstimateEncodeBufLen(const ::Form_pg_attribute attr,
											  const coder::MemoryHeader &header,
											  const ::StringInfo data) const
	{
		return sizeof(uint64) + 2 * (data->len - data->cursor);
	}

	bool
	DeltaDeltaEncoderImpl::Encode(const ::Form_pg_attribute attr,
								  const coder::MemoryHeader &header,
								  const ::StringInfo src,
								  const ::StringInfo dst) const
	{
		header.WriteUncompressedSize(dst, src->len - src->cursor);

		uint32 buflen = EstimateEncodeBufLen(attr, header, src);
		enlargeStringInfo(dst, buflen);

		int32 dstlen = delta_delta_compress_data(src->data + src->cursor,
												 src->len - src->cursor,
												 dst->data + dst->len,
												 dst->maxlen - dst->len);

		if (dstlen < 0)
		{
			elog(ERROR, "fail to encode data using delta-delta");
		}

		dst->len += dstlen;

		return dstlen < (int32)src->len - src->cursor;
	}

	uint32
	DeltaDeltaEncoderImpl::EstimateEncodeBufLen(const ::Form_pg_attribute attr,
												const coder::MemoryHeader &header,
												const ::StringInfo data) const
	{
		return sizeof(uint64) + 2 * (data->len - data->cursor);
	}

	bool
	Lz4EncoderImpl::Encode(const ::Form_pg_attribute attr,
						   const coder::MemoryHeader &header,
						   const ::StringInfo src,
						   const ::StringInfo dst) const
	{
		header.WriteUncompressedSize(dst, src->len - src->cursor);

		uint32 buflen = EstimateEncodeBufLen(attr, header, src);
		enlargeStringInfo(dst, buflen);

		int32 dstlen = lz4_compress_data(src->data + src->cursor,
										 src->len - src->cursor,
										 dst->data + dst->len,
										 dst->maxlen - dst->len);

		if (dstlen < 0)
		{
			elog(ERROR, "fail to compress data using lz4");
		}

		dst->len += dstlen;

		return dstlen != 0 && dstlen < (int32)src->len - src->cursor;
	}

	uint32
	Lz4EncoderImpl::EstimateEncodeBufLen(const ::Form_pg_attribute attr,
										 const coder::MemoryHeader &header,
										 const ::StringInfo data) const
	{
		return lz4_compress_bound(data->len - data->cursor);
	}

	bool
	ZstdEncoderImpl::Encode(const ::Form_pg_attribute attr,
							const coder::MemoryHeader &header,
							const ::StringInfo src,
							const ::StringInfo dst) const
	{
		header.WriteUncompressedSize(dst, src->len - src->cursor);

		uint32 buflen = EstimateEncodeBufLen(attr, header, src);
		enlargeStringInfo(dst, buflen);

		int32 dstlen = zstd_compress_data(src->data + src->cursor,
										  src->len - src->cursor,
										  dst->data + dst->len,
										  dst->maxlen - dst->len);

		if (dstlen < 0)
		{
			elog(ERROR, "fail to compress data using zstd");
		}

		dst->len += dstlen;

		return dstlen < (int32)src->len - src->cursor;
	}

	uint32
	ZstdEncoderImpl::EstimateEncodeBufLen(const ::Form_pg_attribute attr,
										  const coder::MemoryHeader &header,
										  const ::StringInfo data) const
	{
		return zstd_compress_bound(data->len - data->cursor);
	}

} // namespace mars

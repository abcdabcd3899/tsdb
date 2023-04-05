/*-------------------------------------------------------------------------
 *
 * contrib/mars/coder/Decoder.h
 *	  The MARS column decoder.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_DECODER_H
#define MARS_DECODER_H

#include "coder/Coder.h"

#include <memory>
#include <vector>

namespace mars
{

	class DecoderImpl
	{
	protected:
		DecoderImpl() = default;

	public:
		virtual void Decode(const ::Form_pg_attribute attr,
							const coder::MemoryHeader &header,
							const ::StringInfo src,
							const ::StringInfo dst) const = 0;
	};

	class Gorilla8DecoderImpl : public DecoderImpl
	{
	public:
		virtual ~Gorilla8DecoderImpl() = default;

		Gorilla8DecoderImpl() = default;

		void Decode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					const ::StringInfo dst) const override;
	};

	class DeltaDeltaDecoderImpl : public DecoderImpl
	{
	public:
		virtual ~DeltaDeltaDecoderImpl() = default;

		DeltaDeltaDecoderImpl() = default;

		void Decode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					const ::StringInfo dst) const override;
	};

	class Lz4DecoderImpl : public DecoderImpl
	{
	public:
		virtual ~Lz4DecoderImpl() = default;

		Lz4DecoderImpl() = default;

		void Decode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					const ::StringInfo dst) const override;
	};

	class ZstdDecoderImpl : public DecoderImpl
	{
	public:
		virtual ~ZstdDecoderImpl() = default;

		ZstdDecoderImpl() = default;

		void Decode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					const ::StringInfo dst) const override;
	};

	class NoneDecoderImpl : public DecoderImpl
	{
	public:
		virtual ~NoneDecoderImpl() = default;
		NoneDecoderImpl() = default;

		void Decode(const ::Form_pg_attribute attr,
					const coder::MemoryHeader &header,
					const ::StringInfo src,
					const ::StringInfo dst) const override;
	};

	class Decoder
	{
	public:
		Decoder() = delete;

		static uint32 Decode(coder::Version version,
							 const ::Form_pg_attribute attr,
							 const bytea *src,
							 std::vector<::Datum> &values,
							 std::vector<bool> &isnull,
							 uint32 offset);
		static void Decode(const ::Form_pg_attribute attr,
						   const coder::MemoryHeader &header,
						   const ::StringInfo src, ::StringInfo dst);

	protected:
		static const std::shared_ptr<DecoderImpl>
		GetDecoderImpl(coder::Encoding encoding);

	protected:
		static std::vector<std::shared_ptr<DecoderImpl>> impls_;
	};

} // namespace mars

#endif // MARS_DECODER_H

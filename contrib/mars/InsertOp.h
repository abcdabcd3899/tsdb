/*-------------------------------------------------------------------------
 *
 * InsertOp.h
 *	  MARS Column data writer
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/InsertOp.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_INSERTOP_H
#define MATRIXDB_INSERTOP_H

#include "type_traits.hpp"

#include "PreAgg.h"
#include "coder/Encoder.h"

#include <parquet/types.h>
#include <parquet/column_writer.h>
#include <arrow/buffer_builder.h>

#include <memory>
#include <string>

namespace mars
{

	struct InsertOp
	{
		/*!
		 * Make an InsertOp for the given type oid.
		 *
		 * @param version The format version.
		 * @param attr The column attribute.
		 *
		 * @return the InsertOp.
		 * @return nullptr if the type is unsupported.
		 */
		static std::shared_ptr<InsertOp> Make(coder::Version version,
											  const ::Form_pg_attribute attr,
											  bool is_group_col,
											  coder::Encoding compression);

		/*!
		 * Forbid the default ctor, the pre_agg_bits must be filled.
		 */
		InsertOp() = delete;

		InsertOp(coder::Version version, const ::Form_pg_attribute attr)
			: version_(version), attr_(attr)
		{
		}

		virtual void RegisterWriter(parquet::ColumnWriter *rhs) = 0;
		virtual void Flush(const std::vector<::Datum> &values,
						   const std::vector<bool> &isnull,
						   const std::vector<int> &remap) = 0;
		virtual void Reset(void) = 0;

	protected:
		const coder::Version version_;
		const ::Form_pg_attribute attr_;
	};

	template <parquet::Type::type T>
	struct TypedInsertOp : public InsertOp
	{
		using c_type = typename parquet::type_traits<T>::value_type;
		using sum_type = typename type_traits<T>::sum_type;

		TypedInsertOp(coder::Version version, const ::Form_pg_attribute attr)
			: InsertOp(version, attr), columnWriter_(nullptr)
		{
		}

		void RegisterWriter(parquet::ColumnWriter *rhs) override
		{
			columnWriter_ = static_cast<parquet::TypedColumnWriter<parquet::PhysicalType<T>> *>(rhs);
		};

		void Reset(void) override
		{
		}

		virtual ~TypedInsertOp() noexcept
		{
			try
			{
				if (columnWriter_ != nullptr)
				{
					columnWriter_->Close();
				}
			}
			catch (...)
			{
				elog(WARNING,
					 "columnWriter Close() meet exception in TypedInsertOp dtor.");
			}

			columnWriter_ = nullptr;
		};

		void Flush(const std::vector<::Datum> &_values,
				   const std::vector<bool> &isnull,
				   const std::vector<int> &remap) override
		{
			Assert(columnWriter_);

			// TODO: use the WriteBatchSpaced()
			const auto values = _values.data();
			for (auto row : remap)
			{
				if (!isnull[row])
				{
					auto &c_value = *reinterpret_cast<const c_type *>(&values[row]);

					int16_t def_level = 1;
					columnWriter_->WriteBatch(1, &def_level, nullptr, &c_value);
				}
				else
				{
					int16_t def_level = 0;
					columnWriter_->WriteBatch(1, &def_level, nullptr, nullptr);
				}
			}

			columnWriter_->Close();
			columnWriter_ = nullptr;
		}

	private:
		/*!
		 * Typed column writer.
		 *
		 * The object is unowned by us, do not delete it.
		 */
		parquet::TypedColumnWriter<parquet::PhysicalType<T>> *columnWriter_;
	};

	template <>
	struct TypedInsertOp<parquet::Type::BYTE_ARRAY> : public InsertOp
	{

		using c_type = typename parquet::type_traits<parquet::Type::BYTE_ARRAY>::value_type;

		TypedInsertOp(coder::Version version, const ::Form_pg_attribute attr,
					  bool enable_column_encode, coder::Encoding compression)
			: InsertOp(version, attr), columnWriter_(nullptr), encoder_(nullptr)
		{
			if (version_ != coder::Version::V0_1)
			{
				std::vector<coder::Encoding> compressions = {compression};
				encoder_ = std::make_shared<Encoder>(attr,
													 version_,
													 enable_column_encode,
													 compressions);
			}
		}

		TypedInsertOp(coder::Version version, const ::Form_pg_attribute attr)
			: TypedInsertOp(version, attr, false /* enable_column_encode */, coder::Encoding::NONE)
		{
		}

		void RegisterWriter(parquet::ColumnWriter *rhs) override
		{
			columnWriter_ = static_cast<parquet::TypedColumnWriter<parquet::PhysicalType<parquet::Type::BYTE_ARRAY>> *>(rhs);

			if (version_ != coder::Version::V0_1)
			{
				encoder_->Reset();
			}
		};

		void Reset(void) override
		{
			if (version_ != coder::Version::V0_1)
			{
				encoder_->Reset();
			}
		}

		virtual ~TypedInsertOp() noexcept
		{
			try
			{
				if (columnWriter_ != nullptr)
				{
					columnWriter_->Close();
				}
			}
			catch (...)
			{
				elog(WARNING,
					 "columnWriter Close() meet exception in TypedInsertOp dtor.");
			}

			columnWriter_ = nullptr;
		};

		void Flush(const std::vector<::Datum> &_values,
				   const std::vector<bool> &isnull,
				   const std::vector<int> &remap) override
		{
			Assert(columnWriter_);
			if (version_ == coder::Version::V0_1)
			{
				// TODO: use the WriteBatchSpaced()
				const auto values = _values.data();

				for (auto row : remap)
				{
					if (!isnull[row])
					{
						c_type c_value;
						c_value.ptr = reinterpret_cast<const uint8_t *>(values[row]);
						c_value.len = byptr_value_get_length(attr_, values[row]);

						int16_t def_level = 1;
						columnWriter_->WriteBatch(1, &def_level, nullptr, &c_value);
					}
					else
					{
						int16_t def_level = 0;
						columnWriter_->WriteBatch(1, &def_level, nullptr, nullptr);
					}
				}
			}
			else
			{
				encoder_->AppendAll(_values, isnull, remap);

				// encode the data
				auto bytes = encoder_->Encode();

				c_type c_value;
				c_value.ptr = reinterpret_cast<const uint8_t *>(bytes);
				c_value.len = VARSIZE_ANY(bytes);

				int16_t def_level = 1;
				columnWriter_->WriteBatch(1, &def_level, nullptr, &c_value);

				pfree(bytes);
			}

			columnWriter_->Close();
			columnWriter_ = nullptr;
		}

	private:
		/*!
		 * Typed column writer.
		 *
		 * The object is unowned by us, do not delete it.
		 */
		parquet::TypedColumnWriter<parquet::PhysicalType<parquet::Type::BYTE_ARRAY>> *columnWriter_;

		std::shared_ptr<Encoder> encoder_;
	};

} // namespace mars

#endif // MATRIXDB_INSERTOP_H

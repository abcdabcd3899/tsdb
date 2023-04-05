/*-------------------------------------------------------------------------
 *
 * type_traits.hpp
 *	  Type traits between PostgreSQL and Parquet.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/type_traits.hpp
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_TYPE_TRAITS_HPP
#define MATRIXDB_TYPE_TRAITS_HPP

#include "wrapper.hpp"

#include <arrow/memory_pool.h>
#include <arrow/status.h>
#include <parquet/types.h>

#ifndef HAVE_INT128
#error "mars cannot compile without int128"
#endif /* HAVE_INT128 */

namespace mars
{

	/*!
	 * Short names of the StrategyNumber values.
	 *
	 * It is advised to "using mars::OP" if you are a heavy user of them.
	 */
	enum OP
	{
		LT = BTLessStrategyNumber,
		LE = BTLessEqualStrategyNumber,
		EQ = BTEqualStrategyNumber,
		GE = BTGreaterEqualStrategyNumber,
		GT = BTGreaterStrategyNumber,
	};

#define foreach_strategy(strategy) \
	for ((strategy) = OP::LT; (strategy) <= OP::GT; ++(strategy))

	typedef long double float128;
	typedef __int128 Datum128;

	typedef union Datum128_U
	{
		mars::Datum128 dd;
		float128 ld;
		int128 li;
	} Datum128_U;

	static inline Datum128
	Float128GetDatum128(float128 ld)
	{
		Datum128_U du;
		du.ld = ld;
		return du.dd;
	}

	static inline float128
	Datum128GetFloat128(Datum128 dd)
	{
		Datum128_U du;
		du.dd = dd;
		return du.ld;
	}

	static inline int128
	Int128GetDatum128(int128 li)
	{
		Datum128_U du;
		du.li = li;
		return du.dd;
	}

	static inline int128
	Datum128GetInt128(Datum128 dd)
	{
		Datum128_U du;
		du.dd = dd;
		return du.li;
	}

	template <::Oid type_oid>
	struct pg_type_traits
	{
	};

	template <>
	struct pg_type_traits<INT4OID>
	{
		using c_type = int32_t;
		using reader_type = int32_t;

		static constexpr const char *name = "int32";
		static constexpr const ::Oid type_oid = INT4OID;
		static constexpr const auto parquet_type = parquet::Type::INT32;
		static constexpr const auto converted_type = parquet::ConvertedType::INT_32;

		static ::Datum ToDatum(int32_t x) { return Int32GetDatum(x); }
		static int32_t FromDatum(::Datum d) { return DatumGetInt32(d); }
	};

	template <>
	struct pg_type_traits<INT8OID>
	{
		using c_type = int64_t;
		using reader_type = int64_t;

		static constexpr const char *name = "int64";
		static constexpr const ::Oid type_oid = INT8OID;
		static constexpr const auto parquet_type = parquet::Type::INT64;
		static constexpr const auto converted_type = parquet::ConvertedType::INT_64;

		static ::Datum ToDatum(int64_t x) { return Int64GetDatum(x); }
		static int64_t FromDatum(::Datum d) { return DatumGetInt64(d); }
	};

	template <>
	struct pg_type_traits<BOOLOID>
	{
		using c_type = int64_t;
		using reader_type = int64_t;

		static constexpr const char *name = "boolean";
		static constexpr const ::Oid type_oid = BOOLOID;
		static constexpr const auto parquet_type = parquet::Type::INT64;
		static constexpr const auto converted_type = parquet::ConvertedType::INT_64;

		static ::Datum ToDatum(int64_t x) { return BoolGetDatum(x); }
		static int64_t FromDatum(::Datum d) { return DatumGetBool(d); }
	};

	template <>
	struct pg_type_traits<FLOAT4OID>
	{
		using c_type = float;
		using reader_type = float;

		static constexpr const char *name = "float4";
		static constexpr const ::Oid type_oid = FLOAT4OID;
		static constexpr const auto parquet_type = parquet::Type::FLOAT;
		static constexpr const auto converted_type = parquet::ConvertedType::NONE;

		static ::Datum ToDatum(float x) { return Float4GetDatum(x); }
		static float FromDatum(::Datum d) { return DatumGetFloat4(d); }
	};

	template <>
	struct pg_type_traits<FLOAT8OID>
	{
		using c_type = double;
		using reader_type = double;

		static constexpr const char *name = "float8";
		static constexpr const ::Oid type_oid = FLOAT8OID;
		static constexpr const auto parquet_type = parquet::Type::DOUBLE;
		static constexpr const auto converted_type = parquet::ConvertedType::NONE;

		static ::Datum ToDatum(double x) { return Float8GetDatum(x); }
		static double FromDatum(::Datum d) { return DatumGetFloat8(d); }
	};

	template <>
	struct pg_type_traits<TIMESTAMPOID>
	{
		using c_type = ::Timestamp;
		using reader_type = ::Timestamp;

		static constexpr const char *name = "timestamp";
		static constexpr const ::Oid type_oid = TIMESTAMPOID;
		static constexpr const auto parquet_type = parquet::Type::INT64;
		static constexpr const auto converted_type = parquet::ConvertedType::TIMESTAMP_MILLIS;

		static ::Datum ToDatum(::Timestamp x) { return TimestampGetDatum(x); }
		static ::Timestamp FromDatum(::Datum d) { return DatumGetTimestamp(d); }
	};

	template <>
	struct pg_type_traits<TIMESTAMPTZOID>
	{
		using c_type = ::TimestampTz;
		using reader_type = ::TimestampTz;

		static constexpr const char *name = "timestamptz";
		static constexpr const ::Oid type_oid = TIMESTAMPTZOID;
		static constexpr const auto parquet_type = parquet::Type::INT64;
		static constexpr const auto converted_type = parquet::ConvertedType::TIMESTAMP_MILLIS;

		static ::Datum ToDatum(::TimestampTz x) { return TimestampTzGetDatum(x); }
		static ::TimestampTz FromDatum(::Datum d) { return DatumGetTimestampTz(d); }
	};

	template <>
	struct pg_type_traits<TEXTOID>
	{
		using c_type = ::text;
		using reader_type = parquet::ByteArray;

		static constexpr const char *name = "text";
		static constexpr const ::Oid type_oid = TEXTOID;
		static constexpr const auto parquet_type = parquet::Type::BYTE_ARRAY;
		static constexpr const auto converted_type = parquet::ConvertedType::NONE;

		static ::Datum ToDatum(parquet::ByteArray x)
		{
			void *p = ::palloc0(VARSIZE_ANY(x.ptr));
			memcpy(p, x.ptr, VARSIZE_ANY(x.ptr));
			return PointerGetDatum(p);
		}
		static char *FromDatum(::Datum d) { return TextDatumGetCString(d); }
	};

	template <parquet::Type::type TYPE>
	struct type_traits
	{
	};

	template <>
	struct type_traits<parquet::Type::INT32>
	{
		using sum_type = __int128;
	};

	template <>
	struct type_traits<parquet::Type::INT64>
	{
		using sum_type = __int128;
	};

	template <>
	struct type_traits<parquet::Type::FLOAT>
	{
		using sum_type = float128;
	};

	template <>
	struct type_traits<parquet::Type::DOUBLE>
	{
		using sum_type = float128;
	};

	template <>
	struct type_traits<parquet::Type::BYTE_ARRAY>
	{
		using sum_type = void;
	};

	/*!
	 * The palloc() allocator for STL containers.
	 */
	template <class T>
	struct Allocator
	{
		typedef T value_type;

		Allocator() = default;

		template <class U>
		constexpr Allocator(const Allocator<U> &) noexcept
		{
		}

		T *allocate(std::size_t n)
		{
			return static_cast<T *>(::palloc(sizeof(T) * n));
		}

		void deallocate(T *p, std::size_t n)
		{
			::pfree(p);
		}
	};

	static inline int
	byptr_value_get_length(::Form_pg_attribute attr, Datum data)
	{
		int datalen = attr->attlen;
		if (datalen < 0)
		{
			datalen = VARSIZE_ANY(data);
		}

		return datalen;
	}

	/*!
	 * dup data pointing to Datum @value
	 *
	 * @note The return data is allocated by palloc().
	 */
	static inline ::Datum
	byptr_value_dup(::Datum value, ::Form_pg_attribute attr)
	{
		int datalen = byptr_value_get_length(attr, value);
		void *data = ::palloc(datalen);
		::memcpy(data, DatumGetPointer(value), datalen);
		return PointerGetDatum(data);
	}

	/*!
	 * The palloc() memory pool for arrow/parquet objects.
	 */
	class MemoryPool : public arrow::MemoryPool
	{
	protected:
		MemoryPool() = default;

		static MemoryPool default_pool;

	public:
		static arrow::MemoryPool *GetDefault()
		{
			return &default_pool;
		}

		arrow::Status Allocate(int64_t size, uint8_t **out) override
		{
			if (size > 0)
			{
				*out = static_cast<uint8_t *>(::palloc(size));
			}
			else
			{
				*out = nullptr;
			}

			bytes_allocated_ += size;
			return arrow::Status::OK();
		}

		arrow::Status Reallocate(int64_t old_size, int64_t new_size,
								 uint8_t **ptr) override
		{
			if (*ptr && new_size > 0)
			{
				*ptr = static_cast<uint8_t *>(::repalloc(*ptr, new_size));
			}
			else
			{
				if (*ptr)
				{
					pfree(*ptr);
				}

				if (new_size > 0)
				{
					*ptr = static_cast<uint8_t *>(::palloc(new_size));
				}
				else
				{
					*ptr = nullptr;
				}
			}

			bytes_allocated_ += new_size - old_size;
			return arrow::Status::OK();
		}

		void Free(uint8_t *buffer, int64_t size) override
		{
			if (buffer)
			{
				::pfree(buffer);
			}

			bytes_allocated_ -= size;
		}

		int64_t bytes_allocated() const override
		{
			return bytes_allocated_;
		}

		std::string backend_name() const override
		{
			return "palloc";
		}

	protected:
		int64_t bytes_allocated_;
	};

} // namespace mars

#endif // MATRIXDB_TYPE_TRAITS_HPP

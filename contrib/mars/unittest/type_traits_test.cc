#include "gtest/gtest.h"

#include "mock.h"
#include "../type_traits.hpp"
#include "type_traits.hpp"

namespace mars {
namespace test {

TEST(TypeTraitsTest, Int32) {
	EXPECT_EQ(4, sizeof(mars::pg_type_traits<INT4OID>::c_type));
	auto parquet_type = mars::pg_type_traits<INT4OID>::parquet_type;
	EXPECT_EQ(parquet::Type::INT32, parquet_type); 
	auto converted_type = mars::pg_type_traits<INT4OID>::converted_type;
	EXPECT_EQ(parquet::ConvertedType::INT_32, converted_type);

	EXPECT_EQ(16, sizeof(mars::type_traits<mars::pg_type_traits<INT4OID>::parquet_type>::sum_type));
}

TEST(TypeTraitsTest, Int64) {
	EXPECT_EQ(8, sizeof(mars::pg_type_traits<INT8OID>::c_type));
	auto parquet_type = mars::pg_type_traits<INT8OID>::parquet_type;
	EXPECT_EQ(parquet::Type::INT64, parquet_type);
	auto converted_type = mars::pg_type_traits<INT8OID>::converted_type;
	EXPECT_EQ(parquet::ConvertedType::INT_64, converted_type);
	EXPECT_EQ(16, sizeof(mars::type_traits<mars::pg_type_traits<INT8OID>::parquet_type>::sum_type));
}

TEST(TypeTraitsTest, FLOAT4) {
	EXPECT_EQ(4, sizeof(mars::pg_type_traits<FLOAT4OID>::c_type));
	auto parquet_type = mars::pg_type_traits<FLOAT4OID>::parquet_type;
	EXPECT_EQ(parquet::Type::FLOAT, parquet_type);
	auto converted_type = mars::pg_type_traits<FLOAT4OID>::converted_type;
	EXPECT_EQ(parquet::ConvertedType::NONE, converted_type);
	EXPECT_EQ(16, sizeof(mars::type_traits<mars::pg_type_traits<FLOAT4OID>::parquet_type>::sum_type));
}

TEST(TypeTraitsTest, FLOAT8) {
	EXPECT_EQ(8, sizeof(mars::pg_type_traits<FLOAT8OID>::c_type));
	auto parquet_type = mars::pg_type_traits<FLOAT8OID>::parquet_type;
	EXPECT_EQ(parquet::Type::DOUBLE, parquet_type);
	auto converted_type = mars::pg_type_traits<FLOAT8OID>::converted_type;
	EXPECT_EQ(parquet::ConvertedType::NONE, converted_type);
	EXPECT_EQ(16, sizeof(mars::type_traits<mars::pg_type_traits<FLOAT8OID>::parquet_type>::sum_type));
}

TEST(TypeTraitsTest, TIMESTAMP) {
	EXPECT_EQ(8, sizeof(mars::pg_type_traits<TIMESTAMPOID>::c_type));
	auto parquet_type = mars::pg_type_traits<TIMESTAMPOID>::parquet_type;
	EXPECT_EQ(parquet::Type::INT64, parquet_type);
	auto converted_type = mars::pg_type_traits<TIMESTAMPOID>::converted_type;
	EXPECT_EQ(parquet::ConvertedType::TIMESTAMP_MILLIS, converted_type);

}

TEST(TypeTraitsTest, TIMESTAMPTZ) {
	EXPECT_EQ(8, sizeof(mars::pg_type_traits<TIMESTAMPTZOID>::c_type));
	auto parquet_type = mars::pg_type_traits<TIMESTAMPTZOID>::parquet_type;
	EXPECT_EQ(parquet::Type::INT64, parquet_type);
	auto converted_type = mars::pg_type_traits<TIMESTAMPTZOID>::converted_type;
	EXPECT_EQ(parquet::ConvertedType::TIMESTAMP_MILLIS, converted_type);
}

} // namespace test
} // namespace mars

#include "ScanKey_test.h"
#include "Footer_test.h"

#include "../ScanKey.cc"
#include "access/sdir.h"
#include "type_traits.hpp"

extern "C" {

void
GetMARSEntryOption(Oid relid, List** pgrp, List** pgrp2, List** pgrp2_param, List** glb_order, List** loc_order)
{
	*pgrp = NULL;
	*pgrp2 = NULL;
	*pgrp2_param = NULL;
	*glb_order = NULL;
	*loc_order = NULL;
}

}

namespace mars {
namespace test {

static const auto A = mars::ScanKey::MergePolicy::KEEP_A;
static const auto B = mars::ScanKey::MergePolicy::KEEP_B;
static const auto BOTH = mars::ScanKey::MergePolicy::KEEP_BOTH;
static const auto NONE = mars::ScanKey::MergePolicy::KEEP_NONE;

static const auto MISMATCH    = mars::ScanKey::MatchType::BLOCK_MISMATCH;
static const auto MAYBE_MATCH = mars::ScanKey::MatchType::BLOCK_MAYBE_MATCH;
static const auto FULL_MATCH  = mars::ScanKey::MatchType::BLOCK_FULL_MATCH;

static const auto NO_ARRAY  = mars::ScanKey::ArrayKeyType::NO_ARRAY;
static const auto ARRAY_KEY  = mars::ScanKey::ArrayKeyType::ARRAY_KEY;
static const auto ARRAY_FILTER  = mars::ScanKey::ArrayKeyType::ARRAY_FILTER;

using ScanKeyTypes = ::testing::Types<int32_t, int64_t, float, double>;
TYPED_TEST_SUITE(ScanKeyTypedTest, ScanKeyTypes, mars::test::type_names);

TYPED_TEST(ScanKeyTypedTest, Make)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;

	typename T::c_type a = 123;
	typename T::c_type b = 456;

	auto key = mars::ScanKey::Make(T::type_oid, T::type_oid);
	ScanKeyTest::SetScanKey(*key, "=", a);

	ASSERT_TRUE(key->MatchPoint(T::ToDatum(a)));
	ASSERT_FALSE(key->MatchPoint(T::ToDatum(b)));
}

TYPED_TEST(ScanKeyTypedTest, Merge)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;

	auto merge = [](const char *op1, typename T::c_type arg1,
					const char *op2, typename T::c_type arg2) {
		auto a = mars::ScanKey::Make(T::type_oid, T::type_oid);
		auto b = mars::ScanKey::Make(T::type_oid, T::type_oid);
		ScanKeyTest::SetScanKey(*a, op1, arg1);
		ScanKeyTest::SetScanKey(*b, op2, arg2);
		return mars::ScanKey::Merge(*a, *b);
	};

	// below relations are described in the comments of Merge()

	ASSERT_EQ(merge("<" , 1, "<" , 0), B);
	ASSERT_EQ(merge("<" , 1, "<" , 1), A);
	ASSERT_EQ(merge("<" , 1, "<" , 2), A);
	ASSERT_EQ(merge("<" , 1, "<=", 0), B);
	ASSERT_EQ(merge("<" , 1, "<=", 1), A);
	ASSERT_EQ(merge("<" , 1, "<=", 2), A);
	ASSERT_EQ(merge("<" , 1, "=" , 0), B);
	ASSERT_EQ(merge("<" , 1, "=" , 1), NONE);
	ASSERT_EQ(merge("<" , 1, "=" , 2), NONE);
	ASSERT_EQ(merge("<" , 1, ">=", 0), BOTH);
	ASSERT_EQ(merge("<" , 1, ">=", 1), NONE);
	ASSERT_EQ(merge("<" , 1, ">=", 2), NONE);
	ASSERT_EQ(merge("<" , 1, ">" , 0), BOTH);
	ASSERT_EQ(merge("<" , 1, ">" , 1), NONE);
	ASSERT_EQ(merge("<" , 1, ">" , 2), NONE);

	ASSERT_EQ(merge("<=", 1, "<" , 0), B);
	ASSERT_EQ(merge("<=", 1, "<" , 1), B);
	ASSERT_EQ(merge("<=", 1, "<" , 2), A);
	ASSERT_EQ(merge("<=", 1, "<=", 0), B);
	ASSERT_EQ(merge("<=", 1, "<=", 1), B);
	ASSERT_EQ(merge("<=", 1, "<=", 2), A);
	ASSERT_EQ(merge("<=", 1, "=" , 0), B);
	ASSERT_EQ(merge("<=", 1, "=" , 1), B);
	ASSERT_EQ(merge("<=", 1, "=" , 2), NONE);
	ASSERT_EQ(merge("<=", 1, ">=", 0), BOTH);
	ASSERT_EQ(merge("<=", 1, ">=", 1), BOTH);
	ASSERT_EQ(merge("<=", 1, ">=", 2), NONE);
	ASSERT_EQ(merge("<=", 1, ">" , 0), BOTH);
	ASSERT_EQ(merge("<=", 1, ">" , 1), NONE);
	ASSERT_EQ(merge("<=", 1, ">" , 2), NONE);

	ASSERT_EQ(merge("=" , 1, "<" , 0), NONE);
	ASSERT_EQ(merge("=" , 1, "<" , 1), NONE);
	ASSERT_EQ(merge("=" , 1, "<" , 2), A);
	ASSERT_EQ(merge("=" , 1, "<=", 0), NONE);
	ASSERT_EQ(merge("=" , 1, "<=", 1), A);
	ASSERT_EQ(merge("=" , 1, "<=", 2), A);
	ASSERT_EQ(merge("=" , 1, "=" , 0), NONE);
	ASSERT_EQ(merge("=" , 1, "=" , 1), A);
	ASSERT_EQ(merge("=" , 1, "=" , 2), NONE);
	ASSERT_EQ(merge("=" , 1, ">=", 0), A);
	ASSERT_EQ(merge("=" , 1, ">=", 1), A);
	ASSERT_EQ(merge("=" , 1, ">=", 2), NONE);
	ASSERT_EQ(merge("=" , 1, ">" , 0), A);
	ASSERT_EQ(merge("=" , 1, ">" , 1), NONE);
	ASSERT_EQ(merge("=" , 1, ">" , 2), NONE);

	ASSERT_EQ(merge(">=", 1, "<" , 0), NONE);
	ASSERT_EQ(merge(">=", 1, "<" , 1), NONE);
	ASSERT_EQ(merge(">=", 1, "<" , 2), BOTH);
	ASSERT_EQ(merge(">=", 1, "<=", 0), NONE);
	ASSERT_EQ(merge(">=", 1, "<=", 1), BOTH);
	ASSERT_EQ(merge(">=", 1, "<=", 2), BOTH);
	ASSERT_EQ(merge(">=", 1, "=" , 0), NONE);
	ASSERT_EQ(merge(">=", 1, "=" , 1), B);
	ASSERT_EQ(merge(">=", 1, "=" , 2), B);
	ASSERT_EQ(merge(">=", 1, ">=", 0), A);
	ASSERT_EQ(merge(">=", 1, ">=", 1), B);
	ASSERT_EQ(merge(">=", 1, ">=", 2), B);
	ASSERT_EQ(merge(">=", 1, ">" , 0), A);
	ASSERT_EQ(merge(">=", 1, ">" , 1), B);
	ASSERT_EQ(merge(">=", 1, ">" , 2), B);

	ASSERT_EQ(merge(">" , 1, "<" , 0), NONE);
	ASSERT_EQ(merge(">" , 1, "<" , 1), NONE);
	ASSERT_EQ(merge(">" , 1, "<" , 2), BOTH);
	ASSERT_EQ(merge(">" , 1, "<=", 0), NONE);
	ASSERT_EQ(merge(">" , 1, "<=", 1), NONE);
	ASSERT_EQ(merge(">" , 1, "<=", 2), BOTH);
	ASSERT_EQ(merge(">" , 1, "=" , 0), NONE);
	ASSERT_EQ(merge(">" , 1, "=" , 1), NONE);
	ASSERT_EQ(merge(">" , 1, "=" , 2), B);
	ASSERT_EQ(merge(">" , 1, ">=", 0), A);
	ASSERT_EQ(merge(">" , 1, ">=", 1), A);
	ASSERT_EQ(merge(">" , 1, ">=", 2), B);
	ASSERT_EQ(merge(">" , 1, ">" , 0), A);
	ASSERT_EQ(merge(">" , 1, ">" , 1), A);
	ASSERT_EQ(merge(">" , 1, ">" , 2), B);
}

TYPED_TEST(ScanKeyTypedTest, MatchPoint)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;

	auto key = mars::ScanKey::Make(T::type_oid, T::type_oid);
	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;
	auto v1 = T::ToDatum(x1);
	auto v2 = T::ToDatum(x2);
	auto v3 = T::ToDatum(x3);

	ScanKeyTest::SetScanKey(*key, "<" , x2);
	ASSERT_TRUE(key->MatchPoint(v1));
	ASSERT_FALSE(key->MatchPoint(v2));
	ASSERT_FALSE(key->MatchPoint(v3));

	ScanKeyTest::SetScanKey(*key, "<=", x2);
	ASSERT_TRUE(key->MatchPoint(v1));
	ASSERT_TRUE(key->MatchPoint(v2));
	ASSERT_FALSE(key->MatchPoint(v3));

	ScanKeyTest::SetScanKey(*key, "=" , x2);
	ASSERT_FALSE(key->MatchPoint(v1));
	ASSERT_TRUE(key->MatchPoint(v2));
	ASSERT_FALSE(key->MatchPoint(v3));

	ScanKeyTest::SetScanKey(*key, ">=", x2);
	ASSERT_FALSE(key->MatchPoint(v1));
	ASSERT_TRUE(key->MatchPoint(v2));
	ASSERT_TRUE(key->MatchPoint(v3));

	ScanKeyTest::SetScanKey(*key, ">" , x2);
	ASSERT_FALSE(key->MatchPoint(v1));
	ASSERT_FALSE(key->MatchPoint(v2));
	ASSERT_TRUE(key->MatchPoint(v3));
}

TYPED_TEST(ScanKeyTypedTest, MatchBlock_Range)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;
	using FooterTest = FooterTypedTest<TypeParam>;

	mars::footer::Footer footer{0};
	auto column = 0;

	auto key = mars::ScanKey::Make(T::type_oid, T::type_oid);

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;
	typename T::c_type x4 = 4;
	typename T::c_type x5 = 5;

	auto match = [&footer, &key, &column](const char *op,
										  typename T::c_type arg) {
		ScanKeyTest::SetScanKey(*key, op, arg);
		return key->MatchBlock(footer, column);
	};

	FooterTest::SetMinMax(footer, column, x2, x4);
	FooterTest::SetRowCount(footer, column, 100, 0);

	ASSERT_EQ(match("<" , x1), MISMATCH);
	ASSERT_EQ(match("<" , x2), MISMATCH);
	ASSERT_EQ(match("<" , x3), MAYBE_MATCH);
	ASSERT_EQ(match("<" , x4), MAYBE_MATCH);
	ASSERT_EQ(match("<" , x5), FULL_MATCH);

	ASSERT_EQ(match("<=", x1), MISMATCH);
	ASSERT_EQ(match("<=", x2), MAYBE_MATCH);
	ASSERT_EQ(match("<=", x3), MAYBE_MATCH);
	ASSERT_EQ(match("<=", x4), FULL_MATCH);
	ASSERT_EQ(match("<=", x5), FULL_MATCH);

	ASSERT_EQ(match("=" , x1), MISMATCH);
	ASSERT_EQ(match("=" , x2), MAYBE_MATCH);
	ASSERT_EQ(match("=" , x3), MAYBE_MATCH);
	ASSERT_EQ(match("=" , x4), MAYBE_MATCH);
	ASSERT_EQ(match("=" , x5), MISMATCH);

	ASSERT_EQ(match(">=", x1), FULL_MATCH);
	ASSERT_EQ(match(">=", x2), FULL_MATCH);
	ASSERT_EQ(match(">=", x3), MAYBE_MATCH);
	ASSERT_EQ(match(">=", x4), MAYBE_MATCH);
	ASSERT_EQ(match(">=", x5), MISMATCH);

	ASSERT_EQ(match(">" , x1), FULL_MATCH);
	ASSERT_EQ(match(">" , x2), MAYBE_MATCH);
	ASSERT_EQ(match(">" , x3), MAYBE_MATCH);
	ASSERT_EQ(match(">" , x4), MISMATCH);
	ASSERT_EQ(match(">" , x5), MISMATCH);
}

TYPED_TEST(ScanKeyTypedTest, MatchBlock_SingleValue)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;
	using FooterTest = FooterTypedTest<TypeParam>;

	mars::footer::Footer footer{0};
	auto column = 0;

	auto key = mars::ScanKey::Make(T::type_oid, T::type_oid);
	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;

	auto match = [&footer, &key, &column](const char *op,
										  typename T::c_type arg) {
		ScanKeyTest::SetScanKey(*key, op, arg);
		return key->MatchBlock(footer, column);
	};

	FooterTest::SetMinMax(footer, column, x2, x2);
	FooterTest::SetRowCount(footer, column, 100, 0);

	ASSERT_EQ(match("<" , x1), MISMATCH);
	ASSERT_EQ(match("<" , x2), MISMATCH);
	ASSERT_EQ(match("<" , x3), FULL_MATCH);

	ASSERT_EQ(match("<=", x1), MISMATCH);
	ASSERT_EQ(match("<=", x2), FULL_MATCH);
	ASSERT_EQ(match("<=", x3), FULL_MATCH);

	ASSERT_EQ(match("=" , x1), MISMATCH);
	ASSERT_EQ(match("=" , x2), FULL_MATCH);
	ASSERT_EQ(match("=" , x3), MISMATCH);

	ASSERT_EQ(match(">=", x1), FULL_MATCH);
	ASSERT_EQ(match(">=", x2), FULL_MATCH);
	ASSERT_EQ(match(">=", x3), MISMATCH);

	ASSERT_EQ(match(">" , x1), FULL_MATCH);
	ASSERT_EQ(match(">" , x2), MISMATCH);
	ASSERT_EQ(match(">" , x3), MISMATCH);
}

TYPED_TEST(ScanKeyTypedTest, FirstMatchBlock)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;
	using FooterTest = FooterTypedTest<TypeParam>;

	std::vector<mars::footer::Footer> footers;
	auto column = 0;

	auto key = mars::ScanKey::Make(T::type_oid, T::type_oid);
	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;
	typename T::c_type x4 = 4;
	typename T::c_type x5 = 5;

	auto match = [&footers, &key](const char *op,
								  typename T::c_type arg) -> int {
		BlockSlice blocks(::ForwardScanDirection,
						  footers.cbegin(), footers.cend());
		ScanKeyTest::SetScanKey(*key, op, arg);
		key->FirstMatchBlock(blocks);

		BlockSlice::const_iterator begin;
		BlockSlice::const_iterator end;
		blocks.GetRange(begin, end);
		return begin - footers.cbegin();
	};

	footers.resize(5);
	FooterTest::SetMinMax(footers[0], column, x2, x2);
	FooterTest::SetMinMax(footers[1], column, x2, x3);
	FooterTest::SetMinMax(footers[2], column, x3, x3);
	FooterTest::SetMinMax(footers[3], column, x3, x4);
	FooterTest::SetMinMax(footers[4], column, x4, x4);
	FooterTest::SetRowCount(footers[0], column, 100, 0);
	FooterTest::SetRowCount(footers[1], column, 100, 0);
	FooterTest::SetRowCount(footers[2], column, 100, 0);
	FooterTest::SetRowCount(footers[3], column, 100, 0);
	FooterTest::SetRowCount(footers[4], column, 100, 0);

	ASSERT_EQ(match("<" , x1), 0);
	ASSERT_EQ(match("<" , x2), 0);
	ASSERT_EQ(match("<" , x3), 0);
	ASSERT_EQ(match("<" , x4), 0);
	ASSERT_EQ(match("<" , x5), 0);

	ASSERT_EQ(match("<=", x1), 0);
	ASSERT_EQ(match("<=", x2), 0);
	ASSERT_EQ(match("<=", x3), 0);
	ASSERT_EQ(match("<=", x4), 0);
	ASSERT_EQ(match("<=", x5), 0);

	ASSERT_EQ(match("=" , x1), 0);
	ASSERT_EQ(match("=" , x2), 0);
	ASSERT_EQ(match("=" , x3), 1);
	ASSERT_EQ(match("=" , x4), 3);
	ASSERT_EQ(match("=" , x5), 5);

	ASSERT_EQ(match(">=", x1), 0);
	ASSERT_EQ(match(">=", x2), 0);
	ASSERT_EQ(match(">=", x3), 1);
	ASSERT_EQ(match(">=", x4), 3);
	ASSERT_EQ(match(">=", x5), 5);

	ASSERT_EQ(match(">" , x1), 0);
	ASSERT_EQ(match(">" , x2), 1);
	ASSERT_EQ(match(">" , x3), 3);
	ASSERT_EQ(match(">" , x4), 5);
	ASSERT_EQ(match(">" , x5), 5);
}

TYPED_TEST(ScanKeyTypedTest, ArrayKey)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;
	auto v1 = T::ToDatum(x1);
	auto v2 = T::ToDatum(x2);
	auto v3 = T::ToDatum(x3);

	auto key = mars::ScanKey::Make(T::type_oid, T::type_oid);
	ScanKeyTest::SetScanKey(*key, "=", x2);

	ASSERT_EQ(key->GetArrayKeyType(), NO_ARRAY);
	key->ArrayKeyScanBegin(::ForwardScanDirection);
	ASSERT_EQ(key->scankey_.sk_argument, v2);
	ASSERT_FALSE(key->ArrayKeyScanNext(::ForwardScanDirection));
	ASSERT_EQ(key->scankey_.sk_argument, v2);

	::Datum elem_values[] = { v1, v2, v3 };
	bool elem_nulls[] = { false, false, false };
	::IndexArrayKeyInfo arraykey = {
		.scan_key    = &key->scankey_,
		.array_expr  = nullptr,
		.next_elem   = 1,
		.num_elems   = 3,
		.elem_values = elem_values,
		.elem_nulls  = elem_nulls,
	};

	key->BindArrayKey(&arraykey);
	ASSERT_NE(key->GetArrayKeyType(), NO_ARRAY);
	key->ArrayKeyScanBegin(::ForwardScanDirection);
	ASSERT_EQ(key->scankey_.sk_argument, v1);
	ASSERT_TRUE(key->ArrayKeyScanNext(::ForwardScanDirection));
	ASSERT_EQ(key->scankey_.sk_argument, v2);
	ASSERT_TRUE(key->ArrayKeyScanNext(::ForwardScanDirection));
	ASSERT_EQ(key->scankey_.sk_argument, v3);
	ASSERT_FALSE(key->ArrayKeyScanNext(::ForwardScanDirection));
	ASSERT_EQ(key->scankey_.sk_argument, v3);
}

} // namespace test
} // namespace mars

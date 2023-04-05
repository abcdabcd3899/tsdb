#include "ColumnStore_test.h"
#include "ScanKey_test.h"

#include "../ColumnStore.cc"

namespace mars {
namespace test {

using ColumnStoreTypes = ::testing::Types<int32_t, int64_t, float, double>;
TYPED_TEST_SUITE(ColumnStoreTypedTest, ColumnStoreTypes, mars::test::type_names);

TYPED_TEST(ColumnStoreTypedTest, Make)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	// FIXME: the Make() function takes a parquet::ColumnDescriptor argument,
	// which is hard to mock, so it is hard to test the Make() function itself,
	// we only test the ctors directly.
	auto cstore = ColumnStoreTest::Make();
}

TYPED_TEST(ColumnStoreTypedTest, GetPoint)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	const typename T::c_type x0 = 0;
	const typename T::c_type x1 = 1;
	const typename T::c_type x2 = 2;

	auto cstore = ColumnStoreTest::Make({x0, x1, x2}, {1, 1, 1});

	const std::vector<int> *remap = nullptr;
	::Datum datum = 0;
	bool isnull = false;

	cstore->GetPoint(remap, 0, datum, isnull);
	ASSERT_FALSE(isnull);
	ASSERT_EQ(T::FromDatum(datum), x0);

	cstore->GetPoint(remap, 1, datum, isnull);
	ASSERT_FALSE(isnull);
	ASSERT_EQ(T::FromDatum(datum), x1);

	cstore->GetPoint(remap, 2, datum, isnull);
	ASSERT_FALSE(isnull);
	ASSERT_EQ(T::FromDatum(datum), x2);
}

TYPED_TEST(ColumnStoreTypedTest, Compare)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	const typename T::c_type x0 = 0;
	const typename T::c_type x1 = 1;
	const typename T::c_type x2 = 2;

	auto cstore = ColumnStoreTest::Make({x0, x1, x2}, {1, 1, 1});

	ASSERT_FALSE(cstore->Compare(1, 0));
	ASSERT_FALSE(cstore->Compare(1, 1));
	ASSERT_TRUE(cstore->Compare(1, 2));
}

TYPED_TEST(ColumnStoreTypedTest, CompareC)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	const typename T::c_type x0 = 0;
	const typename T::c_type x1 = 1;
	const typename T::c_type x2 = 2;

	auto cstore = ColumnStoreTest::Make({x0, x1, x2}, {1, 1, 1});

	ASSERT_EQ(cstore->CompareC(1, 0), +1);
	ASSERT_EQ(cstore->CompareC(1, 1),  0);
	ASSERT_EQ(cstore->CompareC(1, 2), -1);
}

TYPED_TEST(ColumnStoreTypedTest, SkipCurrentValue)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	const typename T::c_type x1 = 1;
	const typename T::c_type x2 = 2;
	const typename T::c_type x3 = 3;

	auto cstore = ColumnStoreTest::Make({x1, x1, x2, x2, x3, x3},
										{1, 1, 1, 1, 1, 1});

	auto skip = [&cstore](int first, int last,
						  const std::vector<int> *remap = nullptr) {
		RowSlice rows(ForwardScanDirection, first, last);
		cstore->SkipCurrentValue(remap, rows);

		rows.GetRange(first, last);
		return std::make_pair(first, last);
	};

	ASSERT_EQ(skip(0, 6), std::make_pair(2, 6));
	ASSERT_EQ(skip(1, 6), std::make_pair(2, 6));
	ASSERT_EQ(skip(2, 6), std::make_pair(4, 6));
	ASSERT_EQ(skip(3, 6), std::make_pair(4, 6));
	ASSERT_EQ(skip(4, 6), std::make_pair(6, 6));
	ASSERT_EQ(skip(5, 6), std::make_pair(6, 6));
	ASSERT_EQ(skip(6, 6), std::make_pair(6, 6));
}

} // namespace test
} // namespace mars

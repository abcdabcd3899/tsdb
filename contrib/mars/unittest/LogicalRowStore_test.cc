#include "LogicalRowStore_test.h"
#include "ColumnStore_test.h"
#include "ScanKey_test.h"

#include "../LogicalRowStore.cc"

namespace mars {
namespace test {

static const auto MATCH                   = mars::ScanKey::MatchType::MATCH;
static const auto MISMATCH_FIRST_ORDERKEY = mars::ScanKey::MatchType::MISMATCH_FIRST_ORDERKEY;
static const auto MISMATCH_ORDERKEY       = mars::ScanKey::MatchType::MISMATCH_ORDERKEY;
static const auto MISMATCH_FILTER         = mars::ScanKey::MatchType::MISMATCH_FILTER;

using LogicalRowStoreTypes = ::testing::Types<int32_t, int64_t, float, double>;
TYPED_TEST_SUITE(LogicalRowStoreTypedTest, LogicalRowStoreTypes,
				 mars::test::type_names);

TYPED_TEST(LogicalRowStoreTypedTest, Ctor)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	const typename T::c_type x0 = 0;
	const typename T::c_type x1 = 1;
	const typename T::c_type x2 = 2;
	const typename T::c_type x3 = 3;
	const typename T::c_type x4 = 4;
	const typename T::c_type x5 = 5;

	std::vector<std::shared_ptr<mars::ColumnStore>> cstores {
		ColumnStoreTest::Make({x0, x1, x2}, {1, 1, 1}),
		ColumnStoreTest::Make({x3, x4, x5}, {1, 1, 1}),
	};
	const std::vector<int> *remap = nullptr;
	int nrows = 3;

	mars::LogicalRowStore rstore(cstores, remap, nrows);
}

TYPED_TEST(LogicalRowStoreTypedTest, Seek)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	const typename T::c_type x0 = 0;
	const typename T::c_type x1 = 1;
	const typename T::c_type x2 = 2;
	const typename T::c_type x3 = 3;
	const typename T::c_type x4 = 4;
	const typename T::c_type x5 = 5;

	std::vector<std::shared_ptr<mars::ColumnStore>> cstores {
		ColumnStoreTest::Make({x0, x1, x2}, {1, 1, 1}),
		ColumnStoreTest::Make({x3, x4, x5}, {1, 1, 1}),
	};
	const std::vector<int> *remap = nullptr;
	int nrows = 3;

	mars::LogicalRowStore rstore(cstores, remap, nrows);

	// begin()
	ASSERT_EQ(rstore.begin()->row_, 0);

	// at()
	ASSERT_EQ(rstore.begin()->row_, rstore.at(0)->row_);

	// end()
	auto iter = rstore.begin();
	ASSERT_NE(iter, rstore.end());
	ASSERT_NE(++iter, rstore.end());
	ASSERT_NE(++iter, rstore.end());
	ASSERT_EQ(++iter, rstore.end());

	// foreach
	{
		std::vector<int> result { };
		for (const auto &lrow: rstore) {
			result.push_back(lrow.row_);
		}
		ASSERT_EQ(result, std::vector<int>({ 0, 1, 2 }));
	}
}

TYPED_TEST(LogicalRowStoreTypedTest, Iterator)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	const typename T::c_type x0 = 0;
	const typename T::c_type x1 = 1;
	const typename T::c_type x2 = 2;
	const typename T::c_type x3 = 3;
	const typename T::c_type x4 = 4;
	const typename T::c_type x5 = 5;

	std::vector<std::shared_ptr<mars::ColumnStore>> cstores {
		ColumnStoreTest::Make({x0, x1, x2}, {1, 1, 1}),
		ColumnStoreTest::Make({x3, x4, x5}, {1, 1, 1}),
	};
	const std::vector<int> *remap = nullptr;
	int nrows = 3;

	mars::LogicalRowStore rstore(cstores, remap, nrows);

	// operator ==
	ASSERT_EQ(rstore.at(0), rstore.at(0));
	ASSERT_EQ(rstore.at(1), rstore.at(1));

	// operator !=
	ASSERT_NE(rstore.at(0), rstore.at(1));
	ASSERT_NE(rstore.at(1), rstore.at(0));

	// operator -
	ASSERT_EQ(rstore.at(0) - rstore.begin(), 0);
	ASSERT_EQ(rstore.at(1) - rstore.begin(), 1);
	ASSERT_EQ(rstore.at(2) - rstore.begin(), 2);

	// operator *
	ASSERT_EQ((*rstore.at(0)).row_, 0);
	ASSERT_EQ((*rstore.at(1)).row_, 1);
	ASSERT_EQ((*rstore.at(2)).row_, 2);

	// operator ->
	ASSERT_EQ(rstore.at(0)->row_, 0);
	ASSERT_EQ(rstore.at(1)->row_, 1);
	ASSERT_EQ(rstore.at(2)->row_, 2);

	// operator []
	ASSERT_EQ(rstore.begin()[0].row_, 0);
	ASSERT_EQ(rstore.begin()[1].row_, 1);
	ASSERT_EQ(rstore.begin()[2].row_, 2);

	// operator ++iter
	{
		auto iter = rstore.begin();
		ASSERT_EQ((++iter)->row_, 1);
		ASSERT_EQ((++iter)->row_, 2);
		ASSERT_EQ(++iter, rstore.end());
	}

	// operator iter++
	{
		auto iter = rstore.begin();
		ASSERT_EQ((iter++)->row_, 0);
		ASSERT_EQ((iter++)->row_, 1);
		ASSERT_EQ((iter++)->row_, 2);
		ASSERT_EQ(iter, rstore.end());
	}

	// operator =
	{
		auto iter = rstore.begin();
		ASSERT_EQ(iter->row_, 0);

		iter = rstore.at(1);
		ASSERT_EQ(iter->row_, 1);
		iter = rstore.at(2);
		ASSERT_EQ(iter->row_, 2);
	}

	// ctor() and copy ctor()
	{
		// ctor(rstore, row)
		mars::LogicalRowStore::iterator a(rstore, 0);
		// ctor(lrow)
		mars::LogicalRowStore::iterator b(a[0]);
		// copy ctor()
		mars::LogicalRowStore::iterator c(a);

		ASSERT_EQ(a, b);
		ASSERT_EQ(a, c);
	}
}

TYPED_TEST(LogicalRowStoreTypedTest, LogicalRow)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;

	const typename T::c_type x0 = 0;
	const typename T::c_type x1 = 1;
	const typename T::c_type x2 = 2;

	// cstores[1] is useless
	std::vector<std::shared_ptr<mars::ColumnStore>> cstores {
		ColumnStoreTest::Make({x0, x1, x2}, {1, 1, 1}),
		ColumnStoreTest::Make({x2, x1, x0}, {1, 1, 1}),
	};
	const std::vector<int> *remap = nullptr;
	int nrows = 4;

	mars::LogicalRowStore rstore(cstores, remap, nrows);

	// actually we want to verify with at least two scankeys, but that will be
	// too many combinations.
	std::vector<std::shared_ptr<mars::ScanKey>> keys = {
		mars::ScanKey::Make(T::type_oid, T::type_oid),
	};

	// lrow < keys
	{
		auto cmp = [&rstore, &keys](int row,
									const char *op, typename T::c_type arg) {
			ScanKeyTest::SetScanKey(*(keys[0]), op, arg);

			auto lrow = rstore.begin()[row];
			return lrow < keys;
		};

		ASSERT_EQ(cmp(1, "<" , x0), false);
		ASSERT_EQ(cmp(1, "<=", x0), false);
		ASSERT_EQ(cmp(1, "=" , x0), false);
		ASSERT_EQ(cmp(1, ">=", x0), false);
		ASSERT_EQ(cmp(1, ">" , x0), false);

		ASSERT_EQ(cmp(1, "<" , x1), false);
		ASSERT_EQ(cmp(1, "<=", x1), false);
		ASSERT_EQ(cmp(1, "=" , x1), false);
		ASSERT_EQ(cmp(1, ">=", x1), false);
		ASSERT_EQ(cmp(1, ">" , x1), false);

		ASSERT_EQ(cmp(1, "<" , x2), true);
		ASSERT_EQ(cmp(1, "<=", x2), true);
		ASSERT_EQ(cmp(1, "=" , x2), true);
		ASSERT_EQ(cmp(1, ">=", x2), true);
		ASSERT_EQ(cmp(1, ">" , x2), true);
	}

	// keys < lrow
	{
		auto cmp = [&rstore, &keys](const char *op, typename T::c_type arg,
									int row) {
			ScanKeyTest::SetScanKey(*(keys[0]), op, arg);

			auto lrow = rstore.begin()[row];
			return keys < lrow;
		};

		ASSERT_EQ(cmp("<" , x0, 1), true);
		ASSERT_EQ(cmp("<=", x0, 1), true);
		ASSERT_EQ(cmp("=" , x0, 1), true);
		ASSERT_EQ(cmp(">=", x0, 1), true);
		ASSERT_EQ(cmp(">" , x0, 1), true);

		ASSERT_EQ(cmp("<" , x1, 1), false);
		ASSERT_EQ(cmp("<=", x1, 1), false);
		ASSERT_EQ(cmp("=" , x1, 1), false);
		ASSERT_EQ(cmp(">=", x1, 1), false);
		ASSERT_EQ(cmp(">" , x1, 1), false);

		ASSERT_EQ(cmp("<" , x2, 1), false);
		ASSERT_EQ(cmp("<=", x2, 1), false);
		ASSERT_EQ(cmp("=" , x2, 1), false);
		ASSERT_EQ(cmp(">=", x2, 1), false);
		ASSERT_EQ(cmp(">" , x2, 1), false);
	}

	// match
	{
		auto match = [&rstore, &keys](int row,
									  const char *op, typename T::c_type arg) {
			ScanKeyTest::SetScanKey(*(keys[0]), op, arg);

			auto lrow = rstore.begin()[row];
			return lrow.Match(::ForwardScanDirection, keys);
		};

		// TODO: to fully verify the Match() function we need scankeys on at
		// least two different columns.

		ASSERT_EQ(match(1, "<" , x0), MISMATCH_FIRST_ORDERKEY);
		ASSERT_EQ(match(1, "<=", x0), MISMATCH_FIRST_ORDERKEY);
		ASSERT_EQ(match(1, "=" , x0), MISMATCH_FIRST_ORDERKEY);
		ASSERT_EQ(match(1, ">=", x0), MATCH);
		ASSERT_EQ(match(1, ">" , x0), MATCH);

		ASSERT_EQ(match(1, "<" , x1), MISMATCH_FIRST_ORDERKEY);
		ASSERT_EQ(match(1, "<=", x1), MATCH);
		ASSERT_EQ(match(1, "=" , x1), MATCH);
		ASSERT_EQ(match(1, ">=", x1), MATCH);
		ASSERT_EQ(match(1, ">" , x1), MATCH);

		ASSERT_EQ(match(1, "<" , x2), MATCH);
		ASSERT_EQ(match(1, "<=", x2), MATCH);
		ASSERT_EQ(match(1, "=" , x2), MISMATCH_FILTER);
		ASSERT_EQ(match(1, ">=", x2), MATCH);
		ASSERT_EQ(match(1, ">" , x2), MATCH);
	}
}

} // namespace test
} // namespace mars

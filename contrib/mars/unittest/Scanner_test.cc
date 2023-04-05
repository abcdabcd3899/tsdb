#include "Scanner_test.h"
#include "ColumnStore_test.h"
#include "ScanKey_test.h"
#include "Footer_test.h"

#include "../Scanner.cc"

#include <memory>

namespace mars {
namespace test {

static const auto FILTERED   =  mars::Scanner::Stage::FILTERED;
static const auto POSTFILTER =  mars::Scanner::Stage::POSTFILTER;
static const auto PREFILTER  =  mars::Scanner::Stage::PREFILTER;

static const auto MATCH                   = mars::ScanKey::MatchType::MATCH;
static const auto MISMATCH_FIRST_ORDERKEY = mars::ScanKey::MatchType::MISMATCH_FIRST_ORDERKEY;
static const auto MISMATCH_ORDERKEY       = mars::ScanKey::MatchType::MISMATCH_ORDERKEY;
static const auto MISMATCH_FILTER         = mars::ScanKey::MatchType::MISMATCH_FILTER;
static const auto BLOCK_FULL_MATCH        = mars::ScanKey::MatchType::BLOCK_FULL_MATCH;
static const auto BLOCK_MAYBE_MATCH       = mars::ScanKey::MatchType::BLOCK_MAYBE_MATCH;
static const auto BLOCK_MISMATCH          = mars::ScanKey::MatchType::BLOCK_MISMATCH;

using Attrs = mars::test::Scanner::Attrs;
using Columns = mars::test::Scanner::Columns;
using ScanKeys = mars::test::Scanner::ScanKeys;
using ArrayKeys = mars::test::Scanner::ArrayKeys;

using ScannerTypes = ::testing::Types<int32_t, int64_t, float, double>;
TYPED_TEST_SUITE(ScannerTypedTest, ScannerTypes, mars::test::type_names);

TEST(Scanner, Ctor)
{
	mars::test::Scanner scanner(nullptr);
}

TEST(Scanner, SetStoreOrder)
{
	mars::test::Scanner scanner(nullptr);

	scanner.SetStoreOrder({ 2, 1 }, { 3, 4 });
	ASSERT_EQ(scanner._block_store_order(), Attrs({ 2, 1 }));
	ASSERT_EQ(scanner._table_store_order(), Attrs({ 3, 4 }));

	scanner.SetStoreOrder({ 2, 1 }, { });
	ASSERT_EQ(scanner._block_store_order(), Attrs({ 2, 1 }));
	ASSERT_EQ(scanner._table_store_order(), Attrs({ }));

	scanner.SetStoreOrder({ }, { 3, 4 });
	ASSERT_EQ(scanner._block_store_order(), Attrs({ }));
	ASSERT_EQ(scanner._table_store_order(), Attrs({ 3, 4 }));

	scanner.SetStoreOrder({ }, { });
	ASSERT_EQ(scanner._block_store_order(), Attrs({ }));
	ASSERT_EQ(scanner._table_store_order(), Attrs({ }));

	scanner.SetStoreOrder({ 0, 1, 0, 1, 2 }, { 3, 4, 0, 4, 3, 0, 0 });
	ASSERT_EQ(scanner._block_store_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_store_order(), Attrs({ 3, 4 }));
}

TEST(Scanner, SetProjectedColumns)
{
	mars::test::Scanner scanner(nullptr);

	scanner.SetProjectedColumns({ true });
	ASSERT_EQ(scanner._projcols(), Columns({ 0 }));

	scanner.SetProjectedColumns({ false, true, false, true });
	ASSERT_EQ(scanner._projcols(), Columns({ 1, 3 }));
}

TEST(Scanner, SetOrderKeys)
{
	mars::test::Scanner scanner(nullptr);

	//-----------------------------------------------------------------------
	scanner.SetStoreOrder({ }, { });

	// no scan order: just keep native orders
	scanner.SetOrderKeys({ });
	ASSERT_EQ(scanner._scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// table-level reorder is not supported yet, and will raise a postgres
	// assert failure, so we could not test it.

	//-----------------------------------------------------------------------
	scanner.SetStoreOrder({ 1, 2 }, { });

	// no scan order: just keep native orders
	scanner.SetOrderKeys({ });
	ASSERT_EQ(scanner._scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// table-level reorder is not supported yet, and will raise a postgres
	// assert failure, so we could not test it.

	//-----------------------------------------------------------------------
	scanner.SetStoreOrder({ }, { 1, 2 });

	// no scan order: just keep native orders
	scanner.SetOrderKeys({ });
	ASSERT_EQ(scanner._scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is fully equal to table store order: block reorder
	scanner.SetOrderKeys({ 1, 2 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), true);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is partially equal to table store order: block reorder
	scanner.SetOrderKeys({ 1 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), true);
	ASSERT_EQ(scanner._table_reorder(), false);

	//-----------------------------------------------------------------------
	scanner.SetStoreOrder({ 1, 2 }, { 1, 2 });

	// no scan order: just keep native orders
	scanner.SetOrderKeys({ });
	ASSERT_EQ(scanner._scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is fully equal to table store order: just keep native orders
	scanner.SetOrderKeys({ 1, 2 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is partially equal to table store order: keep native orders
	scanner.SetOrderKeys({ 1 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	//-----------------------------------------------------------------------
	scanner.SetStoreOrder({ 2, 1 }, { 1, 2 });

	// no scan order: just keep native orders
	scanner.SetOrderKeys({ });
	ASSERT_EQ(scanner._scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 2, 1 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is fully equal to table store order: block reorder
	scanner.SetOrderKeys({ 1, 2 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), true);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is partially equal to table store order: block reorder
	scanner.SetOrderKeys({ 1 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), true);
	ASSERT_EQ(scanner._table_reorder(), false);

	//-----------------------------------------------------------------------
	scanner.SetStoreOrder({ 1, 2 }, { 1 });

	// no scan order: just keep native orders
	scanner.SetOrderKeys({ });
	ASSERT_EQ(scanner._scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is equal to table store order: just keep native orders
	scanner.SetOrderKeys({ 1 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// table-level reorder is not supported yet, and will raise a postgres
	// assert failure, so we could not test it.

	//-----------------------------------------------------------------------
	scanner.SetStoreOrder({ 1, 2 }, { 2 });

	// no scan order: just keep native orders
	scanner.SetOrderKeys({ });
	ASSERT_EQ(scanner._scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 2 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is fully equal to table store order: block reorder
	scanner.SetOrderKeys({ 2 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 2 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 2 }));
	ASSERT_EQ(scanner._block_reorder(), true);
	ASSERT_EQ(scanner._table_reorder(), false);

	//-----------------------------------------------------------------------
	scanner.SetStoreOrder({ 1 }, { 1, 2 });

	// no scan order: just keep native orders
	scanner.SetOrderKeys({ });
	ASSERT_EQ(scanner._scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is equal to block store order: just keep native orders
	scanner.SetOrderKeys({ 1 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is equal to table store order: block reorder
	scanner.SetOrderKeys({ 1, 2 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), true);
	ASSERT_EQ(scanner._table_reorder(), false);

	//-----------------------------------------------------------------------
	scanner.SetStoreOrder({ 2 }, { 1, 2 });

	// no scan order: just keep native orders
	scanner.SetOrderKeys({ });
	ASSERT_EQ(scanner._scan_order(), Attrs({ }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), false);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is fully equal to table store order: block reorder
	scanner.SetOrderKeys({ 1, 2 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), true);
	ASSERT_EQ(scanner._table_reorder(), false);

	// scan order is partially equal to table store order: block reorder
	scanner.SetOrderKeys({ 1 });
	ASSERT_EQ(scanner._scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._block_scan_order(), Attrs({ 1 }));
	ASSERT_EQ(scanner._table_scan_order(), Attrs({ 1, 2 }));
	ASSERT_EQ(scanner._block_reorder(), true);
	ASSERT_EQ(scanner._table_reorder(), false);
}

TYPED_TEST(ScannerTypedTest, SetScanKeys)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;

	mars::test::Scanner scanner(nullptr);
	ScanKeys scankeys;
	ScanKeys scankeys_copy;

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;
	auto v1 = T::ToDatum(x1);
	auto v2 = T::ToDatum(x2);
	auto v3 = T::ToDatum(x3);

	// SetScanKeys() does not do much useful things itself, it only stores the
	// original scankeys and arraykeys.  The real jobs are done in
	// OptimizeScanKeys().

	scankeys = { };
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	ASSERT_EQ(scanner._scankeys(), ScanKeys({ }));
	ASSERT_EQ(scanner._arraykeys(), nullptr);

	scankeys = {
		ScanKeyTest::MakeScanKey(">", x2, 4), // c4 > 2
		ScanKeyTest::MakeScanKey(">", x2, 4), // c4 > 2, a duplicate
		ScanKeyTest::MakeScanKey("=", x1, 1), // c1 = 1
		ScanKeyTest::MakeScanKey("<", x3, 3), // c3 < 3
		ScanKeyTest::MakeScanKey("=", x2, 2), // c2 = 2
		ScanKeyTest::MakeScanKey(">", x3, 3), // c3 > 3, a paradox
	};

	::Datum elem_values[] = { v1, v2, v3 };
	bool elem_nulls[] = { false, false, false };

	// c1 in ( 1, 2, 3 )
	::IndexArrayKeyInfo arraykey_block = {
		.scan_key    = &scankeys[2]->scankey_,
		.array_expr  = nullptr,
		.next_elem   = 1,
		.num_elems   = 3,
		.elem_values = elem_values,
		.elem_nulls  = elem_nulls,
	};
	scankeys[2]->BindArrayKey(&arraykey_block);

	// c2 in ( 1, 2, 3 )
	::IndexArrayKeyInfo arraykey_table = {
		.scan_key    = &scankeys[4]->scankey_,
		.array_expr  = nullptr,
		.next_elem   = 1,
		.num_elems   = 3,
		.elem_values = elem_values,
		.elem_nulls  = elem_nulls,
	};
	scankeys[4]->BindArrayKey(&arraykey_table);

	auto arraykeys = std::make_shared<std::vector<::IndexArrayKeyInfo>>();
	arraykeys->push_back(arraykey_block);
	arraykeys->push_back(arraykey_table);
	auto arraykeys_copy = arraykeys;

	// conflict and duplicate keys will not be checked at this step, however
	// the keys will be sorted by attno.  the array keys/filters will not be
	// stored in the scankeys.
	scankeys_copy = scankeys;
	scanner.SetScanKeys(scankeys_copy, nullptr, nullptr);
	ASSERT_EQ(scanner._scankeys().at(0)->scankey_.sk_attno, 3);
	ASSERT_EQ(scanner._scankeys().at(1)->scankey_.sk_attno, 3);
	ASSERT_EQ(scanner._scankeys().at(2)->scankey_.sk_attno, 4);
	ASSERT_EQ(scanner._scankeys().at(3)->scankey_.sk_attno, 4);
	ASSERT_EQ(scanner._arraykeys(), nullptr);

	// arraykeys are only meaningful with proper store orders.
	// at the moment we allow at most one array at block level.
	// the arraykeys can only be used as array filters without corresponding
	// scan orders.
	scankeys_copy = scankeys;
	scanner.SetStoreOrder({ 1 }, { 2 });
	scanner.SetOrderKeys({ 2 });
	arraykeys = arraykeys_copy;
	scanner.SetScanKeys(scankeys_copy, std::move(arraykeys), nullptr);
	ASSERT_NE(scanner._arraykeys(), nullptr);
	ASSERT_FALSE(scanner._filters().empty());
	ASSERT_EQ(scanner._block_arraykey(), nullptr);
	ASSERT_NE(scanner._table_arraykey(), nullptr);
	// with the scan orders some of the arraykeys can be actual array keys.
	scankeys_copy = scankeys;
	scanner.SetStoreOrder({ 1 }, { 2 });
	scanner.SetOrderKeys({ 1 });
	arraykeys = arraykeys_copy;
	scanner.SetScanKeys(scankeys_copy, std::move(arraykeys), nullptr);
	ASSERT_NE(scanner._arraykeys(), nullptr);
	ASSERT_FALSE(scanner._filters().empty());
	ASSERT_NE(scanner._block_arraykey(), nullptr);
	ASSERT_EQ(scanner._table_arraykey(), nullptr);
	ASSERT_EQ(&scanner._block_arraykey()->scankey_, arraykey_block.scan_key);

	// at the moment we allow at most one array at table level.
	scankeys_copy = scankeys;
	scanner.SetStoreOrder({ }, { 2 });
	scanner.SetOrderKeys({ 2 });
	arraykeys = arraykeys_copy;
	scanner.SetScanKeys(scankeys_copy, std::move(arraykeys), nullptr);
	ASSERT_NE(scanner._arraykeys(), nullptr);
	ASSERT_EQ(scanner._block_arraykey(), nullptr);
	ASSERT_NE(scanner._table_arraykey(), nullptr);
	ASSERT_EQ(&scanner._table_arraykey()->scankey_, arraykey_table.scan_key);

	// when an arraykey is suitable for both block and table levels, it will
	// only be handled at the table level.
	scankeys_copy = scankeys;
	scanner.SetStoreOrder({ 2 }, { 2 });
	scanner.SetOrderKeys({ 2 });
	arraykeys = arraykeys_copy;
	scanner.SetScanKeys(scankeys_copy, std::move(arraykeys), nullptr);
	ASSERT_NE(scanner._arraykeys(), nullptr);
	ASSERT_EQ(scanner._block_arraykey(), nullptr);
	ASSERT_NE(scanner._table_arraykey(), nullptr);
	ASSERT_EQ(&scanner._table_arraykey()->scankey_, arraykey_table.scan_key);
}

TYPED_TEST(ScannerTypedTest, OptimizeScanKeys)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;

	mars::test::Scanner scanner(nullptr);
	ScanKeys scankeys;

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;

	// to execute OptimizeScanKeys(), the store order, the scan order, and the
	// scankeys, must be set prior to that.
	scankeys = { };
	scanner.SetStoreOrder({ }, { });
	scanner.SetOrderKeys({ });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), false);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), false);

	// paradox keys will result in a NeverMatch flag.
	scankeys = {
		ScanKeyTest::MakeScanKey("=", x1, 1),
		ScanKeyTest::MakeScanKey("=", x2, 1),
	};
	scanner.SetStoreOrder({ }, { });
	scanner.SetOrderKeys({ });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), true);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), true);

	// duplicated keys can be reduced.
	scankeys = {
		ScanKeyTest::MakeScanKey("=", x1, 1),
		ScanKeyTest::MakeScanKey("=", x1, 1),
	};
	scanner.SetStoreOrder({ }, { });
	scanner.SetOrderKeys({ });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), false);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), false);
	ASSERT_EQ(scanner._block_keys().size(), 0);
	ASSERT_EQ(scanner._block_filters().size(), 1);

	// redundant keys can be merged
	scankeys = {
		ScanKeyTest::MakeScanKey(">", x1, 1),
		ScanKeyTest::MakeScanKey(">", x2, 1),
	};
	scanner.SetStoreOrder({ }, { });
	scanner.SetOrderKeys({ });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), false);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), false);
	ASSERT_EQ(scanner._block_keys().size(), 0);
	ASSERT_EQ(scanner._block_filters().size(), 1);

	// keys should be classified as keys and filters.
	scankeys = {
		ScanKeyTest::MakeScanKey(">", x1, 1),
		ScanKeyTest::MakeScanKey("<", x3, 1),
		ScanKeyTest::MakeScanKey(">", x1, 2),
		ScanKeyTest::MakeScanKey("<", x3, 2),
		ScanKeyTest::MakeScanKey(">", x1, 3),
		ScanKeyTest::MakeScanKey("<", x3, 3),
	};

	scanner.SetStoreOrder({ 2, 1 }, { 1 });
	scanner.SetOrderKeys({ 1 });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), false);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), false);
	ASSERT_EQ(scanner._block_keys().size(), 4);
	ASSERT_EQ(scanner._block_filters().size(), 2);
	ASSERT_EQ(scanner._block_eok().primary, 2);
	ASSERT_EQ(scanner._table_keys().size(), 2);
	ASSERT_EQ(scanner._table_filters().size(), 4);
	ASSERT_EQ(scanner._table_eok().primary, 2);
}

TYPED_TEST(ScannerTypedTest, ScanRows_Unordered)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	mars::test::Scanner scanner(nullptr);
	ScanKeys scankeys;

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;

	auto &cstores = scanner._column_stores();
	cstores.resize(1);

	std::vector<typename T::c_type> values = { x1, x2, x1, x2, x3, x3 };
	std::vector<int16_t> isnull(values.size(), 1);
	auto &cstore = cstores[0];
	cstore = ColumnStoreTest::Make(values, isnull);

	scankeys = {
		ScanKeyTest::MakeScanKey(">", x1, 1),
		ScanKeyTest::MakeScanKey("<", x3, 1),
	};
	scanner.SetScanDir(::ForwardScanDirection);
	scanner.SetStoreOrder({ }, { });
	scanner.SetOrderKeys({ });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), false);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), false);

	// first scan with the high-level PostFilter() api.
	{
		int nrows = values.size();

		// reorder is not needed
		scanner.ReorderBlock(nrows);
		ASSERT_EQ(scanner._remap().empty(), true);
		ASSERT_EQ(scanner._stage(), POSTFILTER);

		RowSlice rows(ForwardScanDirection, nrows);

		scanner.PostFilter(rows);
		ASSERT_EQ(rows.GetPosition(), 1);

		rows.Next();
		scanner.PostFilter(rows);
		ASSERT_EQ(rows.GetPosition(), 3);

		rows.Next();
		scanner.PostFilter(rows);
		ASSERT_FALSE(rows.HasNext());
	}

	// next we scan with the *MatchRow() api.
	{
		int nrows = values.size();

		// reorder is not needed
		scanner.ReorderBlock(nrows);
		ASSERT_EQ(scanner._remap().empty(), true);
		ASSERT_EQ(scanner._stage(), POSTFILTER);

		RowSlice rows(ForwardScanDirection, nrows);

		// seek the first match
		scanner.FirstMatchRow(rows);
		ASSERT_EQ(rows.GetPosition(), 1);
		ASSERT_EQ(scanner.MatchRow(rows.GetPosition()), MATCH);

		// next row does not match
		rows.Next();
		ASSERT_EQ(scanner.MatchRow(rows.GetPosition()), MISMATCH_FILTER);
		// seek the next match
		scanner.NextMatchRow(rows);
		ASSERT_EQ(rows.GetPosition(), 3);
		ASSERT_EQ(scanner.MatchRow(rows.GetPosition()), MATCH);

		// next row does not match
		rows.Next();
		ASSERT_EQ(scanner.MatchRow(rows.GetPosition()), MISMATCH_FILTER);
		// and no more match
		scanner.NextMatchRow(rows);
		ASSERT_FALSE(rows.HasNext());
	}
}

TYPED_TEST(ScannerTypedTest, ScanRows_Ordered)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	mars::test::Scanner scanner(nullptr);
	ScanKeys scankeys;

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;

	auto &cstores = scanner._column_stores();
	cstores.resize(1);

	std::vector<typename T::c_type> values = { x1, x1, x2, x2, x3, x3 };
	std::vector<int16_t> isnull(values.size(), 1);
	auto &cstore = cstores[0];
	cstore = ColumnStoreTest::Make(values, isnull);

	scankeys = {
		ScanKeyTest::MakeScanKey(">", x1, 1),
		ScanKeyTest::MakeScanKey("<", x3, 1),
	};
	scanner.SetScanDir(::ForwardScanDirection);
	scanner.SetStoreOrder({ 1 }, { 1 });
	scanner.SetOrderKeys({ 1 });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), false);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), false);

	// first scan with the high-level PostFilter() api.
	{
		int nrows = values.size();

		// reorder is not needed
		scanner.ReorderBlock(nrows);
		ASSERT_EQ(scanner._remap().empty(), true);
		ASSERT_EQ(scanner._stage(), POSTFILTER);

		RowSlice rows(ForwardScanDirection, nrows);

		scanner.PostFilter(rows);
		ASSERT_EQ(rows.GetPosition(), 2);

		rows.Next();
		scanner.PostFilter(rows);
		ASSERT_EQ(rows.GetPosition(), 3);

		rows.Next();
		scanner.PostFilter(rows);
		ASSERT_EQ(rows.HasNext(), false);
	}

	// next we scan with the *MatchRow() api.
	{
		int nrows = values.size();

		// reorder is not needed
		scanner.ReorderBlock(nrows);
		ASSERT_EQ(scanner._remap().empty(), true);
		ASSERT_EQ(scanner._stage(), POSTFILTER);

		RowSlice rows(ForwardScanDirection, nrows);

		// seek the first match
		scanner.FirstMatchRow(rows);
		ASSERT_EQ(rows.GetPosition(), 2);
		ASSERT_EQ(scanner.MatchRow(rows.GetPosition()), MATCH);

		// next row also matches
		rows.Next();
		ASSERT_EQ(scanner.MatchRow(rows.GetPosition()), MATCH);
		// no row is skipped
		scanner.NextMatchRow(rows);
		ASSERT_EQ(rows.GetPosition(), 3);
		ASSERT_EQ(scanner.MatchRow(rows.GetPosition()), MATCH);

		// next row does not match
		rows.Next();
		ASSERT_EQ(scanner.MatchRow(rows.GetPosition()), MISMATCH_FIRST_ORDERKEY);
		// and no more match
		scanner.NextMatchRow(rows);
		ASSERT_EQ(rows.HasNext(), false);
	}
}

TYPED_TEST(ScannerTypedTest, ScanRows_Reordered)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;

	mars::test::Scanner scanner(nullptr);
	ScanKeys scankeys;

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;

	auto &cstores = scanner._column_stores();
	cstores.resize(1);

	std::vector<typename T::c_type> values = { x1, x2, x1, x2, x3, x3 };
	std::vector<int16_t> isnull(values.size(), 1);
	auto &cstore = cstores[0];
	cstore = ColumnStoreTest::Make(values, isnull);

	scankeys = {
		ScanKeyTest::MakeScanKey(">", x1, 1),
		ScanKeyTest::MakeScanKey("<", x3, 1),
	};
	scanner.SetScanDir(::ForwardScanDirection);
	scanner.SetStoreOrder({ 2 }, { 1 });
	scanner.SetOrderKeys({ 1 });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), false);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), false);

	// first scan with the high-level PostFilter() api.
	{
		int nrows = values.size();

		// reorder is needed
		scanner.ReorderBlock(nrows);
		// the remapped values are sorted, only they are already filtered
		ASSERT_EQ(nrows, 2);
		ASSERT_EQ(scanner._remap().size(), 2);
		ASSERT_EQ(scanner._stage(), FILTERED);

		RowSlice rows(ForwardScanDirection, nrows);

		// postfilter is no longer needed, calling it is a no-op
		scanner.PostFilter(rows);
		ASSERT_EQ(rows.GetPosition(), 0);

		rows.Next();
		scanner.PostFilter(rows);
		ASSERT_EQ(rows.GetPosition(), 1);

		rows.Next();
		scanner.PostFilter(rows);
		ASSERT_EQ(rows.HasNext(), false);
	}

	// the *MatchRow() api does not work for reordered data
}

TYPED_TEST(ScannerTypedTest, ScanBlocks_Unordered)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;
	using FooterTest = FooterTypedTest<typename T::c_type>;

	mars::test::Scanner scanner(nullptr);
	ScanKeys scankeys;

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;

	int column = 0;
	std::vector<mars::footer::Footer> footers(5);

	FooterTest::SetMinMax(footers[0], column, x1, x2);
	FooterTest::SetMinMax(footers[1], column, x1, x1);
	FooterTest::SetMinMax(footers[2], column, x2, x2);
	FooterTest::SetMinMax(footers[3], column, x2, x3);
	FooterTest::SetMinMax(footers[4], column, x3, x3);
	FooterTest::SetRowCount(footers[0], column, 100, 0);
	FooterTest::SetRowCount(footers[1], column, 100, 0);
	FooterTest::SetRowCount(footers[2], column, 100, 0);
	FooterTest::SetRowCount(footers[3], column, 100, 0);
	FooterTest::SetRowCount(footers[4], column, 100, 0);

	scankeys = {
		ScanKeyTest::MakeScanKey(">", x1, 1),
		ScanKeyTest::MakeScanKey("<", x3, 1),
	};
	scanner.SetScanDir(::ForwardScanDirection);
	scanner.SetStoreOrder({ }, { });
	scanner.SetOrderKeys({ });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), false);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), false);

	auto begin = footers.cbegin();
	auto end = footers.cend();

	BlockSlice blocks(::ForwardScanDirection, begin, end);

	// seek the first match
	scanner.FirstMatchBlock(blocks);
	ASSERT_EQ(blocks.GetPosition() - begin, 0);
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_MAYBE_MATCH);

	// next block does not match
	blocks.Next();
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_MISMATCH);
	// seek the next match
	scanner.NextMatchBlock(blocks);
	ASSERT_EQ(blocks.GetPosition() - begin, 2);
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_FULL_MATCH);

	// next block also matches
	blocks.Next();
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_MAYBE_MATCH);
	// no block is skipped
	scanner.NextMatchBlock(blocks);
	ASSERT_EQ(blocks.GetPosition() - begin, 3);
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_MAYBE_MATCH);

	// next block does no match
	blocks.Next();
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_MISMATCH);
	// and no more match
	scanner.NextMatchBlock(blocks);
	ASSERT_TRUE(blocks.IsEmpty());
}

TYPED_TEST(ScannerTypedTest, ScanBlocks_Ordered)
{
	using T = mars::pg_type_traits<mars::test::type_traits<TypeParam>::type_oid>;
	using ScanKeyTest = ScanKeyTypedTest<typename T::c_type>;
	using ColumnStoreTest = ColumnStoreTypedTest<typename T::c_type>;
	using FooterTest = FooterTypedTest<typename T::c_type>;

	mars::test::Scanner scanner(nullptr);
	ScanKeys scankeys;

	typename T::c_type x1 = 1;
	typename T::c_type x2 = 2;
	typename T::c_type x3 = 3;

	int column = 0;
	std::vector<mars::footer::Footer> footers(5);

	FooterTest::SetMinMax(footers[0], column, x1, x1);
	FooterTest::SetMinMax(footers[1], column, x1, x2);
	FooterTest::SetMinMax(footers[2], column, x2, x2);
	FooterTest::SetMinMax(footers[3], column, x2, x3);
	FooterTest::SetMinMax(footers[4], column, x3, x3);
	FooterTest::SetRowCount(footers[0], column, 100, 0);
	FooterTest::SetRowCount(footers[1], column, 100, 0);
	FooterTest::SetRowCount(footers[2], column, 100, 0);
	FooterTest::SetRowCount(footers[3], column, 100, 0);
	FooterTest::SetRowCount(footers[4], column, 100, 0);

	scankeys = {
		ScanKeyTest::MakeScanKey(">", x1, 1),
		ScanKeyTest::MakeScanKey("<", x3, 1),
	};
	scanner.SetScanDir(::ForwardScanDirection);
	scanner.SetStoreOrder({ }, { 1 });
	scanner.SetOrderKeys({ 1 });
	scanner.SetScanKeys(scankeys, nullptr, nullptr);
	scanner.OptimizeScanKeys(true /* table_level */);
	ASSERT_EQ(scanner._table_never_match(), false);
	scanner.OptimizeScanKeys(false /* table_level */);
	ASSERT_EQ(scanner._block_never_match(), false);

	auto begin = footers.cbegin();
	auto end = footers.cend();

	BlockSlice blocks(::ForwardScanDirection, begin, end);

	// seek the first match
	scanner.FirstMatchBlock(blocks);
	ASSERT_EQ(blocks.GetPosition() - begin, 1);
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_MAYBE_MATCH);

	// next block also matches
	blocks.Next();
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_FULL_MATCH);
	// no block is skipped
	scanner.NextMatchBlock(blocks);
	ASSERT_EQ(blocks.GetPosition() - begin, 2);
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_FULL_MATCH);

	// next block also matches
	blocks.Next();
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_MAYBE_MATCH);
	// no block is skipped
	scanner.NextMatchBlock(blocks);
	ASSERT_EQ(blocks.GetPosition() - begin, 3);
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), BLOCK_MAYBE_MATCH);

	// next block does no match
	blocks.Next();
	ASSERT_EQ(scanner.MatchBlock(blocks.GetBlock()), MISMATCH_FIRST_ORDERKEY);
	// and no more match
	scanner.NextMatchBlock(blocks);
	ASSERT_TRUE(blocks.IsEmpty());
}

} // namespace test
} // namespace mars

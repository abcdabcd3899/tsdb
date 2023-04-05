/*-------------------------------------------------------------------------
 *
 * Scanner_test.h
 *	  TODO file description
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_SCANNER_TEST_H
#define MARS_SCANNER_TEST_H

#include "gtest/gtest.h"
#include <memory>

#include "mock.h"
#include "type_traits.hpp"
#include "../Scanner.h"
#include "../ScanKey.h"

namespace mars
{
	namespace test
	{

		class Scanner : public mars::Scanner
		{
		public:
			using Attrs = std::vector<::AttrNumber>;
			using Columns = std::vector<int>;
			using ScanKeys = std::vector<std::shared_ptr<mars::ScanKey>>;
			using ArrayKeys = std::shared_ptr<std::vector<::IndexArrayKeyInfo>>;
			using ColumnStores = std::vector<std::shared_ptr<ColumnStore>>;

			Scanner(::Relation rel) : mars::Scanner(rel) {}

			Attrs &_block_store_order() { return block_store_order_; }
			Attrs &_table_store_order() { return table_store_order_; }

			Columns &_projcols() { return projcols_; }

			Attrs &_scan_order() { return scan_order_; }
			Attrs &_block_scan_order() { return block_scan_order_; }
			Attrs &_table_scan_order() { return table_scan_order_; }
			bool &_block_reorder() { return block_reorder_; }
			bool &_table_reorder() { return table_reorder_; }

			ScanKeys &_scankeys() { return scankeys_; }
			ScanKeys &_filters() { return filters_; }
			ScanKeys &_block_keys() { return block_keys_; }
			ScanKeys &_block_filters() { return block_filters_; }
			ScanKeys &_table_keys() { return table_keys_; }
			ScanKeys &_table_filters() { return table_filters_; }

			ArrayKeys &_arraykeys() { return arraykeys_; }
			std::shared_ptr<mars::ScanKey> &_block_arraykey() { return block_arraykey_; }
			std::shared_ptr<mars::ScanKey> &_table_arraykey() { return table_arraykey_; }

			mars::Scanner::EndOfKey &_block_eok() { return block_eok_; }
			mars::Scanner::EndOfKey &_table_eok() { return table_eok_; }

			bool &_block_never_match() { return block_never_match_; }
			bool &_table_never_match() { return table_never_match_; }

			ColumnStores &_column_stores() { return column_stores_; }
			std::shared_ptr<LogicalRowStore> &_row_store() { return row_store_; }
			std::vector<int> &_remap() { return remap_; }
			Stage &_stage() { return stage_; }
		};

		template <typename c_type>
		class ScannerTypedTest : public ::testing::Test
		{
		public:
			using T = mars::pg_type_traits<mars::test::type_traits<c_type>::type_oid>;
		};

	} // namespace test
} // namespace mars

#endif /* MARS_SCANNER_TEST_H */

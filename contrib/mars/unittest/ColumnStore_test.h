/*-------------------------------------------------------------------------
 *
 * ColumnStore_test.h
 *	  TODO file description
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_COLUMNSTORE_TEST_H
#define MARS_COLUMNSTORE_TEST_H

#include "gtest/gtest.h"

#include "mock.h"
#include "type_traits.hpp"
#include "../ColumnStore.h"

#include <memory>

namespace mars
{
	namespace test
	{

		template <typename c_type>
		class ColumnStoreTypedTest : public ::testing::Test
		{
		public:
			using T = mars::pg_type_traits<mars::test::type_traits<c_type>::type_oid>;

			static std::shared_ptr<mars::TypedColumnStore<T::type_oid>>
			Make(const std::vector<typename T::c_type> &values = {},
				 const std::vector<int16_t> isnull = {})
			{
				auto cstore = std::make_shared<mars::TypedColumnStore<T::type_oid>>();
				cstore->SetPoints(values, isnull);
				return std::move(cstore);
			}
		};

	} // namespace test
} // namespace mars

#endif /* MARS_COLUMNSTORE_TEST_H */

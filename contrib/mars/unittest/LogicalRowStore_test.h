/*-------------------------------------------------------------------------
 *
 * LogicalRowStore_test.h
 *	  TODO file description
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_LOGICALROWSTORE_TEST_H
#define MARS_LOGICALROWSTORE_TEST_H

#include "gtest/gtest.h"

#include "mock.h"
#include "type_traits.hpp"
#include "../LogicalRowStore.h"

#include <memory>
#include <vector>

namespace mars
{
	namespace test
	{

		template <typename c_type>
		class LogicalRowStoreTypedTest : public ::testing::Test
		{
		public:
			using T = mars::pg_type_traits<mars::test::type_traits<c_type>::type_oid>;
		};

	} // namespace test
} // namespace mars

#endif /* MARS_LOGICALROWSTORE_TEST_H */

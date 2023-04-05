/*-------------------------------------------------------------------------
 *
 * ScanKey_test.h
 *	  TODO file description
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_SCANKEY_TEST_H
#define MARS_SCANKEY_TEST_H

#include "gtest/gtest.h"

#include "mock.h"
#include "type_traits.hpp"
#include "../ScanKey.h"

#include <memory>
#include <unordered_map>

namespace mars
{
	namespace test
	{

		template <typename c_type>
		class ScanKeyTypedTest : public ::testing::Test
		{
		public:
			using T = mars::pg_type_traits<mars::test::type_traits<c_type>::type_oid>;

			static void SetScanKey(mars::ScanKey &key, const char *op,
								   typename T::c_type argument,
								   ::AttrNumber attno = 1)
			{
				static const std::unordered_map<std::string, ::StrategyNumber> conv{
					{"<", BTLessStrategyNumber},
					{"<=", BTLessEqualStrategyNumber},
					{"=", BTEqualStrategyNumber},
					{">=", BTGreaterEqualStrategyNumber},
					{">", BTGreaterStrategyNumber},
				};
				auto strategy = conv.find(op)->second;

				key.scankey_.sk_strategy = strategy;
				key.scankey_.sk_argument = T::ToDatum(argument);
				key.scankey_.sk_attno = attno;
			}

			static std::shared_ptr<mars::ScanKey> MakeScanKey(const char *op,
															  typename T::c_type argument,
															  ::AttrNumber attno = 1)
			{
				auto key = mars::ScanKey::Make(T::type_oid, T::type_oid);
				SetScanKey(*key, op, argument, attno);
				return key;
			}
		};

	} // namespace test
} // namespace mars

#endif /* MARS_SCANKEY_TEST_H */

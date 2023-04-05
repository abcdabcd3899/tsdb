/*-------------------------------------------------------------------------
 *
 * Footer_test.h
 *	  TODO file description
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_FOOTER_TEST_H
#define MARS_FOOTER_TEST_H

#include "gtest/gtest.h"

#include "mock.h"
#include "type_traits.hpp"
#include "../Footer.h"

namespace mars
{
	namespace test
	{

		template <typename c_type>
		class FooterTypedTest : public ::testing::Test
		{
		public:
			using T = mars::pg_type_traits<mars::test::type_traits<c_type>::type_oid>;

			static void SetMinMax(mars::footer::Footer &footer, int column,
								  typename T::c_type min, typename T::c_type max)
			{
				footer.rgPreAggVec_.resize(1);
				footer.GetMinDatum(column) = T::ToDatum(min);
				footer.GetMaxDatum(column) = T::ToDatum(max);
			}

			/*!
			 * @param row_count The total row count, including null values.
			 * @param null_count The null count.
			 */
			static void SetRowCount(mars::footer::Footer &footer, int column,
									int row_count, int null_count = 0)
			{
				footer.rgPreAggVec_.resize(1);
				footer.rgPreAggVec_[0].rowcount = row_count;
				footer.rgPreAggVec_[0].null_count.resize(column + 1);
				footer.rgPreAggVec_[0].null_count[column] = null_count;
			}
		};

	} // namespace test
} // namespace mars

#endif /* MARS_FOOTER_TEST_H */

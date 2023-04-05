/*-------------------------------------------------------------------------
 *
 * contrib/mars/utils.h
 *	  utilities.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef __MARS_UTILS_H__
#define __MARS_UTILS_H__

#include "wrapper.hpp"

#include <vector>

namespace mars
{
	namespace utils
	{

		std::vector<::AttrNumber> ProcessInferenceElems(::List *infer_elems);

		void GetRelOptions(::Oid relid, std::vector<::MarsDimensionAttr> *groupkeys,
						   std::vector<::MarsDimensionAttr> *global_orderkeys,
						   std::vector<::MarsDimensionAttr> *local_orderkeys);

		std::vector<::AttrNumber> DecideGroupKeyStoreOrder(::Oid relid);
		std::vector<::AttrNumber> DecideLocalStoreOrder(::Oid relid);

	} // namespace utils
} // namespace mars

#endif /* __MARS_UTILS_H__ */

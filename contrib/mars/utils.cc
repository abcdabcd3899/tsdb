/*-------------------------------------------------------------------------
 *
 * contrib/mars/utils.cc
 *	  utilities.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 *-------------------------------------------------------------------------
 */
#include "utils.h"

namespace mars
{
	namespace utils
	{

		std::vector<::AttrNumber> ProcessInferenceElems(::List *infer_elems)
		{
			int i;
			::ListCell *lc;
			std::vector<::AttrNumber> mergeby(::list_length(infer_elems));

			foreach_with_count(lc, infer_elems, i)
			{
				::InferenceElem *elem = lfirst_node(InferenceElem, lc);
				Var *v = castNode(Var, elem->expr);
				mergeby[i] = v->varattno;
			}

			return mergeby;
		}

		static void
		FillVecotrByList(std::vector<::MarsDimensionAttr> *dest, ::List *source)
		{
			dest->clear();

			ListCell *lc;
			foreach (lc, source)
			{
				MarsDimensionAttr *attr = lfirst_node(MarsDimensionAttr, lc);
				dest->push_back(*attr);
			}
		}

		void
		GetRelOptions(::Oid relid, std::vector<::MarsDimensionAttr> *groupkeys,
					  std::vector<::MarsDimensionAttr> *global_orderkeys,
					  std::vector<::MarsDimensionAttr> *local_orderkeys)
		{
			::List *groupkeys_list = nullptr;
			::List *global_orderkeys_list = nullptr;
			::List *local_orderkeys_list = nullptr;

			::GetMARSEntryOption(relid,
								 groupkeys ? &groupkeys_list : nullptr,
								 global_orderkeys ? &global_orderkeys_list : nullptr,
								 local_orderkeys ? &local_orderkeys_list : nullptr);

			if (groupkeys)
			{
				FillVecotrByList(groupkeys, groupkeys_list);
			}

			if (global_orderkeys)
			{
				FillVecotrByList(global_orderkeys, global_orderkeys_list);
			}

			if (local_orderkeys)
			{
				FillVecotrByList(local_orderkeys, local_orderkeys_list);
			}
		}

		std::vector<::AttrNumber>
		DecideGroupKeyStoreOrder(::Oid relid)
		{
			std::vector<::AttrNumber> order;

			std::vector<::MarsDimensionAttr> groupkeys;

			GetRelOptions(relid, &groupkeys,
						  nullptr /* global_orderkeys */,
						  nullptr /* local_orderkeys */);

			order.reserve(groupkeys.size());

			for (const auto &key : groupkeys)
			{
				// try to store with most detailed order, so ignore the intervals
				order.push_back(key.attnum);
			}

			return order;
		}

		std::vector<::AttrNumber>
		DecideLocalStoreOrder(::Oid relid)
		{
			std::vector<::AttrNumber> order;

			std::vector<::MarsDimensionAttr> groupkeys;
			std::vector<::MarsDimensionAttr> global_orderkeys;
			std::vector<::MarsDimensionAttr> local_orderkeys;

			GetRelOptions(relid, &groupkeys, &global_orderkeys, &local_orderkeys);

			const std::vector<::MarsDimensionAttr> *orderkeys =
				(!local_orderkeys.empty() ? &local_orderkeys : !global_orderkeys.empty() ? &global_orderkeys
														   : !groupkeys.empty()			 ? &groupkeys
																						 : nullptr);

			if (orderkeys)
			{
				order.reserve(orderkeys->size());

				for (const auto &key : *orderkeys)
				{
					// try to store with most detailed order, so ignore the intervals
					order.push_back(key.attnum);
				}
			}

			return order;
		}

	} // namespace utils
} // namespace mars

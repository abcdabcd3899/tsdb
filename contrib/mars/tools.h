/*-------------------------------------------------------------------------
 *
 * tools.h
 *	  The MARS debug tools.
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  mars/tools.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_TOOLS_H
#define MATRIXDB_TOOLS_H
#include "wrapper.hpp"
#include <string>

extern "C"
{

	struct ColumnWalkerContext
	{
		AttrNumber attrNumber;
	};

	bool extractcolumns_from_node(Relation rel, Node *expr, bool *cols, AttrNumber natts);

	bool column_walker(Node *node, ColumnWalkerContext *attr);

}; // extern "C"

namespace mars
{

	namespace tools
	{

		inline std::string
		to_string(__int128 __val)
		{
			std::string ret{};
			bool is_neg = false;
			if (__val < 0)
			{
				__val = 0 - __val;
				is_neg = true;
			}
			while (__val > 0)
			{
				char s = __val % 10 + '0';
				ret.insert(ret.begin(), s);
				__val = __val / 10;
			}

			if (is_neg)
			{
				ret.insert(ret.begin(), '-');
			}

			return ret;
		}

		inline std::string
		to_string(long double __val)
		{
			std::string ret{};
			__int128 hi = static_cast<__int128>(__val);
			long double lo = __val - hi;
			ret = to_string(hi);

			ret.append(1, '.');

			while (lo > 0)
			{
				auto one_digit = static_cast<int>(lo * 10);
				char c = '0' + one_digit;
				ret.append(1, c);
				lo = lo * 10.0 - (__int128)(lo * 10);
			}

			return ret;
		}

		Datum
			mars_preagg_sum(PG_FUNCTION_ARGS);

	} // namespace tools

} // namespace mars

#endif // MATRIXDB_TOOLS_H

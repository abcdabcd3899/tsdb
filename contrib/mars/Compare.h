/*-------------------------------------------------------------------------
 *
 * Compare.h
 *	  The MARS Comparison Helpers
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_COMPARE_H
#define MARS_COMPARE_H

#include "type_traits.hpp"

#include <vector>
#include <functional>

namespace mars
{

	/*!
	 * The base of compare helpers.
	 */
	class CompareBase
	{
	public:
		/*!
		 * Get the compare opcode for the given left & right types and strategy.
		 */
		static ::Oid GetCompareOpcode(::StrategyNumber strategy,
									  ::Oid lefttype, ::Oid righttype,
									  bool allow_missing = false);

		/*!
		 * The mappings from strategy number to opname.
		 *
		 * Note that StrategyNumber counts from 1.
		 */
		static const char *opnames[BTMaxStrategyNumber + 1];

	public:
		CompareBase() = delete;

		/*!
		 * Test "a OP b".
		 *
		 * The test is made via the postgres operator functions.
		 *
		 * @param a,b The datums to test, must not be null.
		 * @param op The operator stands for "<", "<=", "=", ">=", ">".
		 *
		 * @return The test result.
		 */
		bool test(::Datum a, mars::OP op, ::Datum b) const
		{
			Assert(comparable());
			auto fcinfo = fcinfos_[op];

			fcinfo->args[0].value = a;
			fcinfo->args[0].isnull = false;
			fcinfo->args[1].value = b;
			fcinfo->args[1].isnull = false;

			return ::DatumGetBool(FunctionCallInvoke(fcinfo));
		}

		bool equal(::Datum a, ::Datum b) const
		{
			if (a == b)
			{
				return true;
			}
			else if (typlen_ != 0 && ::datumIsEqual(a, b, byval_, typlen_))
			{
				return true;
			}
			else
			{
				return test(a, OP::EQ, b);
			}
		}

		bool comparable() const
		{
			return fcinfos_[OP::EQ] != nullptr;
		}

	protected:
		/*!
		 * The ctor.
		 *
		 * @param lefttype,righttype The type oids, can be InvalidOid.
		 * @param collation The collation.
		 *
		 * The comparator is not a valid one when either lefttype or righttype is
		 * InvalidOid.
		 */
		CompareBase(::Oid lefttype, ::Oid righttype, ::Oid collation);

		/*!
		 * The ctor.
		 *
		 * More robust comparison needs Form_pg_attribute as param.
		 * This ctor can do more internal checks.
		 *
		 * @param attr The attribute, can be null.
		 */
		CompareBase(const ::Form_pg_attribute attr);

		/*!
		 * Init comparison functions of the bound argument types.
		 *
		 * The members lefttype_, righttype_, and collation_, must be filled before
		 * calling this.
		 */
		void InitFuncs();

	public:
		const ::Oid lefttype_;	//<! The type oid of the left arg
		const ::Oid righttype_; //<! The type oid of the right arg
		const ::Oid collation_; //<! The collation
		const bool byval_;		//<! True if byval EQ is possible
		const int16 typlen_;	//<! The typlen when byval EQ is possible,
								// 0 means left type != right type

	protected:
		::FmgrInfo flinfos_[BTMaxStrategyNumber + 1];		  //<! pg comparison functions
		::FunctionCallInfo fcinfos_[BTMaxStrategyNumber + 1]; //<! call info
	};

	/*!
	 * Compare two datums of specific types.
	 */
	class DatumCompare : public CompareBase
	{
	public:
		DatumCompare() = delete;

		DatumCompare(::Oid lefttype, ::Oid righttype, ::Oid collation)
			: CompareBase(lefttype, righttype, collation)
		{
		}

		DatumCompare(const ::Form_pg_attribute attr)
			: CompareBase(attr)
		{
		}

		/*!
		 * A handy wrapper for the test() method.
		 *
		 * With this operator we can write "comp(a, EQ, b)" instead of
		 * "comp.test(a, EQ, b)", which is usually easier to read.
		 */
		bool operator()(::Datum a, mars::OP op, ::Datum b) const
		{
			return test(a, op, b);
		}

		/*!
		 * Compare function to be used by lower_bound(), upper_bound(), etc..
		 *
		 * @return "a < b".
		 */
		bool operator()(::Datum a, ::Datum b) const
		{
			return test(a, mars::OP::LT, b);
		}
	};

	/*!
	 * Compare with a custom getter function.
	 */
	template <typename T>
	class CustomCompare : public CompareBase
	{
	public:
		CustomCompare(::Oid lefttype, ::Oid righttype, ::Oid collation,
					  std::function<::Datum(const T &)> getter)
			: CompareBase(lefttype, righttype, collation), getter_(getter)
		{
		}

		CustomCompare(const ::Form_pg_attribute attr,
					  std::function<::Datum(const T &)> getter)
			: CompareBase(attr), getter_(getter)
		{
		}

		/*!
		 * Test "a < getter(b)", used by lower_bound(), upper_bound(), etc..
		 */
		bool operator()(::Datum a, const T &b) const
		{
			return test(a, mars::OP::LT, getter_(b));
		}

		/*!
		 * Test "getter(a) < b", used by lower_bound(), upper_bound(), etc..
		 */
		bool operator()(const T &a, ::Datum b) const
		{
			return test(getter_(a), mars::OP::LT, b);
		}

	protected:
		std::function<::Datum(const T &)> getter_; //<! The getter function.
	};

	class RelationCompare
	{
	public:
		RelationCompare(const TupleDesc tupdesc)
			: tupdesc_(tupdesc)
		{
			auto natts = tupdesc->natts;
			comps_.reserve(natts);

			for (int i = 0; i < natts; ++i)
			{
				comps_.emplace_back(&tupdesc->attrs[i]);
			}
		}

		const DatumCompare &GetComparator(int column) const
		{
			return comps_[column];
		}

		const ::Form_pg_attribute GetAttr(int column) const
		{
			return TupleDescAttr(tupdesc_, column);
		}

	protected:
		const TupleDesc tupdesc_;
		std::vector<DatumCompare> comps_;
	};

} // namespace mars

#endif /* MARS_COMPARE_H */

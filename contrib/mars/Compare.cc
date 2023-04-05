/*-------------------------------------------------------------------------
 *
 * Compare.cc
 *	  The MARS Comparison Helpers
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "Compare.h"

namespace mars
{

	const char *CompareBase::opnames[BTMaxStrategyNumber + 1]{
		nullptr, // InvalidStrategy
		"<",	 // OP::LT
		"<=",	 // OP::LE
		"=",	 // OP::EQ
		">=",	 // OP::GE
		">",	 // OP::GT
	};

	// Derived from oper() in file src/backend/parser/parse_oper.c
	static void
	FindSuitableOpNo(List *opname, ::Oid lefttype, ::Oid righttype, ::Oid *opno)
	{
		/* Get binary operators of given name */
		FuncCandidateList clist = OpernameGetCandidates(opname, 'b', false);

		/* No operators found? Then fail... */
		if (clist != NULL)
		{
			Oid inputOids[2];
			inputOids[0] = lefttype;
			inputOids[1] = righttype;
			mx_oper_select_candidate(2, inputOids, clist, opno);
		}
	}

	static ::Oid
	GetOper(List *opname, ::Oid lefttype, ::Oid righttype)
	{
		auto opno = ::OpernameGetOprid(opname, lefttype, righttype);

		if (!OidIsValid(opno))
		{
			FindSuitableOpNo(opname, lefttype, righttype, &opno);
		}

		return opno;
	}

	::Oid
	CompareBase::GetCompareOpcode(::StrategyNumber strategy,
								  ::Oid lefttype, ::Oid righttype,
								  bool allow_missing)
	{
		Assert(strategy >= OP::LT && strategy <= OP::GT);
		auto opname = const_cast<char *>(opnames[strategy]);
		auto opname_list = ::lcons(::makeString(opname), NULL);
		auto opno = GetOper(opname_list, lefttype, righttype);

		if (!OidIsValid(opno))
		{
			if (allow_missing)
			{
				return InvalidOid;
			}
			else
			{
				elog(ERROR, "mars: cannot find oper for %d %s %d",
					 lefttype, opname, righttype);
			}
		}

		auto opcode = ::get_opcode(opno);
		if (!OidIsValid(opcode))
		{
			if (allow_missing)
			{
				return InvalidOid;
			}
			else
			{
				elog(ERROR, "mars: cannot find oper func for %d %s %d",
					 lefttype, opname, righttype);
			}
		}

		return opcode;
	}

	void
	CompareBase::InitFuncs()
	{
		::StrategyNumber strategy;

		if (!OidIsValid(lefttype_) || !OidIsValid(righttype_))
		{
			// the caller does not know the types, leave the comparator
			// uninitialized.
			::memset(flinfos_, 0, sizeof(flinfos_));
			::memset(fcinfos_, 0, sizeof(fcinfos_));
			return;
		}

		// TODO: we should have a function cache.
		foreach_strategy(strategy)
		{
			auto opcode = GetCompareOpcode(strategy, lefttype_, righttype_);

			auto flinfo = &flinfos_[strategy];
			::fmgr_info(opcode, flinfo);

			// a compare function always takes 2 args
			auto fcinfo = (::FunctionCallInfo)palloc0(SizeForFunctionCallInfo(2));
			fcinfos_[strategy] = fcinfo;

			InitFunctionCallInfoData(*fcinfo, flinfo, 2, collation_,
									 NULL /* Context */, NULL /* Resultinfo */);
		}
	}

	CompareBase::CompareBase(::Oid lefttype, ::Oid righttype, ::Oid collation)
		: lefttype_(lefttype), righttype_(righttype), collation_(collation), byval_(lefttype == righttype && get_typbyval(lefttype)), typlen_(lefttype == righttype ? get_typlen(lefttype) : 0)
	{
		InitFuncs();
	}

	CompareBase::CompareBase(::Form_pg_attribute attr)
		: lefttype_(attr ? attr->atttypid : InvalidOid), righttype_(attr ? attr->atttypid : InvalidOid), collation_(attr ? attr->attcollation : InvalidOid), byval_(attr ? attr->attbyval : false), typlen_(attr ? attr->attlen : 0)
	{
		if (!attr)
		{
			// the caller does not know the types, leave the comparator
			// uninitialized.
			::memset(flinfos_, 0, sizeof(flinfos_));
			::memset(fcinfos_, 0, sizeof(fcinfos_));
			return;
		}

		// The implementation of the comparator is calling pg_operator's function.
		//
		// PG compares variable-length data is quite complex.
		//
		// For example, in PG, when a comparison happens between varchar and
		// varchar, PG will implicitly convert varchar to text.
		//
		// So Mars does not support comparison between variable-length data yet.
		//
		// In the future if we want to support comparison between variable-length
		// data, simulate PG's behavior is necessary.
		if (!attr->attbyval && !::InVarlenTypeAllowedList(attr->atttypid))
		{
			::memset(flinfos_, 0, sizeof(flinfos_));
			::memset(fcinfos_, 0, sizeof(fcinfos_));
			return;
		}

		InitFuncs();
	}

} // namespace mars

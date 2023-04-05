/*-------------------------------------------------------------------------
 *
 * contrib/mars/DirectCompare.h
 *	  Direct comparison functions.
 *
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef MARS_DIRECTCOMPARE_H
#define MARS_DIRECTCOMPARE_H

#include "type_traits.hpp"

namespace mars
{

	using mars::OP;

	template <::Oid type_oid, mars::OP op>
	struct DirectCompare
	{
	};

	template <>
	struct DirectCompare<INT4OID, mars::OP::LT>
	{
		using c_type = pg_type_traits<INT4OID>::c_type;
		static bool match(c_type a, c_type b) { return a < b; }
	};

	template <>
	struct DirectCompare<INT4OID, mars::OP::LE>
	{
		using c_type = pg_type_traits<INT4OID>::c_type;
		static bool match(c_type a, c_type b) { return a <= b; }
	};

	template <>
	struct DirectCompare<INT4OID, mars::OP::EQ>
	{
		using c_type = pg_type_traits<INT4OID>::c_type;
		static bool match(c_type a, c_type b) { return a == b; }
	};

	template <>
	struct DirectCompare<INT4OID, mars::OP::GE>
	{
		using c_type = pg_type_traits<INT4OID>::c_type;
		static bool match(c_type a, c_type b) { return a >= b; }
	};

	template <>
	struct DirectCompare<INT4OID, mars::OP::GT>
	{
		using c_type = pg_type_traits<INT4OID>::c_type;
		static bool match(c_type a, c_type b) { return a > b; }
	};

	template <>
	struct DirectCompare<INT8OID, mars::OP::LT>
	{
		using c_type = pg_type_traits<INT8OID>::c_type;
		static bool match(c_type a, c_type b) { return a < b; }
	};

	template <>
	struct DirectCompare<INT8OID, mars::OP::LE>
	{
		using c_type = pg_type_traits<INT8OID>::c_type;
		static bool match(c_type a, c_type b) { return a <= b; }
	};

	template <>
	struct DirectCompare<INT8OID, mars::OP::EQ>
	{
		using c_type = pg_type_traits<INT8OID>::c_type;
		static bool match(c_type a, c_type b) { return a == b; }
	};

	template <>
	struct DirectCompare<INT8OID, mars::OP::GE>
	{
		using c_type = pg_type_traits<INT8OID>::c_type;
		static bool match(c_type a, c_type b) { return a >= b; }
	};

	template <>
	struct DirectCompare<INT8OID, mars::OP::GT>
	{
		using c_type = pg_type_traits<INT8OID>::c_type;
		static bool match(c_type a, c_type b) { return a > b; }
	};

	template <>
	struct DirectCompare<FLOAT4OID, mars::OP::LT>
	{
		using c_type = pg_type_traits<FLOAT4OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float4_lt(a, b); }
	};

	template <>
	struct DirectCompare<FLOAT4OID, mars::OP::LE>
	{
		using c_type = pg_type_traits<FLOAT4OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float4_le(a, b); }
	};

	template <>
	struct DirectCompare<FLOAT4OID, mars::OP::EQ>
	{
		using c_type = pg_type_traits<FLOAT4OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float4_eq(a, b); }
	};

	template <>
	struct DirectCompare<FLOAT4OID, mars::OP::GE>
	{
		using c_type = pg_type_traits<FLOAT4OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float4_ge(a, b); }
	};

	template <>
	struct DirectCompare<FLOAT4OID, mars::OP::GT>
	{
		using c_type = pg_type_traits<FLOAT4OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float4_gt(a, b); }
	};

	template <>
	struct DirectCompare<FLOAT8OID, mars::OP::LT>
	{
		using c_type = pg_type_traits<FLOAT8OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float8_lt(a, b); }
	};

	template <>
	struct DirectCompare<FLOAT8OID, mars::OP::LE>
	{
		using c_type = pg_type_traits<FLOAT8OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float8_le(a, b); }
	};

	template <>
	struct DirectCompare<FLOAT8OID, mars::OP::EQ>
	{
		using c_type = pg_type_traits<FLOAT8OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float8_eq(a, b); }
	};

	template <>
	struct DirectCompare<FLOAT8OID, mars::OP::GE>
	{
		using c_type = pg_type_traits<FLOAT8OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float8_ge(a, b); }
	};

	template <>
	struct DirectCompare<FLOAT8OID, mars::OP::GT>
	{
		using c_type = pg_type_traits<FLOAT8OID>::c_type;
		static bool match(c_type a, c_type b) { return ::float8_gt(a, b); }
	};

	template <>
	struct DirectCompare<BOOLOID, mars::OP::LT>
	{
		using c_type = pg_type_traits<BOOLOID>::c_type;
		static bool match(c_type a, c_type b) { return a < b; }
	};

	template <>
	struct DirectCompare<BOOLOID, mars::OP::LE>
	{
		using c_type = pg_type_traits<BOOLOID>::c_type;
		static bool match(c_type a, c_type b) { return a <= b; }
	};

	template <>
	struct DirectCompare<BOOLOID, mars::OP::EQ>
	{
		using c_type = pg_type_traits<BOOLOID>::c_type;
		static bool match(c_type a, c_type b) { return a == b; }
	};

	template <>
	struct DirectCompare<BOOLOID, mars::OP::GE>
	{
		using c_type = pg_type_traits<BOOLOID>::c_type;
		static bool match(c_type a, c_type b) { return a >= b; }
	};

	template <>
	struct DirectCompare<BOOLOID, mars::OP::GT>
	{
		using c_type = pg_type_traits<BOOLOID>::c_type;
		static bool match(c_type a, c_type b) { return a > b; }
	};

} // namespace mars

#endif /* MARS_DIRECTCOMPARE_H */

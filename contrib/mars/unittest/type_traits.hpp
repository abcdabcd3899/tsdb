#ifndef MARS_TEST_TYPE_TRAITS_HPP
#define MARS_TEST_TYPE_TRAITS_HPP

#include "../type_traits.hpp"

namespace mars {
namespace test {

template <typename c_type>
struct type_traits {
};

template <>
struct type_traits<int32_t> {
	static constexpr const ::Oid type_oid = INT4OID;
};

template <>
struct type_traits<int64_t> {
	static constexpr const ::Oid type_oid = INT8OID;
};

template <>
struct type_traits<float> {
	static constexpr const ::Oid type_oid = FLOAT4OID;
};

template <>
struct type_traits<double> {
	static constexpr const ::Oid type_oid = FLOAT8OID;
};

struct type_names {
	template <typename c_type>
		static std::string GetName(int) {
			using T = mars::pg_type_traits<mars::test::type_traits<c_type>::type_oid>;
			return T::name;
		}
};

} // namespace test
} // namespace mars

#endif   /* MARS_TEST_TYPE_TRAITS_HPP */

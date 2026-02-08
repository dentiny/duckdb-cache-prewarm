#include "catch/catch.hpp"

#include "utils/include/span.hpp"

using namespace duckdb; // NOLINT

#include <array>
#include <initializer_list>
#include <numeric>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

using duckdb::MakeConstSpan;
using duckdb::MakeSpan;
using duckdb::Span;

namespace {

std::vector<int> MakeRamp(int len, int offset = 0) {
	std::vector<int> v(len);
	std::iota(v.begin(), v.end(), offset);
	return v;
}

template <typename Expected, typename T>
void CheckType(const T & /* value */) {
	static_assert(std::is_same<Expected, T>::value, "Type mismatch");
}

TEST_CASE("EmptyCtors", "[IntSpan]") {
	Span<int> s;
	REQUIRE(s.data() == nullptr);
	REQUIRE(s.size() == 0);
}

TEST_CASE("PtrLenCtor", "[IntSpan]") {
	int a[] = {1, 2, 3};
	Span<int> s(&a[0], 2);
	REQUIRE(s.data() == a);
	REQUIRE(s.size() == 2);
}

TEST_CASE("ArrayCtor", "[IntSpan]") {
	int a[] = {1, 2, 3};
	Span<int> s(a);
	REQUIRE(s.data() == a);
	REQUIRE(s.size() == 3);

	REQUIRE((std::is_constructible<Span<const int>, int[3]>::value));
	REQUIRE((std::is_constructible<Span<const int>, const int[3]>::value));
	REQUIRE_FALSE((std::is_constructible<Span<int>, const int[3]>::value));
	REQUIRE((std::is_convertible<int[3], Span<const int>>::value));
	REQUIRE((std::is_convertible<const int[3], Span<const int>>::value));
}

template <typename T>
void TakesGenericSpan(Span<T>) {
}

TEST_CASE("ContainerCtor", "[IntSpan]") {
	std::vector<int> empty;
	Span<int> s_empty(empty);
	REQUIRE(s_empty.data() == empty.data());
	REQUIRE(s_empty.size() == empty.size());

	std::vector<int> filled {1, 2, 3};
	Span<int> s_filled(filled);
	REQUIRE(s_filled.data() == filled.data());
	REQUIRE(s_filled.size() == filled.size());

	Span<int> s_from_span(filled);
	REQUIRE(s_from_span.data() == s_filled.data());
	REQUIRE(s_from_span.size() == s_filled.size());

	Span<const int> const_filled = filled;
	REQUIRE(const_filled.data() == filled.data());
	REQUIRE(const_filled.size() == filled.size());

	Span<const int> const_from_span = s_filled;
	REQUIRE(const_from_span.data() == s_filled.data());
	REQUIRE(const_from_span.size() == s_filled.size());

	REQUIRE((std::is_convertible<std::vector<int> &, Span<const int>>::value));
	REQUIRE((std::is_convertible<Span<int> &, Span<const int>>::value));

	TakesGenericSpan(Span<int>(filled));
}

// A struct supplying shallow data() const.
struct ContainerWithShallowConstData {
	std::vector<int> storage;
	int *data() const {
		return const_cast<int *>(storage.data());
	}
	int size() const {
		return storage.size();
	}
};

TEST_CASE("ShallowConstness", "[IntSpan]") {
	const ContainerWithShallowConstData c {MakeRamp(20)};
	Span<int> s(c); // We should be able to do this even though data() is const.
	s[0] = -1;
	REQUIRE(c.storage[0] == -1);
}

TEST_CASE("StringCtor", "[CharSpan]") {
	std::string empty = "";
	Span<char> s_empty(empty);
	REQUIRE(s_empty.data() == empty.data());
	REQUIRE(s_empty.size() == empty.size());

	std::string abc = "abc";
	Span<char> s_abc(abc);
	REQUIRE(s_abc.data() == abc.data());
	REQUIRE(s_abc.size() == abc.size());

	Span<const char> s_const_abc = abc;
	REQUIRE(s_const_abc.data() == abc.data());
	REQUIRE(s_const_abc.size() == abc.size());

	REQUIRE_FALSE((std::is_constructible<Span<int>, std::string>::value));
	REQUIRE_FALSE((std::is_constructible<Span<const int>, std::string>::value));
	REQUIRE((std::is_convertible<std::string, Span<const char>>::value));
}

TEST_CASE("FromConstPointer", "[IntSpan]") {
	REQUIRE((std::is_constructible<Span<const int *const>, std::vector<int *>>::value));
	REQUIRE((std::is_constructible<Span<const int *const>, std::vector<const int *>>::value));
	REQUIRE_FALSE((std::is_constructible<Span<const int *>, std::vector<int *>>::value));
	REQUIRE_FALSE((std::is_constructible<Span<int *>, std::vector<const int *>>::value));
}

struct TypeWithMisleadingData {
	int &data() {
		return i;
	}
	int size() {
		return 1;
	}
	int i;
};

struct TypeWithMisleadingSize {
	int *data() {
		return &i;
	}
	const char *size() {
		return "1";
	}
	int i;
};

TEST_CASE("EvilTypes", "[IntSpan]") {
	REQUIRE_FALSE((std::is_constructible<Span<int>, TypeWithMisleadingData &>::value));
	REQUIRE_FALSE((std::is_constructible<Span<int>, TypeWithMisleadingSize &>::value));
}

struct Base {
	int *data() {
		return &i;
	}
	int size() {
		return 1;
	}
	int i;
};
struct Derived : Base {};

TEST_CASE("SpanOfDerived", "[IntSpan]") {
	REQUIRE((std::is_constructible<Span<int>, Base &>::value));
	REQUIRE((std::is_constructible<Span<int>, Derived &>::value));
	REQUIRE_FALSE((std::is_constructible<Span<Base>, std::vector<Derived>>::value));
}

void TestInitializerList(Span<const int> s, const std::vector<int> &v) {
	REQUIRE(std::equal(s.begin(), s.end(), v.begin(), v.end()));
}

TEST_CASE("InitializerListConversion", "[ConstIntSpan]") {
	TestInitializerList({}, {});
	TestInitializerList({1}, {1});
	TestInitializerList({1, 2, 3}, {1, 2, 3});

	REQUIRE_FALSE((std::is_constructible<Span<int>, std::initializer_list<int>>::value));
	REQUIRE_FALSE((std::is_convertible<Span<int>, std::initializer_list<int>>::value));
}

TEST_CASE("Data", "[IntSpan]") {
	int i;
	Span<int> s(&i, 1);
	REQUIRE(&i == s.data());
}

TEST_CASE("SizeLengthEmpty", "[IntSpan]") {
	Span<int> empty;
	REQUIRE(empty.size() == 0);
	REQUIRE(empty.empty());
	REQUIRE(empty.size() == empty.length());

	auto v = MakeRamp(10);
	Span<int> s(v);
	REQUIRE(s.size() == 10);
	REQUIRE_FALSE(s.empty());
	REQUIRE(s.size() == s.length());
}

TEST_CASE("ElementAccess", "[IntSpan]") {
	auto v = MakeRamp(10);
	Span<int> s(v);
	for (int i = 0; i < static_cast<int>(s.size()); ++i) {
		REQUIRE(s[i] == s.at(i));
	}

	REQUIRE(s.front() == s[0]);
	REQUIRE(s.back() == s[9]);
}

TEST_CASE("AtThrows", "[IntSpan]") {
	auto v = MakeRamp(10);
	Span<int> s(v);

	REQUIRE(s.at(9) == 9);
	REQUIRE_THROWS_AS(s.at(10), std::out_of_range);
}

TEST_CASE("RemovePrefixAndSuffix", "[IntSpan]") {
	auto v = MakeRamp(20, 1);
	Span<int> s(v);
	REQUIRE(s.size() == 20);

	s.remove_suffix(0);
	s.remove_prefix(0);
	REQUIRE(s.size() == 20);

	s.remove_prefix(1);
	REQUIRE(s.size() == 19);
	REQUIRE(s[0] == 2);

	s.remove_suffix(1);
	REQUIRE(s.size() == 18);
	REQUIRE(s.back() == 19);

	s.remove_prefix(7);
	REQUIRE(s.size() == 11);
	REQUIRE(s[0] == 9);

	s.remove_suffix(11);
	REQUIRE(s.size() == 0);

	REQUIRE(v == MakeRamp(20, 1));
}

TEST_CASE("Subspan", "[IntSpan]") {
	std::vector<int> empty;
	REQUIRE(MakeSpan(empty).subspan().data() == empty.data());
	REQUIRE(MakeSpan(empty).subspan().size() == empty.size());
	REQUIRE(MakeSpan(empty).subspan(0, 0).data() == empty.data());
	REQUIRE(MakeSpan(empty).subspan(0, 0).size() == empty.size());
	REQUIRE(MakeSpan(empty).subspan(0, Span<const int>::npos).data() == empty.data());
	REQUIRE(MakeSpan(empty).subspan(0, Span<const int>::npos).size() == empty.size());

	auto ramp = MakeRamp(10);
	REQUIRE(MakeSpan(ramp).subspan().data() == ramp.data());
	REQUIRE(MakeSpan(ramp).subspan().size() == ramp.size());
	REQUIRE(MakeSpan(ramp).subspan(0, 10).data() == ramp.data());
	REQUIRE(MakeSpan(ramp).subspan(0, 10).size() == ramp.size());
	REQUIRE(MakeSpan(ramp).subspan(0, Span<const int>::npos).data() == ramp.data());
	REQUIRE(MakeSpan(ramp).subspan(0, Span<const int>::npos).size() == ramp.size());
	REQUIRE(MakeSpan(ramp).subspan(0, 3).data() == ramp.data());
	REQUIRE(MakeSpan(ramp).subspan(0, 3).size() == 3);
	REQUIRE(MakeSpan(ramp).subspan(5, Span<const int>::npos).data() == ramp.data() + 5);
	REQUIRE(MakeSpan(ramp).subspan(5, Span<const int>::npos).size() == 5);
	REQUIRE(MakeSpan(ramp).subspan(3, 3).data() == ramp.data() + 3);
	REQUIRE(MakeSpan(ramp).subspan(3, 3).size() == 3);
	REQUIRE(MakeSpan(ramp).subspan(10, 5).data() == ramp.data() + 10);
	REQUIRE(MakeSpan(ramp).subspan(10, 5).size() == 0);

	REQUIRE_THROWS_AS(MakeSpan(ramp).subspan(11, 5), std::out_of_range);
}

TEST_CASE("First", "[IntSpan]") {
	std::vector<int> empty;
	REQUIRE(MakeSpan(empty).first(0).data() == empty.data());
	REQUIRE(MakeSpan(empty).first(0).size() == empty.size());

	auto ramp = MakeRamp(10);
	REQUIRE(MakeSpan(ramp).first(0).data() == ramp.data());
	REQUIRE(MakeSpan(ramp).first(0).size() == 0);
	REQUIRE(MakeSpan(ramp).first(10).data() == ramp.data());
	REQUIRE(MakeSpan(ramp).first(10).size() == ramp.size());
	REQUIRE(MakeSpan(ramp).first(3).data() == ramp.data());
	REQUIRE(MakeSpan(ramp).first(3).size() == 3);

	REQUIRE_THROWS_AS(MakeSpan(ramp).first(11), std::out_of_range);
}

TEST_CASE("Last", "[IntSpan]") {
	std::vector<int> empty;
	REQUIRE(MakeSpan(empty).last(0).data() == empty.data());
	REQUIRE(MakeSpan(empty).last(0).size() == empty.size());

	auto ramp = MakeRamp(10);
	REQUIRE(MakeSpan(ramp).last(0).data() == ramp.data() + 10);
	REQUIRE(MakeSpan(ramp).last(0).size() == 0);
	REQUIRE(MakeSpan(ramp).last(10).data() == ramp.data());
	REQUIRE(MakeSpan(ramp).last(10).size() == ramp.size());
	REQUIRE(MakeSpan(ramp).last(3).data() == ramp.data() + 7);
	REQUIRE(MakeSpan(ramp).last(3).size() == 3);

	REQUIRE_THROWS_AS(MakeSpan(ramp).last(11), std::out_of_range);
}

TEST_CASE("MakeSpanPtrLength", "[IntSpan]") {
	std::vector<int> empty;
	auto s_empty = MakeSpan(empty.data(), empty.size());
	REQUIRE(s_empty.data() == empty.data());
	REQUIRE(s_empty.size() == empty.size());

	std::array<int, 3> a {{1, 2, 3}};
	auto s = MakeSpan(a.data(), a.size());
	REQUIRE(s.data() == a.data());
	REQUIRE(s.size() == a.size());

	auto cs_empty = MakeConstSpan(empty.data(), empty.size());
	REQUIRE(cs_empty.data() == s_empty.data());
	REQUIRE(cs_empty.size() == s_empty.size());
	auto cs = MakeConstSpan(a.data(), a.size());
	REQUIRE(cs.data() == s.data());
	REQUIRE(cs.size() == s.size());
}

TEST_CASE("MakeSpanTwoPtrs", "[IntSpan]") {
	std::vector<int> empty;
	auto s_empty = MakeSpan(empty.data(), empty.data());
	REQUIRE(s_empty.data() == empty.data());
	REQUIRE(s_empty.size() == empty.size());

	std::vector<int> v {1, 2, 3};
	auto s = MakeSpan(v.data(), v.data() + 1);
	REQUIRE(s.data() == v.data());
	REQUIRE(s.size() == 1);

	auto cs_empty = MakeConstSpan(empty.data(), empty.data());
	REQUIRE(cs_empty.data() == s_empty.data());
	REQUIRE(cs_empty.size() == s_empty.size());
	auto cs = MakeConstSpan(v.data(), v.data() + 1);
	REQUIRE(cs.data() == s.data());
	REQUIRE(cs.size() == s.size());
}

TEST_CASE("MakeSpanContainer", "[IntSpan]") {
	std::vector<int> empty;
	auto s_empty = MakeSpan(empty);
	REQUIRE(s_empty.data() == empty.data());
	REQUIRE(s_empty.size() == empty.size());

	std::vector<int> v {1, 2, 3};
	auto s = MakeSpan(v);
	REQUIRE(s.data() == v.data());
	REQUIRE(s.size() == v.size());

	auto cs_empty = MakeConstSpan(empty);
	REQUIRE(cs_empty.data() == s_empty.data());
	REQUIRE(cs_empty.size() == s_empty.size());
	auto cs = MakeConstSpan(v);
	REQUIRE(cs.data() == s.data());
	REQUIRE(cs.size() == s.size());

	REQUIRE(MakeSpan(s).data() == s.data());
	REQUIRE(MakeSpan(s).size() == s.size());
	REQUIRE(MakeConstSpan(s).data() == s.data());
	REQUIRE(MakeConstSpan(s).size() == s.size());
}

TEST_CASE("MakeSpanString", "[CharSpan]") {
	std::string empty = "";
	auto s_empty = MakeSpan(empty);
	REQUIRE(s_empty.data() == empty.data());
	REQUIRE(s_empty.size() == empty.size());

	std::string str = "abc";
	auto s_str = MakeSpan(str);
	REQUIRE(s_str.data() == str.data());
	REQUIRE(s_str.size() == str.size());

	auto cs_empty = MakeConstSpan(empty);
	REQUIRE(cs_empty.data() == s_empty.data());
	REQUIRE(cs_empty.size() == s_empty.size());
	auto cs_str = MakeConstSpan(str);
	REQUIRE(cs_str.data() == s_str.data());
	REQUIRE(cs_str.size() == s_str.size());
}

TEST_CASE("MakeSpanArray", "[IntSpan]") {
	int a[] = {1, 2, 3};
	auto s = MakeSpan(a);
	REQUIRE(s.data() == a);
	REQUIRE(s.size() == 3);

	const int ca[] = {1, 2, 3};
	auto s_ca = MakeSpan(ca);
	REQUIRE(s_ca.data() == ca);
	REQUIRE(s_ca.size() == 3);

	auto cs = MakeConstSpan(a);
	REQUIRE(cs.data() == s.data());
	REQUIRE(cs.size() == s.size());
	auto cs_ca = MakeConstSpan(ca);
	REQUIRE(cs_ca.data() == s_ca.data());
	REQUIRE(cs_ca.size() == s_ca.size());
}

TEST_CASE("MakeSpanTypes", "[IntSpan]") {
	std::vector<int> vec;
	const std::vector<int> cvec;
	int a[1];
	const int ca[] = {1};
	int *ip = a;
	const int *cip = ca;
	std::string s = "";
	const std::string cs = "";
	CheckType<Span<int>>(MakeSpan(vec));
	CheckType<Span<const int>>(MakeSpan(cvec));
	CheckType<Span<int>>(MakeSpan(ip, ip + 1));
	CheckType<Span<int>>(MakeSpan(ip, 1));
	CheckType<Span<const int>>(MakeSpan(cip, cip + 1));
	CheckType<Span<const int>>(MakeSpan(cip, 1));
	CheckType<Span<int>>(MakeSpan(a));
	CheckType<Span<int>>(MakeSpan(a, a + 1));
	CheckType<Span<int>>(MakeSpan(a, 1));
	CheckType<Span<const int>>(MakeSpan(ca));
	CheckType<Span<const int>>(MakeSpan(ca, ca + 1));
	CheckType<Span<const int>>(MakeSpan(ca, 1));
	CheckType<Span<char>>(MakeSpan(s));
	CheckType<Span<const char>>(MakeSpan(cs));
}

TEST_CASE("MakeConstSpanTypes", "[ConstIntSpan]") {
	std::vector<int> vec;
	const std::vector<int> cvec;
	int array[1];
	const int carray[] = {0};
	int *ptr = array;
	const int *cptr = carray;
	std::string s = "";
	std::string cs = "";
	CheckType<Span<const int>>(MakeConstSpan(vec));
	CheckType<Span<const int>>(MakeConstSpan(cvec));
	CheckType<Span<const int>>(MakeConstSpan(ptr, ptr + 1));
	CheckType<Span<const int>>(MakeConstSpan(ptr, 1));
	CheckType<Span<const int>>(MakeConstSpan(cptr, cptr + 1));
	CheckType<Span<const int>>(MakeConstSpan(cptr, 1));
	CheckType<Span<const int>>(MakeConstSpan(array));
	CheckType<Span<const int>>(MakeConstSpan(carray));
	CheckType<Span<const char>>(MakeConstSpan(s));
	CheckType<Span<const char>>(MakeConstSpan(cs));
}

TEST_CASE("Equality", "[IntSpan]") {
	const int arr1[] = {1, 2, 3, 4, 5};
	int arr2[] = {1, 2, 3, 4, 5};
	std::vector<int> vec1(std::begin(arr1), std::end(arr1));
	std::vector<int> vec2 = vec1;
	std::vector<int> other_vec = {2, 4, 6, 8, 10};
	// These two slices are from different vectors, but have the same size and
	// have the same elements (right now).  They should compare equal. Test both
	// == and !=.
	const Span<const int> from1 = vec1;
	const Span<const int> from2 = vec2;
	REQUIRE(from1 == from1);
	REQUIRE_FALSE(from1 != from1);
	REQUIRE(from1 == from2);
	REQUIRE_FALSE(from1 != from2);

	// These two slices have different underlying vector values. They should be
	// considered not equal. Test both == and !=.
	const Span<const int> from_other = other_vec;
	REQUIRE(from1 != from_other);
	REQUIRE_FALSE(from1 == from_other);

	// Comparison between a vector and its slice should be equal. And vice-versa.
	// This ensures implicit conversion to Span works on both sides of ==.
	REQUIRE(vec1 == from1);
	REQUIRE_FALSE(vec1 != from1);
	REQUIRE(from1 == vec1);
	REQUIRE_FALSE(from1 != vec1);

	// This verifies that Span<T> can be compared freely with Span<const T>.
	const Span<int> mutable_from1(vec1);
	const Span<int> mutable_from2(vec2);
	REQUIRE(from1 == mutable_from1);
	REQUIRE(mutable_from1 == from1);
	REQUIRE(mutable_from1 == mutable_from2);
	REQUIRE(mutable_from2 == mutable_from1);

	// Comparison between a vector and its slice should be equal for mutable
	// Spans as well.
	REQUIRE(vec1 == mutable_from1);
	REQUIRE_FALSE(vec1 != mutable_from1);
	REQUIRE(mutable_from1 == vec1);
	REQUIRE_FALSE(mutable_from1 != vec1);

	// Comparison between convertible-to-Span-of-const and Span-of-mutable. Arrays
	// are used because they're the only value type which converts to a
	// Span-of-mutable.
	REQUIRE(arr1 == mutable_from1);
	REQUIRE_FALSE(arr1 != mutable_from1);
	REQUIRE(mutable_from1 == arr1);
	REQUIRE_FALSE(mutable_from1 != arr1);

	// Comparison between convertible-to-Span-of-mutable and Span-of-const
	REQUIRE(arr2 == from1);
	REQUIRE_FALSE(arr2 != from1);
	REQUIRE(from1 == arr2);
	REQUIRE_FALSE(from1 != arr2);

	// With a different size, the array slices should not be equal.
	REQUIRE(from1 != Span<const int>(from1).subspan(0, from1.size() - 1));

	// With different contents, the array slices should not be equal.
	++vec2.back();
	REQUIRE(from1 != from2);
}

TEST_CASE("OrderComparison", "[IntSpan]") {
	int arr_before_[3] = {1, 2, 3};
	int arr_after_[3] = {1, 2, 4};
	const int carr_after_[3] = {1, 2, 4};
	std::vector<int> vec_before_(std::begin(arr_before_), std::end(arr_before_));
	std::vector<int> vec_after_(std::begin(arr_after_), std::end(arr_after_));
	Span<int> before_(vec_before_);
	Span<int> after_(vec_after_);
	Span<const int> cbefore_(vec_before_);
	Span<const int> cafter_(vec_after_);

	SECTION("CompareSpans") {
		REQUIRE(cbefore_ < cafter_);
		REQUIRE(cbefore_ <= cafter_);
		REQUIRE(cafter_ > cbefore_);
		REQUIRE(cafter_ >= cbefore_);

		REQUIRE_FALSE(cbefore_ > cafter_);
		REQUIRE_FALSE(cafter_ < cbefore_);

		REQUIRE(before_ < after_);
		REQUIRE(before_ <= after_);
		REQUIRE(after_ > before_);
		REQUIRE(after_ >= before_);

		REQUIRE_FALSE(before_ > after_);
		REQUIRE_FALSE(after_ < before_);

		REQUIRE(cbefore_ < after_);
		REQUIRE(cbefore_ <= after_);
		REQUIRE(after_ > cbefore_);
		REQUIRE(after_ >= cbefore_);

		REQUIRE_FALSE(cbefore_ > after_);
		REQUIRE_FALSE(after_ < cbefore_);
	}

	SECTION("SpanOfConstAndContainer") {
		REQUIRE(cbefore_ < vec_after_);
		REQUIRE(cbefore_ <= vec_after_);
		REQUIRE(vec_after_ > cbefore_);
		REQUIRE(vec_after_ >= cbefore_);

		REQUIRE_FALSE(cbefore_ > vec_after_);
		REQUIRE_FALSE(vec_after_ < cbefore_);

		REQUIRE(arr_before_ < cafter_);
		REQUIRE(arr_before_ <= cafter_);
		REQUIRE(cafter_ > arr_before_);
		REQUIRE(cafter_ >= arr_before_);

		REQUIRE_FALSE(arr_before_ > cafter_);
		REQUIRE_FALSE(cafter_ < arr_before_);
	}

	SECTION("SpanOfMutableAndContainer") {
		REQUIRE(vec_before_ < after_);
		REQUIRE(vec_before_ <= after_);
		REQUIRE(after_ > vec_before_);
		REQUIRE(after_ >= vec_before_);

		REQUIRE_FALSE(vec_before_ > after_);
		REQUIRE_FALSE(after_ < vec_before_);

		REQUIRE(before_ < carr_after_);
		REQUIRE(before_ <= carr_after_);
		REQUIRE(carr_after_ > before_);
		REQUIRE(carr_after_ >= before_);

		REQUIRE_FALSE(before_ > carr_after_);
		REQUIRE_FALSE(carr_after_ < before_);
	}

	SECTION("EqualSpans") {
		REQUIRE_FALSE(before_ < before_);
		REQUIRE(before_ <= before_);
		REQUIRE_FALSE(before_ > before_);
		REQUIRE(before_ >= before_);
	}

	SECTION("Subspans") {
		auto subspan = before_.subspan(0, 1);
		REQUIRE(subspan < before_);
		REQUIRE(subspan <= before_);
		REQUIRE(before_ > subspan);
		REQUIRE(before_ >= subspan);

		REQUIRE_FALSE(subspan > before_);
		REQUIRE_FALSE(before_ < subspan);
	}

	SECTION("EmptySpans") {
		Span<int> empty;
		REQUIRE_FALSE(empty < empty);
		REQUIRE(empty <= empty);
		REQUIRE_FALSE(empty > empty);
		REQUIRE(empty >= empty);

		REQUIRE(empty < before_);
		REQUIRE(empty <= before_);
		REQUIRE(before_ > empty);
		REQUIRE(before_ >= empty);

		REQUIRE_FALSE(empty > before_);
		REQUIRE_FALSE(before_ < empty);
	}
}

TEST_CASE("ExposesContainerTypesAndConsts", "[IntSpan]") {
	Span<int> slice;
	CheckType<Span<int>::iterator>(slice.begin());
	REQUIRE((std::is_convertible<decltype(slice.begin()), Span<int>::const_iterator>::value));
	CheckType<Span<int>::const_iterator>(slice.cbegin());
	REQUIRE((std::is_convertible<decltype(slice.end()), Span<int>::const_iterator>::value));
	CheckType<Span<int>::const_iterator>(slice.cend());
	CheckType<Span<int>::reverse_iterator>(slice.rend());
	REQUIRE((std::is_convertible<decltype(slice.rend()), Span<int>::const_reverse_iterator>::value));
	CheckType<Span<int>::const_reverse_iterator>(slice.crend());
	static_assert(std::is_same<int, Span<int>::value_type>::value, "");
	static_assert(std::is_same<int, Span<const int>::value_type>::value, "");
	static_assert(std::is_same<int, Span<int>::element_type>::value, "");
	static_assert(std::is_same<const int, Span<const int>::element_type>::value, "");
	static_assert(std::is_same<int *, Span<int>::pointer>::value, "");
	static_assert(std::is_same<const int *, Span<const int>::pointer>::value, "");
	static_assert(std::is_same<int &, Span<int>::reference>::value, "");
	static_assert(std::is_same<const int &, Span<const int>::reference>::value, "");
	static_assert(std::is_same<const int &, Span<int>::const_reference>::value, "");
	static_assert(std::is_same<const int &, Span<const int>::const_reference>::value, "");
	REQUIRE(static_cast<Span<int>::size_type>(-1) == Span<int>::npos);
}

TEST_CASE("IteratorsAndReferences", "[IntSpan]") {
	auto accept_pointer = [](int *) {
	};
	auto accept_reference = [](int &) {
	};
	auto accept_iterator = [](Span<int>::iterator) {
	};
	auto accept_const_iterator = [](Span<int>::const_iterator) {
	};
	auto accept_reverse_iterator = [](Span<int>::reverse_iterator) {
	};
	auto accept_const_reverse_iterator = [](Span<int>::const_reverse_iterator) {
	};

	int a[1];
	Span<int> s = a;

	accept_pointer(s.data());
	accept_iterator(s.begin());
	accept_const_iterator(s.begin());
	accept_const_iterator(s.cbegin());
	accept_iterator(s.end());
	accept_const_iterator(s.end());
	accept_const_iterator(s.cend());
	accept_reverse_iterator(s.rbegin());
	accept_const_reverse_iterator(s.rbegin());
	accept_const_reverse_iterator(s.crbegin());
	accept_reverse_iterator(s.rend());
	accept_const_reverse_iterator(s.rend());
	accept_const_reverse_iterator(s.crend());

	accept_reference(s[0]);
	accept_reference(s.at(0));
	accept_reference(s.front());
	accept_reference(s.back());
}

TEST_CASE("IteratorsAndReferences_Const", "[IntSpan]") {
	auto accept_pointer = [](int *) {
	};
	auto accept_reference = [](int &) {
	};
	auto accept_iterator = [](Span<int>::iterator) {
	};
	auto accept_const_iterator = [](Span<int>::const_iterator) {
	};
	auto accept_reverse_iterator = [](Span<int>::reverse_iterator) {
	};
	auto accept_const_reverse_iterator = [](Span<int>::const_reverse_iterator) {
	};

	int a[1];
	const Span<int> s = a;

	accept_pointer(s.data());
	accept_iterator(s.begin());
	accept_const_iterator(s.begin());
	accept_const_iterator(s.cbegin());
	accept_iterator(s.end());
	accept_const_iterator(s.end());
	accept_const_iterator(s.cend());
	accept_reverse_iterator(s.rbegin());
	accept_const_reverse_iterator(s.rbegin());
	accept_const_reverse_iterator(s.crbegin());
	accept_reverse_iterator(s.rend());
	accept_const_reverse_iterator(s.rend());
	accept_const_reverse_iterator(s.crend());

	accept_reference(s[0]);
	accept_reference(s.at(0));
	accept_reference(s.front());
	accept_reference(s.back());
}

TEST_CASE("NoexceptTest", "[IntSpan]") {
	int a[] = {1, 2, 3};
	std::vector<int> v;
	REQUIRE(noexcept(Span<const int>()));
	REQUIRE(noexcept(Span<const int>(a, 2)));
	REQUIRE(noexcept(Span<const int>(a)));
	REQUIRE(noexcept(Span<const int>(v)));
	REQUIRE(noexcept(Span<int>(v)));
	REQUIRE(noexcept(Span<const int>({1, 2, 3})));
	REQUIRE(noexcept(MakeSpan(v)));
	REQUIRE(noexcept(MakeSpan(a)));
	REQUIRE(noexcept(MakeSpan(a, 2)));
	REQUIRE(noexcept(MakeSpan(a, a + 1)));
	REQUIRE(noexcept(MakeConstSpan(v)));
	REQUIRE(noexcept(MakeConstSpan(a)));
	REQUIRE(noexcept(MakeConstSpan(a, 2)));
	REQUIRE(noexcept(MakeConstSpan(a, a + 1)));

	Span<int> s(v);
	REQUIRE(noexcept(s.data()));
	REQUIRE(noexcept(s.size()));
	REQUIRE(noexcept(s.length()));
	REQUIRE(noexcept(s.empty()));
	REQUIRE(noexcept(s[0]));
	REQUIRE(noexcept(s.front()));
	REQUIRE(noexcept(s.back()));
	REQUIRE(noexcept(s.begin()));
	REQUIRE(noexcept(s.cbegin()));
	REQUIRE(noexcept(s.end()));
	REQUIRE(noexcept(s.cend()));
	REQUIRE(noexcept(s.rbegin()));
	REQUIRE(noexcept(s.crbegin()));
	REQUIRE(noexcept(s.rend()));
	REQUIRE(noexcept(s.crend()));
	REQUIRE(noexcept(s.remove_prefix(0)));
	REQUIRE(noexcept(s.remove_suffix(0)));
}

struct BigStruct {
	char bytes[10000];
};

TEST_CASE("SpanSize", "[Span]") {
	REQUIRE(sizeof(Span<int>) <= 2 * sizeof(void *));
	REQUIRE(sizeof(Span<BigStruct>) <= 2 * sizeof(void *));
}

} // namespace

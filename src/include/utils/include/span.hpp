// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Adapted from https://github.com/abseil/abseil-cpp/blob/master/absl/types/span.h

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

namespace duckdb {

// Forward declaration
template <typename T>
class Span;

namespace span_internal {

// Helper to get data pointer from containers
template <typename C>
constexpr auto GetDataImpl(C &c, char) noexcept -> decltype(c.data()) {
	return c.data();
}

// Special case for std::string before C++17 (data() returns const char*)
inline char *GetDataImpl(std::string &s, int) noexcept {
	return &s[0];
}

template <typename C>
constexpr auto GetData(C &c) noexcept -> decltype(GetDataImpl(c, 0)) {
	return GetDataImpl(c, 0);
}

// Type traits helpers
template <typename T>
using remove_cv_t = typename std::remove_cv<T>::type;

template <typename T>
using decay_t = typename std::decay<T>::type;

template <typename T>
using remove_reference_t = typename std::remove_reference<T>::type;

// C++14 void_t implementation for C++11 compatibility
template <typename...>
using void_t = void;

// Detection idioms for size() and data()
template <typename C>
using HasSize = std::is_integral<decay_t<decltype(std::declval<C &>().size())>>;

// Check if container has compatible data() method
template <typename T, typename C>
using HasData = std::is_convertible<decay_t<decltype(GetData(std::declval<C &>()))> *, T *const *>;

// Extract element type from container
template <typename C>
struct ElementType {
	using type = typename remove_reference_t<C>::value_type;
};

template <typename T, size_t N>
struct ElementType<T (&)[N]> {
	using type = T;
};

template <typename C>
using ElementT = typename ElementType<C>::type;

// Check if type is a view (same data() return type for const and non-const)
template <typename T, typename = void, typename = void>
struct IsView {
	static constexpr bool value = false;
};

template <typename T>
struct IsView<T, void_t<decltype(GetData(std::declval<const T &>()))>, void_t<decltype(GetData(std::declval<T &>()))>> {
private:
	using Container = typename std::remove_const<T>::type;
	using ConstData = decltype(GetData(std::declval<const Container &>()));
	using MutData = decltype(GetData(std::declval<Container &>()));

public:
	static constexpr bool value = std::is_same<ConstData, MutData>::value;
};

// Enable if helpers
template <typename T>
using EnableIfIsView = typename std::enable_if<IsView<T>::value, int>::type;

template <typename T>
using EnableIfNotIsView = typename std::enable_if<!IsView<T>::value, int>::type;

template <typename From, typename To>
using EnableIfConvertibleTo = typename std::enable_if<std::is_convertible<From, To>::value>::type;

// Comparison helpers
template <template <typename> class SpanT, typename T>
bool EqualImpl(SpanT<T> a, SpanT<T> b) {
	static_assert(std::is_const<T>::value, "");
	return std::equal(a.begin(), a.end(), b.begin(), b.end());
}

template <template <typename> class SpanT, typename T>
bool LessThanImpl(SpanT<T> a, SpanT<T> b) {
	static_assert(std::is_const<T>::value, "");
	return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

} // namespace span_internal

//------------------------------------------------------------------------------
// Span
//------------------------------------------------------------------------------
//
// A Span is a non-owning reference to a contiguous sequence of objects.
// It provides a lightweight way to pass array-like data without copying.
//
template <typename T>
class Span {
private:
	// SFINAE helpers for constructors
	template <typename C>
	using EnableIfConvertibleFrom =
	    typename std::enable_if<span_internal::HasData<T, C>::value && span_internal::HasSize<C>::value>::type;

	template <typename U>
	using EnableIfValueIsConst = typename std::enable_if<std::is_const<T>::value, U>::type;

	template <typename U>
	using EnableIfValueIsMutable = typename std::enable_if<!std::is_const<T>::value, U>::type;

public:
	using element_type = T;
	using value_type = span_internal::remove_cv_t<T>;
	using pointer = T *;
	using const_pointer = const T *;
	using reference = T &;
	using const_reference = const T &;
	using iterator = pointer;
	using const_iterator = const_pointer;
	using reverse_iterator = std::reverse_iterator<iterator>;
	using const_reverse_iterator = std::reverse_iterator<const_iterator>;
	using size_type = size_t;
	using difference_type = ptrdiff_t;

	static const size_type npos = ~(size_type(0));

	// Constructors
	constexpr Span() noexcept : Span(nullptr, 0) {
	}

	constexpr Span(pointer array, size_type length) noexcept : ptr_(array), len_(length) {
	}

	// Implicit conversion from C-style array
	template <size_t N>
	constexpr Span(T (&a)[N]) noexcept // NOLINT(google-explicit-constructor)
	    : Span(a, N) {
	}

	// Explicit constructor for mutable Span from container
	template <typename V, typename = EnableIfConvertibleFrom<V>, typename = EnableIfValueIsMutable<V>,
	          typename = span_internal::EnableIfNotIsView<V>>
	explicit Span(V &v) noexcept // NOLINT(runtime/references)
	    : Span(span_internal::GetData(v), v.size()) {
	}

	// Implicit constructor for const Span from container
	template <typename V, typename = EnableIfConvertibleFrom<V>, typename = EnableIfValueIsConst<V>,
	          typename = span_internal::EnableIfNotIsView<V>>
	constexpr Span(const V &v) noexcept // NOLINT(google-explicit-constructor)
	    : Span(span_internal::GetData(v), v.size()) {
	}

	// Overloads for view types (without lifetime annotations)
	template <typename V, typename = EnableIfConvertibleFrom<V>, typename = EnableIfValueIsMutable<V>,
	          span_internal::EnableIfIsView<V> = 0>
	explicit Span(V &v) noexcept // NOLINT(runtime/references)
	    : Span(span_internal::GetData(v), v.size()) {
	}

	template <typename V, typename = EnableIfConvertibleFrom<V>, typename = EnableIfValueIsConst<V>,
	          span_internal::EnableIfIsView<V> = 0>
	constexpr Span(const V &v) noexcept // NOLINT(google-explicit-constructor)
	    : Span(span_internal::GetData(v), v.size()) {
	}

	// Implicit constructor from initializer_list (only for const T)
	template <typename LazyT = T, typename = EnableIfValueIsConst<LazyT>>
	Span(std::initializer_list<value_type> v) noexcept // NOLINT(runtime/explicit)
	    : Span(v.begin(), v.size()) {
	}

	// Accessors
	constexpr pointer data() const noexcept {
		return ptr_;
	}

	constexpr size_type size() const noexcept {
		return len_;
	}

	constexpr size_type length() const noexcept {
		return size();
	}

	constexpr bool empty() const noexcept {
		return size() == 0;
	}

	constexpr reference operator[](size_type i) const noexcept {
		assert(i < size());
		return ptr_[i];
	}

	reference at(size_type i) const {
		if (i >= size()) {
			throw std::out_of_range("Span::at failed bounds check");
		}
		return ptr_[i];
	}

	constexpr reference front() const noexcept {
		assert(size() > 0);
		return *data();
	}

	constexpr reference back() const noexcept {
		assert(size() > 0);
		return *(data() + size() - 1);
	}

	// Iterators
	constexpr iterator begin() const noexcept {
		return data();
	}

	constexpr const_iterator cbegin() const noexcept {
		return begin();
	}

	constexpr iterator end() const noexcept {
		return data() + size();
	}

	constexpr const_iterator cend() const noexcept {
		return end();
	}

	reverse_iterator rbegin() const noexcept {
		return reverse_iterator(end());
	}

	const_reverse_iterator crbegin() const noexcept {
		return rbegin();
	}

	reverse_iterator rend() const noexcept {
		return reverse_iterator(begin());
	}

	const_reverse_iterator crend() const noexcept {
		return rend();
	}

	// Mutations
	void remove_prefix(size_type n) noexcept {
		assert(size() >= n);
		ptr_ += n;
		len_ -= n;
	}

	void remove_suffix(size_type n) noexcept {
		assert(size() >= n);
		len_ -= n;
	}

	Span subspan(size_type pos = 0, size_type len = npos) const {
		if (pos > size()) {
			throw std::out_of_range("pos > size()");
		}
		return Span(data() + pos, (std::min)(size() - pos, len));
	}

	Span first(size_type len) const {
		if (len > size()) {
			throw std::out_of_range("len > size()");
		}
		return Span(data(), len);
	}

	Span last(size_type len) const {
		if (len > size()) {
			throw std::out_of_range("len > size()");
		}
		return Span(size() - len + data(), len);
	}

private:
	pointer ptr_;
	size_type len_;
};

// Static member definition
template <typename T>
const typename Span<T>::size_type Span<T>::npos;

// Comparison operators
template <typename T>
bool operator==(Span<T> a, Span<T> b) {
	return span_internal::EqualImpl<Span, const T>(a, b);
}

template <typename T>
bool operator==(Span<const T> a, Span<T> b) {
	return span_internal::EqualImpl<Span, const T>(a, b);
}

template <typename T>
bool operator==(Span<T> a, Span<const T> b) {
	return span_internal::EqualImpl<Span, const T>(a, b);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator==(const U &a, Span<T> b) {
	return span_internal::EqualImpl<Span, const T>(Span<const T>(a), b);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator==(Span<T> a, const U &b) {
	return span_internal::EqualImpl<Span, const T>(a, Span<const T>(b));
}

template <typename T>
bool operator!=(Span<T> a, Span<T> b) {
	return !(a == b);
}

template <typename T>
bool operator!=(Span<const T> a, Span<T> b) {
	return !(a == b);
}

template <typename T>
bool operator!=(Span<T> a, Span<const T> b) {
	return !(a == b);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator!=(const U &a, Span<T> b) {
	return !(Span<const T>(a) == b);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator!=(Span<T> a, const U &b) {
	return !(a == Span<const T>(b));
}

template <typename T>
bool operator<(Span<T> a, Span<T> b) {
	return span_internal::LessThanImpl<Span, const T>(a, b);
}

template <typename T>
bool operator<(Span<const T> a, Span<T> b) {
	return span_internal::LessThanImpl<Span, const T>(a, b);
}

template <typename T>
bool operator<(Span<T> a, Span<const T> b) {
	return span_internal::LessThanImpl<Span, const T>(a, b);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator<(const U &a, Span<T> b) {
	return span_internal::LessThanImpl<Span, const T>(Span<const T>(a), b);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator<(Span<T> a, const U &b) {
	return span_internal::LessThanImpl<Span, const T>(a, Span<const T>(b));
}

template <typename T>
bool operator>(Span<T> a, Span<T> b) {
	return b < a;
}

template <typename T>
bool operator>(Span<const T> a, Span<T> b) {
	return b < a;
}

template <typename T>
bool operator>(Span<T> a, Span<const T> b) {
	return b < a;
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator>(const U &a, Span<T> b) {
	return b < a;
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator>(Span<T> a, const U &b) {
	return b < a;
}

template <typename T>
bool operator<=(Span<T> a, Span<T> b) {
	return !(b < a);
}

template <typename T>
bool operator<=(Span<const T> a, Span<T> b) {
	return !(b < a);
}

template <typename T>
bool operator<=(Span<T> a, Span<const T> b) {
	return !(b < a);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator<=(const U &a, Span<T> b) {
	return !(b < a);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator<=(Span<T> a, const U &b) {
	return !(b < a);
}

template <typename T>
bool operator>=(Span<T> a, Span<T> b) {
	return !(a < b);
}

template <typename T>
bool operator>=(Span<const T> a, Span<T> b) {
	return !(a < b);
}

template <typename T>
bool operator>=(Span<T> a, Span<const T> b) {
	return !(a < b);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator>=(const U &a, Span<T> b) {
	return !(a < b);
}

template <typename T, typename U, typename = span_internal::EnableIfConvertibleTo<U, Span<const T>>>
bool operator>=(Span<T> a, const U &b) {
	return !(a < b);
}

// Factory functions
// MakeSpan - constructs a mutable Span<T>
template <typename T>
constexpr Span<T> MakeSpan(T *ptr, size_t size) noexcept {
	return Span<T>(ptr, size);
}

template <typename T>
Span<T> MakeSpan(T *begin, T *end) noexcept {
	assert(begin <= end);
	return Span<T>(begin, static_cast<size_t>(end - begin));
}

template <typename C>
constexpr auto MakeSpan(C &c) noexcept -> decltype(MakeSpan(span_internal::GetData(c), c.size())) {
	return MakeSpan(span_internal::GetData(c), c.size());
}

template <typename T, size_t N>
constexpr Span<T> MakeSpan(T (&array)[N]) noexcept {
	return Span<T>(array, N);
}

// MakeConstSpan - constructs a Span<const T>
template <typename T>
constexpr Span<const T> MakeConstSpan(T *ptr, size_t size) noexcept {
	return Span<const T>(ptr, size);
}

template <typename T>
Span<const T> MakeConstSpan(T *begin, T *end) noexcept {
	assert(begin <= end);
	return Span<const T>(begin, static_cast<size_t>(end - begin));
}

template <typename C>
constexpr auto MakeConstSpan(const C &c) noexcept -> decltype(MakeSpan(c)) {
	return MakeSpan(c);
}

template <typename T, size_t N>
constexpr Span<const T> MakeConstSpan(const T (&array)[N]) noexcept {
	return Span<const T>(array, N);
}

} // namespace duckdb

#pragma once

#include <utility>

namespace duckdb {

template <typename Func>
class ScopeGuard {
public:
	explicit ScopeGuard(Func func_p) : func(std::move(func_p)) {
	}
	ScopeGuard(const ScopeGuard &) = delete;
	ScopeGuard &operator=(const ScopeGuard &) = delete;
	~ScopeGuard() noexcept {
		func();
	}

private:
	Func func;
};

} // namespace duckdb

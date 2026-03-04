#pragma once

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

//! Parse a size string into bytes. Accepts human-readable sizes like '1GB', '100MB',
//! or plain numeric values like '1000000' (treated as bytes).
inline idx_t ParseSizeLimit(const string &input) {
	bool is_numeric = !input.empty();
	for (auto c : input) {
		if (!StringUtil::CharacterIsDigit(c) && c != '.') {
			is_numeric = false;
			break;
		}
	}
	if (is_numeric) {
		return DBConfig::ParseMemoryLimit(input + "bytes");
	}
	return DBConfig::ParseMemoryLimit(input);
}

} // namespace duckdb

#include "utils/include/parse_size.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

idx_t ParseSizeLimit(const string &input) {
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

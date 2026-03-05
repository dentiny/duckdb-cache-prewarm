#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

//! Parse a size string into bytes. Accepts human-readable sizes like '1GB', '100MB',
//! or plain numeric values like '1000000' (treated as bytes).
idx_t ParseSizeLimit(const string &input);

} // namespace duckdb

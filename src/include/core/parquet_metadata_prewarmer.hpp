#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

class ClientContext;

//! Prewarm parquet footer and side-index byte ranges through the normal file/cache stack.
class ParquetMetadataPrewarmer {
public:
	static idx_t Execute(ClientContext &context, FileSystem &fs, const string &pattern);
};

} // namespace duckdb

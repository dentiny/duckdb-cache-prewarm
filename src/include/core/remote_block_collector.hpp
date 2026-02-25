#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Remote Block Info Structure
//===--------------------------------------------------------------------===//

//! Information about a remote block to prewarm
struct RemoteBlockInfo {
	//! Remote file path (e.g., s3://bucket/file.parquet)
	string file_path;
	//! Byte offset in file
	idx_t offset;
	//! Block size in bytes
	int64_t size;
	//! Total file size
	idx_t file_size;

	// Default constructor for vector operations
	RemoteBlockInfo() : offset(0), size(0), file_size(0) {
	}

	RemoteBlockInfo(string file_path_p, idx_t offset_p, int64_t size_p, idx_t file_size_p)
	    : file_path(std::move(file_path_p)), offset(offset_p), size(size_p), file_size(file_size_p) {
	}
};

//===--------------------------------------------------------------------===//
// Remote Block Collector
//===--------------------------------------------------------------------===//

//! Collects remote file blocks for prewarming
class RemoteBlockCollector {
public:
	//! Collect blocks from remote files matching the pattern
	//! @param fs File system to use for file operations
	//! @param pattern Glob pattern of file path
	//! @param block_size Size of each block (from cache_httpfs config)
	//! @return Map of file paths to vector of remote blocks to prewarm
	static unordered_map<string, vector<RemoteBlockInfo>> CollectRemoteBlocks(FileSystem &fs, const string &pattern,
	                                                                          idx_t block_size);
};

} // namespace duckdb

#pragma once

#include "core/prewarm_strategy.hpp"
#include "core/remote_block_collector.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

// Forward declarations for cache_httpfs types
namespace duckdb {

//===--------------------------------------------------------------------===//
// Remote Prewarm Strategy
//===--------------------------------------------------------------------===//

//! Strategy for prewarming remote file blocks into cache
class RemotePrewarmStrategy : public PrewarmStrategy {
public:
	RemotePrewarmStrategy(ClientContext &context_p, FileSystem &fs_p);

	//! Execute prewarm on remote blocks
	//! @param blocks Vector of blocks to prewarm
	//! @param max_blocks Maximum blocks to prewarm (use UINT64_MAX / max idx_t value for no limit)
	//! @return Number of blocks successfully prewarmed
	virtual idx_t Execute(const RemoteFileBlockMap &file_blocks, idx_t max_blocks);

	//! Filter out cached blocks from the given file path and blocks
	virtual vector<RemoteBlockInfo> FilterCachedBlocks(const string &file_path, const vector<RemoteBlockInfo> &blocks);

	//! Calculate maximum number of blocks that can be loaded based on available cache filesystem's capacity
	BufferCapacityInfo CalculateMaxAvailableBlocks() override;

protected:
	//! Get cache filesystem
	virtual FileSystem &GetCacheFileSystem();

	ClientContext &context;
	FileSystem &fs;
};

} // namespace duckdb

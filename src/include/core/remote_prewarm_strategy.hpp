#pragma once

#include "core/remote_block_collector.hpp"
#include "core/prewarm_strategy.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

// Forward declarations for cache_httpfs types
namespace duckdb {
class CacheFileSystem;
class FileHandle;
struct CacheHttpfsInstanceState;
} // namespace duckdb

namespace duckdb {

//===--------------------------------------------------------------------===//
// Remote Cache Mode Enum
//===--------------------------------------------------------------------===//

//! Cache mode for remote prewarming
enum class RemoteCacheMode {
	IN_MEMORY,  //! Use in-memory cache only
	ON_DISK,    //! Use on-disk cache only
	USE_CURRENT //! Use current cache_httpfs settings
};

//===--------------------------------------------------------------------===//
// Remote Prewarm Strategy
//===--------------------------------------------------------------------===//

//! Strategy for prewarming remote file blocks into cache_httpfs cache
class RemotePrewarmStrategy : public PrewarmStrategy {
public:
	RemotePrewarmStrategy(ClientContext &context_p, shared_ptr<CacheHttpfsInstanceState> cache_state_p,
	                      RemoteCacheMode mode_p);

	//! Execute prewarm on remote blocks
	//! @param blocks Vector of blocks to prewarm
	//! @param max_blocks Maximum blocks to prewarm (0 = no limit)
	//! @return Number of blocks successfully prewarmed
	idx_t Execute(const unordered_map<string, vector<RemoteBlockInfo>> &file_blocks, idx_t max_blocks);

private:
	//! Filter blocks that are already in cache
	vector<RemoteBlockInfo> FilterCachedBlocks(const string &file_path, const vector<RemoteBlockInfo> &blocks);

	//! Calculate maximum number of blocks we can cache
	BufferCapacityInfo CalculateMaxAvailableBlocks() override;

	//! Get cache filesystem from the database instance
	CacheFileSystem *GetCacheFileSystem();

	//! Apply the desired cache mode (save and restore original if needed)
	void ApplyCacheMode();

	//! Restore original cache mode
	void RestoreCacheMode();

	ClientContext &context;
	shared_ptr<CacheHttpfsInstanceState> cache_state;
	RemoteCacheMode cache_mode;
	string original_cache_type;
	bool cache_mode_changed;
};

} // namespace duckdb

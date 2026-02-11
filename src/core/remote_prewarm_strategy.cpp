#include "core/remote_prewarm_strategy.hpp"

#include "core/prewarm_strategy.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "scope_guard.hpp"

// Include cache_httpfs headers
#include "cache_filesystem.hpp"
#include "cache_filesystem_config.hpp"
#include "cache_httpfs_instance_state.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "thread_pool.hpp"
#include <future>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// RemotePrewarmStrategy Implementation
//===--------------------------------------------------------------------===//

RemotePrewarmStrategy::RemotePrewarmStrategy(ClientContext &context_p,
                                             shared_ptr<CacheHttpfsInstanceState> cache_state_p, RemoteCacheMode mode_p)
    : PrewarmStrategy(context_p), context(context_p), cache_state(std::move(cache_state_p)), cache_mode(mode_p),
      cache_mode_changed(false) {
}

CacheFileSystem *RemotePrewarmStrategy::GetCacheFileSystem() {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &fs = db.GetFileSystem();
	return &fs.Cast<CacheFileSystem>();
}

void RemotePrewarmStrategy::ApplyCacheMode() {
	if (cache_mode == RemoteCacheMode::USE_CURRENT) {
		// No change needed
		cache_mode_changed = false;
		return;
	}

	// Save original mode
	original_cache_type = cache_state->config.cache_type;

	// Apply new mode
	string new_mode;
	switch (cache_mode) {
	case RemoteCacheMode::IN_MEMORY:
		new_mode = *IN_MEM_CACHE_TYPE;
		break;
	case RemoteCacheMode::ON_DISK:
		new_mode = *ON_DISK_CACHE_TYPE;
		break;
	case RemoteCacheMode::BOTH:
		// For "both", we'll use on-disk cache which typically includes in-memory caching
		new_mode = *ON_DISK_CACHE_TYPE;
		break;
	default:
		return;
	}

	if (new_mode != original_cache_type) {
		cache_state->config.cache_type = new_mode;
		auto state_shared = cache_state;
		cache_state->cache_reader_manager.SetCacheReader(cache_state->config, state_shared);
		cache_mode_changed = true;
	}
}

void RemotePrewarmStrategy::RestoreCacheMode() {
	if (cache_mode_changed) {
		cache_state->config.cache_type = original_cache_type;
		auto state_shared = cache_state;
		cache_state->cache_reader_manager.SetCacheReader(cache_state->config, state_shared);
	}
}

vector<RemoteBlockInfo> RemotePrewarmStrategy::FilterCachedBlocks(const string &file_path,
                                                                  const vector<RemoteBlockInfo> &blocks) {
	// TODO: implement
	return blocks;
}

BufferCapacityInfo RemotePrewarmStrategy::CalculateMaxAvailableBlocks() {
	// TODO: expose capacity, used space, available space info from cache_httpfs's cache reader
	return BufferCapacityInfo {
	    .block_size = cache_state->config.cache_block_size,
	    .max_capacity = UINT64_MAX,
	    .used_space = 0,
	    .available_space = UINT64_MAX,
	    .max_blocks = UINT64_MAX,
	};
}

idx_t RemotePrewarmStrategy::Execute(const unordered_map<string, vector<RemoteBlockInfo>> &file_blocks,
                                     idx_t max_blocks) {
	if (file_blocks.empty()) {
		return 0;
	}

	// Apply cache mode (with RAII restoration)
	ApplyCacheMode();
	auto scope_guard = ScopeGuard([this]() { RestoreCacheMode(); });

	idx_t total_blocks = 0, total_uncached_blocks = 0;
	for (auto blocks : file_blocks) {
		total_blocks += blocks.second.size();
		auto uncached_blocks = FilterCachedBlocks(blocks.first, blocks.second);
		total_uncached_blocks += uncached_blocks.size();
		blocks.second = std::move(uncached_blocks);
	}

	auto capacity_info = CalculateMaxAvailableBlocks();

	// Apply max_blocks parameter
	idx_t blocks_to_prewarm = std::min<idx_t>(total_uncached_blocks, capacity_info.max_blocks);

	if (blocks_to_prewarm < total_uncached_blocks) {
		idx_t blocks_skipped = total_uncached_blocks - blocks_to_prewarm;
		total_uncached_blocks = blocks_to_prewarm;
		DUCKDB_LOG_WARN(context,
		                "Cache capacity limit reached.\n"
		                "  Total blocks: %llu (%llu already cached, %llu uncached)\n"
		                "  Prewarming: %llu blocks (skipping %llu due to capacity)",
		                total_blocks, total_blocks - total_uncached_blocks, total_uncached_blocks, blocks_to_prewarm,
		                blocks_skipped);
	}

	unordered_map<string, unique_ptr<FileHandle>> file_handles;
	for (auto &blocks : file_blocks) {
		auto file_handle = GetCacheFileSystem()->OpenFile(blocks.first, FileOpenFlags::FILE_FLAGS_READ);
		file_handles[blocks.first] = std::move(file_handle);
	}

	const auto task_count = GetThreadCountForSubrequests(blocks_to_prewarm);
	ThreadPool io_threads(task_count);
	vector<std::future<void>> prewarm_futures;
	prewarm_futures.reserve(blocks_to_prewarm);
	idx_t prewarmed_blocks = 0;
	for (auto &blocks : file_blocks) {
		auto file_handle = file_handles[blocks.first].get();
		for (auto &block : blocks.second) {
			if (prewarmed_blocks >= blocks_to_prewarm) {
				break;
			}
			auto future = io_threads.Push([this, block, &file_handle]() {
				// TODO: add a easy buffer pool to reuse the buffer
				auto buffer = unique_ptr<char[]>(new char[block.size]);
				file_handle->Read(buffer.get(), block.size, block.offset);
			});
			prewarm_futures.emplace_back(std::move(future));
			prewarmed_blocks++;
		}
	}

	io_threads.Wait();
	for (auto &future : prewarm_futures) {
		future.get();
	}

	return blocks_to_prewarm;
}

} // namespace duckdb

#include "core/remote_prewarm_strategy.hpp"

#include "core/prewarm_strategy.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "scope_guard.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include <future>

namespace duckdb {

RemotePrewarmStrategy::RemotePrewarmStrategy(ClientContext &context_p, FileSystem &fs_p)
    : PrewarmStrategy(context_p), context(context_p), fs(fs_p) {
}

FileSystem *RemotePrewarmStrategy::GetCacheFileSystem() {
	// TODO: get CacheFileSystem once we integrate with cache_httpfs
	return &fs;
}

vector<RemoteBlockInfo> RemotePrewarmStrategy::FilterCachedBlocks(const string &file_path,
                                                                  const vector<RemoteBlockInfo> &blocks) {
	// TODO: implement
	return blocks;
}

BufferCapacityInfo RemotePrewarmStrategy::CalculateMaxAvailableBlocks() {
	// TODO: expose capacity, used space, available space info from cache_httpfs's cache reader
	return BufferCapacityInfo {
	    .block_size = 1024ULL * 1024ULL,
	    .max_capacity = UINT64_MAX,
	    .used_space = 0,
	    .available_space = UINT64_MAX,
	    .max_blocks = UINT64_MAX,
	};
}

idx_t RemotePrewarmStrategy::Execute(const unordered_map<string, vector<RemoteBlockInfo>> &file_blocks,
                                     idx_t max_blocks = UINT64_MAX) {
	if (file_blocks.empty()) {
		return 0;
	}

	idx_t total_blocks = 0, total_uncached_blocks = 0;
	for (auto blocks : file_blocks) {
		total_blocks += blocks.second.size();
		auto uncached_blocks = FilterCachedBlocks(blocks.first, blocks.second);
		total_uncached_blocks += uncached_blocks.size();
		blocks.second = std::move(uncached_blocks);
	}

	auto capacity_info = CalculateMaxAvailableBlocks();

	idx_t blocks_to_prewarm = std::min<idx_t>({total_uncached_blocks, capacity_info.max_blocks, max_blocks});

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

	// TODO: use ThreadPool
	vector<std::future<void>> prewarm_futures;
	prewarm_futures.reserve(blocks_to_prewarm);
	idx_t prewarmed_blocks = 0;
	for (auto &blocks : file_blocks) {
		auto file_handle = file_handles[blocks.first].get();
		for (auto &block : blocks.second) {
			if (prewarmed_blocks >= blocks_to_prewarm) {
				break;
			}
			auto future = std::async(std::launch::async, [this, block, file_handle]() {
				// TODO: add a easy buffer pool to reuse the buffer
				auto buffer = unique_ptr<char[]>(new char[block.size]);
				file_handle->Read(buffer.get(), block.size, block.offset);
			});
			prewarm_futures.emplace_back(std::move(future));
			prewarmed_blocks++;
		}
	}

	for (auto &future : prewarm_futures) {
		future.get();
	}

	return blocks_to_prewarm;
}

} // namespace duckdb

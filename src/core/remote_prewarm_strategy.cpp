#include "core/remote_prewarm_strategy.hpp"

#include "core/prewarm_strategy.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include <future>

namespace duckdb {

RemotePrewarmStrategy::RemotePrewarmStrategy(ClientContext &context_p, FileSystem &fs_p)
    : PrewarmStrategy(context_p), context(context_p), fs(fs_p) {
}

FileSystem &RemotePrewarmStrategy::GetCacheFileSystem() {
	// TODO: get CacheFileSystem once we integrate with cache_httpfs
	return fs;
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

idx_t RemotePrewarmStrategy::Execute(const RemoteFileBlockMap &file_blocks, idx_t max_blocks) {
	if (file_blocks.empty()) {
		return 0;
	}

	idx_t total_blocks = 0, total_uncached_blocks = 0;
	RemoteFileBlockMap uncached_file_blocks;
	for (const auto &blocks : file_blocks) {
		const auto &file_path = blocks.first;
		const auto &block_list = blocks.second;

		total_blocks += block_list.size();
		auto uncached_blocks = FilterCachedBlocks(file_path, block_list);
		total_uncached_blocks += uncached_blocks.size();
		uncached_file_blocks[file_path] = std::move(uncached_blocks);
	}

	auto capacity_info = CalculateMaxAvailableBlocks();

	idx_t blocks_to_prewarm = std::min<idx_t>({total_uncached_blocks, capacity_info.max_blocks, max_blocks});

	if (blocks_to_prewarm < total_uncached_blocks) {
		idx_t blocks_skipped = total_uncached_blocks - blocks_to_prewarm;

		DUCKDB_LOG_DEBUG(context,
		                 "Cache capacity limit reached.\n"
		                 "  Total blocks: %llu (%llu already cached, %llu uncached)\n"
		                 "  Prewarming: %llu blocks (skipping %llu due to capacity)",
		                 total_blocks, total_blocks - total_uncached_blocks, total_uncached_blocks, blocks_to_prewarm,
		                 blocks_skipped);
		total_uncached_blocks = blocks_to_prewarm;
	}

	// map from file_path to file_handle
	unordered_map<string, unique_ptr<FileHandle>> file_handles;
	for (const auto &blocks : uncached_file_blocks) {
		const auto &file_path = blocks.first;
		const auto &block_list = blocks.second;

		if (block_list.empty()) {
			continue;
		}

		auto file_handle = GetCacheFileSystem().OpenFile(file_path, FileOpenFlags::FILE_FLAGS_READ);
		file_handles[file_path] = std::move(file_handle);
	}

	// TODO: use ThreadPool
	vector<std::future<void>> prewarm_futures;
	prewarm_futures.reserve(blocks_to_prewarm);
	idx_t prewarmed_blocks = 0;
	for (const auto &blocks : uncached_file_blocks) {
		const auto &file_path = blocks.first;
		const auto &block_list = blocks.second;

		if (block_list.empty()) {
			continue;
		}

		auto file_handle = file_handles[file_path].get();
		for (const auto &block : block_list) {
			if (prewarmed_blocks >= blocks_to_prewarm) {
				break;
			}
			auto future = std::async(std::launch::async, [this, block, file_handle]() {
				// TODO: add a easy buffer pool to reuse the buffer
				auto buffer = unique_ptr<char[]>(new char[block.size]);
				// we only care about on-disk cache file, but not return value
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

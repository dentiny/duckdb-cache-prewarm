#include "core/remote_prewarm_strategy.hpp"

#include "core/prewarm_strategy.hpp"
#include "cache_httpfs_instance_state.hpp"
#include "cache_filesystem_config.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "thread_pool_parallel_executor.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/query_context.hpp"

namespace duckdb {

RemotePrewarmStrategy::RemotePrewarmStrategy(ClientContext &context_p, FileSystem &fs_p)
    : PrewarmStrategy(context_p), fs(fs_p) {
}

vector<RemoteBlockInfo> RemotePrewarmStrategy::FilterCachedBlocks(const string & /*file_path*/,
                                                                  const vector<RemoteBlockInfo> &blocks) {
	// TODO: implement a API to do this filtering at cache_httpfs side
	return blocks;
}

BufferCapacityInfo RemotePrewarmStrategy::CalculateMaxAvailableBlocks() {
	// TODO: expose capacity, used space, available space info from cache_httpfs's cache reader
	const auto max_size = NumericLimits<idx_t>::Maximum();
	return BufferCapacityInfo {1024ULL * 1024ULL, max_size, 0, max_size, max_size};
}

idx_t RemotePrewarmStrategy::Execute(const RemoteFileBlockMap &file_blocks, idx_t max_blocks) {
	if (file_blocks.empty()) {
		return 0;
	}

	idx_t total_blocks = 0, total_uncached_blocks = 0;
	RemoteFileBlockMap uncached_file_blocks;
	uncached_file_blocks.reserve(file_blocks.size());
	// map from file_path to file_handle
	unordered_map<string, unique_ptr<FileHandle>> file_handles;
	file_handles.reserve(file_blocks.size());
	for (const auto &blocks : file_blocks) {
		const auto &file_path = blocks.first;
		const auto &block_list = blocks.second;

		total_blocks += block_list.size();
		auto uncached_blocks = FilterCachedBlocks(file_path, block_list);
		total_uncached_blocks += uncached_blocks.size();
		if (uncached_blocks.empty()) {
			// TODO: add a debug logging that we skipped file
			continue;
		}
		uncached_file_blocks[file_path] = std::move(uncached_blocks);
		auto file_handle =
		    fs.OpenFile(file_path, FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (!file_handle) {
			// TODO: add a debug logging that we skipped file
			continue;
		}
		file_handles[file_path] = std::move(file_handle);
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

	const CacheHttpfsInstanceState &instance_state = GetInstanceStateOrThrow(context);
	const auto task_count = GetThreadCountForSubrequests(blocks_to_prewarm, instance_state.config.max_subrequest_count);
	ThreadPoolParallelExecutor executor(task_count);
	auto &allocator = BufferAllocator::Get(context);
	const QueryContext query_context(context);
	idx_t prewarmed_blocks = 0;
	idx_t bytes_prewarmed = 0;
	for (const auto &blocks : uncached_file_blocks) {
		const auto &file_path = blocks.first;
		const auto &block_list = blocks.second;

		auto file_handle = file_handles[file_path].get();
		for (const auto &block : block_list) {
			if (prewarmed_blocks >= blocks_to_prewarm) {
				break;
			}
			executor.Schedule([block, file_handle, query_context, &allocator]() {
				// TODO: add a easy buffer pool to reuse the buffer
				auto buffer = allocator.Allocate(NumericCast<idx_t>(block.size));
				// we only care about on-disk cache file, but not return value
				file_handle->Read(query_context, buffer.get(), NumericCast<idx_t>(block.size), block.offset);
			});
			bytes_prewarmed += static_cast<idx_t>(block.size);
			prewarmed_blocks++;
		}
	}

	executor.WaitAll();

	return bytes_prewarmed;
}

} // namespace duckdb

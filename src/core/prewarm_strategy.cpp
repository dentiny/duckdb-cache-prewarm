#include "core/prewarm_strategy.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

namespace {
//! Maximum fraction of available (unused) buffer pool memory to use for prewarming.
//! Applied to remaining memory after subtracting current buffer pool usage (max_memory - used_memory).
//! The 0.8 ratio leaves 20% headroom for concurrent operations and prevents buffer pool overload.
//! The 0.8 ratio leaves 20% headroom for concurrent operations and prevents buffer pool overload.
constexpr double PREWARM_BUFFER_USAGE_RATIO = 0.8;
} // namespace

idx_t PrewarmStrategy::CalculateBlocksPerTask(idx_t block_size, idx_t max_blocks, idx_t max_threads,
                                              idx_t target_bytes) {
	if (max_blocks == 0) {
		return 0;
	}
	auto target_blocks = std::max<idx_t>(1, target_bytes / block_size);
	auto concurrency = std::max<idx_t>(1, std::min<idx_t>(max_blocks, max_threads));
	auto max_blocks_per_task = std::max<idx_t>(1, max_blocks / concurrency);
	return std::min<idx_t>(target_blocks, max_blocks_per_task);
}

void LocalPrewarmStrategy::CheckDirectIO(const string &strategy_name) {
	if (context.db->config.options.use_direct_io) {
		throw InvalidInputException("%s prewarming strategy is not effective when direct I/O is enabled. "
		                            "Direct I/O bypasses the OS page cache. "
		                            "Use the BUFFER strategy instead to warm DuckDB's internal buffer pool.",
		                            strategy_name);
	}
}

BufferCapacityInfo LocalPrewarmStrategy::CalculateMaxAvailableBlocks() {
	BufferCapacityInfo info;
	info.block_size = block_manager.GetBlockAllocSize();
	info.max_capacity = buffer_manager.GetMaxMemory();
	info.used_space = buffer_manager.GetUsedMemory();
	info.available_space = info.max_capacity > info.used_space ? info.max_capacity - info.used_space : 0;

	// It is possible due to concurrent access for buffer pool
	D_ASSERT(info.used_space <= info.max_capacity);

	// Calculate maximum blocks we can load
	info.max_blocks = static_cast<idx_t>((static_cast<double>(info.available_space) * PREWARM_BUFFER_USAGE_RATIO) /
	                                     static_cast<double>(info.block_size));

	return info;
}

vector<shared_ptr<BlockHandle>>
LocalPrewarmStrategy::GetUnloadedBlockHandles(const unordered_set<block_id_t> &block_ids) {
	vector<shared_ptr<BlockHandle>> unloaded_handles;
	unloaded_handles.reserve(block_ids.size());
	for (block_id_t block_id : block_ids) {
		auto handle = block_manager.RegisterBlock(block_id);
		if (handle->GetState() == BlockState::BLOCK_UNLOADED) {
			unloaded_handles.emplace_back(std::move(handle));
		}
	}

	return unloaded_handles;
}

} // namespace duckdb

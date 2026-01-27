#include "core/buffer_prewarm_strategy.hpp"

#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

idx_t BufferPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	vector<shared_ptr<BlockHandle>> all_handles;
	all_handles.reserve(block_ids.size());
	for (block_id_t block_id : block_ids) {
		auto handle = block_manager.RegisterBlock(block_id);
		all_handles.emplace_back(std::move(handle));
	}

	vector<shared_ptr<BlockHandle>> unloaded_handles;
	for (auto &handle : all_handles) {
		if (handle->GetState() == BlockState::BLOCK_UNLOADED) {
			unloaded_handles.emplace_back(std::move(handle));
		}
	}

	if (unloaded_handles.empty()) {
		return unloaded_handles.size();
	}

	auto capacity_info = CalculateMaxAvailableBlocks();

	idx_t total_blocks = all_handles.size();
	idx_t already_cached = all_handles.size() - unloaded_handles.size();
	idx_t blocks_to_prewarm = unloaded_handles.size();

	if (unloaded_handles.size() > capacity_info.max_blocks) {
		idx_t blocks_skipped = unloaded_handles.size() - capacity_info.max_blocks;
		unloaded_handles.resize(capacity_info.max_blocks);

		DUCKDB_LOG_WARN(context,
		                "Buffer pool capacity limit reached.\n"
		                "  Table blocks: %llu total (%llu already cached, %llu unloaded)\n"
		                "  Prewarming: %llu blocks (skipping %llu due to capacity)\n"
		                "  Memory: %llu bytes available, %llu bytes required for all unloaded blocks",
		                total_blocks, already_cached, blocks_to_prewarm, capacity_info.max_blocks, blocks_skipped,
		                capacity_info.available_memory, blocks_to_prewarm * capacity_info.block_size);
	}

	buffer_manager.Prefetch(unloaded_handles);

	// Return attempted to load blocks count
	return all_handles.size();
}

} // namespace duckdb

#include "core/read_prewarm_strategy.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/storage/storage_info.hpp"
#include <algorithm>

namespace duckdb {

idx_t ReadPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	CheckDirectIO("READ");

	auto unloaded_handles = GetUnloadedBlockHandles(block_ids);
	if (unloaded_handles.empty()) {
		return 0;
	}

	idx_t blocks_read = 0;
	auto block_size = block_manager.GetBlockAllocSize();

	auto capacity_info = CalculateMaxAvailableBlocks();
	idx_t max_batch_size = capacity_info.max_blocks;
	if (max_batch_size == 0) {
		DUCKDB_LOG_WARN(context,
		                "Insufficient memory to prewarm any blocks (available: %llu bytes, block size: %llu bytes)",
		                capacity_info.available_memory, capacity_info.block_size);
		return 0;
	}

	// Sort unloaded block IDs for sequential reading
	std::sort(
	    unloaded_handles.begin(), unloaded_handles.end(),
	    [](const shared_ptr<BlockHandle> &a, const shared_ptr<BlockHandle> &b) { return a->BlockId() < b->BlockId(); });

	// Read blocks in batches where possible
	for (size_t i = 0; i < unloaded_handles.size();) {
		block_id_t first_block = unloaded_handles[i]->BlockId();
		idx_t block_count = 1;

		// Find consecutive blocks and limit the batch size to prevent memory overflow
		while (i + block_count < unloaded_handles.size() &&
		       unloaded_handles[i + block_count]->BlockId() == first_block + block_count &&
		       block_count < max_batch_size && (block_count + 1) * block_size <= INT_MAX) {
			block_count++;
		}

		// Allocate temporary buffer for reading
		auto total_size = block_count * block_size;
		auto temp_buffer = buffer_manager.Allocate(MemoryTag::BASE_TABLE, total_size, true);

		// Read blocks from storage
		try {
			// TODO: we could parallel read blocks here.
			block_manager.ReadBlocks(temp_buffer.GetFileBuffer(), first_block, block_count);
			blocks_read += block_count;
		} catch (const IOException &e) {
			// TODO: the SingleFileBlockManager::ReadBlock sometime throws file out-of-bounds exception, we have to do
			// further investigation and fix it.
			// https://github.com/dentiny/duckdb-cache-prewarm/issues/23
			continue;
		}
		i += block_count;
	}

	return blocks_read;
}

} // namespace duckdb

#include "core/read_prewarm_strategy.hpp"

#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/logging/logger.hpp"
#include <algorithm>

namespace duckdb {

idx_t ReadPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	CheckDirectIO("READ");

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

	// Sort block IDs for sequential reading
	vector<block_id_t> sorted_blocks(block_ids.begin(), block_ids.end());
	std::sort(sorted_blocks.begin(), sorted_blocks.end());

	// Read blocks in batches where possible
	for (size_t i = 0; i < sorted_blocks.size();) {
		block_id_t first_block = sorted_blocks[i];
		block_id_t last_block = first_block;
		idx_t block_count = 1;

		// Find consecutive blocks and limit the batch size to prevent memory overflow
		while (i + block_count < sorted_blocks.size() && sorted_blocks[i + block_count] == first_block + block_count &&
		       block_count < max_batch_size) {
			last_block = sorted_blocks[i + block_count];
			block_count++;
		}

		// Allocate temporary buffer for reading
		auto total_size = block_count * block_size;
		auto temp_buffer = buffer_manager.Allocate(MemoryTag::BASE_TABLE, total_size, true);

		// Read blocks from storage
		block_manager.ReadBlocks(temp_buffer.GetFileBuffer(), first_block, block_count);
		blocks_read += block_count;
		i += block_count;
	}

	return blocks_read;
}

} // namespace duckdb

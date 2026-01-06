#include "core/prewarm_strategies.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>

namespace duckdb {

idx_t BufferPrewarmStrategy::Execute(ClientContext &context, DuckTableEntry &table_entry,
                                     const unordered_set<block_id_t> &block_ids) {
	auto &data_table = table_entry.GetStorage();
	auto &table_io = TableIOManager::Get(data_table);
	auto &block_manager = table_io.GetBlockManagerForRowData();
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	idx_t blocks_loaded = 0;
	vector<shared_ptr<BlockHandle>> handles;

	// Register all blocks first
	for (block_id_t block_id : block_ids) {
		try {
			auto handle = block_manager.RegisterBlock(block_id);
			handles.push_back(handle);
		} catch (const Exception &e) {
			// Block might not exist, skip it
			continue;
		}
	}

	// Load blocks into buffer pool
	// Use Pin for all blocks (can optimize later with BatchRead for sequential blocks)
	for (auto &handle : handles) {
		try {
			// Pin the block - this loads it into the buffer pool
			auto buffer_handle = buffer_manager.Pin(handle);
			if (buffer_handle.IsValid()) {
				blocks_loaded++;
				// Unpin immediately - block is loaded but may be evicted if memory pressure
				buffer_manager.Unpin(handle);
			}
		} catch (const Exception &e) {
			// Failed to load block, continue with next
			continue;
		}
	}

	return blocks_loaded;
}

idx_t ReadPrewarmStrategy::Execute(ClientContext &context, DuckTableEntry &table_entry,
                                   const unordered_set<block_id_t> &block_ids) {
	auto &data_table = table_entry.GetStorage();
	auto &table_io = TableIOManager::Get(data_table);
	auto &block_manager = table_io.GetBlockManagerForRowData();
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	idx_t blocks_read = 0;
	auto block_size = block_manager.GetBlockAllocSize();

	// Sort block IDs for sequential reading
	vector<block_id_t> sorted_blocks(block_ids.begin(), block_ids.end());
	std::sort(sorted_blocks.begin(), sorted_blocks.end());

	// Read blocks in batches where possible
	for (size_t i = 0; i < sorted_blocks.size();) {
		block_id_t first_block = sorted_blocks[i];
		block_id_t last_block = first_block;
		idx_t block_count = 1;

		// Find consecutive blocks
		while (i + block_count < sorted_blocks.size() && sorted_blocks[i + block_count] == first_block + block_count) {
			last_block = sorted_blocks[i + block_count];
			block_count++;
		}

		try {
			// Allocate temporary buffer for reading
			auto total_size = block_count * block_size;
			auto temp_buffer = buffer_manager.Allocate(MemoryTag::BASE_TABLE, total_size, true);

			// Read blocks from storage
			block_manager.ReadBlocks(temp_buffer.GetFileBuffer(), first_block, block_count);
			blocks_read += block_count;

			// Buffer is automatically freed when temp_buffer goes out of scope
		} catch (const Exception &e) {
			// Failed to read, try individual blocks
			for (idx_t j = 0; j < block_count; j++) {
				try {
					auto temp_buffer = buffer_manager.Allocate(MemoryTag::BASE_TABLE, block_size, true);
					block_manager.ReadBlocks(temp_buffer.GetFileBuffer(), sorted_blocks[i + j], 1);
					blocks_read++;
				} catch (const Exception &e2) {
					// Skip this block
					continue;
				}
			}
		}

		i += block_count;
	}

	return blocks_read;
}

idx_t PrefetchPrewarmStrategy::Execute(ClientContext &context, DuckTableEntry &table_entry,
                                       const unordered_set<block_id_t> &block_ids) {
	auto &data_table = table_entry.GetStorage();
	auto &table_io = TableIOManager::Get(data_table);
	auto &block_manager = table_io.GetBlockManagerForRowData();
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	idx_t blocks_prefetched = 0;
	vector<shared_ptr<BlockHandle>> handles;

	// Register all blocks
	for (block_id_t block_id : block_ids) {
		try {
			auto handle = block_manager.RegisterBlock(block_id);
			handles.push_back(handle);
		} catch (const Exception &e) {
			// Block might not exist, skip it
			continue;
		}
	}

	// Prefetch blocks (hint OS to prefetch, non-blocking)
	if (!handles.empty()) {
		buffer_manager.Prefetch(handles);
		blocks_prefetched = handles.size();
	}

	return blocks_prefetched;
}

//===--------------------------------------------------------------------===//
// Strategy Factory
//===--------------------------------------------------------------------===//

unique_ptr<PrewarmStrategy> CreatePrewarmStrategy(PrewarmMode mode) {
	switch (mode) {
	case PrewarmMode::BUFFER:
		return make_uniq<BufferPrewarmStrategy>();
	case PrewarmMode::READ:
		return make_uniq<ReadPrewarmStrategy>();
	case PrewarmMode::PREFETCH:
		return make_uniq<PrefetchPrewarmStrategy>();
	default:
		throw InternalException("Unknown prewarm mode");
	}
}

} // namespace duckdb


#include "core/prewarm_strategies.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>

namespace duckdb {
void PrewarmStrategy::CheckDirectIO(ClientContext &context, const string &strategy_name) {
	if (context.db->config.options.use_direct_io) {
		throw InvalidInputException(
		    StringUtil::Format("%s prewarming strategy is not effective when direct I/O is enabled. "
		                       "Direct I/O bypasses the OS page cache. "
		                       "Use the BUFFER strategy instead to warm DuckDB's internal buffer pool.",
		                       strategy_name));
	}
}

idx_t BufferPrewarmStrategy::Execute(ClientContext &context, DuckTableEntry &table_entry,
                                     const unordered_set<block_id_t> &block_ids) {
	auto &data_table = table_entry.GetStorage();
	auto &table_io = TableIOManager::Get(data_table);
	auto &block_manager = table_io.GetBlockManagerForRowData();
	auto &buffer_manager = BufferManager::GetBufferManager(context);

	idx_t blocks_loaded_before = 0;
	vector<shared_ptr<BlockHandle>> handles;
	handles.reserve(block_ids.size());

	for (block_id_t block_id : block_ids) {
		auto handle = block_manager.RegisterBlock(block_id);
		handles.emplace_back(handle);
	}

	buffer_manager.Prefetch(handles);

	return block_ids.size();
}

idx_t ReadPrewarmStrategy::Execute(ClientContext &context, DuckTableEntry &table_entry,
                                   const unordered_set<block_id_t> &block_ids) {
	CheckDirectIO(context, "READ");

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
	// TODO: use fadvise for linux and fcntl with F_RDADVISE for macOS and BSD
	throw NotImplementedException("PREFETCH prewarm strategy is not yet implemented");
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

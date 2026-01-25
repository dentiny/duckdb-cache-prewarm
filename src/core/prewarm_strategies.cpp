#include "core/prewarm_strategies.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include <cstdio>

namespace duckdb {
void PrewarmStrategy::CheckDirectIO(const string &strategy_name) {
	if (context.db->config.options.use_direct_io) {
		throw InvalidInputException(
		    StringUtil::Format("%s prewarming strategy is not effective when direct I/O is enabled. "
		                       "Direct I/O bypasses the OS page cache. "
		                       "Use the BUFFER strategy instead to warm DuckDB's internal buffer pool.",
		                       strategy_name));
	}
}

idx_t BufferPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	vector<shared_ptr<BlockHandle>> all_handles;
	all_handles.reserve(block_ids.size());
	for (block_id_t block_id : block_ids) {
		auto handle = block_manager.RegisterBlock(block_id);
		all_handles.emplace_back(handle);
	}

	vector<shared_ptr<BlockHandle>> unloaded_handles;
	for (const auto &handle : all_handles) {
		if (handle->GetState() == BlockState::BLOCK_UNLOADED) {
			unloaded_handles.emplace_back(handle);
		}
	}

	if (!unloaded_handles.empty()) {
		auto block_size = block_manager.GetBlockAllocSize();
		auto max_memory = buffer_manager.GetMaxMemory();
		auto used_memory = buffer_manager.GetUsedMemory();
		auto available_memory = max_memory > used_memory ? max_memory - used_memory : 0;

		// Calculate maximum blocks we can load (use 80% of available to avoid eviction churn)
		idx_t max_blocks = (available_memory * 4) / (block_size * 5);

		if (unloaded_handles.size() > max_blocks) {
			idx_t blocks_to_remove = unloaded_handles.size() - max_blocks;
			unloaded_handles.resize(max_blocks);

			fprintf(stderr,
			    "WARNING: Buffer pool capacity limit reached. Only prewarming %llu out of %llu unloaded blocks. "
			    "%llu blocks skipped. Available memory: %llu bytes, required: %llu bytes.\n",
			    max_blocks,
			    max_blocks + blocks_to_remove,
			    blocks_to_remove,
			    available_memory,
			    (max_blocks + blocks_to_remove) * block_size);
		}

		buffer_manager.Prefetch(unloaded_handles);
	}

	// Return attempted to load blocks count
	return all_handles.size();
}

idx_t ReadPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	CheckDirectIO("READ");

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

idx_t PrefetchPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	// TODO: use fadvise for linux and fcntl with F_RDADVISE for macOS and BSD
	throw NotImplementedException("PREFETCH prewarm strategy is not yet implemented");
}

//===--------------------------------------------------------------------===//
// Strategy Factory
//===--------------------------------------------------------------------===//

unique_ptr<PrewarmStrategy> CreatePrewarmStrategy(PrewarmMode mode, BlockManager &block_manager,
                                                  BufferManager &buffer_manager, ClientContext &context) {
	switch (mode) {
	case PrewarmMode::BUFFER:
		return make_uniq<BufferPrewarmStrategy>(block_manager, buffer_manager, context);
	case PrewarmMode::READ:
		return make_uniq<ReadPrewarmStrategy>(block_manager, buffer_manager, context);
	case PrewarmMode::PREFETCH:
		return make_uniq<PrefetchPrewarmStrategy>(block_manager, buffer_manager, context);
	default:
		throw InternalException("Unknown prewarm mode");
	}
}

} // namespace duckdb

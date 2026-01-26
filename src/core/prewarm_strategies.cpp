#include "core/prewarm_strategies.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include <algorithm>

namespace duckdb {

//! Maximum fraction of available buffer pool memory to use for prewarming
static constexpr double PREWARM_BUFFER_USAGE_RATIO = 0.8;

void PrewarmStrategy::CheckDirectIO(const string &strategy_name) {
	if (context.db->config.options.use_direct_io) {
		throw InvalidInputException(
		    StringUtil::Format("%s prewarming strategy is not effective when direct I/O is enabled. "
		                       "Direct I/O bypasses the OS page cache. "
		                       "Use the BUFFER strategy instead to warm DuckDB's internal buffer pool.",
		                       strategy_name));
	}
}

BufferCapacityInfo PrewarmStrategy::CalculateMaxAvailableBlocks() {
	BufferCapacityInfo info;
	info.block_size = block_manager.GetBlockAllocSize();
	info.max_memory = buffer_manager.GetMaxMemory();
	info.used_memory = buffer_manager.GetUsedMemory();
	info.available_memory = info.max_memory > info.used_memory ? info.max_memory - info.used_memory : 0;

	// Calculate maximum blocks we can load
	info.max_blocks = static_cast<idx_t>((static_cast<double>(info.available_memory) * PREWARM_BUFFER_USAGE_RATIO) /
	                                     static_cast<double>(info.block_size));

	return info;
}

idx_t BufferPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	vector<shared_ptr<BlockHandle>> all_handles;
	all_handles.reserve(block_ids.size());
	for (block_id_t block_id : block_ids) {
		auto handle = block_manager.RegisterBlock(block_id);
		all_handles.emplace_back(handle);
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

idx_t ReadPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	CheckDirectIO("READ");

	idx_t blocks_read = 0;
	auto block_size = block_manager.GetBlockAllocSize();

	auto capacity_info = CalculateMaxAvailableBlocks();
	idx_t max_batch_size = capacity_info.max_blocks;
	if (max_batch_size == 0) {
		DUCKDB_LOG_WARN(context, "Insufficient memory to prewarm any blocks (available: %llu bytes, block size: %llu bytes)",
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
		while (i + block_count < sorted_blocks.size() &&
		       sorted_blocks[i + block_count] == first_block + block_count &&
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

idx_t PrefetchPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	// TODO: use fadvise for linux and fcntl with F_RDADVISE for macOS and BSD
	throw NotImplementedException("PREFETCH prewarm strategy is not yet implemented");
}

//===--------------------------------------------------------------------===//
// Strategy Factory
//===--------------------------------------------------------------------===//

unique_ptr<PrewarmStrategy> CreatePrewarmStrategy(ClientContext &context, PrewarmMode mode, BlockManager &block_manager,
                                                  BufferManager &buffer_manager) {
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

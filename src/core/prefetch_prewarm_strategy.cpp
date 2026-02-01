#include "core/prefetch_prewarm_strategy.hpp"
#include "core/os_prefetch.hpp"

#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

idx_t PrefetchPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	CheckDirectIO("PREFETCH");

	auto block_size = block_manager.GetBlockAllocSize();

	// Sort block IDs for sequential prefetch hints
	vector<block_id_t> sorted_blocks(block_ids.begin(), block_ids.end());
	std::sort(sorted_blocks.begin(), sorted_blocks.end());

#ifndef _WIN32
	// Get the database file path from the storage manager
	auto &catalog = table_entry.ParentCatalog();
	auto &storage_manager = StorageManager::Get(catalog);
	string db_path = storage_manager.GetDBPath();

	idx_t blocks_prefetched = OSPrefetchBlocks(db_path, sorted_blocks, block_size);

	return blocks_prefetched;

#else
	// Non-Unix platforms not supported
	throw NotImplementedException(
	    "PREFETCH prewarm strategy is only supported on Unix-like systems (Linux, macOS, BSD)");
#endif
}

} // namespace duckdb

#include "core/block_collector.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

unordered_set<block_id_t> BlockCollector::CollectTableBlocks(DuckTableEntry &table_entry) {
	auto &data_table = table_entry.GetStorage();
	auto &table_io = TableIOManager::Get(data_table);
	auto &block_manager = table_io.GetBlockManagerForRowData();

	// TODO: GetColumnSegmentInfo() will load some blocks for this table into memory as a side effect
	// This is because string columns and other compression types need to read
	// block headers to get dictionary/metadata information.
	// Need to figure out a way to avoid this side effect.
	auto segment_infos = table_entry.GetColumnSegmentInfo();
	unordered_set<block_id_t> block_ids;
	block_ids.reserve(segment_infos.size() * 2);
	for (const auto &segment_info : segment_infos) {
		if (segment_info.persistent) {
			// Add main block
			if (segment_info.block_id != INVALID_BLOCK) {
				block_ids.insert(segment_info.block_id);
			}
			// Add additional blocks (for compressed segments)
			for (block_id_t additional_block : segment_info.additional_blocks) {
				if (additional_block != INVALID_BLOCK) {
					block_ids.insert(additional_block);
				}
			}
		}
	}
	return block_ids;
}

} // namespace duckdb

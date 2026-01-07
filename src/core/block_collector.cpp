#include "core/block_collector.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

unordered_set<block_id_t> BlockCollector::CollectTableBlocks(DuckTableEntry &table_entry) {
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

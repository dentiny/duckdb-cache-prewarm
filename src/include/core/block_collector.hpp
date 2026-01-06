#pragma once

#include "duckdb.hpp"
#include "cache_prewarm_extension.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Block Collector
//===--------------------------------------------------------------------===//

//! Collects block IDs from a table's column segments
class BlockCollector {
public:
	//! Collect all block IDs from a table's ColumnSegmentInfo
	//! Only includes persistent segments
	static void CollectTableBlocks(DuckTableEntry &table_entry, unordered_set<block_id_t> &block_ids);

	//! Collect block IDs from a table entry and return them
	static unordered_set<block_id_t> CollectTableBlocks(DuckTableEntry &table_entry);
};

} // namespace duckdb

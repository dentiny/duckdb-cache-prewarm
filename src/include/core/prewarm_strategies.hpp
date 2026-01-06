#pragma once

#include "cache_prewarm_extension.hpp"
#include "core/block_collector.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Prewarm Strategy Interface
//===--------------------------------------------------------------------===//

//! Base interface for prewarm strategies
class PrewarmStrategy {
public:
	virtual ~PrewarmStrategy() = default;

	//! Execute prewarm operation on the given table and blocks
	//! Returns number of blocks successfully prewarmed
	virtual idx_t Execute(ClientContext &context, DuckTableEntry &table_entry,
	                      const unordered_set<block_id_t> &block_ids) = 0;
};

//===--------------------------------------------------------------------===//
// Concrete Prewarm Strategies
//===--------------------------------------------------------------------===//

//! Prewarm strategy: Load blocks into buffer pool
class BufferPrewarmStrategy : public PrewarmStrategy {
public:
	idx_t Execute(ClientContext &context, DuckTableEntry &table_entry,
	              const unordered_set<block_id_t> &block_ids) override;
};

//! Prewarm strategy: Read blocks directly from storage (not into buffer pool)
class ReadPrewarmStrategy : public PrewarmStrategy {
public:
	idx_t Execute(ClientContext &context, DuckTableEntry &table_entry,
	              const unordered_set<block_id_t> &block_ids) override;
};

//! Prewarm strategy: Hint OS to prefetch blocks (non-blocking)
class PrefetchPrewarmStrategy : public PrewarmStrategy {
public:
	idx_t Execute(ClientContext &context, DuckTableEntry &table_entry,
	              const unordered_set<block_id_t> &block_ids) override;
};

//===--------------------------------------------------------------------===//
// Strategy Factory
//===--------------------------------------------------------------------===//

//! Create a prewarm strategy based on mode
unique_ptr<PrewarmStrategy> CreatePrewarmStrategy(PrewarmMode mode);

} // namespace duckdb

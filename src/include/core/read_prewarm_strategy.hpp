#pragma once

#include "core/prewarm_strategy.hpp"

namespace duckdb {

//! Prewarm strategy: Read blocks directly from storage (not into buffer pool)
class ReadPrewarmStrategy : public LocalPrewarmStrategy {
public:
	ReadPrewarmStrategy(ClientContext &context_p, BlockManager &block_manager_p, BufferManager &buffer_manager_p)
	    : LocalPrewarmStrategy(context_p, block_manager_p, buffer_manager_p) {
	}

	idx_t Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) override;
};

} // namespace duckdb

#pragma once

#include "core/prewarm_strategy.hpp"

namespace duckdb {

//! Prewarm strategy: Read blocks directly from storage (not into buffer pool)
class ReadPrewarmStrategy : public PrewarmStrategy {
public:
	ReadPrewarmStrategy(ClientContext &context, BlockManager &block_manager, BufferManager &buffer_manager)
	    : PrewarmStrategy(context, block_manager, buffer_manager) {
	}

	idx_t Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) override;
};

} // namespace duckdb

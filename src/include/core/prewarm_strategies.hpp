#pragma once

#include "cache_prewarm_extension.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Prewarm Strategy Interface
//===--------------------------------------------------------------------===//

//! Information about buffer pool capacity for prewarming
struct BufferCapacityInfo {
	//! Size of each block in bytes
	idx_t block_size;
	//! Maximum buffer pool memory
	idx_t max_memory;
	//! Currently used buffer pool memory
	idx_t used_memory;
	//! Available buffer pool memory (max - used)
	idx_t available_memory;
	//! Maximum blocks that can be loaded
	idx_t max_blocks;
};

//! Base interface for prewarm strategies
class PrewarmStrategy {
public:
	virtual ~PrewarmStrategy() = default;

	PrewarmStrategy(ClientContext &context_p, BlockManager &block_manager_p, BufferManager &buffer_manager_p)
	    : block_manager(block_manager_p), buffer_manager(buffer_manager_p), context(context_p) {
	}

	//! Execute prewarm operation on the given table and blocks
	//! Returns number of blocks successfully prewarmed
	//! If a provided block_id doesn't exist, it is silently skipped and not counted
	//! in the return value. The method does not throw errors for non-existent blocks.
	virtual idx_t Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) = 0;

protected:
	//! Check if direct I/O is enabled and throw an exception if OS page cache strategies won't work
	//! @param strategy_name The name of the strategy for error messaging
	void CheckDirectIO(const string &strategy_name);

	//! Calculate maximum number of blocks that can be loaded based on available buffer pool memory
	//! Uses 80% of available memory to avoid eviction churn
	//! Returns comprehensive buffer capacity information
	BufferCapacityInfo CalculateMaxAvailableBlocks();

	BlockManager &block_manager;
	BufferManager &buffer_manager;
	ClientContext &context;
};

//===--------------------------------------------------------------------===//
// Concrete Prewarm Strategies
//===--------------------------------------------------------------------===//

//! Prewarm strategy: Load blocks into buffer pool
class BufferPrewarmStrategy : public PrewarmStrategy {
public:
	BufferPrewarmStrategy(ClientContext &context, BlockManager &block_manager, BufferManager &buffer_manager)
	    : PrewarmStrategy(context, block_manager, buffer_manager) {
	}

	idx_t Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) override;
};

//! Prewarm strategy: Read blocks directly from storage (not into buffer pool)
class ReadPrewarmStrategy : public PrewarmStrategy {
public:
	ReadPrewarmStrategy(ClientContext &context, BlockManager &block_manager, BufferManager &buffer_manager)
	    : PrewarmStrategy(context, block_manager, buffer_manager) {
	}

	idx_t Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) override;
};

//! Prewarm strategy: Hint OS to prefetch blocks (non-blocking)
class PrefetchPrewarmStrategy : public PrewarmStrategy {
public:
	PrefetchPrewarmStrategy(ClientContext &context, BlockManager &block_manager, BufferManager &buffer_manager)
	    : PrewarmStrategy(context, block_manager, buffer_manager) {
	}

	idx_t Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) override;
};

//===--------------------------------------------------------------------===//
// Strategy Factory
//===--------------------------------------------------------------------===//

//! Create a prewarm strategy based on mode
unique_ptr<PrewarmStrategy> CreatePrewarmStrategy(ClientContext &context, PrewarmMode mode, BlockManager &block_manager,
                                                  BufferManager &buffer_manager);

} // namespace duckdb

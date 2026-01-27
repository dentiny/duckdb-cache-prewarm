#include "core/prefetch_prewarm_strategy.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

idx_t PrefetchPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	// TODO: use fadvise for linux and fcntl with F_RDADVISE for macOS and BSD
	throw NotImplementedException("PREFETCH prewarm strategy is not yet implemented");
}

} // namespace duckdb

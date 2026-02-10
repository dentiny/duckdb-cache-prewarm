#include "core/prewarm_strategy_factory.hpp"

#include "core/buffer_prewarm_strategy.hpp"
#include "core/read_prewarm_strategy.hpp"
#include "core/prefetch_prewarm_strategy.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Strategy Factory
//===--------------------------------------------------------------------===//

unique_ptr<LocalPrewarmStrategy> CreateLocalPrewarmStrategy(ClientContext &context, PrewarmMode mode,
                                                            BlockManager &block_manager,
                                                            BufferManager &buffer_manager) {
	switch (mode) {
	case PrewarmMode::BUFFER:
		return make_uniq<BufferPrewarmStrategy>(context, block_manager, buffer_manager);
	case PrewarmMode::READ:
		return make_uniq<ReadPrewarmStrategy>(context, block_manager, buffer_manager);
	case PrewarmMode::PREFETCH:
		return make_uniq<PrefetchPrewarmStrategy>(context, block_manager, buffer_manager);
	default:
		throw InternalException("Unknown prewarm mode");
	}
}

} // namespace duckdb

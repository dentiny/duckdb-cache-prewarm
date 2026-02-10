#pragma once

#include "core/prewarm_strategy.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Strategy Factory
//===--------------------------------------------------------------------===//

//! Create a local prewarm strategy based on mode
unique_ptr<LocalPrewarmStrategy> CreateLocalPrewarmStrategy(ClientContext &context, PrewarmMode mode,
                                                            BlockManager &block_manager, BufferManager &buffer_manager);

} // namespace duckdb

#pragma once

#include "cache_prewarm_extension.hpp"

namespace duckdb {

//! Register the prewarm_remote scalar function
void RegisterPrewarmRemoteFunction(ExtensionLoader &loader);

} // namespace duckdb

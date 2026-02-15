#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

//! Register the prewarm_remote scalar function
void RegisterPrewarmRemoteFunction(ExtensionLoader &loader);

} // namespace duckdb

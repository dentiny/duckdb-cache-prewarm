#pragma once

#include "duckdb.hpp"
#include "cache_prewarm_extension.hpp"

namespace duckdb {

//! Register the manual prewarm table function
void RegisterPrewarmFunction(ExtensionLoader &loader);

} // namespace duckdb

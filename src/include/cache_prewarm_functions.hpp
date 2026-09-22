#pragma once

namespace duckdb {

class ExtensionLoader;

//! Register all cache_prewarm scalar functions and their catalog metadata.
void RegisterCachePrewarmFunctions(ExtensionLoader &loader);

} // namespace duckdb

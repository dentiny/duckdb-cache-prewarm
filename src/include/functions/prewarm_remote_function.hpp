#pragma once

class ExtensionLoader;

namespace duckdb {

//! Register the prewarm_remote scalar function
void RegisterPrewarmRemoteFunction(ExtensionLoader &loader);

} // namespace duckdb

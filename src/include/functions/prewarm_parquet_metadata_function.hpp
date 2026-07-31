#pragma once

class ExtensionLoader;

namespace duckdb {

//! Register the prewarm_parquet_metadata scalar function.
void RegisterPrewarmParquetMetadataFunction(ExtensionLoader &loader);

} // namespace duckdb

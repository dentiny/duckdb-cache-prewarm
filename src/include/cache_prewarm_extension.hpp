#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Prewarm operation modes (matching PostgreSQL pg_prewarm)
enum class PrewarmMode {
	PREFETCH, // Load into DuckDB buffer pool via batched reads (blocks not pinned, may be evicted)
	READ,     // Synchronously read from disk into temporary process memory (not buffer pool, buffer freed immediately)
	BUFFER    // Load into DuckDB buffer pool and pin/unpin (default, blocks stay longer)
};

class CachePrewarmExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb

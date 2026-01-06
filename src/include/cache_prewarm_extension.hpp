#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Prewarm operation modes (matching PostgreSQL pg_prewarm)
enum class PrewarmMode {
	PREFETCH, // Hint OS to prefetch (non-blocking)
	READ,     // Synchronously read into process memory (not buffer pool)
	BUFFER    // Load into DuckDB buffer pool (default)
};

class CachePrewarmExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb

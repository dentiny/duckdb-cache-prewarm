#define DUCKDB_EXTENSION_MAIN

#include "cache_prewarm_extension.hpp"
#include "functions/prewarm_function.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

void LoadInternal(ExtensionLoader &loader) {
	RegisterPrewarmFunction(loader);
}

void CachePrewarmExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string CachePrewarmExtension::Name() {
	return "cache_prewarm";
}

std::string CachePrewarmExtension::Version() const {
#ifdef EXT_VERSION_CACHE_PREWARM
	return EXT_VERSION_CACHE_PREWARM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(cache_prewarm, loader) {
	duckdb::LoadInternal(loader);
}
}

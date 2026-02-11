#define DUCKDB_EXTENSION_MAIN

#include "cache_prewarm_extension.hpp"
#include "functions/prewarm_function.hpp"
#include "functions/prewarm_remote_function.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "cache_httpfs_extension.hpp"

namespace duckdb {

namespace {

constexpr const char *CACHE_HTTPFS_EXTENSION = "cache_httpfs";
}

// Whether `cache_httpfs` extension has already been loaded.
bool IsCacheHttpfsExtensionLoaded(DatabaseInstance &db_instance) {
	return db_instance.GetExtensionManager().ExtensionIsLoaded(CACHE_HTTPFS_EXTENSION);
}

void LoadInternal(ExtensionLoader &loader) {
	auto &db_instance = loader.GetDatabaseInstance();
	if (!IsCacheHttpfsExtensionLoaded(db_instance)) {
		auto extension = make_uniq<CacheHttpfsExtension>();
		extension->Load(loader);

		// Register into extension manager to keep compatibility as httpfs.
		auto &extension_manager = ExtensionManager::Get(db_instance);
		auto extension_active_load = extension_manager.BeginLoad(CACHE_HTTPFS_EXTENSION);
		// Manually fill in the extension install info to finalize extension load.
		ExtensionInstallInfo extension_install_info;
		extension_install_info.mode = ExtensionInstallMode::UNKNOWN;
		extension_active_load->FinishLoad(extension_install_info);
	}
	RegisterPrewarmFunction(loader);
	RegisterPrewarmRemoteFunction(loader);
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

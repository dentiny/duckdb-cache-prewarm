#define DUCKDB_EXTENSION_MAIN

#include "cache_prewarm_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void CachePrewarmScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "CachePrewarm " + name.GetString() + " 🐥");
	});
}

inline void CachePrewarmOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "CachePrewarm " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto cache_prewarm_scalar_function = ScalarFunction("cache_prewarm", {LogicalType::VARCHAR}, LogicalType::VARCHAR, CachePrewarmScalarFun);
	loader.RegisterFunction(cache_prewarm_scalar_function);

	// Register another scalar function
	auto cache_prewarm_openssl_version_scalar_function = ScalarFunction("cache_prewarm_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, CachePrewarmOpenSSLVersionScalarFun);
	loader.RegisterFunction(cache_prewarm_openssl_version_scalar_function);
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

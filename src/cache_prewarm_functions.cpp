#include "cache_prewarm_functions.hpp"

#include "functions/prewarm_function.hpp"
#include "functions/prewarm_remote_function.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

namespace {

FunctionDescription MakeDescription(vector<LogicalType> parameter_types, vector<string> parameter_names,
                                    string description, vector<string> examples, vector<string> categories) {
	FunctionDescription result;
	result.parameter_types = std::move(parameter_types);
	result.parameter_names = std::move(parameter_names);
	result.description = std::move(description);
	result.examples = std::move(examples);
	result.categories = std::move(categories);
	return result;
}

void RegisterScalarFunction(ExtensionLoader &loader, ScalarFunctionSet function,
                            vector<FunctionDescription> descriptions) {
	CreateScalarFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions = std::move(descriptions);
	loader.RegisterFunction(std::move(info));
}

} // namespace

void RegisterCachePrewarmFunctions(ExtensionLoader &loader) {
	const string prewarm_description =
	    "Preloads a local DuckDB table into the buffer pool or operating system page cache and returns the number of "
	    "bytes prewarmed.";
	const string remote_description =
	    "Preloads files matching a path or URL pattern into the cache_httpfs cache and returns the number of bytes "
	    "prewarmed.";
	const vector<string> categories = {"storage", "cache"};

	RegisterScalarFunction(loader, GetPrewarmFunction(),
	                       {MakeDescription({LogicalType::VARCHAR}, {"table_name"}, prewarm_description,
	                                        {"SELECT prewarm('events');"}, categories),
	                        MakeDescription({LogicalType::VARCHAR, LogicalType::VARCHAR}, {"table_name", "mode"},
	                                        prewarm_description, {"SELECT prewarm('events', 'prefetch');"}, categories),
	                        MakeDescription({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                                        {"table_name", "mode", "max_size"}, prewarm_description,
	                                        {"SELECT prewarm('events', 'buffer', 1000000);"}, categories),
	                        MakeDescription({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                        {"table_name", "mode", "max_size"}, prewarm_description,
	                                        {"SELECT prewarm('events', 'buffer', '1GB');"}, categories)});

	RegisterScalarFunction(
	    loader, GetPrewarmRemoteFunction(),
	    {MakeDescription({LogicalType::VARCHAR}, {"pattern"}, remote_description,
	                     {"SELECT prewarm_remote('https://example.com/data.parquet');"}, categories),
	     MakeDescription({LogicalType::VARCHAR, LogicalType::BIGINT}, {"pattern", "max_size"}, remote_description,
	                     {"SELECT prewarm_remote('https://example.com/data.parquet', 1000000);"}, categories),
	     MakeDescription({LogicalType::VARCHAR, LogicalType::VARCHAR}, {"pattern", "max_size"}, remote_description,
	                     {"SELECT prewarm_remote('https://example.com/data.parquet', '100MB');"}, categories)});
}

} // namespace duckdb

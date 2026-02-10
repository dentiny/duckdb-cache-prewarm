#include "functions/prewarm_remote_function.hpp"

#include "core/remote_block_collector.hpp"
#include "core/remote_prewarm_strategy.hpp"

// Include cache_httpfs headers
#include "cache_httpfs_instance_state.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

namespace {

//! Parse cache mode from value
RemoteCacheMode ParseCacheMode(const Value &mode_val) {
	if (mode_val.IsNull()) {
		return RemoteCacheMode::USE_CURRENT;
	}
	auto lower_mode = StringUtil::Lower(mode_val.ToString());
	if (lower_mode == "in_mem" || lower_mode == "in_memory") {
		return RemoteCacheMode::IN_MEMORY;
	}
	if (lower_mode == "on_disk" || lower_mode == "disk") {
		return RemoteCacheMode::ON_DISK;
	}
	if (lower_mode == "both") {
		return RemoteCacheMode::BOTH;
	}
	throw InvalidInputException("Invalid cache mode '%s'. Valid modes are: 'in_mem', 'on_disk', 'both'",
	                            mode_val.ToString());
}

//! Get cache_httpfs instance state
shared_ptr<CacheHttpfsInstanceState> GetCacheHttpfsState(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	return GetInstanceStateShared(db);
}

} // anonymous namespace

//===--------------------------------------------------------------------===//
// Prewarm Remote Scalar Function Implementation
//===--------------------------------------------------------------------===//

static void PrewarmRemoteFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	// Validate arguments
	if (args.ColumnCount() == 0) {
		throw InvalidInputException("Pattern cannot be NULL");
	}

	auto pattern_val = args.GetValue(0, 0);
	if (pattern_val.IsNull()) {
		throw InvalidInputException("Pattern cannot be NULL");
	}
	string pattern = pattern_val.ToString();

	// Parse optional cache mode
	RemoteCacheMode cache_mode = RemoteCacheMode::USE_CURRENT;
	if (args.ColumnCount() > 1) {
		cache_mode = ParseCacheMode(args.GetValue(1, 0));
	}

	// Parse optional max_blocks
	idx_t max_blocks = 0;
	if (args.ColumnCount() > 2) {
		auto max_blocks_val = args.GetValue(2, 0);
		if (!max_blocks_val.IsNull()) {
			max_blocks = max_blocks_val.GetValue<int64_t>();
		}
	}

	// Get cache_httpfs instance state
	auto cache_state = GetCacheHttpfsState(context);

	// Get cache block size from config
	idx_t block_size = cache_state->config.cache_block_size;

	// Collect remote blocks
	auto blocks = RemoteBlockCollector::CollectRemoteBlocks(context, pattern, block_size);

	// Execute prewarm strategy
	idx_t blocks_prewarmed = 0;
	if (!blocks.empty()) {
		RemotePrewarmStrategy strategy(context, cache_state, cache_mode);
		blocks_prewarmed = strategy.Execute(blocks, max_blocks);
	}

	// Return result
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<int64_t>(result);
	result_data[0] = NumericCast<int64_t>(blocks_prewarmed);
}

//===--------------------------------------------------------------------===//
// Function Registration
//===--------------------------------------------------------------------===//

void RegisterPrewarmRemoteFunction(ExtensionLoader &loader) {
	// Register prewarm_remote scalar function with multiple signatures
	ScalarFunctionSet prewarm_remote_set("prewarm_remote");

	// prewarm_remote(pattern)
	prewarm_remote_set.AddFunction(ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}},
	                                              /*return_type=*/LogicalType {LogicalTypeId::BIGINT},
	                                              PrewarmRemoteFunction));

	// prewarm_remote(pattern, cache_mode)
	prewarm_remote_set.AddFunction(
	    ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR}},
	                   /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmRemoteFunction));

	// prewarm_remote(pattern, cache_mode, max_blocks)
	prewarm_remote_set.AddFunction(
	    ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR},
	                                   LogicalType {LogicalTypeId::BIGINT}},
	                   /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmRemoteFunction));

	loader.RegisterFunction(prewarm_remote_set);
}

} // namespace duckdb

#include "functions/prewarm_remote_function.hpp"

#include "cache_httpfs_instance_state.hpp"
#include "core/remote_block_collector.hpp"
#include "core/remote_prewarm_strategy.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Prewarm Remote Scalar Function Implementation
//===--------------------------------------------------------------------===//
namespace {

void PrewarmRemoteFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	// Validate arguments
	if (args.ColumnCount() == 0) {
		throw InvalidInputException("prewarm_remote requires at least one argument");
	}

	auto pattern_val = args.GetValue(0, 0);
	if (pattern_val.IsNull()) {
		throw InvalidInputException("Pattern cannot be NULL");
	}
	string pattern = pattern_val.ToString();

	// Parse optional max_blocks
	idx_t max_blocks = std::numeric_limits<idx_t>::max();
	if (args.ColumnCount() > 1) {
		auto max_blocks_val = args.GetValue(1, 0);
		if (!max_blocks_val.IsNull()) {
			max_blocks = max_blocks_val.GetValue<int64_t>();
		}
	}

	auto &instance_state = GetInstanceStateOrThrow(context);
	idx_t block_size = instance_state.config.cache_block_size;

	// Get filesystem from database
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &cache_fs = db.GetFileSystem().Cast<CacheFileSystem>();

	// Collect remote blocks
	auto blocks = RemoteBlockCollector::CollectRemoteBlocks(cache_fs, pattern, block_size);

	// Execute prewarm strategy
	idx_t blocks_prewarmed = 0;
	if (!blocks.empty()) {
		RemotePrewarmStrategy strategy(context, cache_fs);
		blocks_prewarmed = strategy.Execute(blocks, max_blocks);
	}

	// Return result
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<int64_t>(result);
	result_data[0] = NumericCast<int64_t>(blocks_prewarmed);
}

} // namespace

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

	// prewarm_remote(pattern, max_blocks)
	prewarm_remote_set.AddFunction(
	    ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::BIGINT}},
	                   /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmRemoteFunction));

	loader.RegisterFunction(prewarm_remote_set);
}

} // namespace duckdb

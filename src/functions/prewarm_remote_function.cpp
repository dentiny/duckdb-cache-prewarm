#include "functions/prewarm_remote_function.hpp"

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

	// Parse optional max_blocks
	idx_t max_blocks = UINT64_MAX;
	if (args.ColumnCount() > 2) {
		auto max_blocks_val = args.GetValue(2, 0);
		if (!max_blocks_val.IsNull()) {
			max_blocks = max_blocks_val.GetValue<int64_t>();
		}
	}

	// TODO: Get cache block size from config
	idx_t block_size = 1024ULL * 1024ULL;

	// Get filesystem from database
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &fs = db.GetFileSystem();

	// Collect remote blocks
	auto blocks = RemoteBlockCollector::CollectRemoteBlocks(fs, pattern, block_size);

	// Execute prewarm strategy
	idx_t blocks_prewarmed = 0;
	if (!blocks.empty()) {
		RemotePrewarmStrategy strategy(context, fs);
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

	// prewarm_remote(pattern, max_blocks)
	prewarm_remote_set.AddFunction(
	    ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::BIGINT}},
	                   /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmRemoteFunction));

	loader.RegisterFunction(prewarm_remote_set);
}

} // namespace duckdb

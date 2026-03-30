#include "functions/prewarm_parquet_metadata_function.hpp"

#include "core/parquet_metadata_prewarmer.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

namespace {

void PrewarmParquetMetadataFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	if (args.ColumnCount() == 0) {
		throw InvalidInputException("prewarm_parquet_metadata requires at least one argument");
	}

	auto pattern_val = args.GetValue(0, 0);
	if (pattern_val.IsNull()) {
		throw InvalidInputException("Pattern cannot be NULL");
	}

	auto &db = DatabaseInstance::GetDatabase(context);
	auto &fs = db.GetFileSystem();
	auto bytes_prewarmed = ParquetMetadataPrewarmer::Execute(context, fs, pattern_val.ToString());

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<int64_t>(result);
	result_data[0] = NumericCast<int64_t>(bytes_prewarmed);
}

} // namespace

void RegisterPrewarmParquetMetadataFunction(ExtensionLoader &loader) {
	ScalarFunctionSet prewarm_parquet_metadata_set("prewarm_parquet_metadata");
	prewarm_parquet_metadata_set.AddFunction(ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}},
	                                                        /*return_type=*/LogicalType {LogicalTypeId::BIGINT},
	                                                        PrewarmParquetMetadataFunction));
	loader.RegisterFunction(prewarm_parquet_metadata_set);
}

} // namespace duckdb

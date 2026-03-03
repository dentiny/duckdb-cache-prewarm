#include "cache_prewarm_extension.hpp"
#include "core/block_collector.hpp"
#include "core/prewarm_strategy_factory.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

namespace {

//! Parse prewarm mode from value
PrewarmMode ParsePrewarmMode(const Value &mode_val) {
	if (mode_val.IsNull()) {
		return PrewarmMode::BUFFER;
	}
	auto lower_mode = StringUtil::Lower(mode_val.ToString());
	if (lower_mode == "prefetch") {
		return PrewarmMode::PREFETCH;
	}
	if (lower_mode == "read") {
		return PrewarmMode::READ;
	}
	if (lower_mode == "buffer") {
		return PrewarmMode::BUFFER;
	}
	throw InvalidInputException("Invalid prewarm mode '%s'. Valid modes are: 'prefetch', 'read', 'buffer'",
	                            mode_val.ToString());
}

} // namespace

//===--------------------------------------------------------------------===//
// Prewarm Scalar Function Implementation
//===--------------------------------------------------------------------===//

static void PrewarmFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	if (args.ColumnCount() == 0) {
		throw InvalidInputException("Table name cannot be NULL");
	}

	// Parse table name (1st argument)
	auto table_val = args.GetValue(0, 0);
	if (table_val.IsNull()) {
		throw InvalidInputException("Table name cannot be NULL");
	}
	string table_name = table_val.ToString();

	// Parse prewarm mode (2nd argument)
	PrewarmMode mode = PrewarmMode::BUFFER;
	if (args.ColumnCount() > 1) {
		mode = ParsePrewarmMode(args.GetValue(1, 0));
	}

	// Parse schema (3rd argument)
	string schema = "main";
	if (args.ColumnCount() > 2) {
		auto schema_val = args.GetValue(2, 0);
		if (!schema_val.IsNull()) {
			schema = schema_val.ToString();
		}
	}

	// Parse size limit in bytes (4th argument)
	idx_t max_bytes = NumericLimits<idx_t>::Maximum();
	bool has_size_limit = false;
	if (args.ColumnCount() > 3) {
		auto size_val = args.GetValue(3, 0);
		if (!size_val.IsNull()) {
			max_bytes = static_cast<idx_t>(size_val.GetValue<int64_t>());
			has_size_limit = true;
		}
	}

	// Resolve against the catalog of the current default database
	auto &db_manager = DatabaseManager::Get(DatabaseInstance::GetDatabase(context));
	auto &default_db_name = db_manager.GetDefaultDatabase(context);
	shared_ptr<AttachedDatabase> default_db = db_manager.GetDatabase(default_db_name);
	auto &catalog = default_db->GetCatalog();
	auto &table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(context, schema, table_name);
	auto &duck_table = table_catalog_entry.Cast<DuckTableEntry>();

	// Convert max_bytes to max_blocks using the block size
	auto &block_manager = StorageManager::Get(*default_db).GetBlockManager();
	idx_t block_size = block_manager.GetBlockAllocSize();
	idx_t max_blocks = NumericLimits<idx_t>::Maximum();
	if (has_size_limit) {
		max_blocks = max_bytes / block_size;
	}

	// Collect all blocks from the table using BlockCollector
	unordered_set<block_id_t> block_ids = BlockCollector::CollectTableBlocks(duck_table);

	// Execute prewarm using the appropriate strategy
	idx_t blocks_prewarmed = 0;
	if (!block_ids.empty()) {
		auto strategy =
		    CreateLocalPrewarmStrategy(context, mode, block_manager, BufferManager::GetBufferManager(context));
		blocks_prewarmed = strategy->Execute(duck_table, block_ids, max_blocks);
	}

	idx_t bytes_prewarmed = blocks_prewarmed * block_size;
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<int64_t>(result);
	result_data[0] = NumericCast<int64_t>(bytes_prewarmed);
}

//===--------------------------------------------------------------------===//
// Function Registration
//===--------------------------------------------------------------------===//

void RegisterPrewarmFunction(ExtensionLoader &loader) {
	// Register prewarm scalar function (supports optional mode and schema args)
	ScalarFunctionSet prewarm_set("prewarm");
	prewarm_set.AddFunction(ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}},
	                                       /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmFunction));
	prewarm_set.AddFunction(
	    ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR}},
	                   /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmFunction));
	prewarm_set.AddFunction(
	    ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR},
	                                   LogicalType {LogicalTypeId::VARCHAR}},
	                   /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmFunction));
	prewarm_set.AddFunction(
	    ScalarFunction(/*arguments=*/ {LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::VARCHAR},
	                                   LogicalType {LogicalTypeId::VARCHAR}, LogicalType {LogicalTypeId::BIGINT}},
	                   /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmFunction));
	loader.RegisterFunction(prewarm_set);
}

} // namespace duckdb

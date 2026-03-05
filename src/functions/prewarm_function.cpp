#include "cache_prewarm_extension.hpp"
#include "core/block_collector.hpp"
#include "core/prewarm_strategy_factory.hpp"
#include "utils/include/parse_size.hpp"

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
#include "duckdb/parser/qualified_name.hpp"
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

	// Parse table name (1st argument), supports qualified names like "schema.table" or "database.schema.table"
	auto table_val = args.GetValue(0, 0);
	if (table_val.IsNull()) {
		throw InvalidInputException("Table name cannot be NULL");
	}
	auto qualified_name = QualifiedName::Parse(table_val.ToString());

	// Parse prewarm mode (2nd argument)
	PrewarmMode mode = PrewarmMode::BUFFER;
	if (args.ColumnCount() > 1) {
		mode = ParsePrewarmMode(args.GetValue(1, 0));
	}

	// Parse size limit (3rd argument) - accepts human-readable sizes like '1GB', '100MB'
	idx_t max_bytes = NumericLimits<idx_t>::Maximum();
	bool has_size_limit = false;
	if (args.ColumnCount() > 2) {
		auto size_val = args.GetValue(2, 0);
		if (!size_val.IsNull()) {
			max_bytes = ParseSizeLimit(size_val.ToString());
			has_size_limit = true;
		}
	}

	string schema = qualified_name.schema.empty() ? "main" : qualified_name.schema;
	string table_name = std::move(qualified_name.name);

	// Resolve the database: use the catalog from the qualified name if specified, otherwise use the default database
	auto &db_manager = DatabaseManager::Get(DatabaseInstance::GetDatabase(context));
	string db_name =
	    qualified_name.catalog == INVALID_CATALOG ? db_manager.GetDefaultDatabase(context) : qualified_name.catalog;
	shared_ptr<AttachedDatabase> db = db_manager.GetDatabase(db_name);
	if (!db) {
		throw InvalidInputException("Database '%s' not found", db_name);
	}
	auto &catalog = db->GetCatalog();
	auto &table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(context, schema, table_name);
	auto &duck_table = table_catalog_entry.Cast<DuckTableEntry>();

	// Convert max_bytes to max_blocks using the block size
	auto &block_manager = StorageManager::Get(*db).GetBlockManager();
	idx_t block_size = block_manager.GetBlockAllocSize();
	idx_t max_blocks = NumericLimits<idx_t>::Maximum();
	if (has_size_limit) {
		max_blocks = max_bytes / block_size;
	}

	// Collect all blocks from the table using BlockCollector
	unordered_set<block_id_t> block_ids = BlockCollector::CollectTableBlocks(duck_table);

	// Execute prewarm using the appropriate strategy
	idx_t bytes_prewarmed = 0;
	if (!block_ids.empty()) {
		auto strategy = CreateLocalPrewarmStrategy(context, mode, StorageManager::Get(*db).GetBlockManager(),
		                                           BufferManager::GetBufferManager(context));
		bytes_prewarmed = strategy->Execute(duck_table, block_ids, max_blocks);
	}

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<int64_t>(result);
	result_data[0] = NumericCast<int64_t>(bytes_prewarmed);
}

//===--------------------------------------------------------------------===//
// Function Registration
//===--------------------------------------------------------------------===//

void RegisterPrewarmFunction(ExtensionLoader &loader) {
	// Register prewarm scalar function
	// Signature: prewarm(table_name, [mode], [max_bytes])
	// table_name supports qualified names: "table", "schema.table", or "database.schema.table"
	ScalarFunctionSet prewarm_set("prewarm");
	// prewarm(table)
	prewarm_set.AddFunction(ScalarFunction(/*arguments=*/ {/*table=*/LogicalType {LogicalTypeId::VARCHAR}},
	                                       /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmFunction));
	// prewarm(table, mode)
	prewarm_set.AddFunction(ScalarFunction(/*arguments=*/ {/*table=*/LogicalType {LogicalTypeId::VARCHAR},
	                                                       /*mode=*/LogicalType {LogicalTypeId::VARCHAR}},
	                                       /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmFunction));
	// prewarm(table, mode, max_size) - max_size as raw bytes (BIGINT)
	prewarm_set.AddFunction(ScalarFunction(/*arguments=*/ {/*table=*/LogicalType {LogicalTypeId::VARCHAR},
	                                                       /*mode=*/LogicalType {LogicalTypeId::VARCHAR},
	                                                       /*max_size=*/LogicalType {LogicalTypeId::BIGINT}},
	                                       /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmFunction));
	// prewarm(table, mode, max_size) - max_size as human-readable string like '1GB', '100MB'
	prewarm_set.AddFunction(ScalarFunction(/*arguments=*/ {/*table=*/LogicalType {LogicalTypeId::VARCHAR},
	                                                       /*mode=*/LogicalType {LogicalTypeId::VARCHAR},
	                                                       /*max_size=*/LogicalType {LogicalTypeId::VARCHAR}},
	                                       /*return_type=*/LogicalType {LogicalTypeId::BIGINT}, PrewarmFunction));
	loader.RegisterFunction(prewarm_set);
}

} // namespace duckdb

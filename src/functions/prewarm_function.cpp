#include "cache_prewarm_extension.hpp"
#include "core/block_collector.hpp"
#include "core/prewarm_strategies.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

//! Parse prewarm mode from value
static PrewarmMode ParsePrewarmMode(const Value &mode_val) {
	if (mode_val.IsNull()) {
		return PrewarmMode::BUFFER;
	}
	auto lower_mode = StringUtil::Lower(mode_val.ToString());
	if (lower_mode == "prefetch") {
		return PrewarmMode::PREFETCH;
	} else if (lower_mode == "read") {
		return PrewarmMode::READ;
	} else if (lower_mode == "buffer") {
		return PrewarmMode::BUFFER;
	}
	throw InvalidInputException("Invalid prewarm mode '%s'. Valid modes are: 'prefetch', 'read', 'buffer'",
	                            mode_val.ToString());
}

//===--------------------------------------------------------------------===//
// Prewarm Scalar Function Implementation
//===--------------------------------------------------------------------===//

static void PrewarmFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	if (args.ColumnCount() == 0) {
		throw InvalidInputException("Table name cannot be NULL");
	}

	auto table_val = args.GetValue(0, 0);
	if (table_val.IsNull()) {
		throw InvalidInputException("Table name cannot be NULL");
	}
	string table_name = table_val.ToString();

	PrewarmMode mode = PrewarmMode::BUFFER;
	if (args.ColumnCount() > 1) {
		mode = ParsePrewarmMode(args.GetValue(1, 0));
	}

	string schema = "main";
	if (args.ColumnCount() > 2) {
		auto schema_val = args.GetValue(2, 0);
		if (!schema_val.IsNull()) {
			schema = schema_val.ToString();
		}
	}

	// Resolve against the catalog of the current default database
	auto &db_manager = DatabaseManager::Get(DatabaseInstance::GetDatabase(context));
	auto &default_db_name = db_manager.GetDefaultDatabase(context);
	auto &catalog = Catalog::GetCatalog(context, default_db_name);
	auto &table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(context, schema, table_name);
	if (!table_catalog_entry.IsDuckTable()) {
		throw CatalogException("Table '%s.%s' is not a DuckTable", schema, table_name);
	}
	auto &duck_table = table_catalog_entry.Cast<DuckTableEntry>();

	// Collect all blocks from the table using BlockCollector
	unordered_set<block_id_t> block_ids = BlockCollector::CollectTableBlocks(duck_table);

	// Execute prewarm using the appropriate strategy
	idx_t blocks_prewarmed = 0;
	if (!block_ids.empty()) {
		auto strategy = CreatePrewarmStrategy(mode);
		blocks_prewarmed = strategy->Execute(context, duck_table, block_ids);
	}

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<int64_t>(result);
	result_data[0] = NumericCast<int64_t>(blocks_prewarmed);
}

//===--------------------------------------------------------------------===//
// Function Registration
//===--------------------------------------------------------------------===//

void RegisterPrewarmFunction(ExtensionLoader &loader) {
	// Register prewarm scalar function (supports optional mode and schema args)
	ScalarFunctionSet prewarm_set("prewarm");
	prewarm_set.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BIGINT, PrewarmFunction));
	prewarm_set.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BIGINT, PrewarmFunction));
	prewarm_set.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                       LogicalType::BIGINT, PrewarmFunction));
	loader.RegisterFunction(prewarm_set);
}

} // namespace duckdb

#include "catch/catch.hpp"

#include "core/remote_prewarm_strategy.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "test_helpers.hpp"
#include "mock_filesystem.hpp"

#include <fstream>
#include <cstring>

using namespace duckdb; // NOLINT

using duckdb::MockFileHandle;
using duckdb::MockFileSystem;
using duckdb::RemoteBlockInfo;
using duckdb::RemotePrewarmStrategy;
using duckdb::TestCreatePath;

namespace {

//===--------------------------------------------------------------------===//
// Test Helper: MockRemotePrewarmStrategy
//===--------------------------------------------------------------------===//

//! Mock strategy to test protected methods
class MockRemotePrewarmStrategy : public RemotePrewarmStrategy {
public:
	MockRemotePrewarmStrategy(duckdb::ClientContext &context, duckdb::FileSystem &fs)
	    : RemotePrewarmStrategy(context, fs), filter_cached_call_count(0), calculate_capacity_call_count(0),
	      capacity_configured(false) {
	}

	//! Override FilterCachedBlocks to track calls
	duckdb::vector<RemoteBlockInfo> FilterCachedBlocks(const duckdb::string &file_path,
	                                                   const duckdb::vector<RemoteBlockInfo> &blocks) override {
		filter_cached_call_count++;
		filter_cached_calls.emplace_back(file_path, blocks.size());
		// Return all blocks (simulate none are cached)
		return blocks;
	}

	//! Override CalculateMaxAvailableBlocks to track calls
	duckdb::BufferCapacityInfo CalculateMaxAvailableBlocks() override {
		calculate_capacity_call_count++;
		// Return configured capacity or default
		if (capacity_configured) {
			return configured_capacity;
		}
		return RemotePrewarmStrategy::CalculateMaxAvailableBlocks();
	}

	//! Configure capacity for testing
	void ConfigureCapacity(duckdb::BufferCapacityInfo capacity) {
		configured_capacity = capacity;
		capacity_configured = true;
	}

	//! Get number of FilterCachedBlocks calls
	duckdb::idx_t GetFilterCachedCallCount() const {
		return filter_cached_call_count;
	}

	//! Get number of CalculateMaxAvailableBlocks calls
	duckdb::idx_t GetCalculateCapacityCallCount() const {
		return calculate_capacity_call_count;
	}

	//! Get FilterCachedBlocks call details
	struct FilterCachedCall {
		duckdb::string file_path;
		duckdb::idx_t block_count;
		FilterCachedCall(duckdb::string path, duckdb::idx_t count) : file_path(std::move(path)), block_count(count) {
		}
	};

	const duckdb::vector<FilterCachedCall> &GetFilterCachedCalls() const {
		return filter_cached_calls;
	}

private:
	duckdb::idx_t filter_cached_call_count;
	duckdb::idx_t calculate_capacity_call_count;
	duckdb::vector<FilterCachedCall> filter_cached_calls;
	bool capacity_configured;
	duckdb::BufferCapacityInfo configured_capacity;
};

} // anonymous namespace

//===--------------------------------------------------------------------===//
// RemotePrewarmStrategy Tests with Mocks
//===--------------------------------------------------------------------===//

TEST_CASE("RemotePrewarmStrategy - Execute Empty Blocks (Mock)", "[remote_prewarm_strategy]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	MockFileSystem mock_fs;

	MockRemotePrewarmStrategy strategy(context, mock_fs);
	duckdb::unordered_map<duckdb::string, duckdb::vector<RemoteBlockInfo>> empty_blocks;

	auto result = strategy.Execute(empty_blocks, 0);

	REQUIRE(result == 0);

	// Verify no filesystem operations were performed
	REQUIRE(mock_fs.GetOpenFileCallCount() == 0);

	// Verify internal methods were not called for empty input
	REQUIRE(strategy.GetFilterCachedCallCount() == 0);
	REQUIRE(strategy.GetCalculateCapacityCallCount() == 0);
}

TEST_CASE("RemotePrewarmStrategy - Execute Single Block (Mock)", "[remote_prewarm_strategy]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	MockFileSystem mock_fs;

	MockRemotePrewarmStrategy strategy(context, mock_fs);

	const duckdb::string file_path = "/tmp/test_file.parquet";
	const duckdb::idx_t block_size = 1024;

	// Configure mock filesystem
	mock_fs.ConfigureFileSize(file_path, block_size);

	// Create block info
	duckdb::vector<RemoteBlockInfo> blocks;
	blocks.emplace_back(file_path, 0, static_cast<int64_t>(block_size), block_size);

	duckdb::unordered_map<duckdb::string, duckdb::vector<RemoteBlockInfo>> file_blocks;
	file_blocks[file_path] = blocks;

	auto result = strategy.Execute(file_blocks, 100);

	REQUIRE(result == 1);

	// Verify OpenFile was called for the file
	REQUIRE(mock_fs.GetOpenFileCallCount() == 1);
	auto open_calls = mock_fs.GetOpenFileCalls();
	REQUIRE(open_calls[0].path == file_path);

	// Verify file received Read() call
	REQUIRE(mock_fs.GetReadCallCount(file_path) == 1);

	auto read_calls = mock_fs.GetReadCalls(file_path);
	REQUIRE(read_calls[0].offset == 0);
	REQUIRE(read_calls[0].size == block_size);

	// Verify internal methods were called
	REQUIRE(strategy.GetFilterCachedCallCount() == 1);
	REQUIRE(strategy.GetCalculateCapacityCallCount() == 1);

	auto filter_calls = strategy.GetFilterCachedCalls();
	REQUIRE(filter_calls[0].file_path == file_path);
	REQUIRE(filter_calls[0].block_count == 1);
}

TEST_CASE("RemotePrewarmStrategy - Execute Multiple Blocks Same File (Mock)", "[remote_prewarm_strategy]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	MockFileSystem mock_fs;

	MockRemotePrewarmStrategy strategy(context, mock_fs);

	const duckdb::string file_path = "/tmp/test_file.parquet";
	const duckdb::idx_t block_size = 1024;
	const duckdb::idx_t num_blocks = 3;

	// Configure mock filesystem
	mock_fs.ConfigureFileSize(file_path, block_size * num_blocks);

	// Create multiple blocks
	duckdb::vector<RemoteBlockInfo> blocks;
	for (duckdb::idx_t i = 0; i < num_blocks; i++) {
		blocks.emplace_back(file_path, i * block_size, static_cast<int64_t>(block_size), block_size * num_blocks);
	}

	duckdb::unordered_map<duckdb::string, duckdb::vector<RemoteBlockInfo>> file_blocks;
	file_blocks[file_path] = blocks;

	auto result = strategy.Execute(file_blocks, 1000);

	REQUIRE(result == num_blocks);

	// Verify OpenFile was called once (same file)
	REQUIRE(mock_fs.GetOpenFileCallCount() == 1);

	// Verify file received Read() calls for each block
	REQUIRE(mock_fs.GetReadCallCount(file_path) == num_blocks);

	auto read_calls = mock_fs.GetReadCalls(file_path);
	for (duckdb::idx_t i = 0; i < num_blocks; i++) {
		REQUIRE(read_calls[i].offset == i * block_size);
		REQUIRE(read_calls[i].size == block_size);
	}

	// Verify FilterCachedBlocks was called once per file
	REQUIRE(strategy.GetFilterCachedCallCount() == 1);
}

TEST_CASE("RemotePrewarmStrategy - Execute Multiple Files (Mock)", "[remote_prewarm_strategy]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	MockFileSystem mock_fs;

	MockRemotePrewarmStrategy strategy(context, mock_fs);

	const duckdb::string file1 = "/tmp/file1.parquet";
	const duckdb::string file2 = "/tmp/file2.parquet";
	const duckdb::idx_t block_size = 1024;

	// Configure mock filesystem
	mock_fs.ConfigureFileSize(file1, block_size);
	mock_fs.ConfigureFileSize(file2, block_size * 2);

	// Create blocks for file1
	duckdb::vector<RemoteBlockInfo> blocks1;
	blocks1.emplace_back(file1, 0, static_cast<int64_t>(block_size), block_size);

	// Create blocks for file2
	duckdb::vector<RemoteBlockInfo> blocks2;
	blocks2.emplace_back(file2, 0, static_cast<int64_t>(block_size), block_size * 2);
	blocks2.emplace_back(file2, block_size, static_cast<int64_t>(block_size), block_size * 2);

	duckdb::unordered_map<duckdb::string, duckdb::vector<RemoteBlockInfo>> file_blocks;
	file_blocks[file1] = blocks1;
	file_blocks[file2] = blocks2;

	auto result = strategy.Execute(file_blocks, 100);

	REQUIRE(result == 3); // 1 + 2 blocks

	// Verify OpenFile was called for each file
	REQUIRE(mock_fs.GetOpenFileCallCount() == 2);

	// Verify each file received correct Read() calls
	REQUIRE(mock_fs.GetReadCallCount(file1) == 1);
	REQUIRE(mock_fs.GetReadCallCount(file2) == 2);

	// Verify FilterCachedBlocks was called for each file
	REQUIRE(strategy.GetFilterCachedCallCount() == 2);

	auto filter_calls = strategy.GetFilterCachedCalls();
	REQUIRE(filter_calls.size() == 2);
}

TEST_CASE("RemotePrewarmStrategy - Execute with Max Blocks Limit (Mock)", "[remote_prewarm_strategy]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	MockFileSystem mock_fs;

	MockRemotePrewarmStrategy strategy(context, mock_fs);

	const duckdb::string file_path = "/tmp/test_file.parquet";
	const duckdb::idx_t block_size = 1024;
	const duckdb::idx_t num_blocks = 10;
	const duckdb::idx_t max_blocks = 5;

	// Configure mock filesystem
	mock_fs.ConfigureFileSize(file_path, block_size * num_blocks);

	// Create multiple blocks
	duckdb::vector<RemoteBlockInfo> blocks;
	for (duckdb::idx_t i = 0; i < num_blocks; i++) {
		blocks.emplace_back(file_path, i * block_size, static_cast<int64_t>(block_size), block_size * num_blocks);
	}

	duckdb::unordered_map<duckdb::string, duckdb::vector<RemoteBlockInfo>> file_blocks;
	file_blocks[file_path] = blocks;

	// Execute with max_blocks limit
	auto result = strategy.Execute(file_blocks, max_blocks);

	// Result should be limited by max_blocks
	REQUIRE(result <= max_blocks);

	// Verify file received limited Read() calls
	REQUIRE(mock_fs.GetReadCallCount(file_path) <= max_blocks);

	// Verify CalculateMaxAvailableBlocks was called
	REQUIRE(strategy.GetCalculateCapacityCallCount() == 1);
}

TEST_CASE("RemotePrewarmStrategy - Execute with Capacity Limit (Mock)", "[remote_prewarm_strategy]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	MockFileSystem mock_fs;

	MockRemotePrewarmStrategy strategy(context, mock_fs);

	const duckdb::string file_path = "/tmp/test_file.parquet";
	const duckdb::idx_t block_size = 1024;
	const duckdb::idx_t num_blocks = 10;
	const duckdb::idx_t capacity_limit = 3;

	// Configure capacity limit
	duckdb::BufferCapacityInfo capacity {
	    .block_size = block_size,
	    .max_capacity = capacity_limit * block_size,
	    .used_space = 0,
	    .available_space = capacity_limit * block_size,
	    .max_blocks = capacity_limit,
	};
	strategy.ConfigureCapacity(capacity);

	// Configure mock filesystem
	mock_fs.ConfigureFileSize(file_path, block_size * num_blocks);

	// Create multiple blocks
	duckdb::vector<RemoteBlockInfo> blocks;
	for (duckdb::idx_t i = 0; i < num_blocks; i++) {
		blocks.emplace_back(file_path, i * block_size, static_cast<int64_t>(block_size), block_size * num_blocks);
	}

	duckdb::unordered_map<duckdb::string, duckdb::vector<RemoteBlockInfo>> file_blocks;
	file_blocks[file_path] = blocks;

	auto result = strategy.Execute(file_blocks, 100);

	// Result should be limited by capacity
	REQUIRE(result == capacity_limit);

	// Verify file received limited Read() calls
	REQUIRE(mock_fs.GetReadCallCount(file_path) == capacity_limit);
}

TEST_CASE("RemotePrewarmStrategy - RemoteBlockInfo Structure", "[remote_prewarm_strategy]") {
	// Test RemoteBlockInfo structure
	RemoteBlockInfo block1;
	REQUIRE(block1.file_path.empty());
	REQUIRE(block1.offset == 0);
	REQUIRE(block1.size == 0);
	REQUIRE(block1.file_size == 0);

	RemoteBlockInfo block2("s3://bucket/file.parquet", 1024, 2048, 4096);
	REQUIRE(block2.file_path == "s3://bucket/file.parquet");
	REQUIRE(block2.offset == 1024);
	REQUIRE(block2.size == 2048);
	REQUIRE(block2.file_size == 4096);
}

//===--------------------------------------------------------------------===//
// Integration Tests with Real FileSystem
//===--------------------------------------------------------------------===//

TEST_CASE("RemotePrewarmStrategy - Real Execute Single Block", "[remote_prewarm_strategy]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto &fs = duckdb::FileSystem::GetFileSystem(context);

	RemotePrewarmStrategy strategy(context, fs);

	// Create a temporary file for testing
	auto temp_file = TestCreatePath("test_file.parquet");
	const char *test_data = "test data";
	duckdb::idx_t file_size = strlen(test_data);
	{
		std::ofstream outfile(temp_file);
		outfile << test_data;
		outfile.close();
	}

	// Create block info with correct file size
	duckdb::vector<RemoteBlockInfo> blocks;
	blocks.emplace_back(temp_file, 0, static_cast<int64_t>(file_size), file_size);

	duckdb::unordered_map<duckdb::string, duckdb::vector<RemoteBlockInfo>> file_blocks;
	file_blocks[temp_file] = blocks;

	auto result = strategy.Execute(file_blocks, 0);
	REQUIRE(result >= 0);
}

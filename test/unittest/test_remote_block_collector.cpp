#include "catch/catch.hpp"

#include "core/remote_block_collector.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "prewarm_mock_filesystem.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

TEST_CASE("CollectRemoteBlocks - Empty Pattern (Mock)", "[remote_block_collector]") {
	MockFileSystem mock_fs;

	// Configure empty glob results
	mock_fs.ConfigureGlobResults("nonexistent/*.parquet", {});

	auto result = RemoteBlockCollector::CollectRemoteBlocks(mock_fs, "nonexistent/*.parquet", 1024ULL * 1024ULL);

	// Verify empty result
	REQUIRE(result.empty());

	// Verify Glob was called exactly once with correct pattern
	REQUIRE(mock_fs.GetGlobCallCount() == 1);
	auto glob_calls = mock_fs.GetGlobCalls();
	REQUIRE(glob_calls[0].pattern == "nonexistent/*.parquet");

	// Verify no files were opened
	REQUIRE(mock_fs.GetOpenFileCallCount() == 0);
}

TEST_CASE("CollectRemoteBlocks - Single File (Mock)", "[remote_block_collector]") {
	MockFileSystem mock_fs;

	const string file_path = "/tmp/test_file.parquet";
	const idx_t file_size = 1024;

	// Configure mock filesystem
	mock_fs.ConfigureGlobResults(file_path, {file_path});
	mock_fs.ConfigureFileSize(file_path, file_size);

	auto result = RemoteBlockCollector::CollectRemoteBlocks(mock_fs, file_path, 1024ULL * 1024ULL);

	// Verify result
	REQUIRE(result.size() == 1);
	REQUIRE(result.find(file_path) != result.end());

	auto &blocks = result[file_path];
	REQUIRE(blocks.size() == 1);
	REQUIRE(blocks[0].file_path == file_path);
	REQUIRE(blocks[0].offset == 0);
	REQUIRE(blocks[0].size == static_cast<int64_t>(file_size));
	REQUIRE(blocks[0].file_size == file_size);

	// Verify Glob was called
	REQUIRE(mock_fs.GetGlobCallCount() == 1);
	auto glob_calls = mock_fs.GetGlobCalls();
	REQUIRE(glob_calls[0].pattern == file_path);

	// Verify OpenFile was called once for the file
	REQUIRE(mock_fs.GetOpenFileCallCount() == 1);
	auto open_calls = mock_fs.GetOpenFileCalls();
	REQUIRE(open_calls[0].path == file_path);
}

TEST_CASE("CollectRemoteBlocks - Multiple Files (Mock)", "[remote_block_collector]") {
	MockFileSystem mock_fs;

	const string pattern = "/tmp/*.parquet";
	const string file1 = "/tmp/file1.parquet";
	const string file2 = "/tmp/file2.parquet";
	const idx_t file1_size = 1024;
	const idx_t file2_size = 2048;

	// Configure mock filesystem
	mock_fs.ConfigureGlobResults(pattern, {file1, file2});
	mock_fs.ConfigureFileSize(file1, file1_size);
	mock_fs.ConfigureFileSize(file2, file2_size);

	auto result = RemoteBlockCollector::CollectRemoteBlocks(mock_fs, pattern, 1024ULL * 1024ULL);

	// Verify results
	REQUIRE(result.size() == 2);
	REQUIRE(result.find(file1) != result.end());
	REQUIRE(result.find(file2) != result.end());

	// Verify each file has blocks with correct sizes
	REQUIRE(result[file1].size() == 1);
	REQUIRE(result[file1][0].file_size == file1_size);

	REQUIRE(result[file2].size() == 1);
	REQUIRE(result[file2][0].file_size == file2_size);

	// Verify Glob was called once
	REQUIRE(mock_fs.GetGlobCallCount() == 1);

	// Verify OpenFile was called for each file
	REQUIRE(mock_fs.GetOpenFileCallCount() == 2);
	auto open_calls = mock_fs.GetOpenFileCalls();
	REQUIRE((open_calls[0].path == file1 || open_calls[0].path == file2));
	REQUIRE((open_calls[1].path == file1 || open_calls[1].path == file2));
	REQUIRE(open_calls[0].path != open_calls[1].path);
}

TEST_CASE("CollectRemoteBlocks - Block Size Parameter (Mock)", "[remote_block_collector]") {
	MockFileSystem mock_fs;

	const string file_path = "/tmp/large_file.parquet";
	// TODO: use cache httpfs's size literal utils
	const idx_t file_size = 5ULL * 1024ULL * 1024ULL; // 5MB
	const idx_t block_size = 1024ULL * 1024ULL;       // 1MB

	// Configure mock filesystem
	mock_fs.ConfigureGlobResults(file_path, {file_path});
	mock_fs.ConfigureFileSize(file_path, file_size);

	auto result = RemoteBlockCollector::CollectRemoteBlocks(mock_fs, file_path, block_size);

	// Verify result
	REQUIRE(result.size() == 1);
	auto &blocks = result[file_path];
	// Currently implementation returns single block for entire file
	// This test verifies the current behavior
	REQUIRE(blocks.size() >= 1);
	REQUIRE(blocks[0].file_size == file_size);

	// Verify filesystem interactions
	REQUIRE(mock_fs.GetGlobCallCount() == 1);
	REQUIRE(mock_fs.GetOpenFileCallCount() == 1);
}

TEST_CASE("CollectRemoteBlocks - Empty File (Mock)", "[remote_block_collector]") {
	MockFileSystem mock_fs;

	const string file_path = "/tmp/empty_file.parquet";
	const idx_t file_size = 0;

	// Configure mock filesystem
	mock_fs.ConfigureGlobResults(file_path, {file_path});
	mock_fs.ConfigureFileSize(file_path, file_size);

	auto result = RemoteBlockCollector::CollectRemoteBlocks(mock_fs, file_path, 1024ULL * 1024ULL);

	// Verify result
	REQUIRE(result.size() == 1);
	auto &blocks = result[file_path];
	REQUIRE(blocks.size() == 1);
	REQUIRE(blocks[0].file_size == 0);
	REQUIRE(blocks[0].offset == 0);
	REQUIRE(blocks[0].size == 0);

	// Verify filesystem interactions
	REQUIRE(mock_fs.GetGlobCallCount() == 1);
	REQUIRE(mock_fs.GetOpenFileCallCount() == 1);
}

//===--------------------------------------------------------------------===//
// Integration Tests with Real FileSystem
//===--------------------------------------------------------------------===//

TEST_CASE("CollectRemoteBlocks - Real Empty Pattern", "[remote_block_collector]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto &fs = FileSystem::GetFileSystem(context);

	auto result = RemoteBlockCollector::CollectRemoteBlocks(fs, "nonexistent/*.parquet", 1024ULL * 1024ULL);
	REQUIRE(result.empty());
}

TEST_CASE("CollectRemoteBlocks - Real Single File", "[remote_block_collector]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto &fs = FileSystem::GetFileSystem(context);

	// Create a temporary file
	auto temp_file = TestCreatePath("test_file.parquet");
	{
		const string test_data = "test data";
		auto handle = fs.OpenFile(temp_file, FileOpenFlags::FILE_FLAGS_WRITE |
		                                         FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Write(const_cast<char *>(test_data.c_str()), test_data.size());
	}

	// Test with a pattern that matches the file
	auto result = RemoteBlockCollector::CollectRemoteBlocks(fs, temp_file, 1024ULL * 1024ULL);

	REQUIRE(result.size() == 1);
	REQUIRE(result.find(temp_file) != result.end());

	auto &blocks = result[temp_file];
	REQUIRE(blocks.size() == 1);
	REQUIRE(blocks[0].file_path == temp_file);
	REQUIRE(blocks[0].offset == 0);
	REQUIRE(blocks[0].size > 0);
	REQUIRE(blocks[0].file_size > 0);
}

TEST_CASE("CollectRemoteBlocks - Real Multiple Files", "[remote_block_collector]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto &fs = FileSystem::GetFileSystem(context);

	// Create multiple temporary files
	auto temp_dir = TestCreatePath("test_dir");
	fs.CreateDirectory(temp_dir);

	auto file1 = fs.JoinPath(temp_dir, "file1.parquet");
	auto file2 = fs.JoinPath(temp_dir, "file2.parquet");

	{
		const string data1 = "test data 1";
		auto handle1 = fs.OpenFile(file1, FileOpenFlags::FILE_FLAGS_WRITE |
		                                      FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle1->Write(const_cast<char *>(data1.c_str()), data1.size());

		const string data2 = "test data 2";
		auto handle2 = fs.OpenFile(file2, FileOpenFlags::FILE_FLAGS_WRITE |
		                                      FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle2->Write(const_cast<char *>(data2.c_str()), data2.size());
	}

	// Test with a pattern that matches both files
	auto pattern = fs.JoinPath(temp_dir, "*.parquet");
	auto result = RemoteBlockCollector::CollectRemoteBlocks(fs, pattern, 1024ULL * 1024ULL);

	REQUIRE(result.size() == 2);
	REQUIRE(result.find(file1) != result.end());
	REQUIRE(result.find(file2) != result.end());

	// Verify each file has blocks
	REQUIRE(result[file1].size() == 1);
	REQUIRE(result[file2].size() == 1);
}

TEST_CASE("CollectRemoteBlocks - Remote Path Pattern (Mock)", "[remote_block_collector]") {
	MockFileSystem mock_fs;

	// Test with S3-style pattern - configure empty results
	mock_fs.ConfigureGlobResults("s3://bucket/*.parquet", {});

	auto result = RemoteBlockCollector::CollectRemoteBlocks(mock_fs, "s3://bucket/*.parquet", 1024ULL * 1024ULL);

	// Should return empty if no files match
	REQUIRE(result.empty());

	// Verify Glob was called with the S3 pattern
	REQUIRE(mock_fs.GetGlobCallCount() == 1);
	auto glob_calls = mock_fs.GetGlobCalls();
	REQUIRE(glob_calls[0].pattern == "s3://bucket/*.parquet");
}

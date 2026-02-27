#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Mock File Handle
//===--------------------------------------------------------------------===//

//! Mock file handle that delegates to MockFileSystem for tracking
class MockFileHandle : public FileHandle {
public:
	MockFileHandle(FileSystem &fs, const string &path_p, idx_t file_size_p);

	void Close() override;

	void SetShouldFail(bool fail);
	bool ShouldFail() const;
	idx_t GetFileSize() const;

private:
	idx_t file_size;
	bool should_fail;
};

//===--------------------------------------------------------------------===//
// Mock File System
//===--------------------------------------------------------------------===//

//! Mock filesystem that tracks file operations
class MockFileSystem : public FileSystem {
public:
	//! Structure to record a Read() call
	struct ReadCall {
		string path;
		idx_t size;
		idx_t offset;
		ReadCall(string path_p, idx_t size_p, idx_t offset_p);
	};

	//! Structure to record an OpenFile() call
	struct OpenFileCall {
		string path;
		FileOpenFlags flags;
		OpenFileCall(string path_p, FileOpenFlags flags_p);
	};

	//! Structure to record a Glob() call
	struct GlobCall {
		string pattern;
		explicit GlobCall(string pattern_p);
	};

	MockFileSystem();

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	int64_t GetFileSize(FileHandle &handle) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;

	void ConfigureGlobResults(const string &pattern, const vector<string> &results);
	void ConfigureFileSize(const string &path, idx_t size);

	idx_t GetOpenFileCallCount() const;
	vector<OpenFileCall> GetOpenFileCalls() const;

	idx_t GetGlobCallCount() const;
	vector<GlobCall> GetGlobCalls() const;

	idx_t GetReadCallCount(const string &path) const;
	vector<ReadCall> GetReadCalls(const string &path) const;
	idx_t GetTotalReadCallCount() const;

	void Reset();

	//===--------------------------------------------------------------------===//
	// Required FileSystem overrides (minimal implementations)
	//===--------------------------------------------------------------------===//

	string GetName() const override;

private:
	mutable mutex mu;

	vector<OpenFileCall> open_file_calls;
	vector<GlobCall> glob_calls;
	vector<ReadCall> read_calls;
	unordered_map<string, vector<string>> configured_glob_results;
	unordered_map<string, idx_t> configured_file_sizes;
};

} // namespace duckdb

#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include <memory>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Mock File Handle
//===--------------------------------------------------------------------===//

//! Mock file handle that delegates to MockFileSystem for tracking
class MockFileHandle : public FileHandle {
public:
	MockFileHandle(FileSystem &fs, const string &path_p, idx_t file_size_p)
	    : FileHandle(fs, path_p, FileOpenFlags::FILE_FLAGS_READ), file_size(file_size_p), should_fail(false) {
	}

	//! Implementation of Close
	void Close() override {
		// No-op for mock
	}

	//! Configure to fail on Read()
	void SetShouldFail(bool fail) {
		should_fail = fail;
	}

	//! Check if should fail
	bool ShouldFail() const {
		return should_fail;
	}

	//! Get mock file size
	idx_t GetFileSize() const {
		return file_size;
	}

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
		ReadCall(string path_p, idx_t size_p, idx_t offset_p)
		    : path(std::move(path_p)), size(size_p), offset(offset_p) {
		}
	};

	//! Structure to record an OpenFile() call
	struct OpenFileCall {
		string path;
		FileOpenFlags flags;
		OpenFileCall(string path_p, FileOpenFlags flags_p) : path(std::move(path_p)), flags(flags_p) {
		}
	};

	//! Structure to record a Glob() call
	struct GlobCall {
		string pattern;
		explicit GlobCall(string pattern_p) : pattern(std::move(pattern_p)) {
		}
	};

	MockFileSystem() : FileSystem() {
	}

	//! Mock implementation of OpenFile
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		lock_guard<mutex> open_lock(open_mutex);
		open_file_calls.emplace_back(path, flags);

		// Return configured file handle or create default
		idx_t file_size = 1024; // Default size
		if (configured_file_sizes.find(path) != configured_file_sizes.end()) {
			file_size = configured_file_sizes[path];
		}

		auto handle = make_uniq<MockFileHandle>(*this, path, file_size);
		return std::move(handle);
	}

	//! Mock implementation of Glob
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override {
		lock_guard<mutex> glob_lock(glob_mutex);
		glob_calls.emplace_back(path);

		// Return configured results or empty vector
		if (configured_glob_results.find(path) != configured_glob_results.end()) {
			vector<OpenFileInfo> result;
			for (const auto &file_path : configured_glob_results[path]) {
				result.push_back(OpenFileInfo {file_path});
			}
			return result;
		}
		return vector<OpenFileInfo>();
	}

	//! Mock implementation of GetFileSize
	int64_t GetFileSize(FileHandle &handle) override {
		auto mock_handle = dynamic_cast<MockFileHandle *>(&handle);
		if (mock_handle) {
			return static_cast<int64_t>(mock_handle->GetFileSize());
		}
		return 0;
	}

	//! Mock implementation of Read (called by FileHandle::Read)
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		auto mock_handle = dynamic_cast<MockFileHandle *>(&handle);
		if (mock_handle) {
			// Record the read call in the filesystem (thread-safe)
			{
				lock_guard<mutex> read_lock(read_mutex);
				read_calls.emplace_back(handle.path, static_cast<idx_t>(nr_bytes), location);
			}

			if (mock_handle->ShouldFail()) {
				throw IOException("Mock read failure");
			}

			// Simulate reading by filling buffer with pattern
			if (buffer) {
				memset(buffer, 'M', static_cast<size_t>(nr_bytes)); // 'M' for Mock data
			}
		}
	}

	//! Configure glob results for a pattern
	void ConfigureGlobResults(const string &pattern, const vector<string> &results) {
		lock_guard<mutex> glob_lock(glob_mutex);
		configured_glob_results[pattern] = results;
	}

	//! Configure file size for a path
	void ConfigureFileSize(const string &path, idx_t size) {
		lock_guard<mutex> open_lock(open_mutex);
		configured_file_sizes[path] = size;
	}

	//! Get number of OpenFile() calls
	idx_t GetOpenFileCallCount() const {
		lock_guard<mutex> open_lock(open_mutex);
		return open_file_calls.size();
	}

	//! Get all OpenFile() calls
	vector<OpenFileCall> GetOpenFileCalls() const {
		lock_guard<mutex> open_lock(open_mutex);
		return open_file_calls;
	}

	//! Get number of Glob() calls
	idx_t GetGlobCallCount() const {
		lock_guard<mutex> glob_lock(glob_mutex);
		return glob_calls.size();
	}

	//! Get all Glob() calls
	vector<GlobCall> GetGlobCalls() const {
		lock_guard<mutex> glob_lock(glob_mutex);
		return glob_calls;
	}

	//! Get number of Read() calls for a specific file path
	idx_t GetReadCallCount(const string &path) const {
		lock_guard<mutex> read_lock(read_mutex);
		idx_t count = 0;
		for (const auto &call : read_calls) {
			if (call.path == path) {
				count++;
			}
		}
		return count;
	}

	//! Get all Read() calls for a specific file path
	vector<ReadCall> GetReadCalls(const string &path) const {
		lock_guard<mutex> read_lock(read_mutex);
		vector<ReadCall> result;
		for (const auto &call : read_calls) {
			if (call.path == path) {
				result.push_back(call);
			}
		}
		return result;
	}

	//! Get total number of Read() calls across all files
	idx_t GetTotalReadCallCount() const {
		lock_guard<mutex> read_lock(read_mutex);
		return read_calls.size();
	}

	//! Reset all call tracking
	void Reset() {
		lock_guard<mutex> open_lock(open_mutex);
		lock_guard<mutex> glob_lock(glob_mutex);
		lock_guard<mutex> read_lock(read_mutex);
		open_file_calls.clear();
		glob_calls.clear();
		read_calls.clear();
	}

	//===--------------------------------------------------------------------===//
	// Required FileSystem overrides (minimal implementations)
	//===--------------------------------------------------------------------===//

	string GetName() const override {
		return "MockFileSystem";
	}

	bool CanHandleFile(const string &fpath) override {
		return true;
	}

	void FileSync(FileHandle &handle) override {
		// No-op for mock
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return true;
	}

	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		// No-op for mock
	}

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		// No-op for mock
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return true;
	}

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		// No-op for mock
	}

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		return false;
	}

	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override {
		throw NotImplementedException("MockFileSystem::MoveFile");
	}

private:
	mutable mutex open_mutex;
	mutable mutex glob_mutex;
	mutable mutex read_mutex;

	vector<OpenFileCall> open_file_calls;
	vector<GlobCall> glob_calls;
	vector<ReadCall> read_calls;
	unordered_map<string, vector<string>> configured_glob_results;
	unordered_map<string, idx_t> configured_file_sizes;
};

} // namespace duckdb

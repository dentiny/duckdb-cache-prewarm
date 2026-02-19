#include "prewarm_mock_filesystem.hpp"

#include <cstring>

namespace duckdb {

//===--------------------------------------------------------------------===//
// MockFileHandle
//===--------------------------------------------------------------------===//

MockFileHandle::MockFileHandle(FileSystem &fs, const string &path_p, idx_t file_size_p)
    : FileHandle(fs, path_p, FileOpenFlags::FILE_FLAGS_READ), file_size(file_size_p), should_fail(false) {
}

void MockFileHandle::Close() {
}

void MockFileHandle::SetShouldFail(bool fail) {
	should_fail = fail;
}

bool MockFileHandle::ShouldFail() const {
	return should_fail;
}

idx_t MockFileHandle::GetFileSize() const {
	return file_size;
}

//===--------------------------------------------------------------------===//
// MockFileSystem::ReadCall
//===--------------------------------------------------------------------===//

MockFileSystem::ReadCall::ReadCall(string path_p, idx_t size_p, idx_t offset_p)
    : path(std::move(path_p)), size(size_p), offset(offset_p) {
}

//===--------------------------------------------------------------------===//
// MockFileSystem::OpenFileCall
//===--------------------------------------------------------------------===//

MockFileSystem::OpenFileCall::OpenFileCall(string path_p, FileOpenFlags flags_p)
    : path(std::move(path_p)), flags(flags_p) {
}

//===--------------------------------------------------------------------===//
// MockFileSystem::GlobCall
//===--------------------------------------------------------------------===//

MockFileSystem::GlobCall::GlobCall(string pattern_p) : pattern(std::move(pattern_p)) {
}

//===--------------------------------------------------------------------===//
// MockFileSystem
//===--------------------------------------------------------------------===//

MockFileSystem::MockFileSystem() : FileSystem() {
}

unique_ptr<FileHandle> MockFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	lock_guard<mutex> lock(mutex_);
	open_file_calls.emplace_back(path, flags);

	idx_t file_size = 1024; // Default size
	if (configured_file_sizes.find(path) != configured_file_sizes.end()) {
		file_size = configured_file_sizes[path];
	}

	return make_uniq<MockFileHandle>(*this, path, file_size);
}

vector<OpenFileInfo> MockFileSystem::Glob(const string &path, FileOpener *opener) {
	lock_guard<mutex> lock(mutex_);
	glob_calls.emplace_back(path);

	if (configured_glob_results.find(path) != configured_glob_results.end()) {
		vector<OpenFileInfo> result;
		for (const auto &file_path : configured_glob_results[path]) {
			result.push_back(OpenFileInfo {file_path});
		}
		return result;
	}
	return vector<OpenFileInfo>();
}

int64_t MockFileSystem::GetFileSize(FileHandle &handle) {
	auto mock_handle = dynamic_cast<MockFileHandle *>(&handle);
	if (mock_handle) {
		return static_cast<int64_t>(mock_handle->GetFileSize());
	}
	return 0;
}

void MockFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto mock_handle = dynamic_cast<MockFileHandle *>(&handle);
	if (mock_handle) {
		{
			lock_guard<mutex> lock(mutex_);
			read_calls.emplace_back(handle.path, static_cast<idx_t>(nr_bytes), location);
		}

		if (mock_handle->ShouldFail()) {
			throw IOException("Mock read failure");
		}

		if (buffer) {
			memset(buffer, 'M', static_cast<size_t>(nr_bytes));
		}
	}
}

void MockFileSystem::ConfigureGlobResults(const string &pattern, const vector<string> &results) {
	lock_guard<mutex> lock(mutex_);
	configured_glob_results[pattern] = results;
}

void MockFileSystem::ConfigureFileSize(const string &path, idx_t size) {
	lock_guard<mutex> lock(mutex_);
	configured_file_sizes[path] = size;
}

idx_t MockFileSystem::GetOpenFileCallCount() const {
	lock_guard<mutex> lock(mutex_);
	return open_file_calls.size();
}

vector<MockFileSystem::OpenFileCall> MockFileSystem::GetOpenFileCalls() const {
	lock_guard<mutex> lock(mutex_);
	return open_file_calls;
}

idx_t MockFileSystem::GetGlobCallCount() const {
	lock_guard<mutex> lock(mutex_);
	return glob_calls.size();
}

vector<MockFileSystem::GlobCall> MockFileSystem::GetGlobCalls() const {
	lock_guard<mutex> lock(mutex_);
	return glob_calls;
}

idx_t MockFileSystem::GetReadCallCount(const string &path) const {
	lock_guard<mutex> lock(mutex_);
	idx_t count = 0;
	for (const auto &call : read_calls) {
		if (call.path == path) {
			count++;
		}
	}
	return count;
}

vector<MockFileSystem::ReadCall> MockFileSystem::GetReadCalls(const string &path) const {
	lock_guard<mutex> lock(mutex_);
	vector<ReadCall> result;
	for (const auto &call : read_calls) {
		if (call.path == path) {
			result.push_back(call);
		}
	}
	return result;
}

idx_t MockFileSystem::GetTotalReadCallCount() const {
	lock_guard<mutex> lock(mutex_);
	return read_calls.size();
}

void MockFileSystem::Reset() {
	lock_guard<mutex> lock(mutex_);
	open_file_calls.clear();
	glob_calls.clear();
	read_calls.clear();
}

string MockFileSystem::GetName() const {
	return "MockFileSystem";
}

bool MockFileSystem::CanHandleFile(const string &fpath) {
	return true;
}

void MockFileSystem::FileSync(FileHandle &handle) {
}

bool MockFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return true;
}

void MockFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
}

void MockFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
}

bool MockFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return true;
}

void MockFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
}

bool MockFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                               FileOpener *opener) {
	return false;
}

void MockFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("MockFileSystem::MoveFile");
}

} // namespace duckdb

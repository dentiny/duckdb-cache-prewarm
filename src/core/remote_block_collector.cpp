#include "core/remote_block_collector.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Remote Block Collector Implementation
//===--------------------------------------------------------------------===//

unordered_map<string, vector<RemoteBlockInfo>>
RemoteBlockCollector::CollectRemoteBlocks(FileSystem &fs, const string &pattern, idx_t block_size) {
	unordered_map<string, vector<RemoteBlockInfo>> file_blocks; // map from file_path to blocks
	auto glob_results = fs.Glob(pattern);
	if (glob_results.empty()) {
		return file_blocks;
	}

	// Process each file
	for (const auto &file_info : glob_results) {
		// Open file to get size
		auto file_handle = fs.OpenFile(file_info.path, FileOpenFlags::FILE_FLAGS_READ);
		idx_t file_size = fs.GetFileSize(*file_handle);

		// TODO: Divide file into blocks
		// 1. get alignment info
		// 2. split file to blocks base on that info
		vector<RemoteBlockInfo> blocks = {{file_info.path, 0, static_cast<int64_t>(file_size), file_size}};
		file_blocks[file_info.path] = std::move(blocks);
	}

	return file_blocks;
}

} // namespace duckdb

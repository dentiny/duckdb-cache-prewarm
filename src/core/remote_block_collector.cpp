#include "core/remote_block_collector.hpp"

#include "chunk_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//

namespace {

//! Get the filesystem from the client context
FileSystem &GetFileSystem(ClientContext &context) {
	auto &db = DatabaseInstance::GetDatabase(context);
	return db.GetFileSystem();
}

} // anonymous namespace

//===--------------------------------------------------------------------===//
// Remote Block Collector Implementation
//===--------------------------------------------------------------------===//

unordered_map<string, vector<RemoteBlockInfo>>
RemoteBlockCollector::CollectRemoteBlocks(ClientContext &context, const string &pattern, idx_t block_size) {

	auto &fs = GetFileSystem(context);
	unordered_map<string, vector<RemoteBlockInfo>> file_blocks;
	auto glob_results = fs.Glob(pattern);
	if (glob_results.empty()) {
		return file_blocks;
	}

	// Process each file
	for (const auto &file_info : glob_results) {
		// Open file to get size
		auto file_handle = fs.OpenFile(file_info.path, FileOpenFlags::FILE_FLAGS_READ);
		idx_t file_size = fs.GetFileSize(*file_handle);

		// Divide file into blocks
		const ReadRequestParams read_params {
		    .requested_start_offset = 0,
		    .requested_bytes_to_read = file_size,
		    .block_size = block_size,
		};
		const ChunkAlignmentInfo alignment_info = CalculateChunkAlignment(read_params);
		vector<RemoteBlockInfo> blocks;
		blocks.reserve(alignment_info.subrequest_count);

		for (idx_t i = 0; i < alignment_info.subrequest_count; i++) {
			idx_t offset = alignment_info.aligned_start_offset + i * block_size;
			blocks.emplace_back(file_info.path, offset, block_size, file_size);
		}

		file_blocks[file_info.path] = std::move(blocks);
	}

	return file_blocks;
}

} // namespace duckdb

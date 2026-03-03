#include "core/remote_block_collector.hpp"

#include "chunk_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Remote Block Collector Implementation
//===--------------------------------------------------------------------===//

RemoteFileBlockMap RemoteBlockCollector::CollectRemoteBlocks(FileSystem &fs, const string &pattern, idx_t block_size) {
	RemoteFileBlockMap file_blocks;
	auto glob_results = fs.Glob(pattern);
	if (glob_results.empty()) {
		return file_blocks;
	}

	// Process each file
	for (const auto &file_info : glob_results) {
		auto file_handle = fs.OpenFile(file_info.path,
		                               FileOpenFlags::FILE_FLAGS_READ | FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
		if (!file_handle) {
			// TODO: add a debug logging that we skipped file
			continue;
		}
		idx_t file_size = fs.GetFileSize(*file_handle);
		if (file_size == 0) {
			// TODO: add a debug logging that we skipped file
			continue;
		}

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
			idx_t actual_size = std::min(block_size, file_size - offset);
			blocks.emplace_back(file_info.path, offset, static_cast<int64_t>(actual_size), file_size);
		}

		file_blocks[file_info.path] = std::move(blocks);
	}

	return file_blocks;
}

} // namespace duckdb

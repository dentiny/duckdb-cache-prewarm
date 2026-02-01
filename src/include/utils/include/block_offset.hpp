#pragma once

#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

//! Returns the file offset (in bytes) for a given block in a DuckDB single-file database.
//! Matches SingleFileBlockManager::GetBlockLocation.
//! See: https://github.com/duckdb/duckdb/blob/master/src/storage/single_file_block_manager.cpp#L917-L919
inline uint64_t GetBlockFileOffset(block_id_t block_id, idx_t block_size) {
	constexpr uint64_t BLOCK_START = Storage::FILE_HEADER_SIZE * 3;
	return BLOCK_START + (static_cast<uint64_t>(block_id) * block_size);
}

} // namespace duckdb

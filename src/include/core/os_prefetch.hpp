#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

//! Issue OS-level prefetch hints for database blocks
//! Uses platform-specific APIs: posix_fadvise (Linux) or fcntl with F_RDADVISE (macOS/BSD)
//! @param db_path Path to the database file
//! @param sorted_blocks Vector of block IDs sorted for sequential access
//! @param block_size Size of each block in bytes
//! @return Number of blocks successfully prefetched (0 if prefetch failed or not supported)
idx_t OSPrefetchBlocks(const string &db_path, const vector<block_id_t> &sorted_blocks, idx_t block_size);

} // namespace duckdb

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

//! Issue OS-level prefetch hints for a range of database blocks using iterators
//! Uses platform-specific APIs: posix_fadvise (Linux) or fcntl with F_RDADVISE (macOS/BSD)
//! @param db_path Path to the database file
//! @param begin Iterator to the first block ID
//! @param end Iterator past the last block ID
//! @param block_size Size of each block in bytes
//! @return Number of blocks successfully prefetched (0 if prefetch failed or not supported)
idx_t OSPrefetchBlocks(const string &db_path, vector<block_id_t>::const_iterator begin,
                       vector<block_id_t>::const_iterator end, idx_t block_size);

} // namespace duckdb

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "utils/include/span.hpp"

namespace duckdb {

//! Issue OS-level prefetch hints for a range of database blocks using Span
//! Uses platform-specific APIs: posix_fadvise (Linux) or fcntl with F_RDADVISE (macOS/BSD)
//! @param db_path Path to the database file
//! @param block_ids Span of block IDs to prefetch
//! @param block_size Size of each block in bytes
//! @return Number of blocks successfully prefetched (0 if prefetch failed or not supported)
idx_t OSPrefetchBlocks(const string &db_path, Span<const block_id_t> block_ids, idx_t block_size);

} // namespace duckdb

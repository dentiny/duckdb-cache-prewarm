#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "scope_guard.hpp"
#include "utils/include/block_offset.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
#include <sys/fcntl.h>
#endif
#endif

namespace duckdb {

//! Issue OS-level prefetch hints for a range of database blocks using iterators
//! Uses platform-specific APIs: posix_fadvise (Linux) or fcntl with F_RDADVISE (macOS/BSD)
//! @param db_path Path to the database file
//! @param begin Iterator to the first block ID
//! @param end Iterator past the last block ID
//! @param block_size Size of each block in bytes
//! @return Number of blocks successfully prefetched (0 if prefetch failed or not supported)
template <typename Iterator>
idx_t OSPrefetchBlocks(const string &db_path, Iterator begin, Iterator end, idx_t block_size) {
#ifndef _WIN32
	int fd = open(db_path.c_str(), O_RDONLY);
	if (fd < 0) {
		return 0;
	}
	SCOPE_EXIT {
		close(fd);
	};

	// Get file size to avoid prefetching beyond EOF
	struct stat st;
	if (fstat(fd, &st) != 0) {
		return 0;
	}
	off_t file_size = st.st_size;

	idx_t blocks_prefetched = 0;

	for (auto it = begin; it != end; ++it) {
		block_id_t block_id = *it;
		uint64_t offset = GetBlockFileOffset(block_id, block_size);

		// Verify the block offset is within file bounds
		// TODO: https://github.com/dentiny/duckdb-cache-prewarm/issues/23
		if (static_cast<off_t>(offset) >= file_size) {
			// Block starts at or beyond EOF, skip it
			continue;
		}

		// Calculate the actual amount to prefetch (may be less than block_size if near EOF)
		off_t amount = static_cast<off_t>(block_size);
		if (static_cast<off_t>(offset + block_size) > file_size) {
			// Block extends past EOF, only prefetch up to EOF
			amount = file_size - static_cast<off_t>(offset);
			if (amount <= 0) {
				continue;
			}
		}

		// Prefetch this block using OS-specific hints
		// Following PostgreSQL's FilePrefetch implementation
#if defined(__linux__) || (defined(_POSIX_C_SOURCE) && _POSIX_C_SOURCE >= 200112L)
		// Use posix_fadvise with POSIX_FADV_WILLNEED on Linux and POSIX.1-2001 systems
		// This is the simplest standardized interface for prefetching
		int result;
	retry_posix:
		result = posix_fadvise(fd, static_cast<off_t>(offset), amount, POSIX_FADV_WILLNEED);

		// Retry on interrupt signal, following PostgreSQL's pattern
		if (result == EINTR) {
			goto retry_posix;
		}

		if (result == 0) {
			blocks_prefetched++;
		}

#elif defined(__APPLE__)
		// macOS: Use fcntl with F_RDADVISE
		// This is the macOS-specific equivalent to posix_fadvise
		struct radvisory {
			off_t ra_offset; // offset into the file
			int ra_count;    // size of the read
		} ra;

		ra.ra_offset = static_cast<off_t>(offset);
		ra.ra_count = static_cast<int>(std::min(static_cast<off_t>(amount), static_cast<off_t>(INT_MAX)));

		int result = fcntl(fd, F_RDADVISE, &ra);
		// fcntl returns -1 on error, anything else on success
		if (result != -1) {
			blocks_prefetched++;
		}

#else
		// No OS-level prefetch hint is issued on this platform, so do not count this block
		// as successfully prefetched.
#endif
	}

	return blocks_prefetched;
#else
	// Windows: Not supported
#endif // !_WIN32
}

} // namespace duckdb

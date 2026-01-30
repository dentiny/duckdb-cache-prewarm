#include "core/os_prefetch.hpp"
#include "duckdb/storage/storage_info.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
#include <sys/fcntl.h>
#endif
#endif

namespace duckdb {

#ifndef _WIN32

// OS-level prefetch implementation following PostgreSQL's FilePrefetch approach
// See:
// https://github.com/postgres/postgres/blob/228fe0c3e68ef37b7e083fcb513664b9737c4d93/src/backend/storage/file/fd.c#L2054-L2116

idx_t OSPrefetchBlocks(const string &db_path, const vector<block_id_t> &sorted_blocks, idx_t block_size) {
	// Open the database file with read-only access
	int fd = open(db_path.c_str(), O_RDONLY);
	if (fd < 0) {
		return 0; // Failed to open, caller will fall back
	}

	// Get file size to avoid prefetching beyond EOF
	struct stat st;
	if (fstat(fd, &st) != 0) {
		close(fd);
		return 0;
	}
	off_t file_size = st.st_size;

	idx_t blocks_prefetched = 0;

	// File header size in DuckDB (blocks start after header)
	constexpr uint64_t BLOCK_START = Storage::FILE_HEADER_SIZE * 3;

	for (block_id_t block_id : sorted_blocks) {
		// Calculate file offset for this block
		// https://github.com/duckdb/duckdb/blob/6ddac802ffa9bcfbcc3f5f0d71de5dff9b0bc250/src/storage/single_file_block_manager.cpp#L917-L919
		uint64_t offset = BLOCK_START + (static_cast<uint64_t>(block_id) * block_size);

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
		// Other Unix systems: No OS-level prefetch hint available
		// Still count the block to report progress, but no actual prefetch occurs
		blocks_prefetched++;
#endif
	}

	close(fd);
	return blocks_prefetched;
}

#else

// Windows: Not supported
idx_t OSPrefetchBlocks(const string &db_path, const vector<block_id_t> &sorted_blocks, idx_t block_size) {
	return 0;
}

#endif // !_WIN32

} // namespace duckdb

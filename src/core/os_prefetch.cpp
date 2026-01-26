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
		uint64_t offset = BLOCK_START + (static_cast<uint64_t>(block_id) * block_size);

		// Skip blocks that are beyond the file size
		if (static_cast<off_t>(offset + block_size) > file_size) {
			continue;
		}

#if defined(__linux__)
		// Linux: Use posix_fadvise with POSIX_FADV_WILLNEED
		int result = posix_fadvise(fd, static_cast<off_t>(offset), static_cast<off_t>(block_size), POSIX_FADV_WILLNEED);
		if (result == 0) {
			blocks_prefetched++;
		}

#elif defined(__APPLE__)
// macOS: Use fcntl with F_RDADVISE
// The radvisory struct is defined in <sys/fcntl.h> on macOS
#ifdef F_RDADVISE
		struct radvisory {
			off_t ra_offset;
			int ra_count;
		} ra;

		ra.ra_offset = static_cast<off_t>(offset);
		ra.ra_count = static_cast<int>(std::min(block_size, static_cast<idx_t>(INT_MAX)));

		int result = fcntl(fd, F_RDADVISE, &ra);
		if (result == 0) {
			blocks_prefetched++;
		}
#else
		// F_RDADVISE not available, just count the block
		blocks_prefetched++;
#endif

#elif defined(__FreeBSD__) || defined(__OpenBSD__)
		// BSD systems: May not have F_RDADVISE, just count
		blocks_prefetched++;

#else
		// Other Unix systems: Just count the block, no prefetch available
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

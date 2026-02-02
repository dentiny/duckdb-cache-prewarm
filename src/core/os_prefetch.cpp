#include "core/os_prefetch.hpp"

#include "duckdb/storage/storage_info.hpp"
#include "scope_guard.hpp"
#include "utils/include/block_offset.hpp"

#ifndef _WIN32
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <thread>
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__)
#include <sys/fcntl.h>
#endif
#endif

namespace duckdb {

#ifndef _WIN32

// OS-level prefetch implementation following PostgreSQL's FilePrefetch approach
// See:
// https://github.com/postgres/postgres/blob/228fe0c3e68ef37b7e083fcb513664b9737c4d93/src/backend/storage/file/fd.c#L2054-L2116

namespace {

constexpr idx_t PREFETCH_CHUNK_SIZE = Storage::SECTOR_SIZE * 128;

idx_t CalculateBlocksPerTask(idx_t block_size, idx_t total_blocks, idx_t max_threads) {
	if (total_blocks == 0) {
		return 0;
	}
	auto target_blocks = std::max<idx_t>(1, PREFETCH_CHUNK_SIZE / block_size);
	auto concurrency = std::max<idx_t>(1, std::min<idx_t>(total_blocks, max_threads));
	auto max_blocks_per_task = std::max<idx_t>(1, total_blocks / concurrency);
	return std::min(target_blocks, max_blocks_per_task);
}

idx_t OSPrefetchBlocksRange(int fd, const vector<block_id_t> &sorted_blocks, idx_t block_size, idx_t start_idx,
                            idx_t end_idx, off_t file_size) {
	idx_t blocks_prefetched = 0;
	for (idx_t idx = start_idx; idx < end_idx; idx++) {
		auto block_id = sorted_blocks[idx];
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
}

} // namespace

idx_t OSPrefetchBlocks(const string &db_path, const vector<block_id_t> &sorted_blocks, idx_t block_size) {
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

	auto total_blocks = sorted_blocks.size();
	auto max_threads = std::max<idx_t>(1, std::thread::hardware_concurrency());
	auto blocks_per_task = CalculateBlocksPerTask(block_size, total_blocks, max_threads);
	if (blocks_per_task == 0 || total_blocks == 0) {
		return 0;
	}

	if (max_threads == 1 || blocks_per_task >= total_blocks) {
		return OSPrefetchBlocksRange(fd, sorted_blocks, block_size, 0, total_blocks, file_size);
	}

	vector<std::thread> workers;
	auto task_count = (total_blocks + blocks_per_task - 1) / blocks_per_task;
	vector<idx_t> worker_results(task_count, 0);
	for (idx_t task_index = 0; task_index < task_count; task_index++) {
		auto start_idx = task_index * blocks_per_task;
		auto end_idx = std::min<idx_t>(total_blocks, start_idx + blocks_per_task);
		workers.emplace_back([db_path, &sorted_blocks, block_size, start_idx, end_idx, file_size, &worker_results,
		                      task_index]() {
			int local_fd = open(db_path.c_str(), O_RDONLY);
			if (local_fd < 0) {
				return;
			}
			auto local_guard = ScopeGuard([&]() { close(local_fd); });
			worker_results[task_index] =
			    OSPrefetchBlocksRange(local_fd, sorted_blocks, block_size, start_idx, end_idx, file_size);
		});
	}

	idx_t blocks_prefetched = 0;
	for (auto &worker : workers) {
		if (worker.joinable()) {
			worker.join();
		}
	}
	for (auto blocks : worker_results) {
		blocks_prefetched += blocks;
	}

	return blocks_prefetched;
}

#else

// Windows: Not supported
idx_t OSPrefetchBlocks(const string &db_path, const vector<block_id_t> &sorted_blocks, idx_t block_size) {
	return 0;
}

#endif // !_WIN32

} // namespace duckdb

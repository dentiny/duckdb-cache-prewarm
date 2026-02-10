#include "core/prefetch_prewarm_strategy.hpp"
#include "core/os_prefetch.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

namespace {

// Target ~512KiB per hint batch to align with page cache granularity.
constexpr idx_t PREFETCH_CHUNK_SIZE = Storage::SECTOR_SIZE * 128;

class OSPrefetchTask : public BaseExecutorTask {
public:
	OSPrefetchTask(TaskExecutor &executor, const string &db_path_p, Span<const block_id_t> block_ids_p,
	               idx_t block_size_p, atomic<idx_t> &blocks_prefetched_p)
	    : BaseExecutorTask(executor), db_path(db_path_p), block_ids(block_ids_p), block_size(block_size_p),
	      blocks_prefetched(blocks_prefetched_p) {
	}

	void ExecuteTask() override {
		auto count = OSPrefetchBlocks(db_path, block_ids, block_size);
		blocks_prefetched += count;
	}

	string TaskType() const override {
		return "OSPrefetchTask";
	}

private:
	string db_path;
	Span<const block_id_t> block_ids;
	idx_t block_size;
	atomic<idx_t> &blocks_prefetched;
};

} // namespace

idx_t PrefetchPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	CheckDirectIO("PREFETCH");

	auto block_size = block_manager.GetBlockAllocSize();

	// Sort block IDs for sequential prefetch hints
	auto sorted_blocks = vector<block_id_t>(block_ids.begin(), block_ids.end());
	std::sort(sorted_blocks.begin(), sorted_blocks.end());
	auto total_blocks = sorted_blocks.size();

	auto capacity_info = CalculateMaxAvailableBlocks();
	if (total_blocks > capacity_info.max_blocks) {
		idx_t blocks_skipped = total_blocks - capacity_info.max_blocks;
		sorted_blocks.resize(capacity_info.max_blocks);

		DUCKDB_LOG_WARN(context,
		                "Maximum blocks to prefetch limit reached.\n"
		                "  Table blocks: %llu\n"
		                "  Prewarming: %llu blocks (skipping %llu due to capacity)\n"
		                "  Current available memory: %llu bytes, consider increasing memory_limit",
		                total_blocks, capacity_info.max_blocks, blocks_skipped, capacity_info.available_space);
		total_blocks = sorted_blocks.size();
	}

#ifndef _WIN32
	// Get the database file path from the storage manager
	auto &catalog = table_entry.ParentCatalog();
	auto &storage_manager = StorageManager::Get(catalog);
	string db_path = storage_manager.GetDBPath();

	auto thread_count = std::max(1, TaskScheduler::GetScheduler(context).NumberOfThreads());
	auto blocks_per_task = CalculateBlocksPerTask(block_size, total_blocks, thread_count, PREFETCH_CHUNK_SIZE);
	if (blocks_per_task == 0) {
		return 0;
	}

	TaskExecutor executor(context);
	atomic<idx_t> blocks_prefetched {0};

	for (idx_t start_idx = 0; start_idx < total_blocks; start_idx += blocks_per_task) {
		auto count = std::min<idx_t>(blocks_per_task, total_blocks - start_idx);
		Span<const block_id_t> block_ids_span(sorted_blocks.data() + start_idx, count);
		auto task = make_uniq<OSPrefetchTask>(executor, db_path, block_ids_span, block_size, blocks_prefetched);
		executor.ScheduleTask(std::move(task));
	}
	executor.WorkOnTasks();

	return blocks_prefetched;

#else
	// Non-Unix platforms not supported
	throw NotImplementedException(
	    "PREFETCH prewarm strategy is only supported on Unix-like systems (Linux, macOS, BSD)");
#endif
}

} // namespace duckdb

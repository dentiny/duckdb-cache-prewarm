#include "core/read_prewarm_strategy.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/storage_info.hpp"
#include <algorithm>
#include <atomic>

namespace duckdb {

namespace {

// Target ~512KB per read batch to align with page cache granularity while limiting temp buffer usage.
constexpr idx_t READ_PREFETCH_TARGET_BYTES = Storage::SECTOR_SIZE * 128;

class ReadBlockGroupTask : public BaseExecutorTask {
public:
	ReadBlockGroupTask(TaskExecutor &executor, ClientContext &context_p, BlockManager &block_manager_p,
	                   BufferManager &buffer_manager_p, block_id_t first_block_p, idx_t block_count_p,
	                   std::atomic<idx_t> &blocks_read_p)
	    : BaseExecutorTask(executor), block_manager(block_manager_p), buffer_manager(buffer_manager_p),
	      context(context_p), first_block(first_block_p), block_count(block_count_p), blocks_read(blocks_read_p) {
	}

	void ExecuteTask() override {
		try {
			auto block_size = block_manager.GetBlockAllocSize();
			auto total_size = block_count * block_size;
			auto temp_buffer = buffer_manager.Allocate(MemoryTag::BASE_TABLE, total_size, true);
			block_manager.ReadBlocks(temp_buffer.GetFileBuffer(), first_block, block_count);
			blocks_read.fetch_add(block_count, std::memory_order_relaxed);
		} catch (const IOException &e) {
			// TODO: the SingleFileBlockManager::ReadBlock sometimes throws file out-of-bounds exception, we have to do
			// further investigation and fix it.
			// https://github.com/dentiny/duckdb-cache-prewarm/issues/23
			DUCKDB_LOG_WARN(context, "READ prewarm failed for block %lld (count %llu): %s",
			                static_cast<int64_t>(first_block), static_cast<uint64_t>(block_count), e.what());
		}
	}

	string TaskType() const override {
		return "ReadBlockGroupTask";
	}

private:
	BlockManager &block_manager;
	BufferManager &buffer_manager;
	ClientContext &context;
	block_id_t first_block;
	idx_t block_count;
	std::atomic<idx_t> &blocks_read;
};

} // namespace

idx_t ReadPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	CheckDirectIO("READ");

	auto unloaded_handles = GetUnloadedBlockHandles(block_ids);
	if (unloaded_handles.empty()) {
		return 0;
	}

	idx_t blocks_read = 0;
	auto block_size = block_manager.GetBlockAllocSize();

	auto capacity_info = CalculateMaxAvailableBlocks();
	idx_t total_blocks = unloaded_handles.size();
	idx_t max_batch_size = capacity_info.max_blocks;
	if (max_batch_size == 0) {
		DUCKDB_LOG_WARN(context,
		                "Insufficient memory to prewarm any blocks (available: %llu bytes, block size: %llu bytes)",
		                capacity_info.available_memory, capacity_info.block_size);
		return 0;
	}
	if (total_blocks > capacity_info.max_blocks) {
		idx_t blocks_skipped = total_blocks - capacity_info.max_blocks;
		unloaded_handles.resize(capacity_info.max_blocks);

		DUCKDB_LOG_WARN(context,
		                "Maximum blocks to read limit reached.\n"
		                "  Table blocks: %llu\n"
		                "  Prewarming: %llu blocks (skipping %llu due to capacity)\n"
		                "  Current available memory: %llu bytes, consider increasing memory_limit",
		                total_blocks, capacity_info.max_blocks, blocks_skipped,
		                capacity_info.available_memory);
		total_blocks = unloaded_handles.size();
	}

	// Sort unloaded block IDs for sequential reading
	std::sort(
	    unloaded_handles.begin(), unloaded_handles.end(),
	    [](const shared_ptr<BlockHandle> &a, const shared_ptr<BlockHandle> &b) { return a->BlockId() < b->BlockId(); });

	auto thread_count = std::max(1, TaskScheduler::GetScheduler(context).NumberOfThreads());
	auto blocks_per_task = CalculateBlocksPerTask(block_size, max_batch_size, thread_count, READ_PREFETCH_TARGET_BYTES);
	if (blocks_per_task == 0) {
		return 0;
	}

	TaskExecutor executor(context);
	std::atomic<idx_t> parallel_blocks_read {0};

	// Read blocks in batches where possible
	for (size_t i = 0; i < unloaded_handles.size();) {
		block_id_t first_block = unloaded_handles[i]->BlockId();
		idx_t block_count = 1;

		// Find consecutive blocks and limit the batch size to prevent memory overflow
		while (i + block_count < unloaded_handles.size() &&
		       unloaded_handles[i + block_count]->BlockId() == first_block + block_count &&
		       block_count < max_batch_size) {
			block_count++;
		}

		for (idx_t offset = 0; offset < block_count; offset += blocks_per_task) {
			auto task_block_count = std::min<idx_t>(blocks_per_task, block_count - offset);
			auto task_first_block = first_block + static_cast<block_id_t>(offset);
			auto task = make_uniq<ReadBlockGroupTask>(executor, context, block_manager, buffer_manager,
			                                          task_first_block, task_block_count, parallel_blocks_read);
			executor.ScheduleTask(std::move(task));
		}
		i += block_count;
	}

	executor.WorkOnTasks();
	blocks_read = parallel_blocks_read.load(std::memory_order_relaxed);

	return blocks_read;
}

} // namespace duckdb

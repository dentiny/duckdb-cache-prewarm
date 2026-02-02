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

idx_t CalculateBlockGroupSize(idx_t block_size, idx_t max_blocks, idx_t max_threads, idx_t target_bytes) {
	if (max_blocks == 0) {
		return 0;
	}
	auto target_blocks = std::max<idx_t>(1, target_bytes / block_size);
	auto concurrency = std::max<idx_t>(1, std::min<idx_t>(max_blocks, max_threads));
	auto max_blocks_per_task = std::max<idx_t>(1, max_blocks / concurrency);
	return std::min(target_blocks, max_blocks_per_task);
}

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
			                static_cast<long long>(first_block), static_cast<unsigned long long>(block_count), e.what());
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
	idx_t max_batch_size = capacity_info.max_blocks;
	if (max_batch_size == 0) {
		DUCKDB_LOG_WARN(context,
		                "Insufficient memory to prewarm any blocks (available: %llu bytes, block size: %llu bytes)",
		                capacity_info.available_memory, capacity_info.block_size);
		return 0;
	}

	// Sort unloaded block IDs for sequential reading
	std::sort(
	    unloaded_handles.begin(), unloaded_handles.end(),
	    [](const shared_ptr<BlockHandle> &a, const shared_ptr<BlockHandle> &b) { return a->BlockId() < b->BlockId(); });

	auto thread_count = std::max<idx_t>(1, static_cast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()));
	auto blocks_per_task =
	    CalculateBlockGroupSize(block_size, max_batch_size, thread_count, READ_PREFETCH_TARGET_BYTES);
	if (blocks_per_task == 0) {
		return 0;
	}

	TaskExecutor executor(context);
	std::atomic<idx_t> parallel_blocks_read {0};
	bool use_parallel = thread_count > 1 && blocks_per_task < unloaded_handles.size();

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
			if (use_parallel) {
				auto task =
				    make_uniq<ReadBlockGroupTask>(executor, context, block_manager, buffer_manager, task_first_block,
				                                  task_block_count, parallel_blocks_read);
				executor.ScheduleTask(std::move(task));
			} else {
				try {
					auto total_size = task_block_count * block_size;
					auto temp_buffer = buffer_manager.Allocate(MemoryTag::BASE_TABLE, total_size, true);
					block_manager.ReadBlocks(temp_buffer.GetFileBuffer(), task_first_block, task_block_count);
					blocks_read += task_block_count;
				} catch (const IOException &e) {
					// TODO: the SingleFileBlockManager::ReadBlock sometimes throws file out-of-bounds exception, we have to do
					// further investigation and fix it.
					// https://github.com/dentiny/duckdb-cache-prewarm/issues/23
					DUCKDB_LOG_WARN(context, "READ prewarm failed for block %lld (count %llu): %s",
					                static_cast<long long>(task_first_block),
					                static_cast<unsigned long long>(task_block_count), e.what());
					continue;
				}
			}
		}
		i += block_count;
	}

	if (use_parallel) {
		executor.WorkOnTasks();
		blocks_read = parallel_blocks_read.load(std::memory_order_relaxed);
	}

	return blocks_read;
}

} // namespace duckdb

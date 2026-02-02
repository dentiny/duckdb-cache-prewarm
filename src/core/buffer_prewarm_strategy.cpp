#include "core/buffer_prewarm_strategy.hpp"

#include "duckdb/logging/logger.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

namespace {

// Use ~4MB batches (16 * default 256KB blocks) to balance throughput and buffer pool pressure.
constexpr idx_t BUFFER_PREFETCH_TARGET_BYTES = 4ULL * 1024ULL * 1024ULL;

idx_t CalculateBlockGroupSize(idx_t block_size, idx_t max_blocks, idx_t max_threads, idx_t target_bytes) {
	if (max_blocks == 0) {
		return 0;
	}
	auto target_blocks = std::max<idx_t>(1, target_bytes / block_size);
	auto concurrency = std::max<idx_t>(1, std::min<idx_t>(max_blocks, max_threads));
	auto max_blocks_per_task = std::max<idx_t>(1, max_blocks / concurrency);
	return std::min(target_blocks, max_blocks_per_task);
}

class BufferPrefetchTask : public BaseExecutorTask {
public:
	BufferPrefetchTask(TaskExecutor &executor, BufferManager &buffer_manager_p,
	                   shared_ptr<vector<shared_ptr<BlockHandle>>> handles_p, idx_t start_p, idx_t count_p)
	    : BaseExecutorTask(executor), buffer_manager(buffer_manager_p), handles(std::move(handles_p)), start(start_p),
	      count(count_p) {
	}

	void ExecuteTask() override {
		vector<shared_ptr<BlockHandle>> batch;
		batch.reserve(count);
		for (idx_t idx = 0; idx < count; idx++) {
			batch.push_back((*handles)[start + idx]);
		}
		buffer_manager.Prefetch(batch);
	}

	string TaskType() const override {
		return "BufferPrefetchTask";
	}

private:
	BufferManager &buffer_manager;
	shared_ptr<vector<shared_ptr<BlockHandle>>> handles;
	idx_t start;
	idx_t count;
};

} // namespace

idx_t BufferPrewarmStrategy::Execute(DuckTableEntry &table_entry, const unordered_set<block_id_t> &block_ids) {
	auto unloaded_handles = GetUnloadedBlockHandles(block_ids);
	if (unloaded_handles.empty()) {
		return 0;
	}

	auto capacity_info = CalculateMaxAvailableBlocks();

	idx_t total_blocks = block_ids.size();
	idx_t already_cached = total_blocks - unloaded_handles.size();
	idx_t blocks_to_prewarm = unloaded_handles.size();

	if (unloaded_handles.size() > capacity_info.max_blocks) {
		idx_t blocks_skipped = unloaded_handles.size() - capacity_info.max_blocks;
		unloaded_handles.resize(capacity_info.max_blocks);

		DUCKDB_LOG_WARN(context,
		                "Buffer pool capacity limit reached.\n"
		                "  Table blocks: %llu total (%llu already cached, %llu unloaded)\n"
		                "  Prewarming: %llu blocks (skipping %llu due to capacity)\n"
		                "  Memory: %llu bytes available, %llu bytes required for all unloaded blocks",
		                total_blocks, already_cached, blocks_to_prewarm, capacity_info.max_blocks, blocks_skipped,
		                capacity_info.available_memory, blocks_to_prewarm * capacity_info.block_size);
	}

	auto thread_count = std::max<idx_t>(1, static_cast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()));
	auto blocks_per_task =
	    CalculateBlockGroupSize(capacity_info.block_size, capacity_info.max_blocks, thread_count,
	                            BUFFER_PREFETCH_TARGET_BYTES);
	if (blocks_per_task == 0) {
		return 0;
	}

	if (thread_count == 1 || blocks_per_task >= unloaded_handles.size()) {
		for (idx_t start = 0; start < unloaded_handles.size(); start += blocks_per_task) {
			auto count = std::min<idx_t>(blocks_per_task, unloaded_handles.size() - start);
			vector<shared_ptr<BlockHandle>> batch;
			batch.reserve(count);
			for (idx_t idx = 0; idx < count; idx++) {
				batch.push_back(unloaded_handles[start + idx]);
			}
			buffer_manager.Prefetch(batch);
		}
		return unloaded_handles.size();
	}

	std::sort(
	    unloaded_handles.begin(), unloaded_handles.end(),
	    [](const shared_ptr<BlockHandle> &a, const shared_ptr<BlockHandle> &b) { return a->BlockId() < b->BlockId(); });

	TaskExecutor executor(context);
	auto shared_handles = make_shared_ptr<vector<shared_ptr<BlockHandle>>>(unloaded_handles);
	for (idx_t start = 0; start < unloaded_handles.size(); start += blocks_per_task) {
		auto count = std::min<idx_t>(blocks_per_task, unloaded_handles.size() - start);
		auto task = make_uniq<BufferPrefetchTask>(executor, buffer_manager, shared_handles, start, count);
		executor.ScheduleTask(std::move(task));
	}
	executor.WorkOnTasks();

	return unloaded_handles.size();
}

} // namespace duckdb

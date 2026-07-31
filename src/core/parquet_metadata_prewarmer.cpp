#include "core/parquet_metadata_prewarmer.hpp"

#include "parquet_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/client_context.hpp"

#include <algorithm>

namespace duckdb {

namespace {

struct ByteRange {
	idx_t offset;
	idx_t length;
};

void AddRange(vector<ByteRange> &ranges, idx_t offset, idx_t length) {
	if (length == 0) {
		return;
	}
	ranges.push_back(ByteRange {offset, length});
}

void AddOffsetCandidate(vector<idx_t> &candidates, idx_t start, idx_t candidate) {
	if (candidate > start) {
		candidates.push_back(candidate);
	}
}

bool HasExactIndexRange(const duckdb_parquet::ColumnChunk &column_chunk) {
	return (column_chunk.__isset.column_index_offset && column_chunk.__isset.column_index_length &&
	        column_chunk.column_index_length > 0) ||
	       (column_chunk.__isset.offset_index_offset && column_chunk.__isset.offset_index_length &&
	        column_chunk.offset_index_length > 0);
}

void AddExplicitIndexRanges(vector<ByteRange> &ranges, const duckdb_parquet::ColumnChunk &column_chunk) {
	if (column_chunk.__isset.column_index_offset && column_chunk.__isset.column_index_length &&
	    column_chunk.column_index_length > 0) {
		AddRange(ranges, UnsafeNumericCast<idx_t>(column_chunk.column_index_offset),
		         UnsafeNumericCast<idx_t>(column_chunk.column_index_length));
	}
	if (column_chunk.__isset.offset_index_offset && column_chunk.__isset.offset_index_length &&
	    column_chunk.offset_index_length > 0) {
		AddRange(ranges, UnsafeNumericCast<idx_t>(column_chunk.offset_index_offset),
		         UnsafeNumericCast<idx_t>(column_chunk.offset_index_length));
	}
}

void AddBloomFilterRange(vector<ByteRange> &ranges, const duckdb_parquet::ColumnChunk &column_chunk) {
	const auto &metadata = column_chunk.meta_data;
	if (!metadata.__isset.bloom_filter_offset || !metadata.__isset.bloom_filter_length ||
	    metadata.bloom_filter_length <= 0) {
		return;
	}
	AddRange(ranges, UnsafeNumericCast<idx_t>(metadata.bloom_filter_offset),
	         UnsafeNumericCast<idx_t>(metadata.bloom_filter_length));
}

void AddLegacyIndexPageRange(vector<ByteRange> &ranges, const duckdb_parquet::ColumnChunk &column_chunk,
                             idx_t footer_start, idx_t file_size) {
	const auto &metadata = column_chunk.meta_data;
	if (!metadata.__isset.index_page_offset || HasExactIndexRange(column_chunk)) {
		return;
	}

	const auto start = UnsafeNumericCast<idx_t>(metadata.index_page_offset);
	vector<idx_t> candidates;
	candidates.reserve(6);

	if (metadata.__isset.dictionary_page_offset) {
		AddOffsetCandidate(candidates, start, UnsafeNumericCast<idx_t>(metadata.dictionary_page_offset));
	}
	AddOffsetCandidate(candidates, start, UnsafeNumericCast<idx_t>(metadata.data_page_offset));

	if (column_chunk.__isset.column_index_offset) {
		AddOffsetCandidate(candidates, start, UnsafeNumericCast<idx_t>(column_chunk.column_index_offset));
	}
	if (column_chunk.__isset.offset_index_offset) {
		AddOffsetCandidate(candidates, start, UnsafeNumericCast<idx_t>(column_chunk.offset_index_offset));
	}
	if (metadata.__isset.bloom_filter_offset) {
		AddOffsetCandidate(candidates, start, UnsafeNumericCast<idx_t>(metadata.bloom_filter_offset));
	}
	AddOffsetCandidate(candidates, start, footer_start);
	AddOffsetCandidate(candidates, start, file_size);

	if (candidates.empty()) {
		return;
	}

	auto end = *std::min_element(candidates.begin(), candidates.end());
	if (end <= start) {
		return;
	}
	AddRange(ranges, start, end - start);
}

vector<ByteRange> MergeRanges(vector<ByteRange> ranges) {
	if (ranges.empty()) {
		return ranges;
	}

	std::sort(ranges.begin(), ranges.end(), [](const ByteRange &lhs, const ByteRange &rhs) {
		if (lhs.offset != rhs.offset) {
			return lhs.offset < rhs.offset;
		}
		return lhs.length < rhs.length;
	});

	vector<ByteRange> merged;
	merged.reserve(ranges.size());
	merged.push_back(ranges[0]);

	for (idx_t i = 1; i < ranges.size(); i++) {
		auto &current = merged.back();
		const auto current_end = current.offset + current.length;
		const auto next_end = ranges[i].offset + ranges[i].length;
		if (ranges[i].offset <= current_end) {
			current.length = MaxValue(current_end, next_end) - current.offset;
			continue;
		}
		merged.push_back(ranges[i]);
	}

	return merged;
}

vector<ByteRange> CollectRanges(ParquetReader &reader) {
	vector<ByteRange> ranges;

	auto &handle = reader.GetHandle();
	const auto file_size = handle.GetFileSize();
	const auto footer_size = reader.metadata->footer_size + 8;
	const auto footer_start = file_size - footer_size;
	AddRange(ranges, footer_start, footer_size);

	auto metadata = reader.GetFileMetadata();
	for (const auto &row_group : metadata->row_groups) {
		for (const auto &column_chunk : row_group.columns) {
			AddExplicitIndexRanges(ranges, column_chunk);
			AddBloomFilterRange(ranges, column_chunk);
			AddLegacyIndexPageRange(ranges, column_chunk, footer_start, file_size);
		}
	}

	return MergeRanges(std::move(ranges));
}

idx_t PrewarmFile(ClientContext &context, const OpenFileInfo &file) {
	ParquetOptions parquet_options(context);
	ParquetReader reader(context, file, parquet_options);
	auto ranges = CollectRanges(reader);

	idx_t bytes_prewarmed = 0;
	auto &handle = reader.GetHandle();
	for (const auto &range : ranges) {
		data_ptr_t buffer = nullptr;
		auto buffer_handle = handle.Read(buffer, range.length, range.offset);
		D_ASSERT(buffer_handle.IsValid());
		bytes_prewarmed += range.length;
	}

	return bytes_prewarmed;
}

} // namespace

idx_t ParquetMetadataPrewarmer::Execute(ClientContext &context, FileSystem &fs, const string &pattern) {
	idx_t bytes_prewarmed = 0;
	auto files = fs.Glob(pattern);
	for (const auto &file : files) {
		bytes_prewarmed += PrewarmFile(context, file);
	}
	return bytes_prewarmed;
}

} // namespace duckdb

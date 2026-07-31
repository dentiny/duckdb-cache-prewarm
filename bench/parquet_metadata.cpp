// Parquet metadata prewarm benchmark: runs parquet-focused queries with optional
// prewarm_parquet_metadata() via DuckDB C++ API.

#include "cache_prewarm_extension.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <fmt/format.h>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

static void Usage(const char *prog) {
	std::cerr << "Usage: " << prog << " [options]\n"
	          << "Options:\n"
	          << "  -p <path>    Parquet file path (default: parquet_metadata_bench.parquet)\n"
	          << "  -d <path>    Database path (default: in-memory)\n"
	          << "  -q <path>    Path to queries.sql (default: parquet_metadata_queries.sql)\n"
	          << "  -r <int>     Number of times to repeat each query (default: 1)\n"
	          << "  -i <spec>    Run i-th query, or a range of queries (default: all)\n"
	          << "  -m <mode>    Mode: baseline | metadata (default: baseline)\n"
	          << "  --purge <true|false>  Clear OS page cache between queries (default: true)\n";
}

enum class Mode { Baseline, Metadata };

static Mode ParseMode(const std::string &s) {
	if (s == "baseline") {
		return Mode::Baseline;
	}
	if (s == "metadata") {
		return Mode::Metadata;
	}
	throw std::invalid_argument("mode must be baseline or metadata");
}

static std::string ModeStr(Mode mode) {
	switch (mode) {
		case Mode::Baseline:
			return "baseline";
		case Mode::Metadata:
			return "metadata";
	}
	return "?";
}

static std::string SqlQuote(const std::string &value) {
	std::string result = "'";
	for (const auto ch : value) {
		if (ch == '\'') {
			result += "''";
		} else {
			result += ch;
		}
	}
	result += "'";
	return result;
}

static std::string ReplaceAll(std::string input, const std::string &needle, const std::string &replacement) {
	size_t pos = 0;
	while ((pos = input.find(needle, pos)) != std::string::npos) {
		input.replace(pos, needle.size(), replacement);
		pos += replacement.size();
	}
	return input;
}

// Parse "all", "5", "1-10", "1,3,5", "1-5,10" into 0-based indices.
static std::vector<size_t> ParseQueryIndices(const std::string &spec, size_t max_query) {
	std::vector<size_t> out;
	if (spec == "all") {
		for (size_t i = 0; i < max_query; i++) {
			out.push_back(i);
		}
		return out;
	}

	std::string remaining = spec;
	for (;;) {
		size_t comma = remaining.find(',');
		std::string part = (comma == std::string::npos) ? remaining : remaining.substr(0, comma);
		size_t dash = part.find('-');
		if (dash != std::string::npos) {
			int start = std::stoi(part.substr(0, dash));
			int end = std::stoi(part.substr(dash + 1));
			for (int idx = start; idx <= end; idx++) {
				if (idx < 1 || static_cast<size_t>(idx) > max_query) {
					std::cerr << "Query index " << idx << " out of range (1-" << max_query << ")\n";
					throw std::invalid_argument("query index out of range");
				}
				out.push_back(static_cast<size_t>(idx - 1));
			}
		} else {
			int idx = std::stoi(part);
			if (idx < 1 || static_cast<size_t>(idx) > max_query) {
				std::cerr << "Query index " << idx << " out of range (1-" << max_query << ")\n";
				throw std::invalid_argument("query index out of range");
			}
			out.push_back(static_cast<size_t>(idx - 1));
		}
		if (comma == std::string::npos) {
			break;
		}
		remaining = remaining.substr(comma + 1);
	}
	return out;
}

static void DoPurge() {
#ifdef __linux__
	(void)std::system("sync 2>/dev/null; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1");
#elif defined(__APPLE__)
	(void)std::system("purge 2>/dev/null");
#endif
}

static std::vector<std::string> LoadQueries(const std::string &queries_path, const std::string &parquet_path) {
	std::ifstream query_file(queries_path);
	if (!query_file) {
		throw std::runtime_error("cannot open queries file: " + queries_path);
	}

	std::vector<std::string> queries;
	std::string line;
	const auto sql_parquet_path = SqlQuote(parquet_path);
	while (std::getline(query_file, line)) {
		while (!line.empty() && (line.back() == '\r' || line.back() == ' ' || line.back() == '\t')) {
			line.pop_back();
		}
		if (line.empty() || line[0] == '#') {
			continue;
		}
		queries.push_back(ReplaceAll(line, "__PARQUET_PATH__", sql_parquet_path));
	}

	if (queries.empty()) {
		throw std::runtime_error("no queries in " + queries_path);
	}
	return queries;
}

int main(int argc, char **argv) {
	std::string parquet_path = "parquet_metadata_bench.parquet";
	std::string db_path;
	std::string queries_path = "parquet_metadata_queries.sql";
	bool purge_between = true;
	Mode mode = Mode::Baseline;
	std::string query_indices_spec = "all";
	int repeat = 1;

	int arg_idx = 1;
	while (arg_idx < argc) {
		std::string arg = argv[arg_idx];
		if (arg == "-p" && arg_idx + 1 < argc) {
			parquet_path = argv[++arg_idx];
			arg_idx++;
			continue;
		}
		if (arg == "-d" && arg_idx + 1 < argc) {
			db_path = argv[++arg_idx];
			arg_idx++;
			continue;
		}
		if (arg == "-q" && arg_idx + 1 < argc) {
			queries_path = argv[++arg_idx];
			arg_idx++;
			continue;
		}
		if (arg == "-m" && arg_idx + 1 < argc) {
			mode = ParseMode(argv[++arg_idx]);
			arg_idx++;
			continue;
		}
		if (arg == "-i" && arg_idx + 1 < argc) {
			query_indices_spec = argv[++arg_idx];
			arg_idx++;
			continue;
		}
		if (arg == "-r" && arg_idx + 1 < argc) {
			repeat = std::atoi(argv[++arg_idx]);
			arg_idx++;
			continue;
		}
		if (arg == "--purge" && arg_idx + 1 < argc) {
			std::string value = argv[++arg_idx];
			if (value == "true") {
				purge_between = true;
			} else if (value == "false") {
				purge_between = false;
			} else {
				std::cerr << "Unknown option: " << arg << "\n";
				Usage(argv[0]);
				return 1;
			}
			arg_idx++;
			continue;
		}
		if (arg == "-h" || arg == "--help") {
			Usage(argv[0]);
			return 0;
		}
		if (!arg.empty() && arg[0] == '-') {
			std::cerr << "Unknown option: " << arg << "\n";
			Usage(argv[0]);
			return 1;
		}
		arg_idx++;
	}

	std::vector<std::string> all_queries;
	try {
		all_queries = LoadQueries(queries_path, parquet_path);
	} catch (const std::exception &ex) {
		std::cerr << "Error: " << ex.what() << "\n";
		return 1;
	}

	std::vector<size_t> indices;
	try {
		indices = ParseQueryIndices(query_indices_spec, all_queries.size());
	} catch (const std::exception &) {
		return 1;
	}

	std::cout << "Running " << indices.size() << " queries with mode: " << ModeStr(mode) << "\n\n";

	const auto prewarm_sql = duckdb_fmt::format("SELECT prewarm_parquet_metadata({})", SqlQuote(parquet_path));

	try {
		for (size_t query_list_idx = 0; query_list_idx < indices.size(); query_list_idx++) {
			std::vector<double> prewarm_times;
			std::vector<double> query_times;
			std::string error;

			for (int repeat_idx = 0; repeat_idx < repeat; repeat_idx++) {
				size_t query_idx = indices[query_list_idx];
				const auto &query = all_queries[query_idx];
				size_t query_num = query_idx + 1;

				if (purge_between) {
					DoPurge();
				}

				duckdb::DuckDB db(db_path.empty() ? nullptr : db_path.c_str());
				duckdb::Connection con(db);

				auto parquet_result = con.Query("LOAD parquet");
				if (parquet_result->HasError()) {
					error = duckdb_fmt::format("Loading parquet failed: {}", parquet_result->GetError());
					break;
				}

				duckdb::ExtensionLoader loader(duckdb::DatabaseInstance::GetDatabase(*con.context), "cache_prewarm");
				duckdb::CachePrewarmExtension cache_prewarm;
				cache_prewarm.Load(loader);

				if (mode == Mode::Metadata) {
					auto start = std::chrono::steady_clock::now();
					auto prewarm_result = con.Query(prewarm_sql);
					auto end = std::chrono::steady_clock::now();
					if (prewarm_result->HasError()) {
						error = duckdb_fmt::format("Prewarm failed: {}", prewarm_result->GetError());
						break;
					}
					prewarm_times.push_back(std::chrono::duration<double, std::milli>(end - start).count());
				}

				auto start = std::chrono::steady_clock::now();
				auto result = con.Query(query);
				auto end = std::chrono::steady_clock::now();
				if (result->HasError()) {
					error = duckdb_fmt::format("Query {} error: {}", query_num, result->GetError());
					break;
				}
				query_times.push_back(std::chrono::duration<double, std::milli>(end - start).count());
			}

			if (!error.empty()) {
				std::cerr << error << "\n";
				return 1;
			}

			if (mode == Mode::Metadata) {
				double prewarm_time_min = *std::min_element(prewarm_times.begin(), prewarm_times.end());
				double prewarm_time_max = *std::max_element(prewarm_times.begin(), prewarm_times.end());
				double prewarm_time_average =
				    std::accumulate(prewarm_times.begin(), prewarm_times.end(), 0.0) / static_cast<double>(prewarm_times.size());
				std::cout << "Prewarm time: min: " << prewarm_time_min << " ms - max: " << prewarm_time_max
				          << " ms - average: " << prewarm_time_average << " ms\n";
			}

			double query_time_min = *std::min_element(query_times.begin(), query_times.end());
			double query_time_max = *std::max_element(query_times.begin(), query_times.end());
			double query_time_average =
			    std::accumulate(query_times.begin(), query_times.end(), 0.0) / static_cast<double>(query_times.size());
			std::cout << "Query time: min: " << query_time_min << " ms - max: " << query_time_max
			          << " ms - average: " << query_time_average << " ms\n";
		}
	} catch (const std::exception &ex) {
		std::cerr << "Unhandled error: " << ex.what() << "\n";
		return 1;
	}

	return 0;
}

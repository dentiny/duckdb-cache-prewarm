// Cache prewarm benchmark: runs queries with optional prewarm via DuckDB C++ API.
// Loads the cache_prewarm extension using ExtensionHelper::LoadExternalExtension.

#include "cache_prewarm_extension.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include <fmt/format.h>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

static void usage(const char *prog) {
	std::cerr << "Usage: " << prog << " [options] <mode> [query_indices]\n"
	          << "  mode: baseline | buffer | read | prefetch\n"
	          << "  query_indices: all (default) | 5 | 1-10 | 1,3,5 | 1-5,10\n"
	          << "Options:\n"
	          << "  -d <path>    Database path (default: hits.db)\n"
	          << "  -e <path>   Path to cache_prewarm.duckdb_extension (required or set CACHE_PREWARM_EXTENSION)\n"
	          << "  -q <path>    Path to queries.sql (default: queries.sql in bench dir or cwd)\n"
	          << "  --purge     Clear OS page cache between queries (Linux: drop_caches; macOS: purge; may need sudo)\n";
}

enum class Mode { Baseline, Buffer, Read, Prefetch };

static Mode parseMode(const std::string &s) {
	if (s == "baseline") {
		return Mode::Baseline;
	}
	if (s == "buffer") {
		return Mode::Buffer;
	}
	if (s == "read") {
		return Mode::Read;
	}
	if (s == "prefetch") {
		return Mode::Prefetch;
	}
	throw std::invalid_argument("mode must be baseline, buffer, read, or prefetch");
}

static std::string modeStr(Mode m) {
	switch (m) {
		case Mode::Baseline: return "baseline";
		case Mode::Buffer: return "buffer";
		case Mode::Read: return "read";
		case Mode::Prefetch: return "prefetch";
	}
	return "?";
}

// Parse "all", "5", "1-10", "1,3,5", "1-5,10" into 0-based indices; maxQuery is total number of queries.
static std::vector<size_t> parseQueryIndices(const std::string &spec, size_t maxQuery) {
	std::vector<size_t> out;
	if (spec == "all") {
		for (size_t i = 0; i < maxQuery; i++) {
			out.push_back(i);
		}
		return out;
	}
	std::string s = spec;
	for (;;) {
		size_t comma = s.find(',');
		std::string part = (comma == std::string::npos) ? s : s.substr(0, comma);
		size_t dash = part.find('-');
		if (dash != std::string::npos) {
			int start = std::stoi(part.substr(0, dash));
			int end = std::stoi(part.substr(dash + 1));
			for (int i = start; i <= end; i++) {
				if (i < 1 || (size_t)i > maxQuery) {
					std::cerr << "Query index " << i << " out of range (1-" << maxQuery << ")\n";
					throw std::invalid_argument("query index out of range");
				}
				out.push_back((size_t)(i - 1));
			}
		} else {
			int idx = std::stoi(part);
			if (idx < 1 || (size_t)idx > maxQuery) {
				std::cerr << "Query index " << idx << " out of range (1-" << maxQuery << ")\n";
				throw std::invalid_argument("query index out of range");
			}
			out.push_back((size_t)(idx - 1));
		}
		if (comma == std::string::npos) {
			break;
		}
		s = s.substr(comma + 1);
	}
	return out;
}

static void doPurge() {
#ifdef __linux__
	(void)std::system("sync 2>/dev/null; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1");
#elif defined(__APPLE__)
	(void)std::system("purge 2>/dev/null");
#endif
}

int main(int argc, char **argv) {
	std::string dbPath = "hits.db";
	std::string extensionPath;
	std::string queriesPath = "queries.sql";
	bool purgeBetween = false;
	Mode mode = Mode::Baseline;
	std::string queryIndicesSpec = "all";

	int i = 1;
	while (i < argc) {
		std::string a = argv[i];
		if (a == "-d" && i + 1 < argc) {
			dbPath = argv[++i];
			i++;
			continue;
		}
		if (a == "-e" && i + 1 < argc) {
			extensionPath = argv[++i];
			i++;
			continue;
		}
		if (a == "-q" && i + 1 < argc) {
			queriesPath = argv[++i];
			i++;
			continue;
		}
		if (a == "--purge") {
			purgeBetween = true;
			i++;
			continue;
		}
		if (a == "-h" || a == "--help") {
			usage(argv[0]);
			return 0;
		}
		if (a[0] == '-') {
			std::cerr << "Unknown option: " << a << "\n";
			usage(argv[0]);
			return 1;
		}
		// positional: mode [query_indices]
		mode = parseMode(a);
		i++;
		if (i < argc && argv[i][0] != '-') {
			queryIndicesSpec = argv[i];
			i++;
		}
		break;
	}

	// Load queries from file
	std::ifstream qf(queriesPath);
	if (!qf) {
		std::cerr << "Error: cannot open queries file: " << queriesPath << "\n";
		return 1;
	}
	std::vector<std::string> allQueries;
	std::string line;
	while (std::getline(qf, line)) {
		// trim trailing whitespace
		while (!line.empty() && (line.back() == '\r' || line.back() == ' ' || line.back() == '\t')) {
			line.pop_back();
		}
		if (!line.empty()) {
			allQueries.push_back(line);
		}
	}
	qf.close();
	if (allQueries.empty()) {
		std::cerr << "Error: no queries in " << queriesPath << "\n";
		return 1;
	}

	std::vector<size_t> indices;
	try {
		indices = parseQueryIndices(queryIndicesSpec, allQueries.size());
	} catch (const std::exception &e) {
		return 1;
	}

	std::cout << "Running " << indices.size() << " queries with mode: " << modeStr(mode) << "\n\n";

	try {
		for (size_t k = 0; k < indices.size(); k++) {
			size_t idx = indices[k];
			const std::string &query = allQueries[idx];
			size_t queryNum = idx + 1;

			if (purgeBetween) {
				doPurge();
			}

			duckdb::DuckDB db(dbPath);
			duckdb::Connection con(db);

			duckdb::ExtensionLoader loader(duckdb::DatabaseInstance::GetDatabase(*con.context), "cache_prewarm");
			duckdb::CachePrewarmExtension cache_prewarm;
			cache_prewarm.Load(loader);

			// Prewarm for non-baseline modes
			if (mode != Mode::Baseline) {
				auto start = std::chrono::steady_clock::now();
				std::string prewarmSql = duckdb_fmt::format("SELECT prewarm('hits', '{}')", modeStr(mode));
				auto prewarmResult = con.Query(prewarmSql);
				if (prewarmResult->HasError()) {
					std::cerr << "Prewarm failed: " << prewarmResult->GetError() << "\n";
					return 1;
				}
				auto end = std::chrono::steady_clock::now();
				double ms = std::chrono::duration<double, std::milli>(end - start).count();
				std::cout << "Prewarm time: " << ms << " ms\n";
			}

			// Run query with timing
			auto start = std::chrono::steady_clock::now();
			auto result = con.Query(query);
			auto end = std::chrono::steady_clock::now();
			if (result->HasError()) {
				std::cerr << "Query " << queryNum << " error: " << result->GetError() << "\n";
				return 1;
			}
			double ms = std::chrono::duration<double, std::milli>(end - start).count();
			std::cout << "Query " << queryNum << ": " << ms << " ms\n";
			std::cout << "  " << query << "\n";
		}
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << "\n";
		return 1;
	}

	return 0;
}

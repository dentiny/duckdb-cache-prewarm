// ClickBench / cache prewarm benchmark: runs queries with optional prewarm via DuckDB C++ API.
// Loads the cache_prewarm extension using ExtensionLoader + CachePrewarmExtension::Load(loader)
// (extension is linked statically; no .duckdb_extension file required).

#include "cache_prewarm_extension.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <fmt/format.h>
#include <iostream>
#include <string>
#include <vector>

static void Usage(const char *prog) {
	std::cerr << "Usage: " << prog << " [options] <mode> [query_indices]\n"
	          << "  mode: baseline | buffer | read | prefetch\n"
	          << "  query_indices: all (default) | 5 | 1-10 | 1,3,5 | 1-5,10\n"
	          << "Options:\n"
              << "  -i <int>,<int>,<int>-<int>     Run i-th query, or a range of queries (default: all)\n"
              << "  -m <mode>    Mode: baseline | buffer | read | prefetch (default: baseline)\n"
	          << "  -d <path>    Database path (default: clickbench.db)\n"
	          << "  -q <path>    Path to queries.sql (default: queries.sql)\n"
	          << "  -r <int>     Number of times to repeat each query (default: 1)\n"
	          << "  --purge      Clear OS page cache between queries (Linux/macOS; may need sudo) (default: true)\n";
}

enum class Mode { Baseline, Buffer, Read, Prefetch };

static Mode ParseMode(const std::string &s) {
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

static std::string ModeStr(Mode m) {
	switch (m) {
		case Mode::Baseline:
			return "baseline";
		case Mode::Buffer:
			return "buffer";
		case Mode::Read:
			return "read";
		case Mode::Prefetch:
			return "prefetch";
	}
	return "?";
}

// Parse "all", "5", "1-10", "1,3,5", "1-5,10" into 0-based indices.
static std::vector<size_t> parse_query_indices(const std::string &spec, size_t maxQuery) {
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

static void DoPurge() {
#ifdef __linux__
	(void)std::system("sync 2>/dev/null; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1");
#elif defined(__APPLE__)
	(void)std::system("purge 2>/dev/null");
#endif
}

int main(int argc, char **argv) {
	std::string dbPath = "clickbench.db";
	std::string queriesPath = "queries.sql";
	bool purgeBetween = true;
	Mode mode = Mode::Baseline;
	std::string queryIndicesSpec = "all";
    int repeat = 1;

	int i = 1;
	while (i < argc) {
		std::string a = argv[i];
		if (a == "-d" && i + 1 < argc) {
			dbPath = argv[++i];
			i++;
			continue;
		}
		if (a == "-q" && i + 1 < argc) {
			queriesPath = argv[++i];
			i++;
			continue;
		}
        if (a == "-m" && i + 1 < argc) {
            mode = ParseMode(argv[++i]);
            i++;
            continue;
        }
        if (a == "-i" && i + 1 < argc) {
            queryIndicesSpec = argv[++i];
            i++;
            continue;
        }
        if (a == "-r" && i + 1 < argc) {
            repeat = std::atoi(argv[++i]);
            i++;
            continue;
        }
		if (a == "--purge") {
            std::string s = argv[++i];
            if (s == "true") {
                purgeBetween = true;
            } else if (s == "false") {
                purgeBetween = false;
            } else {
                std::cerr << "Unknown option: " << a << "\n";
                Usage(argv[0]);
                return 1;
            }
			i++;
			continue;
		}
		if (a == "-h" || a == "--help") {
			Usage(argv[0]);
			return 0;
		}
		if (a[0] == '-') {
			std::cerr << "Unknown option: " << a << "\n";
			Usage(argv[0]);
			return 1;
		}
		i++;
		break;
	}

	std::ifstream qf(queriesPath);
	if (!qf) {
		std::cerr << "Error: cannot open queries file: " << queriesPath << "\n";
		return 1;
	}
	std::vector<std::string> allQueries;
	std::string line;
	while (std::getline(qf, line)) {
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
		indices = parse_query_indices(queryIndicesSpec, allQueries.size());
	} catch (const std::exception &) {
		return 1;
	}

	std::cout << "Running " << indices.size() << " queries with mode: " << ModeStr(mode) << "\n\n";

	try {
		for (size_t k = 0; k < indices.size(); k++) {
            // measure the min, max, and average time of prewarm time and query time
            std::vector<double> prewarmTimes;
            std::vector<double> queryTimes;
            std::string error;
            for (int i = 0; i < repeat; i++) {
                size_t idx = indices[k];
                const std::string &query = allQueries[idx];
                size_t queryNum = idx + 1;

                if (purgeBetween) {
                    DoPurge();
                }

                duckdb::DuckDB db(dbPath);
                duckdb::Connection con(db);

                // Load cache_prewarm extension via C++ API (ExtensionLoader + CachePrewarmExtension::Load)
                duckdb::ExtensionLoader loader(duckdb::DatabaseInstance::GetDatabase(*con.context), "cache_prewarm");
                duckdb::CachePrewarmExtension cache_prewarm;
                cache_prewarm.Load(loader);

                if (mode != Mode::Baseline) {
                    std::string prewarmSql = duckdb_fmt::format("SELECT prewarm('hits', '{}')", ModeStr(mode));
                    auto start = std::chrono::steady_clock::now();
                    auto prewarmResult = con.Query(prewarmSql);
                    auto end = std::chrono::steady_clock::now();
                    prewarmTimes.push_back(std::chrono::duration<double, std::milli>(end - start).count());
                    if (prewarmResult->HasError()) {
                        error = duckdb_fmt::format("Prewarm failed: {}", prewarmResult->GetError());
                    }
                }

                auto start = std::chrono::steady_clock::now();
                auto result = con.Query(query);
                auto end = std::chrono::steady_clock::now();
                queryTimes.push_back(std::chrono::duration<double, std::milli>(end - start).count());
                if (result->HasError()) {
                    error = duckdb_fmt::format("Query {} error: {}", queryNum, result->GetError());
                }
            }
            if (!error.empty()) {
                std::cerr << error << "\n";
                return 1;
            }
            if (mode != Mode::Baseline) {
                double prewarmTimeMin = *std::min_element(prewarmTimes.begin(), prewarmTimes.end());
                double prewarmTimeMax = *std::max_element(prewarmTimes.begin(), prewarmTimes.end());
                double prewarmTimeAverage = std::accumulate(prewarmTimes.begin(), prewarmTimes.end(), 0.0) / static_cast<double>(prewarmTimes.size());
                std::cout << "Prewarm time: " << "min: " << prewarmTimeMin << " ms - max: " << prewarmTimeMax << " ms - average: " << prewarmTimeAverage << " ms\n";
            }
            double queryTimeMin = *std::min_element(queryTimes.begin(), queryTimes.end());
            double queryTimeMax = *std::max_element(queryTimes.begin(), queryTimes.end());
            double queryTimeAverage = std::accumulate(queryTimes.begin(), queryTimes.end(), 0.0) / static_cast<double>(queryTimes.size());
            std::cout << "Query time: " << "min: " << queryTimeMin << " ms - max: " << queryTimeMax << " ms - average: " << queryTimeAverage << " ms\n";
		}
	} catch (const std::exception &e) {
		std::cerr << "Error: " << e.what() << "\n";
		return 1;
	}

	return 0;
}

# DuckDB Cache Prewarm Benchmark

## ClickBench

See `bench/clickbench.cpp` for implementation details.

```bash
EXT_FLAGS="-DBUILD_BENCHMARK=1" make

cd bench

# Download the data
wget --continue --progress=dot:giga 'https://datasets.clickhouse.com/hits_compatible/hits.parquet' -O hits.parquet

# Create the database
../build/release/duckdb clickbench.db -c "CREATE TABLE hits AS SELECT * FROM read_parquet('hits.parquet');"

# Run the benchmark with 3 repeats of query 1st sql in queries.sql with buffer mode
../build/release/bench/clickbench -d clickbench.db -q queries.sql -r 3 -m buffer -i 1

# or, run all queries with all modes, each query is run 3 times
./run_all.sh
```

Output:

```bash
Running 1 queries with mode: buffer

Prewarm time: min: 19163.1 ms - max: 24579.2 ms - average: 21706.7 ms
Query time: min: 5.38162 ms - max: 34.369 ms - average: 15.9057 ms
```

## Parquet Metadata Prewarm

See `bench/parquet_metadata.cpp` for the runner and `bench/run_parquet_metadata.sh` for the end-to-end workflow.

```bash
EXT_FLAGS="-DBUILD_BENCHMARK=1" make

cd bench

# The helper script will generate a synthetic parquet file if it is missing.
./run_parquet_metadata.sh | tee parquet_metadata_bench.log
```

Defaults:

- Rows: `5,000,000`
- Row group size: `1,000`
- Repeats: `3`
- Modes: `baseline`, `metadata`

The query file is `bench/parquet_metadata_queries.sql`. It uses a `__PARQUET_PATH__` placeholder so the runner can benchmark any parquet file:

```bash
../build/release/bench/parquet_metadata_bench \
  -p parquet_metadata_bench.parquet \
  -q parquet_metadata_queries.sql \
  -r 3 \
  -m metadata \
  -i 1-5
```

Useful overrides for the helper script:

```bash
PARQUET_METADATA_BENCH_ROWS=2000000 \
PARQUET_METADATA_BENCH_ROW_GROUP_SIZE=500 \
PARQUET_METADATA_BENCH_REPEATS=5 \
./run_parquet_metadata.sh
```

## Check Hardware

### Linux

```bash
echo "Memory: $(free -g | awk '/^Mem:/ {print $2 " GB"}')"
echo "CPU cores: $(nproc)"
echo "CPU: $(lscpu | grep "Model name" | cut -d: -f2 | xargs)"
```

### macOS

```bash
echo "Memory: $(sysctl -n hw.memsize | awk '{printf "%.2f GB\n", $1 / (1024*1024*1024)}')"
echo "CPU cores: $(sysctl -n hw.logicalcpu)"
echo "CPU: $(sysctl -n machdep.cpu.brand_string)"
```

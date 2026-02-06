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

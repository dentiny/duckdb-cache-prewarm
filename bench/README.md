This benchmark only works on Linux.

1. at project root, run
```bash
EXT_FLAGS="-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache" GEN=ninja  make -j $(nproc)
```
2. `cd bench` at bench directory,
     -  run `./benchmark.sh <mode> [query_indices]`
     - mode can be `baseline`, `buffer`, `read`, `prefetch`
     - query_indices (optional):
       - `all`: Run all queries (default)
       - `5`: Run only query 5
       - `1-10`: Run queries 1 through 10
       - `1,3,5`: Run queries 1, 3, and 5
       - `1-5,10,15-20`: Run queries 1-5, 10, and 15-20

## Examples

```bash
# Run all queries with baseline (no prewarm)
./benchmark.sh baseline

# Run all queries with buffer mode
./benchmark.sh buffer

# Run only query 5 with buffer mode
./benchmark.sh buffer 5

# Run queries 1-10 with read mode
./benchmark.sh read 1-10

# Run specific queries (1, 3, 5) with prefetch mode
./benchmark.sh prefetch 1,3,5

# Run queries 1-5, 10, and 15-20 with buffer mode
./benchmark.sh buffer 1-5,10,15-20

# Use run.sh directly for more control
./run.sh buffer 1-5
```

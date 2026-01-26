This benchmark only works on Linux.

1. at project root, run
```bash
EXT_FLAGS="-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache" GEN=ninja  make -j $(nproc)
```
2. `cd bench` at bench directory,
     -  run `./benchmark.sh <mode>`
     - mode can be `baseline`, `buffer`, `read`, `prefetch`

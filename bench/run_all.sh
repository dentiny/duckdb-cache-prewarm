#!/bin/bash
# Run compare_modes.sh for each query 1-43

cd "$(dirname "$0")"

# clickbench release build is the pre-requisite
if [ ! -f "../build/release/bench/clickbench" ]; then
    echo "Error: clickbench is not built. Please see the bench/README.md for instructions."
    exit 1
fi

for i in $(seq 1 43); do
    echo "========== Query $i =========="
    ../build/release/bench/clickbench -d clickbench.db -q queries.sql -r 3 -m baseline -i "$i"
    ../build/release/bench/clickbench -d clickbench.db -q queries.sql -r 3 -m buffer -i "$i"
    ../build/release/bench/clickbench -d clickbench.db -q queries.sql -r 3 -m read -i "$i"
    ../build/release/bench/clickbench -d clickbench.db -q queries.sql -r 3 -m prefetch -i "$i"
done

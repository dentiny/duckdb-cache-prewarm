#!/bin/bash

# Accept mode parameter (baseline, buffer, read, prefetch)
MODE=${1:-baseline}

# Validate mode
case "$MODE" in
    baseline|buffer|read|prefetch)
        ;;
    *)
        echo "Usage: $0 [baseline|buffer|read|prefetch]"
        echo "  baseline: No prewarm (default)"
        echo "  buffer: Prewarm with 'buffer' mode"
        echo "  read: Prewarm with 'read' mode"
        echo "  prefetch: Prewarm with 'prefetch' mode"
        exit 1
        ;;
esac

echo "Running benchmark with mode: $MODE"

# Load the data
wget --continue --progress=dot:giga 'https://datasets.clickhouse.com/hits_compatible/hits.parquet' -O hits.parquet
# or use the smaller dataset https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet

echo -n "Load time: "
# Use gtime on macOS (brew install gnu-time), time on Linux
if command -v gtime &> /dev/null; then
    gtime -f '%e' ../build/release/duckdb hits.db -storage_version latest -f create.sql -f load.sql
else
    command time -f '%e' ../build/release/duckdb hits.db -storage_version latest -f create.sql -f load.sql
fi

# Run the queries with the unified script
echo "Executing: ./run.sh $MODE"
./run.sh "$MODE" 2>&1 | tee log.txt

echo -n "Data size: "
wc -c hits.db

cat log.txt |
  grep -E '^[0-9]|Killed|Segmentation|^Run Time \(s\): real' |
  sed -E -e 's/^.*(Killed|Segmentation).*$/null\nnull\nnull/; s/^Run Time \(s\): real[[:space:]]*([0-9.]+).*$/\1/' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'

# remove db file
rm hits.db

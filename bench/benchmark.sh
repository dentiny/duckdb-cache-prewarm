#!/bin/bash

# Accept mode parameter (baseline, buffer, read, prefetch)
MODE=${1:-baseline}
# Accept optional query indices (e.g., "1" or "1-5" or "1,3,5")
QUERY_INDICES=${2:-all}

# Validate mode
case "$MODE" in
    baseline|buffer|read|prefetch)
        ;;
    *)
        echo "Usage: $0 [baseline|buffer|read|prefetch] [query_indices]"
        echo "  baseline: No prewarm (default)"
        echo "  buffer: Prewarm with 'buffer' mode"
        echo "  read: Prewarm with 'read' mode"
        echo "  prefetch: Prewarm with 'prefetch' mode"
        echo ""
        echo "Query indices examples:"
        echo "  all: Run all queries (default)"
        echo "  5: Run only query 5"
        echo "  1-10: Run queries 1 through 10"
        echo "  1,3,5: Run queries 1, 3, and 5"
        echo "  1-5,10,15-20: Run queries 1-5, 10, and 15-20"
        exit 1
        ;;
esac

echo "Running benchmark with mode: $MODE, queries: $QUERY_INDICES"

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
echo "Executing: ./run.sh $MODE $QUERY_INDICES"
./run.sh "$MODE" "$QUERY_INDICES" 2>&1 | tee log.txt

echo -n "Data size: "
wc -c hits.db

cat log.txt |
  grep -E '^[0-9]|Killed|Segmentation|^Run Time \(s\): real' |
  sed -E -e 's/^.*(Killed|Segmentation).*$/null\nnull\nnull/; s/^Run Time \(s\): real[[:space:]]*([0-9.]+).*$/\1/' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'

# remove db file
rm hits.db

#!/bin/bash

# Load the data
wget --continue --progress=dot:giga 'https://datasets.clickhouse.com/hits_compatible/hits.parquet' -o hits.parquet
# or use the smaller dataset https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_1.parquet

echo -n "Load time: "
# Use gtime on macOS (brew install gnu-time), time on Linux
if command -v gtime &> /dev/null; then
    gtime -f '%e' ../build/release/duckdb hits.db -storage_version latest -f create.sql -f load.sql
else
    command time -f '%e' ../build/release/duckdb hits.db -storage_version latest -f create.sql -f load.sql
fi

# Run the queries

./run.sh 2>&1 | tee log.txt

echo -n "Data size: "
wc -c hits.db

cat log.txt |
  grep -E '^[0-9]|Killed|Segmentation|^Run Time \(s\): real' |
  sed -E -e 's/^.*(Killed|Segmentation).*$/null\nnull\nnull/; s/^Run Time \(s\): real[[:space:]]*([0-9.]+).*$/\1/' |
  awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'

# remove db file
rm hits.db

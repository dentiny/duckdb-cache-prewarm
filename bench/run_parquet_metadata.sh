#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

BENCH_BIN="../build/release/bench/parquet_metadata_bench"
DUCKDB_BIN="../build/release/duckdb"
PARQUET_FILE="${PARQUET_METADATA_BENCH_FILE:-parquet_metadata_bench.parquet}"
QUERY_FILE="${PARQUET_METADATA_QUERY_FILE:-parquet_metadata_queries.sql}"
ROWS="${PARQUET_METADATA_BENCH_ROWS:-5000000}"
ROW_GROUP_SIZE="${PARQUET_METADATA_BENCH_ROW_GROUP_SIZE:-1000}"
REPEATS="${PARQUET_METADATA_BENCH_REPEATS:-3}"
DB_PATH="${PARQUET_METADATA_BENCH_DB:-}"

if [ ! -f "${BENCH_BIN}" ]; then
	echo "Error: parquet_metadata_bench is not built. Please see bench/README.md for instructions."
	exit 1
fi

if [ ! -f "${DUCKDB_BIN}" ]; then
	echo "Error: duckdb CLI is not built."
	exit 1
fi

if [ ! -f "${PARQUET_FILE}" ]; then
	echo "Creating ${PARQUET_FILE} with ${ROWS} rows and row_group_size=${ROW_GROUP_SIZE}..."
	if [ -n "${DB_PATH}" ]; then
		"${DUCKDB_BIN}" "${DB_PATH}" -c "
	LOAD parquet;
	SET threads=4;
	COPY (
		SELECT
			i AS row_id,
			CAST(i / ${ROW_GROUP_SIZE} AS BIGINT) AS rg_id,
			CAST(i % 10 AS INTEGER) AS metric_a,
			CAST(i % 100 AS INTEGER) AS metric_b,
			CAST(i % 1000 AS INTEGER) AS metric_c,
			CAST(i % 10000 AS INTEGER) AS metric_d,
			CAST(i % 100000 AS INTEGER) AS metric_e,
			DATE '2024-01-01' + CAST(i / ${ROW_GROUP_SIZE} AS INTEGER) AS event_date,
			CAST((i * 17) % 1000000 AS BIGINT) AS hash_a,
			CAST((i * 31) % 1000000 AS BIGINT) AS hash_b
		FROM range(${ROWS}) tbl(i)
	) TO '${PARQUET_FILE}' (
		FORMAT parquet,
		COMPRESSION zstd,
		ROW_GROUP_SIZE ${ROW_GROUP_SIZE}
	);
	"
	else
		"${DUCKDB_BIN}" -c "
	LOAD parquet;
	SET threads=4;
	COPY (
		SELECT
			i AS row_id,
			CAST(i / ${ROW_GROUP_SIZE} AS BIGINT) AS rg_id,
			CAST(i % 10 AS INTEGER) AS metric_a,
			CAST(i % 100 AS INTEGER) AS metric_b,
			CAST(i % 1000 AS INTEGER) AS metric_c,
			CAST(i % 10000 AS INTEGER) AS metric_d,
			CAST(i % 100000 AS INTEGER) AS metric_e,
			DATE '2024-01-01' + CAST(i / ${ROW_GROUP_SIZE} AS INTEGER) AS event_date,
			CAST((i * 17) % 1000000 AS BIGINT) AS hash_a,
			CAST((i * 31) % 1000000 AS BIGINT) AS hash_b
		FROM range(${ROWS}) tbl(i)
	) TO '${PARQUET_FILE}' (
		FORMAT parquet,
		COMPRESSION zstd,
		ROW_GROUP_SIZE ${ROW_GROUP_SIZE}
	);
	"
	fi
fi

QUERY_COUNT=$(grep -Evc '^[[:space:]]*$|^[[:space:]]*#' "${QUERY_FILE}")

for i in $(seq 1 "${QUERY_COUNT}"); do
	echo "========== Query ${i} =========="
	if [ -n "${DB_PATH}" ]; then
		"${BENCH_BIN}" -p "${PARQUET_FILE}" -d "${DB_PATH}" -q "${QUERY_FILE}" -r "${REPEATS}" -m baseline -i "${i}"
		"${BENCH_BIN}" -p "${PARQUET_FILE}" -d "${DB_PATH}" -q "${QUERY_FILE}" -r "${REPEATS}" -m metadata -i "${i}"
	else
		"${BENCH_BIN}" -p "${PARQUET_FILE}" -q "${QUERY_FILE}" -r "${REPEATS}" -m baseline -i "${i}"
		"${BENCH_BIN}" -p "${PARQUET_FILE}" -q "${QUERY_FILE}" -r "${REPEATS}" -m metadata -i "${i}"
	fi
done

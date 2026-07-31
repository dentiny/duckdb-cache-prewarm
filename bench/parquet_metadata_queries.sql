# Metadata-only parquet introspection
SELECT COUNT(*) FROM parquet_file_metadata(__PARQUET_PATH__);
SELECT COUNT(*) FROM parquet_metadata(__PARQUET_PATH__);

# Selective scans that should benefit from cold footer/index reads being prewarmed
SELECT SUM(metric_a) FROM read_parquet(__PARQUET_PATH__) WHERE rg_id = 1234;
SELECT SUM(metric_b) FROM read_parquet(__PARQUET_PATH__) WHERE rg_id BETWEEN 1234 AND 1236;
SELECT SUM(metric_c) FROM read_parquet(__PARQUET_PATH__) WHERE row_id BETWEEN 1234000 AND 1234999;

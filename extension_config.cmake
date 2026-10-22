# This file is included by DuckDB's build system. It specifies which extension
# to load

# Extension from this repo
duckdb_extension_load(cache_prewarm SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
                      EXTENSION_VERSION 0.3.0 LOAD_TESTS)
duckdb_extension_statically_link(cache_prewarm)

# Any extra extensions that should be built e.g.: duckdb_extension_load(json)

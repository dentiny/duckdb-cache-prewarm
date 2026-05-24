---
name: upgrade-duckdb-extension
description: Upgrade the cache_prewarm DuckDB extension to a new DuckDB release. Use when the user asks to upgrade DuckDB, bump the duckdb submodule, sync to a new DuckDB tag, or update the duckdb / duck-read-cache-fs / extension-ci-tools submodules and CI pins together.
---

# Upgrade cache_prewarm to a new DuckDB release

The root extension builds against DuckDB internals and links cache_httpfs sources from `duck-read-cache-fs`, so the DuckDB, cache filesystem, extension-ci-tools, and distribution workflow pins normally move together. Then build, run the extension tests, and note any DuckDB C++ API adjustments needed for this extension.

## Inputs

Before starting, confirm the target DuckDB version (for example, `v1.5.3`). Everything else is derived from it.

## Workflow

Track these as a checklist; do not skip ahead:

- 1. Pin `duckdb` submodule to `tags/$TARGET`
- 2. Pin `extension-ci-tools` submodule to `origin/$TARGET` or the matching release tag
- 3. Pin `duck-read-cache-fs` submodule to its matching "Upgrade DuckDB $TARGET" commit, then update its recursive submodules
- 4. Update `.github/workflows/MainDistributionPipeline.yml` `uses`, `duckdb_version`, and `ci_tools_version` pins to `$TARGET`
- 5. Reconcile `CMakeLists.txt` with any source list changes from `duck-read-cache-fs` or `duckdb-httpfs`
- 6. Build: `EXT_FLAGS="-DCMAKE_C_COMPILER_LAUNCHER=ccache -DCMAKE_CXX_COMPILER_LAUNCHER=ccache" GEN=ninja make -j 30`
- 7. Run checks:
  + Format: `make format-all`
  + SQL test: `./build/release/test/unittest "test/sql/cache_prewarm.test"`
  + Full release tests when practical: `make test`

## Reference: historical upgrade commits

- `1047d2a` - `Update duckdb to 1.5.2 (#64)`.
- `cb52c2f` - `Bump duck-read-cache-fs, duckdb, and extension-ci-tools to 1.5.1 (#62)`.
- `daedc41` - `Update DuckDB to v1.5 (#49)`.

# 0.2.0

For more details, please refer to the [release note](https://github.com/dentiny/duckdb-cache-prewarm/releases/tag/v0.2.0).

## Improved

- Update DuckDB to v1.5 ([#49](https://github.com/dentiny/duckdb-cache-prewarm/pull/49))

# 0.1.0

For more details, please refer to the [release note](https://github.com/dentiny/duckdb-cache-prewarm/releases/tag/v0.1.0).

## Fixed

- Fix compile error in `buffer_prewarm_strategy.cpp` ([#36](https://github.com/dentiny/duckdb-cache-prewarm/pull/36))

## Improved

- Upgrade DuckDB to v1.4.4 ([#19](https://github.com/dentiny/duckdb-cache-prewarm/pull/19))
- Use buffer pool manager prefetch for buffer prewarm mode and add direct IO check ([#8](https://github.com/dentiny/duckdb-cache-prewarm/pull/8))
- Utilize task-based parallelism for block prewarm and add max blocks limit for `read` and `prefetch` mode ([#28](https://github.com/dentiny/duckdb-cache-prewarm/pull/28))
- Return actual loaded block count from prewarm ([#21](https://github.com/dentiny/duckdb-cache-prewarm/pull/21))
- Support size-based prewarm limit ([#54](https://github.com/dentiny/duckdb-cache-prewarm/pull/54))
- Remove unnecessary OpenSSL dependency ([#38](https://github.com/dentiny/duckdb-cache-prewarm/pull/38))

## Added

- Support cache prewarm with schema + table name and three prewarm modes (`buffer`, `read`, `prefetch`) ([#1](https://github.com/dentiny/duckdb-cache-prewarm/pull/1))
- Implement OS-level page cache prewarm ([#17](https://github.com/dentiny/duckdb-cache-prewarm/pull/17))
- Add `LocalPrewarmStrategy` to separate local prewarm logic ([#41](https://github.com/dentiny/duckdb-cache-prewarm/pull/41))
- Add remote prewarm support with cache-fs API ([#43](https://github.com/dentiny/duckdb-cache-prewarm/pull/43), [#52](https://github.com/dentiny/duckdb-cache-prewarm/pull/52))
- Integrate `duck-read-cache-fs` as submodule for remote caching ([#46](https://github.com/dentiny/duckdb-cache-prewarm/pull/46))
- Add ClickBench benchmarks ([#26](https://github.com/dentiny/duckdb-cache-prewarm/pull/26))
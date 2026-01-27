# CachePrewarm

This repository is based on https://github.com/duckdb/extension-template, check it out if you want to build and ship your own DuckDB extension.

---

This extension, CachePrewarm, allow you to prewarm the cache of a table by loading the data blocks into the buffer pool.

Now we can use the features from the extension directly in DuckDB. The template contains a single scalar function `cache_prewarm()` that takes a string arguments and returns a string:
```sql
CREATE TABLE events (
    event_id BIGINT,
    user_id INTEGER,
    session_id VARCHAR,
    event_type VARCHAR,
    event_data VARCHAR,
    timestamp TIMESTAMP,
    value DOUBLE
);

INSERT INTO events
SELECT
    i AS event_id,
    (random() * 10000)::INTEGER AS user_id,
    'session_' || (random() * 5000)::INTEGER AS session_id,
    (ARRAY['click', 'view', 'purchase', 'signup', 'logout'])[1 + (random() * 4)::INTEGER] AS event_type,
    'data_' || (random() * 1000)::INTEGER AS event_data,
    '2024-01-01 00:00:00'::TIMESTAMP + INTERVAL (i) SECOND AS timestamp,
    random() * 1000 AS value
FROM range(500000) t(i);

-- Use Checkpoint to ensure the blocks are written to disk or restart the database
CHECKPOINT;
┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ 0 rows  │
└─────────┘

-- Prewarm the duckdb cache for the events table
-- the result is the number of blocks prewarmed, which may vary depending on how the compression is applied to the table
-- NOTICE that, for some reasons, the result is mostly accurate and only for reference
SELECT prewarm('events'); -- or prewarm('events', 'buffer')
┌─────────────────────┐
│ prewarm('events')   │
│        int64        │
├─────────────────────┤
│         17          │
└─────────────────────┘

-- Prewarm the os page cache for the events table using the read strategy
SELECT prewarm('events', 'read');
┌───────────────────────────┐
│ prewarm('events', 'read') │
│           int64           │
├───────────────────────────┤
│            19             │
└───────────────────────────┘
```

## TODO

- [ ] Support prewarm with block id range
- [ ] Support prewarm with index name
- [ ] Table, Index Inspector to see which blocks is belong to which table or index
- [ ] Prewarm remote tables and files and return numbers of bytes prewarmed instead of numbers of blocks cause remote stuffs don't go into buffer pool
  - [ ] Try to leverage https://duckdb.org/community_extensions/extensions/cache_httpfs
- [ ] Autoprewarm, just like what PostgreSQL pg_prewarm extension does

## References
- [pg_prewarm extension](https://www.postgresql.org/docs/current/pgprewarm.html)
- [pg_prewarm extension implementation](https://github.com/postgres/postgres/tree/master/contrib/pg_prewarm)

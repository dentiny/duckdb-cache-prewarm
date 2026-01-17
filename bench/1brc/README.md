## Ingest 1B Records into duckdb table

```bash
git clone https://github.com/gunnarmorling/1brc.git
cd 1brc

mvn install

./create_measurements.sh 1000000000

cd ..
```

```sql
./build/release/duckdb benchmark.db
D CREATE TABLE measurements (
    station_name VARCHAR,
    temperature DOUBLE
);
D COPY measurements FROM '1brc/measurements.txt' (DELIMITER ';', HEADER false);
```

## Run Query with Different Prewarm Mode

### No Prewarm

```sql
./build/release/duckdb benchmark.db <<'EOF'
.timer on
.echo on

SELECT '=== COLD RUN (No Prewarm) ===' as test;

-- Run 1
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;

-- Run 2
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;

-- Run 3
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
EOF
```

```
SELECT '=== COLD RUN (No Prewarm) ===' as test;
┌───────────────────────────────┐
│             test              │
│            varchar            │
├───────────────────────────────┤
│ === COLD RUN (No Prewarm) === │
└───────────────────────────────┘
Run Time (s): real 0.000 user 0.000242 sys 0.000160

-- Run 1
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
100% ▕██████████████████████████████████████▏ (00:00:02.05 elapsed)
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 2.051 user 10.244446 sys 1.216105

-- Run 2
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.391 user 10.679554 sys 0.483645

-- Run 3
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.450 user 10.688356 sys 0.103861
```

### Buffer Mode

#### Query

```sql
./build/release/duckdb benchmark.db <<'EOF'
.timer on
.echo on

SELECT '=== BUFFER MODE ===' as test;

-- Prewarm with BUFFER mode (pins blocks, keeps them in buffer pool longer)
SELECT prewarm('measurements', 'buffer') as blocks_prewarmed;

-- Run 1
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;

-- Run 2
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;

-- Run 3
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
EOF
```

#### Result

```sql
SELECT '=== BUFFER MODE ===' as test;
┌─────────────────────┐
│        test         │
│       varchar       │
├─────────────────────┤
│ === BUFFER MODE === │
└─────────────────────┘
Run Time (s): real 0.000 user 0.000200 sys 0.000102

-- Prewarm with BUFFER mode (pins blocks, keeps them in buffer pool longer)
SELECT prewarm('measurements', 'buffer') as blocks_prewarmed;
┌──────────────────┐
│ blocks_prewarmed │
│      int64       │
├──────────────────┤
│      13796       │
└──────────────────┘
Run Time (s): real 4.254 user 0.125156 sys 0.701479

-- Run 1
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.532 user 10.683462 sys 0.681542

-- Run 2
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.274 user 10.532579 sys 0.080675

-- Run 3
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.422 user 10.568563 sys 0.101426
```

### Prefetch Mode

#### Query

```sql
./build/release/duckdb benchmark.db <<'EOF'
.timer on
.echo on

SELECT '=== PREFETCH MODE ===' as test;

-- Prewarm with PREFETCH mode (batched reads, blocks may be evicted)
SELECT prewarm('measurements', 'prefetch') as blocks_prewarmed;

-- Run 1
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;

-- Run 2
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;

-- Run 3
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
EOF
```

#### Result

```sql
SELECT '=== PREFETCH MODE ===' as test;
┌───────────────────────┐
│         test          │
│        varchar        │
├───────────────────────┤
│ === PREFETCH MODE === │
└───────────────────────┘
Run Time (s): real 0.000 user 0.000195 sys 0.000146

-- Prewarm with PREFETCH mode (batched reads, blocks may be evicted)
SELECT prewarm('measurements', 'prefetch') as blocks_prewarmed;
┌──────────────────┐
│ blocks_prewarmed │
│      int64       │
├──────────────────┤
│      13796       │
└──────────────────┘
Run Time (s): real 1.579 user 0.131589 sys 0.589079

-- Run 1
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.290 user 10.897837 sys 0.603226

-- Run 2
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.191 user 10.811776 sys 0.047813

-- Run 3
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.260 user 10.764544 sys 0.068631
```

### Read Mode

#### Query

```sql
./build/release/duckdb benchmark.db <<'EOF'
.timer on
.echo on

SELECT '=== READ MODE ===' as test;

-- Prewarm with READ mode (reads to temp memory, then frees - warms OS page cache only)
SELECT prewarm('measurements', 'read') as blocks_prewarmed;

-- Run 1
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;

-- Run 2
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;

-- Run 3
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
EOF
```

#### Result

```sql
SELECT '=== READ MODE ===' as test;
┌───────────────────┐
│       test        │
│      varchar      │
├───────────────────┤
│ === READ MODE === │
└───────────────────┘
Run Time (s): real 0.004 user 0.000169 sys 0.000839

-- Prewarm with READ mode (reads to temp memory, then frees - warms OS page cache only)
SELECT prewarm('measurements', 'read') as blocks_prewarmed;
┌──────────────────┐
│ blocks_prewarmed │
│      int64       │
├──────────────────┤
│      13796       │
└──────────────────┘
Run Time (s): real 1.349 user 0.130673 sys 0.415222

-- Run 1
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
100% ▕██████████████████████████████████████▏ (00:00:02.14 elapsed)
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 2.153 user 10.535754 sys 1.242278

-- Run 2
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.743 user 10.670558 sys 0.801851

-- Run 3
SELECT station_name, MIN(temperature), MAX(temperature), ROUND(AVG(temperature),1)
FROM measurements GROUP BY station_name ORDER BY station_name;
┌──────────────────┬──────────────────┬───┬──────────────────────┐
│   station_name   │ min(temperature) │ … │ round(avg(temperat…  │
│     varchar      │      double      │   │        double        │
├──────────────────┼──────────────────┼───┼──────────────────────┤
│ Abha             │            -30.2 │ … │                 18.0 │
│ Abidjan          │            -23.3 │ … │                 26.0 │
│ Abéché           │            -23.8 │ … │                 29.4 │
│ Accra            │            -25.3 │ … │                 26.4 │
│ Addis Ababa      │            -32.9 │ … │                 16.0 │
│ Adelaide         │            -37.4 │ … │                 17.3 │
│ Aden             │            -19.0 │ … │                 29.1 │
│ Ahvaz            │            -26.1 │ … │                 25.4 │
│ Albuquerque      │            -36.3 │ … │                 14.0 │
│ Alexandra        │            -44.9 │ … │                 11.0 │
│ Alexandria       │            -35.0 │ … │                 20.0 │
│ Algiers          │            -35.5 │ … │                 18.2 │
│ Alice Springs    │            -31.0 │ … │                 21.0 │
│ Almaty           │            -39.0 │ … │                 10.0 │
│ Amsterdam        │            -46.0 │ … │                 10.2 │
│ Anadyr           │            -54.5 │ … │                 -6.9 │
│ Anchorage        │            -46.8 │ … │                  2.8 │
│ Andorra la Vella │            -38.9 │ … │                  9.8 │
│ Ankara           │            -42.2 │ … │                 12.0 │
│ Antananarivo     │            -32.5 │ … │                 17.9 │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│      ·           │              ·   │ · │                   ·  │
│ Washington, D.C. │            -38.1 │ … │                 14.6 │
│ Wau              │            -23.4 │ … │                 27.8 │
│ Wellington       │            -36.5 │ … │                 12.9 │
│ Whitehorse       │            -52.3 │ … │                 -0.1 │
│ Wichita          │            -34.6 │ … │                 13.9 │
│ Willemstad       │            -18.2 │ … │                 28.0 │
│ Winnipeg         │            -49.1 │ … │                  3.0 │
│ Wrocław          │            -39.1 │ … │                  9.6 │
│ Xi'an            │            -34.9 │ … │                 14.1 │
│ Yakutsk          │            -59.6 │ … │                 -8.8 │
│ Yangon           │            -22.1 │ … │                 27.5 │
│ Yaoundé          │            -26.4 │ … │                 23.8 │
│ Yellowknife      │            -53.5 │ … │                 -4.3 │
│ Yerevan          │            -36.4 │ … │                 12.4 │
│ Yinchuan         │            -38.7 │ … │                  9.0 │
│ Zagreb           │            -40.6 │ … │                 10.7 │
│ Zanzibar City    │            -23.4 │ … │                 26.0 │
│ Zürich           │            -43.9 │ … │                  9.3 │
│ Ürümqi           │            -41.6 │ … │                  7.4 │
│ İzmir            │            -32.3 │ … │                 17.9 │
├──────────────────┴──────────────────┴───┴──────────────────────┤
│ 413 rows (40 shown)                        4 columns (3 shown) │
└────────────────────────────────────────────────────────────────┘
Run Time (s): real 1.269 user 10.529249 sys 0.069973
```

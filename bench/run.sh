#!/bin/bash

# Accept mode parameter (baseline, buffer, read, prefetch)
MODE=${1:-baseline}
TRIES=3

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

cat queries.sql | while read -r query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    echo "$query";
    cli_params=()

    # Add prewarm command if not baseline
    if [ "$MODE" != "baseline" ]; then
        cli_params+=("-c")
        cli_params+=("SELECT prewarm('hits', '${MODE}')")
    fi

    cli_params+=("-c")
    cli_params+=(".timer on")
    for i in $(seq 1 $TRIES); do
      cli_params+=("-c")
      cli_params+=("${query}")
    done;
    echo "${cli_params[@]}"
    ../build/release/duckdb hits.db "${cli_params[@]}"
done;

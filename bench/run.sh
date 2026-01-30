#!/bin/bash

# Accept mode parameter (baseline, buffer, read, prefetch)
MODE=${1:-baseline}
# Accept optional query indices (e.g., "1" or "1-5" or "1,3,5")
QUERY_INDICES=${2:-all}
TRIES=3

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

# Parse query indices and create a list of queries to run
declare -a QUERY_LIST
if [ "$QUERY_INDICES" = "all" ]; then
    # Read all queries
    mapfile -t QUERY_LIST < queries.sql
else
    # Read all queries into array
    mapfile -t ALL_QUERIES < queries.sql
    TOTAL_QUERIES=${#ALL_QUERIES[@]}

    # Parse the query indices specification
    IFS=',' read -ra RANGES <<< "$QUERY_INDICES"
    declare -a SELECTED_INDICES

    for range in "${RANGES[@]}"; do
        if [[ "$range" =~ ^([0-9]+)-([0-9]+)$ ]]; then
            # Range specification (e.g., 1-5)
            start=${BASH_REMATCH[1]}
            end=${BASH_REMATCH[2]}
            for ((i=start; i<=end; i++)); do
                SELECTED_INDICES+=($i)
            done
        elif [[ "$range" =~ ^[0-9]+$ ]]; then
            # Single query index
            SELECTED_INDICES+=($range)
        else
            echo "Error: Invalid query index format: $range"
            exit 1
        fi
    done

    # Build query list from selected indices
    for idx in "${SELECTED_INDICES[@]}"; do
        if [ $idx -lt 1 ] || [ $idx -gt $TOTAL_QUERIES ]; then
            echo "Error: Query index $idx out of range (1-$TOTAL_QUERIES)"
            exit 1
        fi
        # Array is 0-indexed, query numbers are 1-indexed
        QUERY_LIST+=("${ALL_QUERIES[$((idx-1))]}")
    done
fi

echo "Running ${#QUERY_LIST[@]} queries with mode: $MODE"
echo ""

# Iterate through selected queries
query_num=0
for query in "${QUERY_LIST[@]}"; do
    query_num=$((query_num + 1))
    sync
    # Clear OS cache based on platform
    if [[ "$(uname -s)" == "Darwin" ]]; then
        sudo purge
    else
        echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null
    fi

    echo "Query $query_num: $query";
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

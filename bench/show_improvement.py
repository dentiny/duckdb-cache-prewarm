#!/usr/bin/env python3
"""
Script to extract benchmark results and show improvement numbers in text format.

Example output:
Query 1: baseline 55.55 ms, buffer mode is 62.96x faster, metadata mode is 2.34x faster
Query 2: ...
"""

import re
import sys
import os


def parse_benchmark_data(file_path):
    """Parses benchmark text, returns dict of {query_num: {mode: avg_time}}."""
    with open(file_path, 'r') as f:
        text = f.read()

    data = {}

    # Split by Query sections
    query_sections = re.split(r'={10} Query (\d+) ={10}', text)

    for i in range(1, len(query_sections), 2):
        query_num = int(query_sections[i])
        content = query_sections[i + 1]

        # Split by Mode
        mode_sections = re.split(r'Running 1 queries with mode: (\w+)', content)

        for j in range(1, len(mode_sections), 2):
            mode_name = mode_sections[j]
            stats_text = mode_sections[j + 1]

            # Extract Query Time
            q_match = re.search(
                r'Query time: min: ([\d.]+) ms - max: ([\d.]+) ms - average: ([\d.]+) ms',
                stats_text
            )

            if q_match:
                avg_time = float(q_match.group(3))
                if query_num not in data:
                    data[query_num] = {}
                data[query_num][mode_name] = avg_time

    return data


def format_speedup(speedup):
    """Format speedup value nicely."""
    if speedup >= 10:
        return f"{speedup:.1f}x"
    elif speedup >= 1:
        return f"{speedup:.2f}x"
    else:
        # Slower than baseline - show how much slower
        slowdown = 1 / speedup
        return f"{slowdown:.2f}x slower"


def format_time(ms):
    """Format time in appropriate units."""
    if ms >= 1000:
        return f"{ms / 1000:.2f} s"
    else:
        return f"{ms:.2f} ms"


def show_improvements(data):
    """Print improvement numbers for each query."""
    preferred_order = ['buffer', 'read', 'prefetch', 'metadata']

    for query_num in sorted(data.keys()):
        query_data = data[query_num]

        if 'baseline' not in query_data:
            print(f"Query {query_num}: (skipped - no baseline data)")
            continue

        baseline_time = query_data['baseline']
        parts = [f"Query {query_num}: baseline {format_time(baseline_time)}"]

        other_modes = [mode for mode in query_data.keys() if mode != 'baseline']
        ordered_modes = [mode for mode in preferred_order if mode in other_modes]
        ordered_modes.extend(sorted(mode for mode in other_modes if mode not in preferred_order))

        for mode in ordered_modes:
            mode_time = query_data[mode]
            if mode_time > 0:
                speedup = baseline_time / mode_time
                if speedup >= 1:
                    parts.append(f"{mode} mode is {format_speedup(speedup)} faster")
                else:
                    parts.append(f"{mode} mode is {format_speedup(speedup)}")
            else:
                parts.append(f"{mode} mode: N/A")

        print(", ".join(parts))


def main():
    input_file = 'bench.log'
    if len(sys.argv) > 1:
        input_file = sys.argv[1]

    # If relative path, try looking in the same directory as the script
    if not os.path.exists(input_file):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        alt_path = os.path.join(script_dir, input_file)
        if os.path.exists(alt_path):
            input_file = alt_path

    if os.path.exists(input_file):
        print(f"Processing {input_file}...\n")
        data = parse_benchmark_data(input_file)
        if data:
            show_improvements(data)
            print(f"\nTotal queries with data: {len(data)}")
        else:
            print("No valid data found.")
    else:
        print(f"File '{input_file}' not found.")
        print("Usage: python show_improvement.py [benchmark_log_file]")
        sys.exit(1)


if __name__ == "__main__":
    main()

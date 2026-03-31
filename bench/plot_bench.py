import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
import os

def parse_benchmark_data(file_path):
    """Parses benchmark text, handling errors gracefully."""
    with open(file_path, 'r') as f:
        text = f.read()

    data = []

    # Split by Query sections
    query_sections = re.split(r'={10} Query (\d+) ={10}', text)

    for i in range(1, len(query_sections), 2):
        query_num = int(query_sections[i])
        content = query_sections[i+1]

        # Split by Mode
        mode_sections = re.split(r'Running 1 queries with mode: (\w+)', content)

        for j in range(1, len(mode_sections), 2):
            mode_name = mode_sections[j]
            stats_text = mode_sections[j+1]

            entry = {
                'Query': f"Q{query_num}", # Short label for X-axis
                'Query_Num': query_num,
                'Mode': mode_name
            }

            # --- Extract Query Time ---
            # We use a try-except/check approach because Query 19 has an error message
            # instead of statistics.
            q_match = re.search(r'Query time: min: ([\d.]+) ms - max: ([\d.]+) ms - average: ([\d.]+) ms', stats_text)

            if q_match:
                entry['Query_Avg'] = float(q_match.group(3))
                entry['Query_Min'] = float(q_match.group(1))
                entry['Query_Max'] = float(q_match.group(2))
            else:
                # If no stats found (e.g., Query 19 Error), skip adding this specific mode entry
                # or mark it as NaN. Here we skip to keep the chart clean.
                continue

            # --- Extract Prewarm Time ---
            # Baseline mode has no prewarm; use NaN (excluded from prewarm plot anyway)
            p_match = re.search(r'Prewarm time: min: ([\d.]+) ms - max: ([\d.]+) ms - average: ([\d.]+) ms', stats_text)
            if p_match:
                entry['Prewarm_Avg'] = float(p_match.group(3))
                entry['Prewarm_Min'] = float(p_match.group(1))
                entry['Prewarm_Max'] = float(p_match.group(2))
            elif mode_name.lower() == 'baseline':
                entry['Prewarm_Avg'] = np.nan
                entry['Prewarm_Min'] = np.nan
                entry['Prewarm_Max'] = np.nan
            else:
                raise ValueError(f"Prewarm time not found for query {query_num} and mode {mode_name}")

            data.append(entry)

    return pd.DataFrame(data)

def plot_benchmark(df, output_prefix='benchmark'):
    """Generates two separate figures: performance improvement and prewarm duration."""
    # Sort by Query Number, then Mode
    df = df.sort_values(by=['Query_Num', 'Mode'])

    queries = df['Query'].unique()
    modes = df['Mode'].unique()

    # --- Dynamic Sizing ---
    width_per_query = 0.6  # inches
    fig_width = max(12, len(queries) * width_per_query)
    fig_height = 7

    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']  # Blue, Orange, Green, Red
    x = np.arange(len(queries))
    bar_width = 0.8 / len(modes)

    # --- Figure 1: Performance improvement (Query execution time) ---
    fig1, ax1 = plt.subplots(figsize=(fig_width, fig_height))
    for i, mode in enumerate(modes):
        mode_data = df[df['Mode'] == mode].set_index('Query').reindex(queries).reset_index()
        q_means = mode_data['Query_Avg'].fillna(0)
        q_mins = mode_data['Query_Min'].fillna(0)
        q_maxs = mode_data['Query_Max'].fillna(0)
        q_err = [
            np.maximum(0, q_means - q_mins),
            np.maximum(0, q_maxs - q_means)
        ]
        offset = (i - len(modes) / 2 + 0.5) * bar_width
        ax1.bar(x + offset, q_means, bar_width, label=mode,
                yerr=q_err, capsize=3, color=colors[i % len(colors)], alpha=0.9)

    ax1.set_ylabel('Execution Time (ms)', fontsize=12, fontweight='bold')
    ax1.set_title(f'Query Performance by Mode (n={len(queries)})', fontsize=14, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(queries, rotation=0, fontsize=10)
    ax1.grid(True, axis='y', linestyle='--', alpha=0.5)
    ax1.legend()
    plt.tight_layout()
    perf_file = f'{output_prefix}_performance.png'
    plt.savefig(perf_file)
    plt.close()
    print(f"Performance chart saved to {perf_file}")

    # --- Figure 2: Prewarm duration ---
    prewarm_modes = [m for m in modes if m.lower() != 'baseline']
    if not prewarm_modes:
        return

    prewarm_bar_width = 0.8 / len(prewarm_modes)
    fig2, ax2 = plt.subplots(figsize=(fig_width, fig_height))
    for i, mode in enumerate(prewarm_modes):
        mode_data = df[df['Mode'] == mode].set_index('Query').reindex(queries).reset_index()
        p_means = mode_data['Prewarm_Avg'].fillna(0)
        p_mins = mode_data['Prewarm_Min'].fillna(0)
        p_maxs = mode_data['Prewarm_Max'].fillna(0)
        p_err = [
            np.maximum(0, p_means - p_mins),
            np.maximum(0, p_maxs - p_means)
        ]
        prewarm_offset = (i - len(prewarm_modes) / 2 + 0.5) * prewarm_bar_width
        color_idx = list(modes).index(mode)
        ax2.bar(x + prewarm_offset, p_means, prewarm_bar_width, label=mode,
                yerr=p_err, capsize=3, color=colors[color_idx % len(colors)], alpha=0.9)

    ax2.set_ylabel('Prewarm Time (ms)', fontsize=12, fontweight='bold')
    ax2.set_title(f'Prewarm Duration by Mode (n={len(queries)})', fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(queries, rotation=0, fontsize=10)
    ax2.grid(True, axis='y', linestyle='--', alpha=0.5)
    ax2.legend()
    plt.tight_layout()
    prewarm_file = f'{output_prefix}_prewarm.png'
    plt.savefig(prewarm_file)
    plt.close()
    print(f"Prewarm duration chart saved to {prewarm_file}")

if __name__ == "__main__":
    # 1. Create a file named 'data.txt' with your content
    # 2. Run: python plot_benchmark.py

    input_file = 'data.txt'
    if len(sys.argv) > 1:
        input_file = sys.argv[1]

    if os.path.exists(input_file):
        print(f"Processing {input_file}...")
        df = parse_benchmark_data(input_file)
        if not df.empty:
            plot_benchmark(df)
            print("Note: Query 19 was excluded due to SQL errors in the source text.")
        else:
            print("No valid data found.")
    else:
        print(f"File '{input_file}' not found. Please save your text to 'data.txt'.")

#!/usr/bin/env python3
"""
Plotting tool for the  benchmark results.

Reads: benchmark_results.csv
Outputs: fig/<phase>.png for each benchmarked phase

Each figure contains the 3 technologies (DuckDB, PostgreSQL, BerkeleyDB)
plotted on the same chart with log-scale on Y when needed.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt

CSV_FILE = "benchmark_results.csv"
OUTPUT_DIR = "fig"

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load CSV
df = pd.read_csv(CSV_FILE)

# Clean engine names for nicer plots

# Force engine column to string
df["engine"] = df["engine"].astype(str)

df["engine"] = df["engine"].str.replace("postgresql", "PostgreSQL") \
                           .str.replace("duckdb", "DuckDB") \
                           .str.replace("berkeleydb", "BerkeleyDB")

# List of unique phases
phases = sorted(df["phase"].unique())

# Colors per DB
COLORS = {
    "BerkeleyDB": "tab:green",
    "DuckDB": "tab:blue",
    "PostgreSQL": "tab:red",
}

print(f"Found phases: {phases}")

# Generate a figure per phase

for phase in phases:
    sub = df[df["phase"] == phase].sort_values("n_rows")

    plt.figure(figsize=(8, 5))
    plt.title(f"{phase} — Average Duration by Engine", fontsize=14)
    plt.xlabel("Number of Rows (N)", fontsize=12)
    plt.ylabel("Average Duration (s)", fontsize=12)

    engines = sub["engine"].unique()

    for eng in engines:
        part = sub[sub["engine"] == eng]
        plt.plot(
            part["n_rows"],
            part["avg_duration_s"],
            marker="o",
            linewidth=2,
            markersize=6,
            color=COLORS.get(eng, None),
            label=eng
        )

    # Use log scale if values vary a lot
    if sub["avg_duration_s"].max() / max(sub["avg_duration_s"].min(), 1e-6) > 50:
        plt.yscale("log")

    plt.grid(alpha=0.3)
    plt.legend()
    plt.tight_layout()

    out_path = os.path.join(OUTPUT_DIR, f"{phase}.png")
    plt.savefig(out_path, dpi=200)
    plt.close()

    print(f"Saved: {out_path}")

print("\nAll plots generated in ./fig/")

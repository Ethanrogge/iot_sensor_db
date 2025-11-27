#!/usr/bin/env python3
"""
plotv3.py — Plot benchmark results including
Solution 2: FULL_PROFILE_SQL for DuckDB & PostgreSQL,
and FULL_PROFILE (local stats) for BerkeleyDB.

Generates:
  - One graph per phase
  - Combined graph FULL_PROFILE comparing DuckDB, PostgreSQL, BDB
"""

import os
import pandas as pd
import matplotlib.pyplot as plt

INPUT_CSV = "app_benchmark_results.csv"
OUTPUT_DIR = "plots_v3"

PHASES_SQL = [
    "AVG_SENSOR",
    "DAILY_SUMMARY",
    "OUTAGES",
    "TOPK_SENSORS",
]

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def load_data():
    df = pd.read_csv(INPUT_CSV)
    return df


def plot_phase(df, phase, ylabel="Time (s)"):
    """
    Plot execution time per engine for a given phase.
    """
    sub = df[df["phase"] == phase]
    if sub.empty:
        return

    plt.figure(figsize=(10, 6))
    for engine in sub["engine"].unique():
        d = sub[sub["engine"] == engine]
        plt.plot(
            d["n_rows"],
            d["avg_duration_s"],
            marker="o",
            label=engine,
        )

    plt.title(f"{phase} — performance comparison")
    plt.xlabel("N rows")
    plt.ylabel(ylabel)
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()

    fname = os.path.join(OUTPUT_DIR, f"{phase.lower()}.png")
    plt.savefig(fname, bbox_inches="tight")
    plt.close()
    print(f"[OK] Saved {fname}")


def compute_full_profile_sql(df):
    """
    For DuckDB & PostgreSQL:
      FULL_PROFILE_SQL = sum of phase timings:
          AVG_SENSOR + DAILY_SUMMARY + OUTAGES + TOPK_SENSORS

    For BerkeleyDB:
      FULL_PROFILE = existing FULL_PROFILE time (local stats)

    Returns a new dataframe with:
      engine, n_rows, phase='FULL_PROFILE', avg_duration_s
    """
    engines = ["duckdb", "postgresql"]

    rows = []

    # 1. Compute FULL_PROFILE_SQL for DuckDB + PostgreSQL
    for engine in engines:
        df_eng = df[df["engine"] == engine]

        for n in df_eng["n_rows"].unique():
            total = 0
            valid = True
            for p in PHASES_SQL:
                v = df_eng[(df_eng["n_rows"] == n) & (df_eng["phase"] == p)]
                if len(v) == 0:
                    valid = False
                    break
                total += float(v["avg_duration_s"].values[0])

            if valid:
                rows.append({
                    "engine": engine,
                    "n_rows": n,
                    "phase": "FULL_PROFILE",
                    "avg_duration_s": total,
                })

    # 2. Add BDB FULL_PROFILE (already measured)
    df_bdb = df[(df["engine"] == "berkeleydb") & (df["phase"] == "FULL_PROFILE")]
    for _, r in df_bdb.iterrows():
        rows.append({
            "engine": "berkeleydb",
            "n_rows": r["n_rows"],
            "phase": "FULL_PROFILE",
            "avg_duration_s": r["avg_duration_s"],
        })

    out = pd.DataFrame(rows)
    return out


def plot_full_profile(df):
    """
    Plot FULL_PROFILE: DuckDB, PostgreSQL, BerkeleyDB together.
    """
    plt.figure(figsize=(10, 6))

    for engine in df["engine"].unique():
        d = df[df["engine"] == engine]
        plt.plot(
            d["n_rows"],
            d["avg_duration_s"],
            marker="o",
            label=engine,
        )

    plt.title("FULL_PROFILE — Combined profile workload")
    plt.xlabel("N rows")
    plt.ylabel("Time (s)")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.legend()

    fname = os.path.join(OUTPUT_DIR, "full_profile.png")
    plt.savefig(fname, bbox_inches="tight")
    plt.close()
    print(f"[OK] Saved {fname}")


def main():
    ensure_dir(OUTPUT_DIR)
    df = load_data()

    # Plot all individual SQL phases + BDB phases
    phases = df["phase"].unique()
    for p in phases:
        plot_phase(df, p)

    # Compute combined FULL_PROFILE
    df_full = compute_full_profile_sql(df)

    # Plot FULL_PROFILE combining all engines
    plot_full_profile(df_full)

    print("\nAll plots generated in ./plots_v3/")


if __name__ == "__main__":
    main()

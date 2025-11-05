#!/usr/bin/env python3
"""
Visualization of the application-level IoT benchmark results.

Reads app_benchmark_results.csv and generates:
 - one plot per phase (LOAD, LAST_READINGS, AVG_SENSOR, HOT_SENSORS, ANOMALIES)
 - comparison of average durations between DuckDB, PostgreSQL and BerkeleyDB
 - optional combined overview chart

Outputs PNG files in ./plots_app/
"""

import os
import pandas as pd
import matplotlib.pyplot as plt

RESULTS_CSV = "app_benchmark_results.csv"
OUTPUT_DIR = "plots_app"


def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def plot_phase(df, phase):
    """
    Plot average duration (s) vs number of rows (N)
    for a specific phase, with one curve per engine.
    """
    phase_df = df[df["phase"] == phase].copy()
    if phase_df.empty:
        print(f"[WARN] No data for phase {phase}")
        return

    plt.figure(figsize=(8, 5))
    for engine in sorted(phase_df["engine"].unique()):
        sub = phase_df[phase_df["engine"] == engine].sort_values("n_rows")
        plt.plot(
            sub["n_rows"],
            sub["avg_duration_s"],
            marker="o",
            linestyle="-",
            label=engine,
        )

    plt.xlabel("Number of rows (N)")
    plt.ylabel("Average duration (s)")
    plt.title(f"{phase} – Application-Level Performance")
    plt.legend()
    plt.grid(True)
    plt.xscale("log")
    plt.yscale("log")
    filename = os.path.join(OUTPUT_DIR, f"{phase.lower()}_duration.png")
    plt.savefig(filename, bbox_inches="tight")
    plt.close()
    print(f"[OK] Saved {filename}")


def plot_overview(df):
    """
    Combined overview comparing engines on all phases
    (bar chart version, safe alignment across engines)
    """
    summary = (
        df.groupby(["engine", "phase"])["avg_duration_s"]
        .mean()
        .reset_index()
    )

    phases = sorted(df["phase"].unique())
    engines = sorted(df["engine"].unique())

    # Largeur totale d’un groupe de barres
    bar_width = 0.25
    positions = range(len(phases))

    plt.figure(figsize=(10, 6))

    for i, engine in enumerate(engines):
        engine_data = summary[summary["engine"] == engine]
        # map les phases pour avoir 0 si absentes
        heights = []
        for phase in phases:
            val = engine_data.loc[engine_data["phase"] == phase, "avg_duration_s"]
            heights.append(val.iloc[0] if not val.empty else 0.0)

        offsets = [p + i * bar_width for p in positions]
        plt.bar(offsets, heights, width=bar_width, label=engine)

    plt.xticks(
        [p + bar_width for p in positions],
        phases,
        rotation=30,
    )
    plt.ylabel("Average Duration (s)")
    plt.title("Overall Comparison – Application-Level Phases")
    plt.legend()
    plt.grid(axis="y", linestyle="--", alpha=0.5)

    filename = os.path.join(OUTPUT_DIR, "overview_comparison.png")
    plt.savefig(filename, bbox_inches="tight")
    plt.close()
    print(f"[OK] Saved {filename}")



def main():
    if not os.path.exists(RESULTS_CSV):
        print(f"[ERROR] {RESULTS_CSV} not found.")
        return

    ensure_dir(OUTPUT_DIR)
    df = pd.read_csv(RESULTS_CSV)

    required = {"engine", "n_rows", "phase", "avg_duration_s"}
    if not required.issubset(df.columns):
        print(f"[ERROR] CSV missing expected columns: {required}")
        print("Columns found:", df.columns.tolist())
        return

    phases = sorted(df["phase"].unique())
    print(f"Phases detected: {phases}")

    for phase in phases:
        plot_phase(df, phase)

    plot_overview(df)
    print(f"\n✅ All plots saved in '{OUTPUT_DIR}/'.")


if __name__ == "__main__":
    main()

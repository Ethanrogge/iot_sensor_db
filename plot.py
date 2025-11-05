#!/usr/bin/env python3
"""
Script de visualisation des résultats du benchmark kv_iot_results.csv.

Pour chaque phase (LOAD, READ, UPDATE, DELETE), on génère :
  - un graphique Throughput (ops/s) vs N (n_objects)
  - un graphique Average Duration (s) vs N (n_objects)

Les courbes sont séparées par moteur (duckdb, berkeleydb, postgresql).
Les figures sont enregistrées au format PNG dans le dossier courant.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt

RESULTS_CSV = "kv_iot_results.csv"
OUTPUT_DIR = "plots"


def ensure_output_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)


def plot_throughput(df, phase):
    """
    Graphique : throughput_ops_s en fonction de n_objects
    pour un phase donnée, avec une courbe par moteur.
    """
    phase_df = df[df["phase"] == phase].copy()
    if phase_df.empty:
        print(f"[WARN] No data for phase {phase}, skipping throughput plot.")
        return

    plt.figure()
    for engine in sorted(phase_df["engine"].unique()):
        sub = phase_df[phase_df["engine"] == engine].sort_values("n_objects")
        plt.plot(
            sub["n_objects"],
            sub["throughput_ops_s"],
            marker="o",
            linestyle="-",
            label=engine,
        )

    plt.xlabel("Number of objects (N)")
    plt.ylabel("Throughput (ops/s)")
    plt.title(f"{phase} – Throughput vs N")
    plt.legend()
    plt.grid(True)
    plt.xscale("log")  # pratique pour 1K / 10K / 100K / 1M
    out_path = os.path.join(OUTPUT_DIR, f"{phase.lower()}_throughput.png")
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()
    print(f"[OK] Saved {out_path}")


def plot_duration(df, phase):
    """
    Graphique : avg_duration_s en fonction de n_objects
    pour un phase donnée, avec une courbe par moteur.
    """
    phase_df = df[df["phase"] == phase].copy()
    if phase_df.empty:
        print(f"[WARN] No data for phase {phase}, skipping duration plot.")
        return

    plt.figure()
    for engine in sorted(phase_df["engine"].unique()):
        sub = phase_df[phase_df["engine"] == engine].sort_values("n_objects")
        plt.plot(
            sub["n_objects"],
            sub["avg_duration_s"],
            marker="o",
            linestyle="-",
            label=engine,
        )

    plt.xlabel("Number of objects (N)")
    plt.ylabel("Average duration (s)")
    plt.title(f"{phase} – Average duration vs N")
    plt.legend()
    plt.grid(True)
    plt.xscale("log")
    out_path = os.path.join(OUTPUT_DIR, f"{phase.lower()}_duration.png")
    plt.savefig(out_path, bbox_inches="tight")
    plt.close()
    print(f"[OK] Saved {out_path}")


def main():
    if not os.path.exists(RESULTS_CSV):
        print(f"[ERROR] {RESULTS_CSV} not found in current directory.")
        return

    ensure_output_dir(OUTPUT_DIR)

    df = pd.read_csv(RESULTS_CSV)
    # sécurité pour les noms de colonnes
    expected_cols = {
        "engine",
        "n_objects",
        "phase",
        "operations",
        "avg_duration_s",
        "throughput_ops_s",
    }
    if not expected_cols.issubset(df.columns):
        print("[ERROR] CSV columns do not match expected benchmark schema.")
        print("Expected at least:", expected_cols)
        print("Got:", df.columns.tolist())
        return

    phases = sorted(df["phase"].unique())
    print(f"Phases detected in CSV: {phases}")

    for phase in phases:
        plot_throughput(df, phase)
        plot_duration(df, phase)

    print(f"\nAll plots saved in ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    main()

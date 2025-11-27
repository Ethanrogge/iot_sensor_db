#!/usr/bin/env python3
"""
Benchmark aligned with the IoT application requirements.

We compare DuckDB, PostgreSQL and BerkeleyDB on:

  - LOAD: ingest N sensor readings
  - LAST_READINGS: get last K readings of random sensors
  - AVG_SENSOR: average temperature of random sensors (SQL engines)
  - HOT_SENSORS: sensors with high average temperature (SQL engines)
  - ANOMALIES: anomaly detection per sensor (SQL + local logic)
  - DAILY_SUMMARY: per-day aggregates over the dataset (SQL engines)
  - TOPK_SENSORS: sensors with highest average temperature
  - OUTAGES: detect gaps in time series (window functions)
  - FULL_PROFILE: local stats on cached readings (BerkeleyDB only)

The data comes from the real intel_sensors.csv dataset.
"""

import os
import csv
import time
import random
from collections import defaultdict

import duckdb
import psycopg2
from psycopg2.extras import execute_values
from bsddb3 import db as bdb

# ================== CONFIG ==================

DATASET_PATH = "/home/ethan/datasets/intel_sensors.csv"  # adapte si besoin

N_LIST = [1_000, 10_000, 100_000]   # tailles testées
RUNS = 6                             # 1 warmup + 5 mesures
RANDOM_SEED = 42

PG_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sensors",
    "user": "ethan",
    "password": "password",
}

RESULTS_CSV = "app_benchmark_results.csv"

LAST_K = 50               # nb de relevés retournés par "last readings"
N_SENSORS_QUERIES = 100   # nb de capteurs pour lesquels on fait des queries (AVG, anomalies, etc.)
TEMP_THRESHOLD = 30.0
MIN_READINGS_HOT = 100


# ================== HELPERS ==================

def now():
    return time.time()


def run_phase(fn, runs=RUNS):
    times = []
    for i in range(runs):
        t0 = now()
        fn()
        t1 = now()
        times.append(t1 - t0)
    useful = times[1:] if len(times) > 1 else times
    avg = sum(useful) / len(useful) if useful else 0.0
    return avg, times


def record(results, engine, n, phase, avg_time, extra=""):
    print(f"[{engine:10}] N={n:8d} {phase:12} | avg={avg_time:8.4f}s {extra}")
    results.append({
        "engine": engine,
        "n_rows": n,
        "phase": phase,
        "avg_duration_s": round(avg_time, 6),
        "note": extra,
    })


# ================== LOAD DATASET ==================

def load_raw_rows(max_n):
    """
    Charge jusqu'à max_n lignes du CSV réel, triées par (moteid, epoch).
    Retourne une liste de tuples:
      (epoch, moteid, temperature, humidity, light, voltage)
    """
    print(f"Loading dataset from {DATASET_PATH} ...")
    rows = []
    with open(DATASET_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((
                int(row["epoch"]),
                int(row["moteid"]),
                float(row["temperature"]),
                float(row["humidity"]),
                float(row["light"]),
                float(row["voltage"]),
            ))
    # trier par moteid puis epoch
    rows.sort(key=lambda x: (x[1], x[0]))
    if max_n is not None and len(rows) > max_n:
        rows = rows[:max_n]
    print(f"Loaded {len(rows):,} rows from real dataset.")
    return rows


FULL_DATA = load_raw_rows(max_n=max(N_LIST))


# ================== DUCKDB BENCH ==================

def bench_duckdb(n, data, results):
    """
    DuckDB benchmark aligned with the IoT application logic.

    Phases:
      - LOAD: insert N rows into a fresh DuckDB file
      - LAST_READINGS: read last K readings for random sensors
      - AVG_SENSOR: average temperature per random sensor
      - HOT_SENSORS: sensors with high average temperature
      - ANOMALIES: z-score based anomaly scan on recent readings
      - DAILY_SUMMARY: per-day aggregates over all readings
      - TOPK_SENSORS: top sensors by average temperature
      - OUTAGES: detect large time gaps between consecutive readings
      - COMPARE_TWO: correlation of temperatures between two sensors
    """
    engine = "duckdb"
    dbfile = f"duck_app_{n}.db"
    if os.path.exists(dbfile):
        os.remove(dbfile)

    con = duckdb.connect(dbfile)
    con.execute("""
        CREATE TABLE readings (
            epoch BIGINT,
            moteid INTEGER,
            temperature DOUBLE,
            humidity DOUBLE,
            light DOUBLE,
            voltage DOUBLE
        );
    """)

    subset = data[:n]
    con.executemany(
        "INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?);",
        subset,
    )

    sensor_ids = sorted({m for _, m, *_ in subset})
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))

    # ---------- LOAD ----------
    def load_phase():
        con.execute("DELETE FROM readings;")
        con.executemany(
            "INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?);",
            subset,
        )

    avg, _ = run_phase(load_phase)
    record(results, engine, n, "LOAD", avg)

    # ---------- LAST_READINGS ----------
    def last_readings_phase():
        for mote in random_sensors:
            con.execute(
                "SELECT * FROM readings WHERE moteid = ? "
                "ORDER BY epoch DESC LIMIT ?;",
                [mote, LAST_K],
            ).fetchall()

    avg, _ = run_phase(last_readings_phase)
    record(results, engine, n, "LAST_READINGS", avg, f"(for {len(random_sensors)} sensors)")

    # ---------- AVG_SENSOR ----------
    def avg_sensor_phase():
        for mote in random_sensors:
            con.execute(
                "SELECT AVG(temperature) FROM readings WHERE moteid = ?;",
                [mote],
            ).fetchone()

    avg, _ = run_phase(avg_sensor_phase)
    record(results, engine, n, "AVG_SENSOR", avg, f"(for {len(random_sensors)} sensors)")

    # ---------- HOT_SENSORS ----------
    def hot_sensors_phase():
        con.execute(
            """
            SELECT moteid,
                   COUNT(*) AS n,
                   AVG(temperature) AS avg_temp
            FROM readings
            GROUP BY moteid
            HAVING n >= ? AND avg_temp >= ?;
            """,
            [MIN_READINGS_HOT, TEMP_THRESHOLD],
        ).fetchall()

    avg, _ = run_phase(hot_sensors_phase)
    record(results, engine, n, "HOT_SENSORS", avg)

    # ---------- ANOMALIES ----------
    def anomalies_phase():
        for mote in random_sensors:
            mean, std = con.execute(
                """
                SELECT AVG(temperature), STDDEV_POP(temperature)
                FROM readings WHERE moteid = ?;
                """,
                [mote],
            ).fetchone()
            if std is None or std == 0:
                continue
            recent = con.execute(
                """
                SELECT epoch, temperature
                FROM readings
                WHERE moteid = ?
                ORDER BY epoch DESC
                LIMIT ?;
                """,
                [mote, LAST_K],
            ).fetchall()
            for epoch, temp in recent:
                z = abs(temp - mean) / std
                if z > 2.5:
                    pass  # just simulate some work

    avg, _ = run_phase(anomalies_phase)
    record(results, engine, n, "ANOMALIES", avg, f"(for {len(random_sensors)} sensors)")

    # ---------- DAILY_SUMMARY (Option 5) ----------
    def daily_summary_phase():
        con.execute(
            """
            SELECT date_trunc('day', to_timestamp(epoch)) AS d,
                   COUNT(*)         AS n,
                   AVG(temperature) AS avg_temp,
                   MIN(temperature) AS min_temp,
                   MAX(temperature) AS max_temp
            FROM readings
            GROUP BY d
            ORDER BY d;
            """
        ).fetchall()

    avg, _ = run_phase(daily_summary_phase)
    record(results, engine, n, "DAILY_SUMMARY", avg)

    # ---------- TOPK_SENSORS (Option 6) ----------
    def topk_phase():
        con.execute(
            """
            SELECT moteid,
                   COUNT(*)         AS n,
                   AVG(temperature) AS avg_temp
            FROM readings
            GROUP BY moteid
            ORDER BY avg_temp DESC
            LIMIT 5;
            """
        ).fetchall()

    avg, _ = run_phase(topk_phase)
    record(results, engine, n, "TOPK_SENSORS", avg)

    # ---------- OUTAGES (Option 7) ----------
    def outages_phase():
        con.execute(
            """
            WITH ordered AS (
              SELECT epoch,
                     LAG(epoch) OVER (ORDER BY epoch) AS prev_epoch
              FROM readings
            )
            SELECT epoch, prev_epoch, (epoch - prev_epoch) AS gap_sec
            FROM ordered
            WHERE prev_epoch IS NOT NULL
              AND (epoch - prev_epoch) > 600;
            """
        ).fetchall()

    avg, _ = run_phase(outages_phase)
    record(results, engine, n, "OUTAGES", avg)

    # ---------- COMPARE_TWO (Option 8) ----------
    def compare_two_phase():
        if len(random_sensors) < 2:
            return
        a, b = random_sensors[:2]
        con.execute(
            """
            SELECT corr(a.temperature, b.temperature)
            FROM (
              SELECT epoch, temperature
              FROM readings
              WHERE moteid = ?
            ) a
            JOIN (
              SELECT epoch, temperature
              FROM readings
              WHERE moteid = ?
            ) b USING(epoch);
            """,
            [a, b],
        ).fetchone()

    avg, _ = run_phase(compare_two_phase)
    record(results, engine, n, "COMPARE_TWO", avg)

    con.close()


# ================== POSTGRES BENCH ==================

def bench_postgres(n, data, results):
    """
    PostgreSQL benchmark aligned with the IoT application logic.

    Same phases as DuckDB for a fair comparison:
      - LOAD
      - LAST_READINGS
      - AVG_SENSOR
      - HOT_SENSORS
      - ANOMALIES
      - DAILY_SUMMARY
      - TOPK_SENSORS
      - OUTAGES
      - COMPARE_TWO
    """
    engine = "postgresql"
    conn = psycopg2.connect(**PG_PARAMS)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS readings;")
    cur.execute("""
        CREATE TABLE readings (
            epoch BIGINT,
            moteid INTEGER,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            light DOUBLE PRECISION,
            voltage DOUBLE PRECISION
        );
    """)

    subset = data[:n]
    execute_values(
        cur,
        "INSERT INTO readings VALUES %s;",
        subset,
        page_size=10_000,
    )
    conn.commit()

    sensor_ids = sorted({m for _, m, *_ in subset})
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))

    # ---------- LOAD ----------
    def load_phase():
        cur.execute("DELETE FROM readings;")
        execute_values(
            cur,
            "INSERT INTO readings VALUES %s;",
            subset,
            page_size=10_000,
        )
        conn.commit()

    avg, _ = run_phase(load_phase)
    record(results, engine, n, "LOAD", avg)

    # ---------- LAST_READINGS ----------
    def last_readings_phase():
        for mote in random_sensors:
            cur.execute(
                """
                SELECT * FROM readings
                WHERE moteid = %s
                ORDER BY epoch DESC
                LIMIT %s;
                """,
                (mote, LAST_K),
            )
            cur.fetchall()

    avg, _ = run_phase(last_readings_phase)
    record(results, engine, n, "LAST_READINGS", avg, f"(for {len(random_sensors)} sensors)")

    # ---------- AVG_SENSOR ----------
    def avg_sensor_phase():
        for mote in random_sensors:
            cur.execute(
                "SELECT AVG(temperature) FROM readings WHERE moteid = %s;",
                (mote,),
            )
            cur.fetchone()

    avg, _ = run_phase(avg_sensor_phase)
    record(results, engine, n, "AVG_SENSOR", avg, f"(for {len(random_sensors)} sensors)")

    # ---------- HOT_SENSORS ----------
    def hot_sensors_phase():
        cur.execute(
            """
            SELECT moteid,
                   COUNT(*)         AS n,
                   AVG(temperature) AS avg_temp
            FROM readings
            GROUP BY moteid
            HAVING COUNT(*) >= %s AND AVG(temperature) >= %s;
            """,
            (MIN_READINGS_HOT, TEMP_THRESHOLD),
        )
        cur.fetchall()

    avg, _ = run_phase(hot_sensors_phase)
    record(results, engine, n, "HOT_SENSORS", avg)

    # ---------- ANOMALIES ----------
    def anomalies_phase():
        for mote in random_sensors:
            cur.execute(
                """
                SELECT AVG(temperature), STDDEV_POP(temperature)
                FROM readings WHERE moteid = %s;
                """,
                (mote,),
            )
            mean, std = cur.fetchone()
            if std is None or std == 0:
                continue
            cur.execute(
                """
                SELECT epoch, temperature
                FROM readings
                WHERE moteid = %s
                ORDER BY epoch DESC
                LIMIT %s;
                """,
                (mote, LAST_K),
            )
            recent = cur.fetchall()
            for epoch, temp in recent:
                z = abs(temp - mean) / std
                if z > 2.5:
                    pass

    avg, _ = run_phase(anomalies_phase)
    record(results, engine, n, "ANOMALIES", avg, f"(for {len(random_sensors)} sensors)")

    # ---------- DAILY_SUMMARY (Option 5) ----------
    def daily_summary_phase():
        cur.execute(
            """
            SELECT date_trunc('day', to_timestamp(epoch)) AS d,
                   COUNT(*)         AS n,
                   AVG(temperature) AS avg_temp,
                   MIN(temperature) AS min_temp,
                   MAX(temperature) AS max_temp
            FROM readings
            GROUP BY d
            ORDER BY d;
            """
        )
        cur.fetchall()

    avg, _ = run_phase(daily_summary_phase)
    record(results, engine, n, "DAILY_SUMMARY", avg)

    # ---------- TOPK_SENSORS (Option 6) ----------
    def topk_phase():
        cur.execute(
            """
            SELECT moteid,
                   COUNT(*)         AS n,
                   AVG(temperature) AS avg_temp
            FROM readings
            GROUP BY moteid
            ORDER BY avg_temp DESC
            LIMIT 5;
            """
        )
        cur.fetchall()

    avg, _ = run_phase(topk_phase)
    record(results, engine, n, "TOPK_SENSORS", avg)

    # ---------- OUTAGES (Option 7) ----------
    def outages_phase():
        cur.execute(
            """
            WITH ordered AS (
              SELECT epoch,
                     LAG(epoch) OVER (ORDER BY epoch) AS prev_epoch
              FROM readings
            )
            SELECT epoch, prev_epoch, (epoch - prev_epoch) AS gap_sec
            FROM ordered
            WHERE prev_epoch IS NOT NULL
              AND (epoch - prev_epoch) > 600;
            """
        )
        cur.fetchall()

    avg, _ = run_phase(outages_phase)
    record(results, engine, n, "OUTAGES", avg)

    # ---------- COMPARE_TWO (Option 8) ----------
    def compare_two_phase():
        if len(random_sensors) < 2:
            return
        a, b = random_sensors[:2]
        cur.execute(
            """
            SELECT corr(a.temperature, b.temperature)
            FROM (
              SELECT epoch, temperature
              FROM readings
              WHERE moteid = %s
            ) a
            JOIN (
              SELECT epoch, temperature
              FROM readings
              WHERE moteid = %s
            ) b USING(epoch);
            """,
            (a, b),
        )
        cur.fetchone()

    avg, _ = run_phase(compare_two_phase)
    record(results, engine, n, "COMPARE_TWO", avg)

    cur.close()
    conn.close()


# ================== BERKELEYDB BENCH ==================

def bench_berkeley(n, data, results):
    """
    BerkeleyDB benchmark: plays the role of an embedded key-value cache
    storing the recent readings per sensor.

    Phases:
      - LOAD: build the per-sensor cache
      - LAST_READINGS: fetch the last K readings for random sensors
      - FULL_PROFILE: local min/max/avg on cached readings
    """
    engine = "berkeleydb"
    dbfile = f"bdb_app_{n}.db"
    if os.path.exists(dbfile):
        os.remove(dbfile)

    # Build an in-memory per-sensor structure first
    per_sensor = defaultdict(list)
    for epoch, mote, temp, hum, light, volt in data[:n]:
        per_sensor[mote].append((epoch, temp, hum, light, volt))

    sensor_ids = sorted(per_sensor.keys())
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))

    def open_db():
        database = bdb.DB()
        database.open(dbfile, None, bdb.DB_BTREE, bdb.DB_CREATE)
        return database

    # ---------- LOAD ----------
    def load_phase():
        database = open_db()
        database.truncate()
        for mote, readings in per_sensor.items():
            # encode all recent readings into a single value:
            # "epoch|temp|hum|light|volt;epoch|temp|..."
            val = ";".join(
                f"{epoch}|{temp:.2f}|{hum:.2f}|{light:.0f}|{volt:.2f}"
                for epoch, temp, hum, light, volt in readings
            )
            database.put(str(mote).encode("utf-8"), val.encode("utf-8"))
        database.close()

    avg, _ = run_phase(load_phase)
    record(results, engine, n, "LOAD", avg)

    # ---------- LAST_READINGS ----------
    def last_readings_phase():
        database = open_db()
        for mote in random_sensors:
            val = database.get(str(mote).encode("utf-8"))
            if not val:
                continue
            parts = val.decode("utf-8").split(";")
            last = parts[-LAST_K:] if len(parts) > LAST_K else parts
            # parse to simulate some CPU usage
            for p in last:
                _ = p.split("|")  # (epoch, temp, hum, light, volt)
        database.close()

    avg, _ = run_phase(last_readings_phase)
    record(results, engine, n, "LAST_READINGS", avg, f"(for {len(random_sensors)} sensors)")

    # ---------- FULL_PROFILE (Option 9 - local stats only) ----------
    def full_profile_phase():
        database = open_db()
        for mote in random_sensors[:5]:
            val = database.get(str(mote).encode("utf-8"))
            if not val:
                continue
            parts = val.decode("utf-8").split(";")
            temps = []
            for p in parts:
                if not p:
                    continue
                fields = p.split("|")
                if len(fields) < 2:
                    continue
                temps.append(float(fields[1]))
            if temps:
                # local min / max / avg for this sensor
                _ = (min(temps), max(temps), sum(temps) / len(temps))
        database.close()

    avg, _ = run_phase(full_profile_phase)
    record(results, engine, n, "FULL_PROFILE", avg, "(local stats)")


# ================== MAIN ==================

def main():
    random.seed(RANDOM_SEED)
    results = []

    import csv as _csv
    for n in N_LIST:
        print(f"\n===== N = {n} rows (application-level workload) =====")
        subset = FULL_DATA[:n]

        bench_duckdb(n, subset, results)
        bench_postgres(n, subset, results)
        bench_berkeley(n, subset, results)

    with open(RESULTS_CSV, "w", newline="") as f:
        writer = _csv.DictWriter(
            f,
            fieldnames=["engine", "n_rows", "phase", "avg_duration_s", "note"],
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nResults written to {RESULTS_CSV}")


if __name__ == "__main__":
    main()

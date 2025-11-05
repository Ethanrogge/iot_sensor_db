#!/usr/bin/env python3
"""
Benchmark aligned with the IoT application requirements.

We compare DuckDB, PostgreSQL and BerkeleyDB on:

  - LOAD: ingest N sensor readings
  - LAST_READINGS: get last K readings of random sensors
  - AVG_SENSOR: average temperature of random sensors (SQL engines)
  - HOT_SENSORS: sensors with high average temperature (SQL engines)
  - ANOMALIES: anomaly detection per sensor (SQL + BDB hybrid for info)

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
    sensor_ids = sorted({m for _, m, *_ in subset})
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))

    def load_phase():
        con.execute("DELETE FROM readings;")
        con.executemany(
            "INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?);",
            subset
        )

    avg, _ = run_phase(load_phase)
    record(results, engine, n, "LOAD", avg)

    # last readings pour des capteurs aléatoires
    def last_readings_phase():
        for mote in random_sensors:
            con.execute(
                """
                SELECT * FROM readings
                WHERE moteid = ?
                ORDER BY epoch DESC
                LIMIT ?;
                """,
                [mote, LAST_K],
            ).fetchall()

    avg, _ = run_phase(last_readings_phase)
    record(results, engine, n, "LAST_READINGS", avg, f"(for {len(random_sensors)} sensors)")

    # AVG temp pour capteurs aléatoires
    def avg_sensor_phase():
        for mote in random_sensors:
            con.execute(
                "SELECT AVG(temperature) FROM readings WHERE moteid = ?;",
                [mote],
            ).fetchone()

    avg, _ = run_phase(avg_sensor_phase)
    record(results, engine, n, "AVG_SENSOR", avg, f"(for {len(random_sensors)} sensors)")

    # capteurs chauds (avg temp > threshold)
    def hot_sensors_phase():
        con.execute(
            """
            SELECT moteid, COUNT(*) AS n, AVG(temperature) AS avg_temp
            FROM readings
            GROUP BY moteid
            HAVING n >= ? AND avg_temp >= ?;
            """,
            [MIN_READINGS_HOT, TEMP_THRESHOLD],
        ).fetchall()

    avg, _ = run_phase(hot_sensors_phase)
    record(results, engine, n, "HOT_SENSORS", avg)

    # anomalies pour capteurs aléatoires
    def anomalies_phase():
        for mote in random_sensors:
            # moyenne + stddev globales
            mean, std = con.execute(
                """
                SELECT AVG(temperature), STDDEV_POP(temperature)
                FROM readings WHERE moteid = ?;
                """,
                [mote],
            ).fetchone()
            if std is None or std == 0:
                continue
            # charger les dernières lectures
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
            # calcul du z-score en Python
            for epoch, temp in recent:
                z = abs(temp - mean) / std
                if z > 2.5:
                    pass  # on ne fait rien, juste simuler la détection

    avg, _ = run_phase(anomalies_phase)
    record(results, engine, n, "ANOMALIES", avg, f"(for {len(random_sensors)} sensors)")

    con.close()


# ================== POSTGRES BENCH ==================

def bench_postgres(n, data, results):
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
    conn.commit()

    subset = data[:n]
    sensor_ids = sorted({m for _, m, *_ in subset})
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))

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

    def avg_sensor_phase():
        for mote in random_sensors:
            cur.execute(
                "SELECT AVG(temperature) FROM readings WHERE moteid = %s;",
                (mote,),
            )
            cur.fetchone()

    avg, _ = run_phase(avg_sensor_phase)
    record(results, engine, n, "AVG_SENSOR", avg, f"(for {len(random_sensors)} sensors)")

    def hot_sensors_phase():
        cur.execute(
            """
            SELECT moteid,
                COUNT(*) AS n,
                AVG(temperature) AS avg_temp
            FROM readings
            GROUP BY moteid
            HAVING COUNT(*) >= %s
                AND AVG(temperature) >= %s;
            """,
            (MIN_READINGS_HOT, TEMP_THRESHOLD),
        )
        cur.fetchall()


    avg, _ = run_phase(hot_sensors_phase)
    record(results, engine, n, "HOT_SENSORS", avg)

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

    cur.close()
    conn.close()


# ================== BERKELEYDB BENCH ==================

def bench_berkeley(n, data, results):
    """
    BerkeleyDB joue le rôle de cache des dernières mesures par capteur.
    On le benchmark seulement sur :
      - LOAD (construction du cache)
      - LAST_READINGS (récupération rapide par capteur)
    """
    engine = "berkeleydb"
    dbfile = f"bdb_app_{n}.db"
    if os.path.exists(dbfile):
        os.remove(dbfile)

    # construire structure per sensor
    per_sensor = defaultdict(list)
    for epoch, mote, temp, hum, light, volt in data[:n]:
        per_sensor[mote].append((epoch, temp, hum, light, volt))

    sensor_ids = sorted(per_sensor.keys())
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))

    def open_db():
        database = bdb.DB()
        database.open(dbfile, None, bdb.DB_BTREE, bdb.DB_CREATE)
        return database

    def load_phase():
        database = open_db()
        database.truncate()
        for mote, readings in per_sensor.items():
            # encode dernière N mesures (tout) en une seule valeur
            val = ";".join(
                f"{epoch}|{temp:.2f}|{hum:.2f}|{light:.0f}|{volt:.2f}"
                for epoch, temp, hum, light, volt in readings
            )
            database.put(str(mote).encode("utf-8"), val.encode("utf-8"))
        database.close()

    avg, _ = run_phase(load_phase)
    record(results, engine, n, "LOAD", avg)

    def last_readings_phase():
        database = open_db()
        for mote in random_sensors:
            val = database.get(str(mote).encode("utf-8"))
            if not val:
                continue
            parts = val.decode("utf-8").split(";")
            last = parts[-LAST_K:] if len(parts) > LAST_K else parts
            # on peut parser si on veut simuler le travail CPU
            for p in last:
                _ = p.split("|")  # (epoch, temp, hum, light, volt)
        database.close()

    avg, _ = run_phase(last_readings_phase)
    record(results, engine, n, "LAST_READINGS", avg, f"(for {len(random_sensors)} sensors)")


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

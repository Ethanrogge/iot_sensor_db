#!/usr/bin/env python3
"""
Engines:
  - DuckDB (embedded analytical SQL)
  - PostgreSQL (server-based relational baseline)
  - BerkeleyDB (embedded key-value cache)

Common phases (per engine, when applicable):
  - LOAD
  - READ
  - UPDATE
  - DELETE
  - LAST_READINGS
  - AVG_SENSOR          (SQL engines)
  - HOT_SENSORS         (SQL engines)
  - ANOMALIES           (SQL engines)
  - DAILY_SUMMARY       (SQL engines)
  - TOPK_SENSORS        (SQL engines)
  - OUTAGES             (SQL engines)
  - COMPARE_TWO         (DuckDB + PostgreSQL)
  - FULL_PROFILE        (DuckDB + PostgreSQL: analytical bundle, BDB: local stats)
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

# CONFIG

DATASET_PATH = "/home/ethan/datasets/intel_sensors.csv"  # adapte si besoin

N_LIST = [1_000, 10_000, 100_000, 1_000_000]   # tailles testées
RUNS = 6                             # 1 warmup + 5 mesures
RANDOM_SEED = 42

# facteurs pour READ / UPDATE / DELETE
READ_FACTOR = 1.0
UPDATE_FACTOR = 0.5
DELETE_FACTOR = 0.1

PG_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sensors",
    "user": "ethan",
    "password": "password",
}

RESULTS_CSV = "benchmark_results.csv"

LAST_K = 50               # nb de relevés retournés par "last readings"
N_SENSORS_QUERIES = 100   # nb de capteurs pour lesquels on fait des queries (AVG, anomalies, etc.)
TEMP_THRESHOLD = 30.0
MIN_READINGS_HOT = 100


# HELPERS

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


def record(results, engine, n, phase, avg_time, note=""):
    print(f"[{engine:10}] N={n:8d} {phase:12} | avg={avg_time:8.4f}s {note}")
    results.append({
        "engine": engine,
        "n_rows": n,
        "phase": phase,
        "avg_duration_s": round(avg_time, 6),
        "note": note,
    })


# LOAD DATASET

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


# DUCKDB BENCH

def bench_duckdb(n, data, results):
    """
    DuckDB benchmark with both KV-style phases and application-level queries.

    Phases:
      - LOAD: insert N rows into a fresh DuckDB file (with id key)
      - READ: point lookups by id
      - UPDATE: update temperature for random ids
      - DELETE: delete random ids
      - LAST_READINGS: get last K readings for random sensors
      - AVG_SENSOR: average temperature per random sensor
      - HOT_SENSORS: sensors with high average temperature
      - ANOMALIES: z-score based anomaly scan on recent readings
      - DAILY_SUMMARY: per-day aggregates over all readings
      - TOPK_SENSORS: sensors with highest average temperature
      - OUTAGES: detect gaps in time series
      - COMPARE_TWO: correlation of temperatures between two sensors
    """
    engine = "duckdb"
    dbfile = f"duck_merged_{n}.db"
    if os.path.exists(dbfile):
        os.remove(dbfile)

    con = duckdb.connect(dbfile)
    con.execute("""
        CREATE TABLE readings (
            id        INTEGER,
            epoch     BIGINT,
            moteid    INTEGER,
            temperature DOUBLE,
            humidity    DOUBLE,
            light       DOUBLE,
            voltage     DOUBLE
        );
    """)

    subset = data[:n]
    # on ajoute un id séquentiel pour simuler la clé
    subset_with_id = [(i,) + row for i, row in enumerate(subset)]
    con.executemany(
        "INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?, ?);",
        subset_with_id,
    )

    sensor_ids = sorted({m for _, m, *_ in subset})
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))
    ids = list(range(n))
    n_read = int(READ_FACTOR * n)
    n_upd = int(UPDATE_FACTOR * n)
    n_del = int(DELETE_FACTOR * n)

    ids_read = random.choices(ids, k=n_read)
    ids_upd = random.choices(ids, k=n_upd)
    ids_del = random.sample(ids, min(n_del, len(ids)))

    # LOAD
    def load_phase():
        con.execute("DELETE FROM readings;")
        con.executemany(
            "INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?, ?);",
            subset_with_id,
        )

    avg, _ = run_phase(load_phase)
    record(results, engine, n, "LOAD", avg)

    # READ
    def read_phase():
        for k in ids_read:
            con.execute(
                "SELECT epoch, moteid, temperature FROM readings WHERE id = ?;",
                [k],
            ).fetchone()

    avg, _ = run_phase(read_phase)
    record(results, engine, n, "READ", avg, f"(ops={n_read})")

    # UPDATE 
    def update_phase():
        for k in ids_upd:
            con.execute(
                "UPDATE readings SET temperature = temperature + 0.1 WHERE id = ?;",
                [k],
            )

    avg, _ = run_phase(update_phase)
    record(results, engine, n, "UPDATE", avg, f"(ops={n_upd})")

    # DELETE
    def delete_phase():
        for k in ids_del:
            con.execute(
                "DELETE FROM readings WHERE id = ?;",
                [k],
            )

    avg, _ = run_phase(delete_phase)
    record(results, engine, n, "DELETE", avg, f"(ops={n_del})")

    # LAST_READINGS
    def last_readings_phase():
        for mote in random_sensors:
            con.execute(
                "SELECT * FROM readings WHERE moteid = ? "
                "ORDER BY epoch DESC LIMIT ?;",
                [mote, LAST_K],
            ).fetchall()

    avg, _ = run_phase(last_readings_phase)
    record(results, engine, n, "LAST_READINGS", avg, f"(for {len(random_sensors)} sensors)")

    #  AVG_SENSOR 
    def avg_sensor_phase():
        for mote in random_sensors:
            con.execute(
                "SELECT AVG(temperature) FROM readings WHERE moteid = ?;",
                [mote],
            ).fetchone()

    avg, _ = run_phase(avg_sensor_phase)
    record(results, engine, n, "AVG_SENSOR", avg, f"(for {len(random_sensors)} sensors)")

    #  HOT_SENSORS 
    def hot_sensors_phase():
        con.execute(
            """
            SELECT moteid,
                   COUNT(*)         AS n,
                   AVG(temperature) AS avg_temp
            FROM readings
            GROUP BY moteid
            HAVING n >= ? AND avg_temp >= ?;
            """,
            [MIN_READINGS_HOT, TEMP_THRESHOLD],
        ).fetchall()

    avg, _ = run_phase(hot_sensors_phase)
    record(results, engine, n, "HOT_SENSORS", avg)

    #  ANOMALIES 
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
                    pass  # simulate some work

    avg, _ = run_phase(anomalies_phase)
    record(results, engine, n, "ANOMALIES", avg, f"(for {len(random_sensors)} sensors)")

    #  DAILY_SUMMARY 
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

    #  TOPK_SENSORS 
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

    #  OUTAGES 
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

    #  COMPARE_TWO 
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


#  POSTGRES BENCH 

def bench_postgres(n, data, results):
    """
    PostgreSQL benchmark with KV-style phases and application-level queries.

    Phases:
      - LOAD
      - READ
      - UPDATE
      - DELETE
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
            id          INTEGER,
            epoch       BIGINT,
            moteid      INTEGER,
            temperature DOUBLE PRECISION,
            humidity    DOUBLE PRECISION,
            light       DOUBLE PRECISION,
            voltage     DOUBLE PRECISION
        );
    """)

    subset = data[:n]
    subset_with_id = [(i,) + row for i, row in enumerate(subset)]
    execute_values(
        cur,
        "INSERT INTO readings VALUES %s;",
        subset_with_id,
        page_size=10_000,
    )
    conn.commit()

    sensor_ids = sorted({m for _, m, *_ in subset})
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))

    ids = list(range(n))
    n_read = int(READ_FACTOR * n)
    n_upd = int(UPDATE_FACTOR * n)
    n_del = int(DELETE_FACTOR * n)

    ids_read = random.choices(ids, k=n_read)
    ids_upd = random.choices(ids, k=n_upd)
    ids_del = random.sample(ids, min(n_del, len(ids)))

    #  LOAD 
    def load_phase():
        cur.execute("DELETE FROM readings;")
        execute_values(
            cur,
            "INSERT INTO readings VALUES %s;",
            subset_with_id,
            page_size=10_000,
        )
        conn.commit()

    avg, _ = run_phase(load_phase)
    record(results, engine, n, "LOAD", avg)

    #  READ 
    def read_phase():
        for k in ids_read:
            cur.execute(
                "SELECT epoch, moteid, temperature FROM readings WHERE id = %s;",
                (k,),
            )
            cur.fetchone()

    avg, _ = run_phase(read_phase)
    record(results, engine, n, "READ", avg, f"(ops={n_read})")

    #  UPDATE 
    def update_phase():
        for k in ids_upd:
            cur.execute(
                "UPDATE readings SET temperature = temperature + 0.1 WHERE id = %s;",
                (k,),
            )
        conn.commit()

    avg, _ = run_phase(update_phase)
    record(results, engine, n, "UPDATE", avg, f"(ops={n_upd})")

    #  DELETE 
    def delete_phase():
        for k in ids_del:
            cur.execute(
                "DELETE FROM readings WHERE id = %s;",
                (k,),
            )
        conn.commit()

    avg, _ = run_phase(delete_phase)
    record(results, engine, n, "DELETE", avg, f"(ops={n_del})")

    #  LAST_READINGS 
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

    #  AVG_SENSOR 
    def avg_sensor_phase():
        for mote in random_sensors:
            cur.execute(
                "SELECT AVG(temperature) FROM readings WHERE moteid = %s;",
                (mote,),
            )
            cur.fetchone()

    avg, _ = run_phase(avg_sensor_phase)
    record(results, engine, n, "AVG_SENSOR", avg, f"(for {len(random_sensors)} sensors)")

    #  HOT_SENSORS 
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

    #  ANOMALIES 
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

    #  DAILY_SUMMARY 
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

    #  TOPK_SENSORS 
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

    #  OUTAGES 
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

    #  COMPARE_TWO 
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


#  BERKELEYDB BENCH 

def bench_berkeley(n, data, results):
    """
    BerkeleyDB benchmark: embedded key-value cache on sensor data.

    Phases:
      - LOAD: store per-sensor readings as one value per sensor
      - READ: random get() on sensor keys
      - UPDATE: modify stored blob for random sensor keys
      - DELETE: delete random sensor keys
      - LAST_READINGS: parse last K readings for random sensors
      - FULL_PROFILE: local min/max/avg on cached readings
    """
    engine = "berkeleydb"
    dbfile = f"bdb_merged_{n}.db"
    if os.path.exists(dbfile):
        os.remove(dbfile)

    # Build an in-memory per-sensor structure first
    per_sensor = defaultdict(list)
    for epoch, mote, temp, hum, light, volt in data[:n]:
        per_sensor[mote].append((epoch, temp, hum, light, volt))

    sensor_ids = sorted(per_sensor.keys())
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))
    n_read = int(READ_FACTOR * n)
    n_upd = int(UPDATE_FACTOR * n)
    n_del = int(DELETE_FACTOR * n)

    keys_read = random.choices(sensor_ids, k=n_read)
    keys_upd = random.choices(sensor_ids, k=n_upd)
    keys_del = random.sample(sensor_ids, min(n_del, len(sensor_ids)))

    def open_db():
        database = bdb.DB()
        database.open(dbfile, None, bdb.DB_BTREE, bdb.DB_CREATE)
        return database

    #  LOAD 
    def load_phase():
        database = open_db()
        database.truncate()
        for mote, readings in per_sensor.items():
            # encode all readings into a single value:
            # "epoch|temp|hum|light|volt;epoch|temp|..."
            val = ";".join(
                f"{epoch}|{temp:.2f}|{hum:.2f}|{light:.0f}|{volt:.2f}"
                for epoch, temp, hum, light, volt in readings
            )
            database.put(str(mote).encode("utf-8"), val.encode("utf-8"))
        database.close()

    avg, _ = run_phase(load_phase)
    record(results, engine, n, "LOAD", avg)

    #  READ 
    def read_phase():
        database = open_db()
        for mote in keys_read:
            _ = database.get(str(mote).encode("utf-8"))
        database.close()

    avg, _ = run_phase(read_phase)
    record(results, engine, n, "READ", avg, f"(ops={n_read})")

    #  UPDATE 
    def update_phase():
        database = open_db()
        for mote in keys_upd:
            key = str(mote).encode("utf-8")
            val = database.get(key)
            if val:
                database.put(key, val + b"_u")
        database.close()

    avg, _ = run_phase(update_phase)
    record(results, engine, n, "UPDATE", avg, f"(ops={n_upd})")

    #  DELETE 
    def delete_phase():
        database = open_db()
        for mote in keys_del:
            try:
                database.delete(str(mote).encode("utf-8"))
            except bdb.DBNotFoundError:
                pass
        database.close()

    avg, _ = run_phase(delete_phase)
    record(results, engine, n, "DELETE", avg, f"(ops={n_del})")

    #  LAST_READINGS 
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

    #  FULL_PROFILE 
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
                _ = (min(temps), max(temps), sum(temps) / len(temps))
        database.close()

    avg, _ = run_phase(full_profile_phase)
    record(results, engine, n, "FULL_PROFILE", avg, "(local stats)")


#  MAIN 

def main():
    random.seed(RANDOM_SEED)
    results = []

    import csv as _csv
    for n in N_LIST:
        print(f"\n===== N = {n} rows (merged workload) =====")
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

"""
Realistic benchmark for embedded and relational databases on an IoT sensor dataset.

Engines:
  - DuckDB (embedded analytical SQL)
  - PostgreSQL (server-based relational baseline)
  - BerkeleyDB (embedded key-value store)

Phases :
  - LOAD
  - READ
  - UPDATE
  - DELETE
  - LAST_READINGS
  - AVG_SENSOR          (DuckDB, PostgreSQL)
  - HOT_SENSORS         (DuckDB, PostgreSQL)
  - ANOMALIES           (DuckDB, PostgreSQL)
  - DAILY_SUMMARY       (DuckDB, PostgreSQL)
  - TOPK_SENSORS        (DuckDB, PostgreSQL)
  - OUTAGES             (DuckDB, PostgreSQL)
  - COMPARE_TWO         (DuckDB, PostgreSQL)
  - FULL_PROFILE        (DuckDB, PostgreSQL, BerkeleyDB)
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

#  CONFIG 

DATASET_PATH = "/home/ethan/datasets/intel_sensors.csv"

N_LIST = [1_000,10_000,100_000,1_000_000]
RUNS = 6
RANDOM_SEED = 42

READ_FACTOR = 1.0
UPDATE_FACTOR = 0.5
DELETE_FACTOR = 0.1

LAST_K = 50
N_SENSORS_QUERIES = 100
TEMP_THRESHOLD = 30.0
MIN_READINGS_HOT = 100

RESULTS_CSV = "benchmark_results.csv"

PG_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sensors",
    "user": "ethan",
    "password": "password",
}


#  UTILS 

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


#  DATASET 

def load_raw_rows(max_n):
    """
    Load up to max_n rows from the real CSV, sorted by (moteid, epoch).
    Returns a list of tuples:
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
    rows.sort(key=lambda x: (x[1], x[0]))  # (moteid, epoch)
    if max_n is not None and len(rows) > max_n:
        rows = rows[:max_n]
    print(f"Loaded {len(rows):,} rows from real dataset.")
    return rows


FULL_DATA = load_raw_rows(max_n=max(N_LIST))


#  DUCKDB 

def bench_duckdb(n, data, results):
    engine = "duckdb"
    dbfile = f"duck_real_{n}.db"
    if os.path.exists(dbfile):
        os.remove(dbfile)

    con = duckdb.connect(dbfile)
    con.execute("""
        CREATE TABLE readings (
            id          INTEGER,
            epoch       BIGINT,
            moteid      INTEGER,
            temperature DOUBLE,
            humidity    DOUBLE,
            light       DOUBLE,
            voltage     DOUBLE
        );
    """)

    subset = data[:n]
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

    # AVG_SENSOR
    def avg_sensor_phase():
        for mote in random_sensors:
            con.execute(
                "SELECT AVG(temperature) FROM readings WHERE moteid = ?;",
                [mote],
            ).fetchone()

    avg, _ = run_phase(avg_sensor_phase)
    record(results, engine, n, "AVG_SENSOR", avg, f"(for {len(random_sensors)} sensors)")

    # HOT_SENSORS
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

    # ANOMALIES
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
                    pass

    avg, _ = run_phase(anomalies_phase)
    record(results, engine, n, "ANOMALIES", avg, f"(for {len(random_sensors)} sensors)")

    # DAILY_SUMMARY
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

    # TOPK_SENSORS
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

    # OUTAGES
    def outages_phase():
        con.execute(
            """
            WITH ordered AS (
              SELECT moteid, epoch,
                     LAG(epoch) OVER (PARTITION BY moteid ORDER BY epoch) AS prev_epoch
              FROM readings
            )
            SELECT moteid, epoch, prev_epoch, (epoch - prev_epoch) AS gap_sec
            FROM ordered
            WHERE prev_epoch IS NOT NULL
              AND (epoch - prev_epoch) > 600;
            """
        ).fetchall()

    avg, _ = run_phase(outages_phase)
    record(results, engine, n, "OUTAGES", avg)

    # COMPARE_TWO
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

    # FULL_PROFILE = bundle of several analytical queries
    def full_profile_phase():
        avg_sensor_phase()
        daily_summary_phase()
        outages_phase()
        topk_phase()

    avg, _ = run_phase(full_profile_phase)
    record(results, engine, n, "FULL_PROFILE", avg, "(analytical bundle)")

    con.close()


#  POSTGRES 

def bench_postgres(n, data, results):
    engine = "postgresql"
    conn = psycopg2.connect(**PG_PARAMS)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS readings;")
    cur.execute("""
        CREATE TABLE readings (
            id          INTEGER PRIMARY KEY,
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

    # LOAD
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

    # READ
    def read_phase():
        for k in ids_read:
            cur.execute(
                "SELECT epoch, moteid, temperature FROM readings WHERE id = %s;",
                (k,),
            )
            cur.fetchone()

    avg, _ = run_phase(read_phase)
    record(results, engine, n, "READ", avg, f"(ops={n_read})")

    # UPDATE
    def update_phase():
        for k in ids_upd:
            cur.execute(
                "UPDATE readings SET temperature = temperature + 0.1 WHERE id = %s;",
                (k,),
            )
        conn.commit()

    avg, _ = run_phase(update_phase)
    record(results, engine, n, "UPDATE", avg, f"(ops={n_upd})")

    # DELETE
    def delete_phase():
        for k in ids_del:
            cur.execute(
                "DELETE FROM readings WHERE id = %s;",
                (k,),
            )
        conn.commit()

    avg, _ = run_phase(delete_phase)
    record(results, engine, n, "DELETE", avg, f"(ops={n_del})")

    # LAST_READINGS
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

    # AVG_SENSOR
    def avg_sensor_phase():
        for mote in random_sensors:
            cur.execute(
                "SELECT AVG(temperature) FROM readings WHERE moteid = %s;",
                (mote,),
            )
            cur.fetchone()

    avg, _ = run_phase(avg_sensor_phase)
    record(results, engine, n, "AVG_SENSOR", avg, f"(for {len(random_sensors)} sensors)")

    # HOT_SENSORS
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

    # ANOMALIES
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

    # DAILY_SUMMARY
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

    # TOPK_SENSORS
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

    # OUTAGES
    def outages_phase():
        cur.execute(
            """
            WITH ordered AS (
              SELECT moteid, epoch,
                     LAG(epoch) OVER (PARTITION BY moteid ORDER BY epoch) AS prev_epoch
              FROM readings
            )
            SELECT moteid, epoch, prev_epoch, (epoch - prev_epoch) AS gap_sec
            FROM ordered
            WHERE prev_epoch IS NOT NULL
              AND (epoch - prev_epoch) > 600;
            """
        )
        cur.fetchall()

    avg, _ = run_phase(outages_phase)
    record(results, engine, n, "OUTAGES", avg)

    # COMPARE_TWO
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

    # FULL_PROFILE
    def full_profile_phase():
        avg_sensor_phase()
        daily_summary_phase()
        outages_phase()
        topk_phase()

    avg, _ = run_phase(full_profile_phase)
    record(results, engine, n, "FULL_PROFILE", avg, "(analytical bundle)")

    cur.close()
    conn.close()


#  BERKELEYDB (REALISTIC) 

def bench_berkeley(n, data, results):
    """
    BerkeleyDB benchmark (realistic design):

    - One entry per reading:
      key = "moteid:epoch" (zero-padded so BTREE order == (moteid, epoch))
      value = "epoch|temp|hum|light|volt"

    - READ / UPDATE / DELETE:
      operate on individual keys (random subset).

    - LAST_READINGS:
      uses an in-memory index: per_sensor_keys[moteid] = [key1, key2, ...]
      and get() only the LAST_K keys for each sensor.

    - FULL_PROFILE:
      for a few sensors, read all their values from DB and compute
      min/max/avg temperature (local stats).
    """
    engine = "berkeleydb"
    dbfile = f"bdb_real_{n}.db"
    if os.path.exists(dbfile):
        os.remove(dbfile)

    subset = data[:n]

    # Build record list and in-memory “index” per sensor
    records = []
    per_sensor_keys = defaultdict(list)
    for epoch, moteid, temp, hum, light, volt in subset:
        key = f"{moteid:03d}:{epoch:010d}".encode("utf-8")
        val = f"{epoch}|{temp:.2f}|{hum:.2f}|{light:.0f}|{volt:.2f}".encode("utf-8")
        records.append((key, val, moteid))
        per_sensor_keys[moteid].append(key)

    sensor_ids = sorted(per_sensor_keys.keys())
    random_sensors = random.sample(sensor_ids, min(N_SENSORS_QUERIES, len(sensor_ids)))

    n_read = int(READ_FACTOR * n)
    n_upd = int(UPDATE_FACTOR * n)
    n_del = int(DELETE_FACTOR * n)

    all_keys = [kv[0] for kv in records]
    keys_read = random.choices(all_keys, k=n_read)
    keys_upd = random.choices(all_keys, k=n_upd)
    keys_del = random.sample(all_keys, min(n_del, len(all_keys)))

    def open_db():
        database = bdb.DB()
        database.open(dbfile, None, bdb.DB_BTREE, bdb.DB_CREATE)
        return database

    # LOAD
    def load_phase():
        database = open_db()
        database.truncate()
        for key, val, _moteid in records:
            database.put(key, val)
        database.close()

    avg, _ = run_phase(load_phase)
    record(results, engine, n, "LOAD", avg)

    # READ (random keys)
    def read_phase():
        database = open_db()
        for key in keys_read:
            _ = database.get(key)
        database.close()

    avg, _ = run_phase(read_phase)
    record(results, engine, n, "READ", avg, f"(ops={n_read})")

    # UPDATE (append a marker in value)
    def update_phase():
        database = open_db()
        for key in keys_upd:
            val = database.get(key)
            if val is not None:
                database.put(key, val + b"_u")
        database.close()

    avg, _ = run_phase(update_phase)
    record(results, engine, n, "UPDATE", avg, f"(ops={n_upd})")

    # DELETE
    def delete_phase():
        database = open_db()
        for key in keys_del:
            try:
                database.delete(key)
            except bdb.DBNotFoundError:
                pass
        database.close()

    avg, _ = run_phase(delete_phase)
    record(results, engine, n, "DELETE", avg, f"(ops={n_del})")

    # LAST_READINGS: get last K keys per sensor (using in-memory index)
    def last_readings_phase():
        database = open_db()
        for mote in random_sensors:
            keys_for_sensor = per_sensor_keys[mote]
            last_keys = keys_for_sensor[-LAST_K:] if len(keys_for_sensor) > LAST_K else keys_for_sensor
            for key in last_keys:
                _ = database.get(key)
        database.close()

    avg, _ = run_phase(last_readings_phase)
    record(results, engine, n, "LAST_READINGS", avg, f"(for {len(random_sensors)} sensors)")

    # FULL_PROFILE: local stats (min/max/avg temp) for a few sensors
    def full_profile_phase():
        database = open_db()
        for mote in random_sensors[:5]:
            keys_for_sensor = per_sensor_keys[mote]
            temps = []
            for key in keys_for_sensor:
                val = database.get(key)
                if not val:
                    continue
                parts = val.decode("utf-8").split("|")
                if len(parts) < 2:
                    continue
                temps.append(float(parts[1]))
            if temps:
                _ = (min(temps), max(temps), sum(temps) / len(temps))
        database.close()

    avg, _ = run_phase(full_profile_phase)
    record(results, engine, n, "FULL_PROFILE", avg, "(local stats)")


#  MAIN 

def main():
    random.seed(RANDOM_SEED)
    results = []

    for n in N_LIST:
        print(f"\n===== N = {n} rows (realistic benchmark) =====")
        subset = FULL_DATA[:n]

        bench_duckdb(n, subset, results)
        bench_postgres(n, subset, results)
        bench_berkeley(n, subset, results)

    import csv as _csv
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


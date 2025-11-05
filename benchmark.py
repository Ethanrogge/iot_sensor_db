#!/usr/bin/env python3
"""
Benchmark comparatif DuckDB / PostgreSQL / BerkeleyDB
sur le dataset Intel Berkeley Lab (intel_sensors.csv).

On construit des paires (k, v) à partir du CSV :
  - k : entier séquentiel (0..N-1)
  - v : ligne capteur sérialisée "epoch,moteid,temperature,humidity,light,voltage"

Chaque phase est exécutée RUNS fois (1 warmup + moyenne sur RUNS-1).
Les résultats sont écrits dans kv_iot_results.csv.
"""

import os
import csv
import time
import random

import duckdb
import psycopg2
from psycopg2.extras import execute_values
from bsddb3 import db as bdb

# -------------------- CONFIG GÉNÉRALE --------------------

# Chemin vers CSV complet
DATASET_PATH = "/home/ethan/datasets/intel_sensors.csv"  

# Tailles de benchmark (objets = lignes k/v)
N_OBJECTS_LIST = [1_000, 10_000, 100_000]

# Facteurs d'opérations
READ_FACTOR = 1.0     # nombre de READ = N * READ_FACTOR
UPDATE_FACTOR = 0.5   # nombre d'UPDATE
DELETE_FACTOR = 0.1   # nombre de DELETE

RUNS = 6              # 1 warmup + 5 mesures
RANDOM_SEED = 42

# Fichiers physiques pour DuckDB et BDB
DUCKDB_FILE_TEMPLATE = "duckdb_kv_iot_{n}.db"
BDB_FILE_TEMPLATE = "bdb_kv_iot_{n}.db"

# Connexion PostgreSQL 
PG_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "sensors",
    "user": "ethan",
    "password": "password",
}

RESULTS_CSV = "kv_iot_results.csv"


# -------------------- UTILS GÉNÉRALES --------------------

def now():
    return time.time()


def run_phase(fn, runs=RUNS):
    """Exécute fn() plusieurs fois, retourne (avg_time, durations)."""
    durations = []
    for i in range(runs):
        t0 = now()
        fn()
        t1 = now()
        durations.append(t1 - t0)
    useful = durations[1:] if len(durations) > 1 else durations
    avg = sum(useful) / len(useful) if useful else 0.0
    return avg, durations


def record_result(results, engine, n, phase, ops, avg_time):
    thr = ops / avg_time if avg_time > 0 else float("inf")
    results.append({
        "engine": engine,
        "n_objects": n,
        "phase": phase,
        "operations": ops,
        "avg_duration_s": round(avg_time, 6),
        "throughput_ops_s": round(thr, 2),
    })
    print(f"[{engine:10}] N={n:8d} {phase:6} | "
          f"ops={ops:8d} | avg={avg_time:8.4f}s | thr={thr:10.2f} ops/s")


# -------------------- CHARGEMENT DU DATASET & CONSTRUCTION K/V --------------------

def build_kv_from_csv(max_n):
    """
    Lit intel_sensors.csv et construit jusqu'à max_n paires (k,v)
    en essayant d'équilibrer par capteur, mais en FORÇANT le nombre
    de lignes à max_n si le dataset contient suffisamment de données.

    v = "epoch,moteid,temperature,humidity,light,voltage"
    k = index séquentiel (0..max_n-1)
    """
    from collections import defaultdict

    print(f"Lecture du dataset réel : {DATASET_PATH}")
    per_sensor = defaultdict(list)

    # 1) lecture & regroupement par capteur
    with open(DATASET_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mote = int(row["moteid"])
            epoch = int(row["epoch"])
            value = "{epoch},{mote},{temp},{hum},{light},{volt}".format(
                epoch=epoch,
                mote=mote,
                temp=row["temperature"],
                hum=row["humidity"],
                light=row["light"],
                volt=row["voltage"],
            )
            per_sensor[mote].append((epoch, value))

    total_sensors = len(per_sensor)
    total_rows = sum(len(v) for v in per_sensor.values())
    print(f"Capteurs détectés : {total_sensors} | lignes totales : {total_rows:,}")

    # On ne peut pas dépasser les lignes réelles
    if max_n is None or max_n > total_rows:
        max_n = total_rows
    print(f"max_n effectif = {max_n:,}")

    # 2) trier chaque capteur par epoch
    for mote in per_sensor:
        per_sensor[mote].sort(key=lambda x: x[0])

    # 3) première passe : même nombre de lignes par capteur
    per_sensor_limit = max_n // total_sensors
    if per_sensor_limit < 1:
        per_sensor_limit = 1
    print(f"On vise ~{per_sensor_limit} lignes par capteur (1ère passe).")

    balanced_values = []
    index_by_sensor = {}

    for mote, readings in per_sensor.items():
        take = min(per_sensor_limit, len(readings))
        subset = readings[:take]
        balanced_values.extend(v for _, v in subset)
        index_by_sensor[mote] = take  # où on en est pour ce capteur

    # 4) seconde passe : compléter jusqu'à max_n en parcourant les sensors
    remaining = max_n - len(balanced_values)
    print(f"Après 1ère passe : {len(balanced_values):,} lignes, "
          f"on complète avec {remaining:,} supplémentaires pour atteindre max_n.")

    # boucle round-robin sur les capteurs tant qu'il reste des lignes
    sensors = list(per_sensor.keys())
    s_idx = 0
    while remaining > 0:
        mote = sensors[s_idx]
        readings = per_sensor[mote]
        idx = index_by_sensor[mote]
        if idx < len(readings):
            balanced_values.append(readings[idx][1])
            index_by_sensor[mote] = idx + 1
            remaining -= 1
        # passer au capteur suivant
        s_idx = (s_idx + 1) % len(sensors)
        # sécurité : si aucun capteur n'a plus de données, on sort
        if all(index_by_sensor[m] >= len(per_sensor[m]) for m in sensors):
            break

    # 5) au cas où on a (théoriquement) dépassé
    if len(balanced_values) > max_n:
        balanced_values = balanced_values[:max_n]

    print(f"{len(balanced_values):,} values sélectionnées au total.")

    # 6) attribution de clés séquentielles
    kv_pairs = [(i, v) for i, v in enumerate(balanced_values)]
    return kv_pairs


# On charge une fois pour toutes jusqu'au max de N_OBJECTS_LIST
MAX_N = max(N_OBJECTS_LIST)
FULL_DATA = build_kv_from_csv(MAX_N)
DATASET_SIZE = len(FULL_DATA)


# -------------------- DUCKDB --------------------

def duckdb_connect(n):
    dbfile = DUCKDB_FILE_TEMPLATE.format(n=n)
    if os.path.exists(dbfile):
        os.remove(dbfile)
    con = duckdb.connect(dbfile)
    con.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT);")
    return con


def duckdb_load(con, data_subset):
    con.executemany("INSERT INTO kv (k, v) VALUES (?, ?);", data_subset)


def duckdb_read(con, keys):
    for k in keys:
        con.execute("SELECT v FROM kv WHERE k = ?;", [k]).fetchone()


def duckdb_update(con, keys):
    for k in keys:
        con.execute("UPDATE kv SET v = v || '_u' WHERE k = ?;", [k])


def duckdb_delete(con, keys):
    for k in keys:
        con.execute("DELETE FROM kv WHERE k = ?;", [k])


def benchmark_duckdb(results, n, data_subset):
    engine = "duckdb"
    con = duckdb_connect(n)

    keys = [k for (k, _) in data_subset]
    n_read = int(READ_FACTOR * n)
    n_upd = int(UPDATE_FACTOR * n)
    n_del = int(DELETE_FACTOR * n)

    keys_read = random.choices(keys, k=n_read)
    keys_upd = random.choices(keys, k=n_upd)
    keys_del = random.sample(keys, min(n_del, len(keys)))

    def load_phase():
        con.execute("DELETE FROM kv;")
        duckdb_load(con, data_subset)

    avg, _ = run_phase(load_phase)
    record_result(results, engine, n, "LOAD", n, avg)

    def read_phase():
        duckdb_read(con, keys_read)

    avg, _ = run_phase(read_phase)
    record_result(results, engine, n, "READ", n_read, avg)

    def upd_phase():
        duckdb_update(con, keys_upd)

    avg, _ = run_phase(upd_phase)
    record_result(results, engine, n, "UPDATE", n_upd, avg)

    def del_phase():
        duckdb_delete(con, keys_del)

    avg, _ = run_phase(del_phase)
    record_result(results, engine, n, "DELETE", n_del, avg)

    con.close()


# -------------------- POSTGRESQL --------------------

def pg_connect():
    return psycopg2.connect(**PG_PARAMS)


def pg_prepare_table(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS kv;")
        cur.execute("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT);")
    conn.commit()


def pg_load(conn, data_subset):
    with conn.cursor() as cur:
        execute_values(cur, "INSERT INTO kv (k, v) VALUES %s;", data_subset, page_size=10_000)
    conn.commit()


def pg_read(conn, keys):
    with conn.cursor() as cur:
        for k in keys:
            cur.execute("SELECT v FROM kv WHERE k = %s;", (k,))
            cur.fetchone()


def pg_update(conn, keys):
    with conn.cursor() as cur:
        for k in keys:
            cur.execute("UPDATE kv SET v = v || '_u' WHERE k = %s;", (k,))
    conn.commit()


def pg_delete(conn, keys):
    with conn.cursor() as cur:
        for k in keys:
            cur.execute("DELETE FROM kv WHERE k = %s;", (k,))
    conn.commit()


def benchmark_postgres(results, n, data_subset):
    engine = "postgresql"
    conn = pg_connect()

    keys = [k for (k, _) in data_subset]
    n_read = int(READ_FACTOR * n)
    n_upd = int(UPDATE_FACTOR * n)
    n_del = int(DELETE_FACTOR * n)

    keys_read = random.choices(keys, k=n_read)
    keys_upd = random.choices(keys, k=n_upd)
    keys_del = random.sample(keys, min(n_del, len(keys)))

    def load_phase():
        pg_prepare_table(conn)
        pg_load(conn, data_subset)

    avg, _ = run_phase(load_phase)
    record_result(results, engine, n, "LOAD", n, avg)

    def read_phase():
        pg_read(conn, keys_read)

    avg, _ = run_phase(read_phase)
    record_result(results, engine, n, "READ", n_read, avg)

    def upd_phase():
        pg_update(conn, keys_upd)

    avg, _ = run_phase(upd_phase)
    record_result(results, engine, n, "UPDATE", n_upd, avg)

    def del_phase():
        pg_delete(conn, keys_del)

    avg, _ = run_phase(del_phase)
    record_result(results, engine, n, "DELETE", n_del, avg)

    conn.close()


# -------------------- BERKELEY DB --------------------

def bdb_open(fname):
    if os.path.exists(fname):
        os.remove(fname)
    database = bdb.DB()
    database.open(fname, None, bdb.DB_BTREE, bdb.DB_CREATE)
    return database


def bdb_load(database, data_subset):
    for k, v in data_subset:
        database.put(str(k).encode("utf-8"), v.encode("utf-8"))


def bdb_read(database, keys):
    for k in keys:
        database.get(str(k).encode("utf-8"))


def bdb_update(database, keys):
    for k in keys:
        key = str(k).encode("utf-8")
        val = database.get(key)
        if val is not None:
            database.put(key, val + b"_u")


def bdb_delete(database, keys):
    for k in keys:
        try:
            database.delete(str(k).encode("utf-8"))
        except bdb.DBNotFoundError:
            pass


def benchmark_berkeley(results, n, data_subset):
    engine = "berkeleydb"
    fname = BDB_FILE_TEMPLATE.format(n=n)
    database = bdb_open(fname)

    keys = [k for (k, _) in data_subset]
    n_read = int(READ_FACTOR * n)
    n_upd = int(UPDATE_FACTOR * n)
    n_del = int(DELETE_FACTOR * n)

    keys_read = random.choices(keys, k=n_read)
    keys_upd = random.choices(keys, k=n_upd)
    keys_del = random.sample(keys, min(n_del, len(keys)))

    def load_phase():
        # vide la base avant chaque run
        database.truncate()
        bdb_load(database, data_subset)

    avg, _ = run_phase(load_phase)
    record_result(results, engine, n, "LOAD", n, avg)

    def read_phase():
        bdb_read(database, keys_read)

    avg, _ = run_phase(read_phase)
    record_result(results, engine, n, "READ", n_read, avg)

    def upd_phase():
        bdb_update(database, keys_upd)

    avg, _ = run_phase(upd_phase)
    record_result(results, engine, n, "UPDATE", n_upd, avg)

    def del_phase():
        bdb_delete(database, keys_del)

    avg, _ = run_phase(del_phase)
    record_result(results, engine, n, "DELETE", n_del, avg)

    database.close()


# -------------------- MAIN --------------------

def main():
    random.seed(RANDOM_SEED)
    results = []

    for n in N_OBJECTS_LIST:
        if n > DATASET_SIZE:
            print(f"\n===== N = {n} > DATASET_SIZE ({DATASET_SIZE}), on saute. =====")
            continue

        print(f"\n===== N = {n} objets (paires k/v issues du dataset réel) =====")
        data_subset = FULL_DATA[:n]

        benchmark_duckdb(results, n, data_subset)
        benchmark_postgres(results, n, data_subset)
        benchmark_berkeley(results, n, data_subset)

    # écriture CSV
    import csv as _csv
    with open(RESULTS_CSV, "w", newline="") as f:
        writer = _csv.DictWriter(
            f,
            fieldnames=["engine", "n_objects", "phase",
                        "operations", "avg_duration_s", "throughput_ops_s"],
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nRésultats enregistrés dans {RESULTS_CSV}")


if __name__ == "__main__":
    main()

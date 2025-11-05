#!/usr/bin/env python3
"""
IoT Sensor Mini-App (version full columns) using BerkeleyDB + DuckDB.

Dataset CSV: intel_sensors.csv
Columns: date,time,epoch,moteid,temperature,humidity,light,voltage
"""

import os
import csv
from collections import defaultdict

import duckdb
from bsddb3 import db as bdb

# -------------------- CONFIG --------------------

DATASET_PATH = "/home/ethan/datasets/intel_sensors.csv"  # adapte si besoin

BDB_FILE = "iot_sensors_full_bdb.db"
DUCKDB_FILE = "iot_sensors_full_duck.db"

MAX_ROWS = 10_044     # max de lignes à charger (peut être None pour tout)
MAX_PER_SENSOR = 10_000   # dernières mesures par capteur en BDB


# -------------------- CHARGEMENT DU CSV COMPLET --------------------

def load_sensor_dataset(limit=MAX_ROWS):
    """
    Charge le dataset complet et sélectionne un nombre équilibré de lignes
    par capteur (~ limit / nb de capteurs), conservant l'ordre chronologique.

    Colonnes attendues :
      date,time,epoch,moteid,temperature,humidity,light,voltage
    Retour :
      [(date, time, epoch, moteid, temperature, humidity, light, voltage), ...]
    """
    import csv
    from collections import defaultdict
    import random

    print(f"Loading sensor dataset from {DATASET_PATH} ...")
    data_per_sensor = defaultdict(list)

    # 1) Lire toutes les lignes et regrouper par capteur
    with open(DATASET_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mote = int(row["moteid"])
            entry = (
                row["date"],
                row["time"],
                int(row["epoch"]),
                mote,
                float(row["temperature"]),
                float(row["humidity"]),
                float(row["light"]),
                float(row["voltage"]),
            )
            data_per_sensor[mote].append(entry)

    total_sensors = len(data_per_sensor)
    total_rows = sum(len(v) for v in data_per_sensor.values())
    print(f"Detected {total_sensors} sensors with {total_rows:,} total rows.")

    # 2) Trier chaque capteur par timestamp
    for mote in data_per_sensor:
        data_per_sensor[mote].sort(key=lambda x: x[2])  # x[2] = epoch

    # 3) Déterminer combien de lignes par capteur
    if limit is None or limit > total_rows:
        limit = total_rows
    per_sensor_limit = max(1, limit // total_sensors)
    print(f"Selecting ~{per_sensor_limit} chronological rows per sensor.")

    # 4) Prendre un sous-ensemble équitable par capteur, en respectant l'ordre
    balanced_rows = []
    for mote, readings in data_per_sensor.items():
        if len(readings) > per_sensor_limit:
            # Choisir une fenêtre continue aléatoire dans la série temporelle
            start_index = random.randint(0, len(readings) - per_sensor_limit)
            subset = readings[start_index : start_index + per_sensor_limit]
            balanced_rows.extend(subset)
        else:
            balanced_rows.extend(readings)

    # 5) Mélanger globalement pour ne pas avoir tout un capteur d’un bloc
    random.shuffle(balanced_rows)

    print(f"Loaded {len(balanced_rows):,} balanced chronological rows from all sensors.")
    return balanced_rows


# -------------------- BERKELEYDB : stockage recent par capteur --------------------

def build_berkeleydb(data):
    """
    key = moteid
    value = liste de mesures encodées en texte :
      "epoch|date|time|temp|hum|light|volt;..."
    """
    if os.path.exists(BDB_FILE):
        os.remove(BDB_FILE)

    print("Building BerkeleyDB (moteid → recent readings)...")

    per_sensor = defaultdict(list)
    for date, time_, epoch, mote, temp, hum, light, volt in data:
        per_sensor[mote].append((epoch, date, time_, temp, hum, light, volt))

    database = bdb.DB()
    database.open(BDB_FILE, None, bdb.DB_BTREE, bdb.DB_CREATE)

    for mote, readings in per_sensor.items():
        if len(readings) > MAX_PER_SENSOR:
            readings = readings[-MAX_PER_SENSOR:]
        value_str = ";".join(
            f"{epoch}|{date}|{time_}|{temp:.2f}|{hum:.2f}|{light:.0f}|{volt:.2f}"
            for epoch, date, time_, temp, hum, light, volt in readings
        )
        database.put(str(mote).encode("utf-8"), value_str.encode("utf-8"))

    database.close()
    print(f"BerkeleyDB built with {len(per_sensor):,} sensors.")


def get_sensor_readings_bdb(mote_id):
    """
    Retourne la liste des mesures pour un capteur depuis BDB :

      [(epoch, date, time, temp, hum, light, volt), ...]
    """
    database = bdb.DB()
    database.open(BDB_FILE, None, bdb.DB_BTREE, bdb.DB_RDONLY)
    val = database.get(str(mote_id).encode("utf-8"))
    database.close()

    if val is None:
        return None

    readings = []
    for part in val.decode("utf-8").split(";"):
        if not part:
            continue
        epoch_s, date, time_, temp_s, hum_s, light_s, volt_s = part.split("|")
        readings.append((
            int(epoch_s),
            date,
            time_,
            float(temp_s),
            float(hum_s),
            float(light_s),
            float(volt_s),
        ))
    return readings


def show_last_readings_bdb(mote_id, limit=10):
    readings = get_sensor_readings_bdb(mote_id)
    if not readings:
        print(f"❌ Sensor {mote_id} not found in BerkeleyDB.")
        return
    print(f"Last {min(limit, len(readings))} readings for sensor {mote_id}:")
    for epoch, date, time_, temp, hum, light, volt in readings[-limit:]:
        print(f"  {date} {time_} | epoch={epoch} | "
              f"temp={temp:.2f}°C | hum={hum:.2f}% | light={light:.0f} | volt={volt:.2f}V")


# -------------------- DUCKDB : base analytique complète --------------------

def build_duckdb(data):
    """
    Table readings avec toutes les colonnes :
      date, time, epoch, moteid, temperature, humidity, light, voltage
    """
    if os.path.exists(DUCKDB_FILE):
        os.remove(DUCKDB_FILE)

    print("Building DuckDB analytical database...")
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("""
        CREATE TABLE readings (
            date TEXT,
            time TEXT,
            epoch BIGINT,
            moteid INTEGER,
            temperature DOUBLE,
            humidity DOUBLE,
            light DOUBLE,
            voltage DOUBLE
        );
    """)
    con.executemany(
        "INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
        data,
    )
    con.close()
    print("DuckDB ready.")


def avg_temperature_for_sensor(mote_id):
    con = duckdb.connect(DUCKDB_FILE)
    avg_temp = con.execute(
        "SELECT AVG(temperature) FROM readings WHERE moteid = ?;",
        [mote_id],
    ).fetchone()[0]
    con.close()
    if avg_temp is None:
        print(f"No data for sensor {mote_id}.")
    else:
        print(f"Average temperature for sensor {mote_id}: {avg_temp:.2f}°C")


def sensors_with_high_avg_temp(threshold=30.0, min_readings=100):
    con = duckdb.connect(DUCKDB_FILE)
    res = con.execute(
        """
        SELECT moteid, COUNT(*) AS n, AVG(temperature) AS avg_temp
        FROM readings
        GROUP BY moteid
        HAVING n >= ? AND avg_temp >= ?
        ORDER BY avg_temp DESC;
        """,
        [min_readings, threshold],
    ).fetchall()
    con.close()

    if not res:
        print("No sensors exceed the threshold.")
        return

    print(f"Sensors with avg temperature >= {threshold}°C (n >= {min_readings}):")
    for moteid, n, avg_temp in res:
        print(f"  Sensor {moteid:<5} | avg={avg_temp:.2f}°C | readings={n}")


# -------------------- HYBRIDE : anomalies --------------------

def detect_anomalies_for_sensor(mote_id, z_threshold=2.5):
    """
    DuckDB: calcule moyenne & écart-type global pour ce capteur.
    BerkeleyDB: regarde les mesures récentes et trouve celles loin de la moyenne.
    """
    con = duckdb.connect(DUCKDB_FILE)
    row = con.execute(
        "SELECT AVG(temperature), STDDEV_POP(temperature) "
        "FROM readings WHERE moteid = ?;",
        [mote_id],
    ).fetchone()
    con.close()

    if not row or row[0] is None or row[1] is None or row[1] == 0:
        print(f"Not enough data for sensor {mote_id}.")
        return

    mean_temp, std_temp = row
    readings = get_sensor_readings_bdb(mote_id)
    if not readings:
        print(f"No recent readings in BerkeleyDB for sensor {mote_id}.")
        return

    anomalies = []
    for epoch, date, time_, temp, hum, light, volt in readings:
        z = abs(temp - mean_temp) / std_temp
        if z > z_threshold:
            anomalies.append((epoch, date, time_, temp, z))

    print(f"Global mean for sensor {mote_id}: {mean_temp:.2f}°C (σ={std_temp:.2f})")
    if not anomalies:
        print(f"No anomalies detected above z={z_threshold:.1f}.")
        return

    print(f"Anomalies for sensor {mote_id} (z > {z_threshold:.1f}):")
    for epoch, date, time_, temp, z in anomalies[-10:]:
        print(f"  {date} {time_} | epoch={epoch} | temp={temp:.2f}°C | z={z:.2f}")


# -------------------- MAIN MENU --------------------

def main():
    # s'il n'y a pas encore nos DB, on les (re)construit à partir du CSV complet
    if not (os.path.exists(BDB_FILE) and os.path.exists(DUCKDB_FILE)):
        data = load_sensor_dataset(limit=MAX_ROWS)
        build_berkeleydb(data)
        build_duckdb(data)
    else:
        print("Using existing BerkeleyDB and DuckDB files.")

    while True:
        print("\n========= IoT Sensor Mini App (full) =========")
        print("1. Show last readings of a sensor (BerkeleyDB)")
        print("2. Show average temperature of a sensor (DuckDB)")
        print("3. Show sensors with high average temperature (DuckDB)")
        print("4. Detect anomalies for a sensor (Hybrid)")
        print("5. Exit")
        choice = input("Select option: ").strip()

        if choice == "1":
            mote = int(input("Enter sensor ID (moteid): "))
            show_last_readings_bdb(mote)
        elif choice == "2":
            mote = int(input("Enter sensor ID (moteid): "))
            avg_temperature_for_sensor(mote)
        elif choice == "3":
            th = float(input("Threshold temperature (°C), e.g. 30: "))
            sensors_with_high_avg_temp(threshold=th)
        elif choice == "4":
            mote = int(input("Enter sensor ID (moteid): "))
            detect_anomalies_for_sensor(mote)
        elif choice == "5":
            print("Bye 👋")
            break
        else:
            print("Invalid choice, try again.")


if __name__ == "__main__":
    main()

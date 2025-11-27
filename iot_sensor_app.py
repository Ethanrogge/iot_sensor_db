"""
Unified IoT Sensor Application (BerkeleyDB + DuckDB)
----------------------------------------------------
Dataset: intel_sensors.csv
Columns: date,time,epoch,moteid,temperature,humidity,light,voltage
"""

import os
import csv
import random
from collections import defaultdict
from datetime import datetime

import duckdb
from bsddb3 import db as bdb


# =============================
# CONFIGURATION
# =============================

DATASET_PATH = "/home/ethan/datasets/intel_sensors.csv"
BDB_FILE = "iot_sensors_full_bdb.db"
DUCKDB_FILE = "iot_sensors_full_duck.db"
MAX_ROWS = 10_044
MAX_PER_SENSOR = 10_000
LAST_K = 50
TEMP_THRESHOLD_DEFAULT = 30.0
MIN_READINGS_HOT = 100


# =============================
# UTILITAIRES GÉNÉRIQUES
# =============================

def fmt_epoch(ep):
    try:
        return datetime.utcfromtimestamp(int(ep)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ep)

def print_table(rows, headers):
    if not rows:
        print("(no data)")
        return
    widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0))
              for i, h in enumerate(headers)]
    print(" | ".join(str(h).ljust(widths[i]) for i, h in enumerate(headers)))
    print("-+-".join("-"*w for w in widths))
    for r in rows:
        print(" | ".join(str(r[i]).ljust(widths[i]) for i, h in enumerate(headers)))


# =============================
# CHARGEMENT ET CONSTRUCTION DB
# =============================

def load_dataset(limit=MAX_ROWS):
    print(f"Loading dataset from {DATASET_PATH} ...")
    per_sensor = defaultdict(list)
    with open(DATASET_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mote = int(row["moteid"])
            per_sensor[mote].append((
                row["date"], row["time"], int(row["epoch"]), mote,
                float(row["temperature"]), float(row["humidity"]),
                float(row["light"]), float(row["voltage"])
            ))
    for mote in per_sensor:
        per_sensor[mote].sort(key=lambda x: x[2])
    total = sum(len(v) for v in per_sensor.values())
    print(f"{len(per_sensor)} sensors, {total:,} total rows.")
    if limit < total:
        per_sensor_limit = limit // len(per_sensor)
        rows = []
        for v in per_sensor.values():
            rows.extend(v[:per_sensor_limit])
        random.shuffle(rows)
        return rows
    else:
        return [r for v in per_sensor.values() for r in v]


def build_bdb(data):
    if os.path.exists(BDB_FILE):
        os.remove(BDB_FILE)
    print("Building BerkeleyDB...")
    dbh = bdb.DB()
    dbh.open(BDB_FILE, None, bdb.DB_BTREE, bdb.DB_CREATE)
    per_sensor = defaultdict(list)
    for d, t, e, m, te, h, l, v in data:
        per_sensor[m].append((e, d, t, te, h, l, v))
    for mote, readings in per_sensor.items():
        readings = readings[-MAX_PER_SENSOR:]
        val = ";".join(f"{e}|{d}|{t}|{te:.2f}|{h:.2f}|{l:.0f}|{v:.2f}" for e, d, t, te, h, l, v in readings)
        dbh.put(str(mote).encode(), val.encode())
    dbh.close()
    print("✅ BerkeleyDB ready.")


def build_duckdb(data):
    if os.path.exists(DUCKDB_FILE):
        os.remove(DUCKDB_FILE)
    print("Building DuckDB...")
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("""
        CREATE TABLE readings (
            date TEXT, time TEXT, epoch BIGINT, moteid INT,
            temperature DOUBLE, humidity DOUBLE, light DOUBLE, voltage DOUBLE
        );
    """)
    con.executemany("INSERT INTO readings VALUES (?, ?, ?, ?, ?, ?, ?, ?);", data)
    con.close()
    print("✅ DuckDB ready.")


def ensure_databases():
    if not (os.path.exists(BDB_FILE) and os.path.exists(DUCKDB_FILE)):
        data = load_dataset()
        build_bdb(data)
        build_duckdb(data)
    else:
        print("Using existing databases.")


# =============================
# BDB ACCESS
# =============================

def get_recent_from_bdb(moteid, k=LAST_K):
    if not os.path.exists(BDB_FILE):
        return None
    dbh = bdb.DB()
    dbh.open(BDB_FILE, None, bdb.DB_BTREE, bdb.DB_RDONLY)
    val = dbh.get(str(moteid).encode())
    dbh.close()
    if not val:
        return None
    parts = val.decode().split(";")
    last = parts[-k:] if len(parts) > k else parts
    out = []
    for p in last:
        ep, d, t, te, h, l, v = p.split("|")
        out.append((int(ep), float(te), float(h), float(l), float(v)))
    out.sort(key=lambda x: x[0], reverse=True)
    return out


# =============================
# FONCTIONNALITÉS
# =============================

def last_readings(moteid):
    rec = get_recent_from_bdb(moteid)
    if not rec:
        print("No data.")
        return
    rows = [(fmt_epoch(e), round(t,2), round(h,2), int(l), round(v,2)) for e,t,h,l,v in rec]
    print_table(rows[:10], ["time","temp","hum","light","volt"])

def avg_temp(moteid):
    with duckdb.connect(DUCKDB_FILE) as con:
        val = con.execute("SELECT AVG(temperature) FROM readings WHERE moteid=?;", [moteid]).fetchone()[0]
    print(f"Average temperature for sensor {moteid}: {round(val,2) if val else 'NA'} °C")

def sensors_hot(threshold=30.0, min_n=100):
    with duckdb.connect(DUCKDB_FILE) as con:
        rows = con.execute("""
            SELECT moteid, COUNT(*) n, AVG(temperature) avg
            FROM readings GROUP BY moteid
            HAVING n>=? AND avg>=? ORDER BY avg DESC;
        """, [min_n, threshold]).fetchall()
    print_table(rows, ["moteid","n","avg_temp"])

def anomalies(moteid, z=2.5):
    with duckdb.connect(DUCKDB_FILE) as con:
        mean, sd = con.execute("SELECT AVG(temperature), STDDEV_POP(temperature) FROM readings WHERE moteid=?;", [moteid]).fetchone()
    if not sd or sd == 0:
        print("Not enough data.")
        return
    recent = get_recent_from_bdb(moteid)
    out = []
    for e,t,h,l,v in recent:
        zz = abs(t - mean)/sd
        if zz > z:
            out.append((fmt_epoch(e), round(t,2), round(zz,2)))
    print(f"Anomalies for sensor {moteid} (z>{z}):")
    print_table(out, ["time","temp","zscore"])

def daily_summary(moteid):
    with duckdb.connect(DUCKDB_FILE) as con:
        rows = con.execute("""
            SELECT date_trunc('day', to_timestamp(epoch)) d,
                   COUNT(*), ROUND(AVG(temperature),2),
                   ROUND(MIN(temperature),2), ROUND(MAX(temperature),2)
            FROM readings WHERE moteid=? GROUP BY d ORDER BY d;
        """,[moteid]).fetchall()
    print_table(rows,["day","n","avg","min","max"])

def top_k(metric="temp", k=10):
    col = "temperature" if metric.startswith("t") else "humidity"
    with duckdb.connect(DUCKDB_FILE) as con:
        rows = con.execute(f"""
            SELECT moteid, COUNT(*) n, ROUND(AVG({col}),2) avg
            FROM readings GROUP BY moteid ORDER BY avg DESC LIMIT ?;
        """,[k]).fetchall()
    print_table(rows,["moteid","n","avg"])

def outages(moteid, gap=600):
    with duckdb.connect(DUCKDB_FILE) as con:
        rows = con.execute("""
            WITH o AS (
              SELECT epoch, LAG(epoch) OVER (ORDER BY epoch) prev
              FROM readings WHERE moteid=?)
            SELECT prev, epoch, (epoch-prev)
            FROM o WHERE prev IS NOT NULL AND (epoch-prev)>?;""",[moteid,gap]).fetchall()
    out = [(fmt_epoch(a), fmt_epoch(b), g) for a,b,g in rows]
    print_table(out,["prev","next","gap_sec"])

def compare_two(a,b):
    with duckdb.connect(DUCKDB_FILE) as con:
        avgs = con.execute("SELECT moteid,AVG(temperature) FROM readings WHERE moteid IN (?,?) GROUP BY moteid;",[a,b]).fetchall()
        corr = con.execute("""
            SELECT corr(a.temperature,b.temperature)
            FROM (SELECT epoch,temperature FROM readings WHERE moteid=?) a
            JOIN (SELECT epoch,temperature FROM readings WHERE moteid=?) b USING(epoch);
        """,[a,b]).fetchone()[0]
    print(f"Avg temp A({a})={avgs[0][1]:.2f}°C, B({b})={avgs[1][1]:.2f}°C")
    print(f"Correlation: {round(corr,3) if corr else 'NA'}")

def sensor_profile(moteid):
    recent = get_recent_from_bdb(moteid)
    rows = [(fmt_epoch(e), round(t,2), round(h,2), int(l), round(v,2)) for e,t,h,l,v in recent[:10]]
    print_table(rows,["time","temp","hum","light","volt"])
    with duckdb.connect(DUCKDB_FILE) as con:
        n,avgT,minT,maxT,avgH = con.execute("SELECT COUNT(*),AVG(temperature),MIN(temperature),MAX(temperature),AVG(humidity) FROM readings WHERE moteid=?;",[moteid]).fetchone()
    print(f"\nStats: n={n}, avg={avgT:.2f}°C, min={minT:.2f}, max={maxT:.2f}, hum={avgH:.2f}%")

def sql_console():
    print("DuckDB SQL console (empty line to quit)")
    with duckdb.connect(DUCKDB_FILE) as con:
        while True:
            q = input("SQL> ").strip()
            if not q: break
            try:
                res = con.execute(q).fetchmany(10)
                for r in res: print(r)
            except Exception as e:
                print("Error:", e)


# =============================
# MENU UNIQUE
# =============================

def print_menu():
    print("""
========= IoT Sensor Application =========
 1. Show last readings of a sensor
 2. Average temperature of a sensor
 3. Sensors with high avg temperature
 4. Detect anomalies for a sensor
 5. Daily summary for a sensor
 6. Top-K sensors (temp/hum)
 7. Detect outages (gaps)
 8. Compare two sensors
 9. Full sensor profile
10. SQL console
11. Exit
""")

def main():
    ensure_databases()
    while True:
        print_menu()
        c = input("Select option: ").strip()
        if c == "1":
            last_readings(int(input("Sensor id: ")))
        elif c == "2":
            avg_temp(int(input("Sensor id: ")))
        elif c == "3":
            sensors_hot()
        elif c == "4":
            anomalies(int(input("Sensor id: ")))
        elif c == "5":
            daily_summary(int(input("Sensor id: ")))
        elif c == "6":
            metric = input("Metric (temp/hum): ").strip()
            top_k(metric)
        elif c == "7":
            outages(int(input("Sensor id: ")))
        elif c == "8":
            a = int(input("Sensor A: "))
            b = int(input("Sensor B: "))
            compare_two(a,b)
        elif c == "9":
            sensor_profile(int(input("Sensor id: ")))
        elif c == "10":
            sql_console()
        elif c == "11":
            print("Bye")
            break
        else:
            print("Invalid choice.")

if __name__ == "__main__":
    main()

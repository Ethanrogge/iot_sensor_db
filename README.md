# 🛰️ IoT Sensor Embedded Database Project
**Hybrid local data management using DuckDB and BerkeleyDB on the Intel Berkeley Research Lab dataset**

## 📖 Overview
This project demonstrates how **embedded databases** can efficiently handle **IoT sensor data** directly on local devices — without a centralized DB server.

It combines:
- **BerkeleyDB** → lightweight key–value store for recent sensor readings (fast lookup per sensor)
- **DuckDB** → analytical in-process database for local queries, statistics, and anomaly detection
- (Optionally, **PostgreSQL**) → as a baseline for performance comparison

The system uses the **Intel Berkeley Research Lab dataset**, containing real sensor readings from 54 wireless motes (temperature, humidity, light, voltage).  
This dataset provides over **2.3 million records** collected every 31 seconds — an ideal case for embedded database experiments.

## ⚙️ Features
- ✅ Full use of the original dataset (all columns preserved)
- ✅ Balanced sampling across all 54 sensors (~equal number of rows per sensor)
- ✅ Chronological consistency within each sensor’s time series
- ✅ Automatic build of both BerkeleyDB and DuckDB databases
- ✅ Interactive CLI for analysis:
  1. Show last readings of a sensor (BerkeleyDB)
  2. Show average temperature of a sensor (DuckDB)
  3. List sensors exceeding a temperature threshold (DuckDB)
  4. Detect anomalies (hybrid use of both databases)

## 🧰 Requirements
Python ≥ 3.9  
Required libraries:
```bash
pip install duckdb bsddb3
```

## 🧠 Dataset: Intel Berkeley Research Lab Sensor Data
- 📦 Source: [Kaggle — Intel Berkeley Research Lab Sensor Data](https://www.kaggle.com/datasets/divyansh22/intel-berkeley-research-lab-sensor-data)
- Original columns:
  ```
  date,time,epoch,moteid,temperature,humidity,light,voltage
  ```
- The file is typically `data.txt` (space-separated).  
  Convert it once to CSV format (comma-separated):
```bash
cd ~/datasets/intel_lab

python3 - <<'PYCODE'
import csv
with open("data.txt") as fin, open("intel_sensors.csv", "w", newline="") as fout:
    writer = csv.writer(fout)
    writer.writerow(["date","time","epoch","moteid","temperature","humidity","light","voltage"])
    fin.readline()  # skip header
    for line in fin:
        parts = line.strip().split()
        if len(parts) == 8:
            writer.writerow(parts)
print("✅ intel_sensors.csv created successfully.")
PYCODE
```

Result:
```
date,time,epoch,moteid,temperature,humidity,light,voltage
2004-02-28,04:00:00.000,1077926400,1,23.5,45.9,364,2.64
...
```

## 🧩 Project Structure
```
iot-sensor-app/
│
├── iot_sensor_app_full.py        # Main application script
├── intel_sensors.csv             # Cleaned dataset
├── iot_sensors_full_duck.db      # DuckDB database (auto-generated)
├── iot_sensors_full_bdb.db       # BerkeleyDB database (auto-generated)
├── README.md                     # You are here
└── requirements.txt              # Optional dependency list
```

## 🚀 Usage
### 1. Configure paths
Edit the variable in `iot_sensor_app.py`:
```python
DATASET_PATH = "/home/ethan/datasets/intel_lab/intel_sensors.csv"
```

### 2. Run the application
```bash
python3 iot_sensor_app.py
```

On first run:
- Loads up to `MAX_ROWS` (default = 1 000 000)
- Builds balanced and chronological samples across all sensors
- Creates `iot_sensors_full_duck.db` and `iot_sensors_full_bdb.db`

### 3. Interactive menu
```
========= IoT Sensor Mini App (full) =========
1. Show last readings of a sensor (BerkeleyDB)
2. Show average temperature of a sensor (DuckDB)
3. Show sensors with high average temperature (DuckDB)
4. Detect anomalies for a sensor (Hybrid)
5. Exit
```

## 📊 Benchmark Overview

The file [`benchmark.py`](benchmark.py) performs a **systematic performance comparison** between:

| Database | Type | Description |
|-----------|------|-------------|
| **DuckDB** | Embedded, columnar | Fast analytical queries in-process |
| **BerkeleyDB** | Embedded, key-value | Lightweight, low-latency data store |
| **PostgreSQL** | Server-based | Full relational baseline |

---

## 🧮 Benchmark Design

Each benchmark iteration measures the performance of four core operations:

| Operation | Description |
|------------|-------------|
| **LOAD**   | Insert all rows (initial data loading) |
| **READ**   | Random reads (SELECT/GET) |
| **UPDATE** | Modify random rows |
| **DELETE** | Remove random keys |

For each operation, the script computes:
- Average duration across 6 runs (1 warmup + 5 measurements)  
- Throughput in operations per second (`ops/s`)

The dataset is read directly from the **real Intel CSV**, with pairs `(k, v)` built as:
```
k = unique integer key
v = "epoch,moteid,temperature,humidity,light,voltage"
```

The number of test rows (`N`) is configurable in:
```python
N_OBJECTS_LIST = [1_000, 10_000, 100_000, 1_000_000]
```

---

## 🧠 Sampling Logic

Originally, the dataset was balanced across sensors.  
To better simulate large-scale testing, the benchmark now **forces exactly N rows**, even if that slightly breaks the per-sensor balance.

The new sampling strategy:
1. Takes ~equal number of rows per sensor (chronologically sorted)
2. Fills the remaining quota in a round-robin fashion until reaching exactly `max_n`

Example console output:

```
Capteurs détectés : 55 | lignes totales : 2,219,802
max_n effectif = 100000
On vise ~1818 lignes par capteur (1ère passe).
Après 1ère passe : 98,982 lignes, on complète avec 1,018 supplémentaires pour atteindre max_n.
100,000 values sélectionnées au total.
```

---

## ⚙️ Database Setup

### PostgreSQL Configuration
Create the database and user before running the benchmark:

```bash
sudo -u postgres psql
CREATE DATABASE sensors;
CREATE USER ethan WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE sensors TO ethan;
GRANT CREATE ON SCHEMA public TO ethan;
ALTER SCHEMA public OWNER TO ethan;
\q
```

Edit connection parameters if needed in the benchmark script:
```python
PG_PARAMS = {
    "dbname": "sensors",
    "user": "ethan",
    "password": "password",
    "host": "localhost",
    "port": 5432
}
```

---

## 🧪 Running the Benchmark

```bash
python3 benchmark_iot_kv.py
```

Example output snippet:

```
===== N = 10000 objets (paires k/v issues du dataset réel) =====

[duckdb     ] N=10000 LOAD   | ops=10000 | avg=0.4215s | thr=23725.88 ops/s
[duckdb     ] N=10000 READ   | ops=10000 | avg=0.0574s | thr=174241.55 ops/s
[berkeleydb ] N=10000 LOAD   | ops=10000 | avg=0.2914s | thr=34299.41 ops/s
[postgresql ] N=10000 LOAD   | ops=10000 | avg=1.2253s | thr=8162.42 ops/s
...
Résultats enregistrés dans kv_iot_results.csv
```

---

## 📈 Result Storage

All measurements are exported to `kv_iot_results.csv` with the following columns:

| Column | Description |
|---------|-------------|
| `engine` | duckdb / berkeleydb / postgresql |
| `n_objects` | number of key/value pairs tested |
| `phase` | LOAD / READ / UPDATE / DELETE |
| `operations` | total operations performed |
| `avg_duration_s` | mean duration (seconds) |
| `throughput_ops_s` | throughput (operations per second) |

Example session:
```
Select option: 2
Enter sensor ID (moteid): 17
Average temperature for sensor 17: 24.18°C
```

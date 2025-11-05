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
Edit the variable in `iot_sensor_app_full.py`:
```python
DATASET_PATH = "/home/ethan/datasets/intel_lab/intel_sensors.csv"
```

### 2. Run the application
```bash
python3 iot_sensor_app_full.py
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

Example session:
```
Select option: 2
Enter sensor ID (moteid): 17
Average temperature for sensor 17: 24.18°C
```

## 📊 Benchmark Integration
This application can serve as the **core workload** for your benchmark comparison:

| Database | Use | Metric examples |
|-----------|-----|----------------|
| **BerkeleyDB** | Local key-value access (recent readings) | Access latency (GET/PUT), memory footprint |
| **DuckDB** | Analytical queries (averages, thresholds) | Query execution time, scalability with rows |
| **PostgreSQL** | Relational baseline | Query time vs. DuckDB (1K, 10K, 100K, 1M rows) |

You can reuse the same `intel_sensors.csv` for all three systems and measure:
- loading time  
- query execution time  
- scaling behavior (linear vs. exponential)

## 🧮 Sampling Logic
To ensure balanced datasets:
```python
per_sensor_limit = MAX_ROWS // total_sensors
```
Each sensor contributes roughly this number of rows, chosen as a **chronological window** in its timeline:
```python
start_index = random.randint(0, len(readings) - per_sensor_limit)
subset = readings[start_index:start_index + per_sensor_limit]
```
This preserves the time order while ensuring fair distribution.

## 🧠 Example Report Paragraph
> To simulate realistic embedded data processing, we used the Intel Berkeley Research Lab dataset.  
> Each sensor (mote) provided a balanced subset of approximately *N* records to form a representative sample across all 54 devices.  
> BerkeleyDB was used for recent data caching and per-sensor retrieval, while DuckDB handled analytical queries such as temperature averages and anomaly detection.  
> This hybrid architecture reflects edge analytics patterns where data is stored and processed locally, minimizing latency and external dependencies.

## 🧪 Future Extensions
- Add **CLI arguments** (`--limit`, `--seed`)
- Integrate **PostgreSQL** for external benchmark comparison
- Add **matplotlib** visualizations (temperature over time)
- Include **parallel loading** for faster preprocessing

## 📄 License
MIT License — you’re free to use, modify, and distribute with attribution.

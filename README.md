# Benchmarking Embedded Databases for IoT Workloads

This project benchmarks three database management systems in the context of an
IoT sensor workload:

- **BerkeleyDB** — embedded key–value store  
- **DuckDB** — embedded analytical SQL engine  
- **PostgreSQL** — traditional relational DBMS (baseline reference)

The objective is to evaluate how these systems behave under different workloads
(OLTP-like vs analytical) and dataset sizes ranging from **1 000 to 1 000 000 rows**.

---

## 1. Project Structure

```
.
├── benchmark.py
├── plot.py
├── benchmark_results.csv
├── fig/
│   └── *.png
├── data/
│   └── sensors.csv
└── README.md
```

---

## 2. Dataset (From Scratch)

### 2.1 Dataset Description

This project uses the **Intel Berkeley Research Lab sensor dataset**, a real-world
IoT dataset containing time-series measurements collected from **54 wireless
sensors** deployed in an indoor environment.

Each record contains:
- date
- time
- epoch
- moteid
- temperature
- humidity
- light
- voltage

The full dataset contains more than **2.3 million sensor readings**.

---

### 2.2 Downloading the Dataset

The dataset can be obtained from Kaggle:

1. Go to https://www.kaggle.com/datasets/divyansh22/intel-berkeley-research-lab-sensor-data?resource=download
2. Search for **Intel Berkeley Research Lab Sensor Data**
3. Download

---

### 2.3 Preparing the Dataset

```bash
unzip data.txt.zip
mkdir -p data
sed 's/[[:space:]]\+/,/g' data.txt > data/sensors.csv
```

The resulting file must be located at:

```
data/sensors.csv
```

⚠️ The dataset is intentionally not included in the repository due to its size.

---

## 3. Requirements

```bash
pip install duckdb psycopg2-binary pandas matplotlib numpy
sudo apt install libdb-dev python3-bsddb3 postgresql
```

---

## 4. Running the Benchmark

```bash
python benchmark.py
```

---

## 5. Generating the Plots

```bash
python plot.py
```

---

## 6. Benchmark Phases

OLTP-like:
- LOAD
- READ
- UPDATE
- DELETE
- LAST_READINGS

Analytical:
- AVG_SENSOR
- HOT_SENSORS
- ANOMALIES
- DAILY_SUMMARY
- OUTAGES
- TOPK_SENSORS
- COMPARE_TWO
- FULL_PROFILE

---

## 7. Reproducibility

All measurements are stored in `benchmark_results.csv`.

---

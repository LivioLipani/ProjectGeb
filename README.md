# Seismic Detection Demo - Project GEB

Final project for the **Technologies for Advanced Programming (TAP)** course —
Bachelor's degree in Computer Science, University of Catania.

The project is a real-time ML pipeline that classifies seismic signals as earthquake or noise.
A Random Forest model trained on the STEAD dataset runs fully offline inside a Docker Compose environment,
from data ingestion (Redpanda) through inference (MLflow + Spark) to storage (ClickHouse) and live visualization (Grafana).

---

# Setup

## Requirements
- Docker ≥ 24 + Docker Compose v2
- Git LFS

## Project structure
```
seismic-detection-demo/
├── docker-compose.yml
├── prepare_subset.py
├── .gitattributes              # Git LFS for .h5 files
├── .gitignore
├── README.md
├── data/                       # STEAD dataset (.h5 via Git LFS) + metadata
├── producer/                   # Kafka producer (pandas + h5py)
├── inference/                  # consumer + MLflow inference
├── model/
│   └── mlflow_export/
│       └── modello_sismico_rf/ # MLflow artifact (offline)
├── clickhouse/                 # init SQL: Kafka engine + MV + MergeTree table
└── grafana/                    # datasource + dashboard provisioning
```

## Dataset
Copy into `./data`: `metadata.csv` + `waveforms.h5` (.h5 tracked via Git LFS).

Source: [STEAD — Stanford Earthquake Dataset](https://github.com/smousavi05/STEAD)

## .env
```dotenv
REDPANDA_BROKER=redpanda:9092
TOPIC_RAW=seismic-raw-data
TOPIC_PREDICTIONS=seismic-predictions
DATASET_DIR=/data
PLAYBACK_DELAY=0.2
CH_HOST=clickhouse
CH_HTTP_PORT=8123
CH_NATIVE_PORT=9000
CH_DB=seismic
GF_SECURITY_ADMIN_USER=admin
GF_SECURITY_ADMIN_PASSWORD=admin
```

## Run
```bash
docker compose up -d
```

## Ports
| Service | Port |
|---------|------|
| Redpanda (Kafka API) | 9092 |
| Redpanda Admin | 9644 |
| Redpanda Console | 8080 |
| ClickHouse HTTP | 8123 |
| ClickHouse native | 9000 |
| Grafana | 3000 |

## Verify
```bash
# topics
docker compose exec redpanda rpk topic consume seismic-raw-data --num 1
docker compose exec redpanda rpk topic consume seismic-predictions --num 1

# clickhouse (earthquake predictions)
docker compose exec clickhouse clickhouse-client -q \
  "SELECT class, count() FROM seismic.predictions GROUP BY class"
```

## Teardown
```bash
docker compose down      # stops containers, keeps volumes
docker compose down -v   # stops containers and wipes volumes
```
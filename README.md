# Seismic Detection Demo - Project GEB -
 
Final project for the **Technologies for Advanced Programming (TAP)** course —
Bachelor's degree in Computer Science, University of Catania.
 
The project is a real-time ML pipeline that classifies seismic signals as earthquake or noise.
A Random Forest model trained on the STEAD dataset runs fully offline inside a Docker Compose environment,
from data ingestion (Redpanda) through inference (MLflow + Spark) to storage (ClickHouse) and live visualization (Grafana).


---


# Setup
 
## Prerequisiti
- Docker ≥ 24 + Docker Compose v2
- Git LFS

## Struttura cartelle
```
seismic-detection-demo/
├── docker-compose.yml
├── prepare_subset.py
├── .gitattributes              # Git LFS per i .h5
├── .gitignore
├── README.md
├── data/                       # dataset STEAD (.h5 via Git LFS) + metadata
├── producer/                   # producer Kafka (pandas + h5py)
├── inference/                  # consumer + inferenza MLflow
├── model/
│   └── mlflow_export/
│       └── modello_sismico_rf/ # artifact MLflow (offline)
├── clickhouse/                 # init SQL: Kafka engine + MV + tabella MergeTree
└── grafana/                    # provisioning datasource + dashboard
```
 
## Dataset
Copiare in `./data`: `metadata.csv` + `waveforms.h5` (.h5 tracciati con Git LFS).
 
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
# 1. Infra (gli init SQL in ./clickhouse vengono caricati all'avvio)
docker compose up -d redpanda clickhouse grafana
 
# 2. Topic
docker compose exec redpanda rpk topic create seismic-raw-data seismic-predictions
 
# 3. Inference
docker compose up -d inference
 
# 4. Producer
docker compose up -d producer
 
# 5. Grafana
# http://localhost:3000  (admin / admin)
```
 
## Porte
| Servizio | Porta |
|----------|-------|
| Redpanda (Kafka API) | 9092 |
| Redpanda Admin | 9644 |
| Redpanda Console | 8080 |
| ClickHouse HTTP | 8123 |
| ClickHouse native | 9000 |
| Grafana | 3000 |
 
## Verifica
```bash
# topic
docker compose exec redpanda rpk topic consume seismic-raw-data --num 1
docker compose exec redpanda rpk topic consume seismic-predictions --num 1
 
# clickhouse (predizioni earthquake)
docker compose exec clickhouse clickhouse-client -q \
  "SELECT class, count() FROM seismic.predictions GROUP BY class"
```
 
## Teardown
```bash
docker compose down       # mantiene i volumi
docker compose down -v    # cancella i volumi
```


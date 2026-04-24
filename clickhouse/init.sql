-- ──────────────────────────────────────────────
-- 1. KAFKA ENGINE TABLE
--    Timestamps stored as String to handle ISO 8601 format
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS seismic_predictions_queue
(
    class         LowCardinality(String),
    confidence    Float32,
    lat           Float64,
    lon           Float64,
    coord_source  LowCardinality(String),
    sensor_id     String,
    trace_name    String,
    timestamp     String,
    inference_ts  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'redpanda:9092',
    kafka_topic_list           = 'seismic-predictions',
    kafka_group_name           = 'clickhouse-consumer',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_skip_broken_messages = 10;


-- ──────────────────────────────────────────────
-- 2. MERGETREE TABLE
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS seismic_predictions
(
    class         LowCardinality(String),
    confidence    Float32,
    lat           Float64,
    lon           Float64,
    coord_source  LowCardinality(String),
    sensor_id     String,
    trace_name    String,
    timestamp     DateTime64(3),
    inference_ts  DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (inference_ts, class)
PARTITION BY toYYYYMMDD(inference_ts);


-- ──────────────────────────────────────────────
-- 3. MATERIALIZED VIEW
--    Parses ISO 8601 strings with timezone into DateTime64
-- ──────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS seismic_predictions_mv
TO seismic_predictions
AS
SELECT
    class,
    confidence,
    lat,
    lon,
    coord_source,
    sensor_id,
    trace_name,
    parseDateTime64BestEffort(timestamp,   3) AS timestamp,
    parseDateTime64BestEffort(inference_ts, 3) AS inference_ts
FROM seismic_predictions_queue;
# inference/app/inference.py
import os
import json
import logging
from datetime import datetime, timezone

import numpy as np
from scipy.stats import skew, kurtosis
import mlflow.spark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import StructType, StructField
from confluent_kafka import Consumer, Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
KAFKA_BROKER            = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "seismic-raw-data")
KAFKA_TOPIC_PREDICTIONS = os.getenv("KAFKA_TOPIC_PREDICTIONS", "seismic-predictions")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "inference-group")
MODEL_PATH              = os.getenv("MODEL_PATH", "/model/mlflow_export/modello_sismico_rf")


# ──────────────────────────────────────────────
# SPARK SESSION
# ──────────────────────────────────────────────
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SeismicInference")
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


# ──────────────────────────────────────────────
# FEATURE EXTRACTION
# Identica alla UDF del notebook Databricks
# ──────────────────────────────────────────────
def extract_features_from_channel(channel: np.ndarray) -> list:
    arr = np.array(channel, dtype=float)
    if len(arr) == 0:
        return [0.0] * 6

    mean            = float(np.mean(arr))
    std             = float(np.std(arr))
    peak            = float(np.max(np.abs(arr)))
    skewness        = float(skew(arr))
    kurt            = float(kurtosis(arr))
    fft_vals        = np.fft.fft(arr)
    spectral_energy = float(np.sum(np.abs(fft_vals) ** 2) / len(arr))

    return [mean, std, peak, skewness, kurt, spectral_energy]


def extract_features(waveform: list) -> np.ndarray:
    w = np.array(waveform, dtype=float)

    # Normalizza a shape (3, 6000) — channels first
    if w.shape == (6000, 3):
        w = w.T

    features = []
    for ch in range(3):
        features.extend(extract_features_from_channel(w[ch]))

    return np.array(features, dtype=float)  # shape: (18,)


# ──────────────────────────────────────────────
# INFERENCE
# ──────────────────────────────────────────────
def predict(spark: SparkSession, model, features: np.ndarray) -> dict:
    schema = StructType([
        StructField("features", VectorUDT(), True)
    ])

    feature_vector = Vectors.dense(features.tolist())
    df = spark.createDataFrame([(feature_vector,)], schema=schema)

    result = model.transform(df)
    row = result.select("prediction", "probability").first()

    prediction  = int(row["prediction"])
    probability = row["probability"]
    confidence  = float(probability[prediction])

    label = "earthquake" if prediction == 1 else "noise"
    return {"class": label, "confidence": confidence}


# ──────────────────────────────────────────────
# KAFKA
# ──────────────────────────────────────────────
def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "linger.ms": 5,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    })


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for trace %s: %s", msg.key(), err)
    else:
        log.debug("Delivered prediction %s → partition %d offset %d",
                  msg.key(), msg.partition(), msg.offset())


# ──────────────────────────────────────────────
# MAIN LOOP
# ──────────────────────────────────────────────
def main():
    log.info("Starting Spark session...")
    spark = build_spark()

    log.info("Loading MLflow model from %s", MODEL_PATH)
    model = mlflow.spark.load_model(MODEL_PATH)
    log.info("Model loaded successfully.")

    consumer = build_consumer()
    producer = build_producer()
    consumer.subscribe([KAFKA_TOPIC_RAW])
    log.info("Subscribed to topic: %s", KAFKA_TOPIC_RAW)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            payload    = json.loads(msg.value().decode("utf-8"))
            trace_name = payload["trace_name"]

            features = extract_features(payload["waveform"])
            result   = predict(spark, model, features)

            output = {
                "class":        result["class"],
                "confidence":   result["confidence"],
                "lat":          payload["lat"],
                "lon":          payload["lon"],
                "coord_source": payload["coord_source"],
                "sensor_id":    payload["sensor_id"],
                "trace_name":   trace_name,
                "timestamp":    payload["timestamp"],
                "inference_ts": datetime.now(timezone.utc).isoformat(),
            }

            producer.produce(
                topic=KAFKA_TOPIC_PREDICTIONS,
                key=trace_name,
                value=json.dumps(output),
                on_delivery=delivery_report,
            )
            producer.poll(0)

            log.info(
                "→ [%s] trace=%s  confidence=%.4f  lat=%.4f  lon=%.4f",
                output["class"],
                trace_name,
                output["confidence"],
                output["lat"],
                output["lon"],
            )

    except KeyboardInterrupt:
        log.info("Shutting down.")
    finally:
        consumer.close()
        producer.flush()
        spark.stop()


if __name__ == "__main__":
    main()
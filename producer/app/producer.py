import os
import time
import json
import logging
import numpy as np
import pandas as pd
import h5py
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# Environment variables with sensible defaults for local development
KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_RAW   = os.getenv("KAFKA_TOPIC_RAW", "seismic-raw-data")
METADATA_PATH     = os.getenv("METADATA_PATH", "/data/metadata.csv")
WAVEFORMS_PATH    = os.getenv("WAVEFORMS_PATH", "/data/local_earthquakes.h5")
PLAYBACK_DELAY_MS = int(os.getenv("PLAYBACK_DELAY_MS", "500"))


def build_producer() -> Producer:
    # Connects to the broker with automatic retry on transient failures
    return Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "linger.ms": 5,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
    })


def delivery_report(err, msg):
    # Kafka calls this after each send
    if err:
        log.error("Delivery failed for trace %s: %s", msg.key(), err)
    else:
        log.debug("Delivered trace %s → partition %d offset %d",
                  msg.key(), msg.partition(), msg.offset())


def resolve_coordinates(row: pd.Series) -> dict:
    # Use the epicenter when available, fall back to the station location
    src_lat = row.get("source_latitude")
    src_lon = row.get("source_longitude")

    if pd.notna(src_lat) and pd.notna(src_lon):
        return {
            "lat": float(src_lat),
            "lon": float(src_lon),
            "coord_source": "epicenter"
        }
    return {
        "lat": float(row["receiver_latitude"]),
        "lon": float(row["receiver_longitude"]),
        "coord_source": "station"
    }


def build_payload(row: pd.Series, waveform: np.ndarray) -> dict:
    # Builds the JSON message that gets published to the topic
    coords = resolve_coordinates(row)

    return {
        "trace_name":     row["trace_name"],
        "trace_category": row["trace_category"],
        "sensor_id":      f"{row['receiver_code']}.{row['network_code']}",
        "timestamp":      row["trace_start_time"],
        "lat":            coords["lat"],
        "lon":            coords["lon"],
        "coord_source":   coords["coord_source"],
        "waveform":       waveform.tolist(),  # (3, 6000) as a nested list
    }


def load_waveform(hf: h5py.File, row: pd.Series) -> np.ndarray | None:
    trace_name = row["trace_name"]

    try:
        waveform = np.array(hf["data"][trace_name])
    except KeyError:
        log.warning("Trace not found: data/%s — skipping", trace_name)
        return None

    # Make sure the array is always (channels, samples)
    if waveform.shape == (6000, 3):
        waveform = waveform.T

    return waveform


def main():
    log.info("Loading metadata from %s", METADATA_PATH)
    metadata = pd.read_csv(METADATA_PATH)
    log.info("%d traces loaded", len(metadata))

    producer = build_producer()

    run = 0
    while True:
        run += 1
        log.info("Starting replay run #%d", run)

        # Shuffle on every run to simulate a realistic stream
        shuffled = metadata.sample(frac=1).reset_index(drop=True)

        with h5py.File(WAVEFORMS_PATH, "r") as hf:
            for _, row in shuffled.iterrows():

                waveform = load_waveform(hf, row)
                if waveform is None:
                    continue

                payload = build_payload(row, waveform)

                producer.produce(
                    topic=KAFKA_TOPIC_RAW,
                    key=row["trace_name"],
                    value=json.dumps(payload),
                    on_delivery=delivery_report,
                )
                producer.poll(0)

                log.info(
                    "→ [%s] trace=%s  coord_source=%s  lat=%.4f  lon=%.4f",
                    payload["trace_category"],
                    payload["trace_name"],
                    payload["coord_source"],
                    payload["lat"],
                    payload["lon"],
                )

                time.sleep(PLAYBACK_DELAY_MS / 1000.0)

        producer.flush()
        log.info("Run #%d done. Restarting...", run)


if __name__ == "__main__":
    main()
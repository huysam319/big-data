#!/usr/bin/env python3
"""
producer.py
---------------------------------
Utility script that continuously pushes intentionally abnormal vital-sign events
into Kafka so the downstream consumer + AI recommendation pipeline can be tested.

Environment variables (defaults in parentheses):
    KAFKA_BOOTSTRAP   - Kafka bootstrap servers ("localhost:9092")
    KAFKA_TOPIC       - Topic to publish to ("mimic_chartevents_stream")
    PRODUCER_SLEEP    - Seconds to sleep between messages ("1.0")
    PRODUCER_BATCH    - Number of messages to send; <=0 means infinite loop ("0")
    SUBJECT_ID        - Base subject_id to simulate (10017531)
    HADM_ID           - Base hadm_id to simulate (22580355)

The payload matches the structure that consumer.py expects.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "mimic_chartevents_stream")
SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP", "1.0"))
TOTAL_MESSAGES = int(os.getenv("PRODUCER_BATCH", "1"))  # 0 or less => infinite
BASE_SUBJECT_ID = int(os.getenv("SUBJECT_ID", "10017531"))
BASE_HADM_ID = int(os.getenv("HADM_ID", "22580355"))


VITAL_DEFINITIONS = [
    {
        "itemid": 220045,
        "vital_name": "heart_rate",
        "valueuom": "bpm",
        "abnormal_range": (130, 170),  # tachycardia
    },
    {
        "itemid": 220050,
        "vital_name": "systolic_bp",
        "valueuom": "mmHg",
        "abnormal_range": (180, 220),  # severe hypertension
    },
    {
        "itemid": 220051,
        "vital_name": "diastolic_bp",
        "valueuom": "mmHg",
        "abnormal_range": (110, 140),  # hypertension
    },
    {
        "itemid": 220277,
        "vital_name": "oxygen_saturation",
        "valueuom": "%",
        "abnormal_range": (70, 85),  # hypoxemia
    },
    {
        "itemid": 220210,
        "vital_name": "respiratory_rate",
        "valueuom": "breaths/min",
        "abnormal_range": (30, 45),  # tachypnea
    },
    {
        "itemid": 223762,
        "vital_name": "temperature",
        "valueuom": "°C",
        "abnormal_range": (39.5, 41.0),  # hyperthermia
    },
]


def _as_iso(ts: datetime) -> str:
    """Return ISO-8601 string with Z suffix."""
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_payload(seq: int) -> dict:
    """Create a single abnormal vital record."""
    vital = random.choice(VITAL_DEFINITIONS)
    value = round(random.uniform(*vital["abnormal_range"]), 2)

    now = datetime.now(timezone.utc)
    event_time = _as_iso(now)

    payload = {
        "subject_id": BASE_SUBJECT_ID + (seq % 5),
        "hadm_id": BASE_HADM_ID + (seq % 3),
        "itemid": vital["itemid"],
        "vital_name": vital["vital_name"],
        "value": f"{value}",
        "valuenum": value,
        "valueuom": vital["valueuom"],
        "charttime": event_time,
        "storetime": event_time,
        "event_time": event_time,
    }
    return payload


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        retries=3,
    )

    print(
        f"[producer] Sending abnormal events to topic '{KAFKA_TOPIC}' "
        f"(bootstrap: {KAFKA_BOOTSTRAP})"
    )
    print(
        f"[producer] Sleep between messages: {SLEEP_SECONDS}s | "
        f"Total messages: {'infinite' if TOTAL_MESSAGES <= 0 else TOTAL_MESSAGES}"
    )

    seq = 0
    try:
        while TOTAL_MESSAGES <= 0 or seq < TOTAL_MESSAGES:
            payload = generate_payload(seq)
            producer.send(KAFKA_TOPIC, value=payload)
            if seq % 10 == 0:
                producer.flush()

            print(
                f"[producer] sent #{seq + 1} "
                f"({payload['vital_name']}={payload['valuenum']} {payload['valueuom']})"
            )
            seq += 1
            time.sleep(SLEEP_SECONDS)
    except KeyboardInterrupt:
        print("\n[producer] Interrupted by user. Shutting down.")
    finally:
        producer.flush()
        producer.close()
        print("[producer] Closed producer.")


if __name__ == "__main__":
    main()


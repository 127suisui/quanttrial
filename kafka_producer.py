#!/usr/bin/env python3
"""
Stream quotes from l1_day.csv to Kafka topic `mock_l1_stream`
at (optionally accelerated) real-time pace.

Usage example:
    python kafka_producer.py --csv l1_day.csv --speed 10
"""

import argparse
import json
import time
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to l1_day.csv")
    p.add_argument("--bootstrap", default="localhost:9092",
                   help="Kafka bootstrap servers")
    p.add_argument("--topic", default="mock_l1_stream",
                   help="Kafka topic name")
    p.add_argument("--speed", type=float, default=1.0,
                   help="Playback speed factor (>1=faster)")
    return p.parse_args()


def load_window(path):
    """Read CSV and keep rows between 13:36:32 – 13:45:14 UTC."""
    df = pd.read_csv(path)
    if df["ts_event"].dtype == "int64":
        df["ts_event"] = pd.to_datetime(df["ts_event"], unit="us", utc=True)
    else:
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True)

    start = pd.Timestamp("1970-01-01 13:36:32", tz="UTC")
    end   = pd.Timestamp("1970-01-01 13:45:14", tz="UTC")
    mask  = (df["ts_event"].dt.time >= start.time()) & (df["ts_event"].dt.time <= end.time())
    df = df.loc[mask].sort_values("ts_event")
    return df[["ts_event", "publisher_id", "ask_px_00", "ask_sz_00"]]


def main():
    args = parse_args()
    df = load_window(args.csv)
    if df.empty:
        raise ValueError("Filtered window returned 0 rows; check ts_event format")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=0,
    )

    print(f"⏩ Streaming {len(df)} rows → {args.topic}  (speed={args.speed}x)")
    prev_ts = None
    for idx, row in df.iterrows():
        ts = row["ts_event"]
        if prev_ts is not None:
            delta = (ts - prev_ts).total_seconds() / args.speed
            if delta > 0:
                time.sleep(delta)
        prev_ts = ts

        producer.send(args.topic, {
            "ts_event": ts.isoformat(),
            "publisher_id": int(row["publisher_id"]),
            "ask_px_00": float(row["ask_px_00"]),
            "ask_sz_00": int(row["ask_sz_00"]),
        })

        if idx % 500 == 0:
            print(f"  sent {idx} / {len(df)}")

    producer.flush()
    print("✅  all snapshots streamed successfully")


if __name__ == "__main__":
    main()

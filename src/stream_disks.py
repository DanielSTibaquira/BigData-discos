"""Aggregate Backblaze Kafka events in seven-day event-time windows."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CHECKPOINT = ROOT / "results" / "checkpoints" / "backblaze-q2-2026"
DEFAULT_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="backblaze-drives")
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT)
    parser.add_argument("--packages", default=DEFAULT_PACKAGES)
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    parser.add_argument("--max-offsets-per-trigger", type=int, default=50000)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.shuffle_partitions < 1:
        raise ValueError("--shuffle-partitions debe ser positivo")
    if args.max_offsets_per_trigger < 1:
        raise ValueError("--max-offsets-per-trigger debe ser positivo")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    args.checkpoint.mkdir(parents=True, exist_ok=True)

    event_schema = StructType(
        [
            StructField("event_time", TimestampType(), nullable=False),
            StructField("model", StringType(), nullable=False),
            StructField("failure", IntegerType(), nullable=False),
            StructField("weight", DoubleType(), nullable=False),
        ]
    )
    spark = (
        SparkSession.builder.appName("BackblazeDriveStatsKafkaStreaming")
        .master("local[*]")
        .config("spark.jars.packages", args.packages)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        kafka_events = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("subscribe", args.topic)
            .option("startingOffsets", args.starting_offsets)
            .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
            .option("failOnDataLoss", "true")
            .load()
        )
        events = (
            kafka_events.select(
                from_json(col("value").cast("string"), event_schema).alias("event")
            )
            .select("event.*")
            .withWatermark("event_time", "2 days")
        )
        aggregates = (
            events.groupBy(window("event_time", "7 days"), col("model"))
            .agg(
                spark_sum("weight").alias("drive_days"),
                spark_sum(col("failure") * col("weight")).alias("failures"),
            )
            .withColumn(
                "afr_percent",
                col("failures") * 365.0 / col("drive_days") * 100.0,
            )
        )
        query = (
            aggregates.writeStream.outputMode("update")
            .format("console")
            .option("truncate", "false")
            .option("checkpointLocation", str(args.checkpoint))
            .start()
        )
        print(
            f"Streaming {args.topic} desde {args.bootstrap_servers}; "
            "ventanas de 7 días, watermark de 2 días. Ctrl+C para detener.",
            flush=True,
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

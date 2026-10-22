"""Persist Kafka drive-window aggregates in MongoDB and query the top five models."""

from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from pymongo import ASCENDING, DESCENDING, MongoClient, ReplaceOne
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, first, from_json, sum as spark_sum, window
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CHECKPOINT = ROOT / "results" / "checkpoints" / "backblaze-q2-mongodb"
DEFAULT_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
DEFAULT_DATABASE = "drive_stats"
DEFAULT_COLLECTION = "model_windows"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Leer Kafka y guardar agregados en MongoDB.")
    run.add_argument("--bootstrap-servers", default="localhost:9092")
    run.add_argument("--topic", default="backblaze-drives-mongo")
    run.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT)
    run.add_argument("--packages", default=DEFAULT_PACKAGES)
    run.add_argument(
        "--mongodb-uri",
        default=os.environ.get("MONGODB_URI", "mongodb://localhost:27017"),
    )
    run.add_argument("--database", default=DEFAULT_DATABASE)
    run.add_argument("--collection", default=DEFAULT_COLLECTION)
    run.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    run.add_argument("--shuffle-partitions", type=int, default=8)
    run.add_argument("--max-offsets-per-trigger", type=int, default=50000)

    top5 = commands.add_parser("top5", help="Consultar los cinco AFR mayores de una ventana.")
    top5.add_argument("--run-id", required=True)
    top5.add_argument(
        "--window-start",
        required=True,
        type=parse_utc_datetime,
        help="Inicio UTC de ventana ISO-8601, por ejemplo 2026-04-02T00:00:00Z.",
    )
    top5.add_argument("--min-drive-days", type=float, default=100000)
    top5.add_argument(
        "--mongodb-uri",
        default=os.environ.get("MONGODB_URI", "mongodb://localhost:27017"),
    )
    top5.add_argument("--database", default=DEFAULT_DATABASE)
    top5.add_argument("--collection", default=DEFAULT_COLLECTION)
    return parser.parse_args(argv)


def parse_utc_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError("Usa una fecha ISO-8601, por ejemplo ...Z") from error
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("La fecha de ventana debe incluir zona horaria UTC")
    if parsed.utcoffset().total_seconds() != 0:
        raise argparse.ArgumentTypeError("La fecha de ventana debe estar expresada en UTC")
    return parsed.astimezone(timezone.utc)


def create_indexes(collection) -> None:
    collection.create_index(
        [
            ("run_id", ASCENDING),
            ("window_start", ASCENDING),
            ("model", ASCENDING),
        ],
        unique=True,
        name="uniq_run_window_model",
    )
    collection.create_index(
        [
            ("run_id", ASCENDING),
            ("window_start", ASCENDING),
            ("drive_days", DESCENDING),
            ("afr_percent", DESCENDING),
        ],
        name="top5_by_window_and_exposure",
    )


def save_batch(batch: DataFrame, collection, batch_id: int) -> int:
    operations = []
    for row in batch.toLocalIterator():
        drive_days = row["drive_days"]
        failures = row["failures"]
        sample_fraction = row["sample_fraction"]
        if (
            not row["run_id"]
            or not row["model"]
            or row["window_start"] is None
            or row["window_end"] is None
            or drive_days is None
            or failures is None
            or row["afr_percent"] is None
            or sample_fraction is None
            or not math.isfinite(float(drive_days))
            or not math.isfinite(float(failures))
            or not math.isfinite(float(row["afr_percent"]))
            or not math.isfinite(float(sample_fraction))
            or drive_days <= 0
            or failures < 0
            or failures > drive_days
            or not 0 < sample_fraction <= 1
        ):
            raise ValueError(
                f"El micro-lote {batch_id} contiene un agregado inválido"
            )
        afr_percent = float(failures) * 365.0 / float(drive_days) * 100.0
        if not math.isclose(
            float(row["afr_percent"]), afr_percent, rel_tol=1e-9, abs_tol=1e-9
        ):
            raise ValueError(
                f"El micro-lote {batch_id} contiene una AFR inconsistente"
            )
        document = {
            "run_id": row["run_id"],
            "model": row["model"],
            "window_start": row["window_start"],
            "window_end": row["window_end"],
            "drive_days": float(drive_days),
            "failures": float(failures),
            "afr_percent": afr_percent,
            "sample_fraction": float(sample_fraction),
            "sampled": sample_fraction < 1,
        }
        identity = {
            "run_id": document["run_id"],
            "model": document["model"],
            "window_start": document["window_start"],
        }
        operations.append(ReplaceOne(identity, document, upsert=True))

    if operations:
        collection.bulk_write(operations, ordered=False)
    return len(operations)


def query_top5(collection, run_id: str, window_start: datetime, min_drive_days: float):
    if not run_id.strip():
        raise ValueError("--run-id no puede estar vacío")
    if min_drive_days <= 0:
        raise ValueError("--min-drive-days debe ser positivo")
    return list(
        collection.find(
            {
                "run_id": run_id,
                "window_start": window_start,
                "drive_days": {"$gte": min_drive_days},
            },
            {
                "_id": 0,
                "run_id": 1,
                "model": 1,
                "window_start": 1,
                "window_end": 1,
                "drive_days": 1,
                "failures": 1,
                "afr_percent": 1,
                "sample_fraction": 1,
                "sampled": 1,
            },
        )
        .sort(
            [
                ("afr_percent", DESCENDING),
                ("drive_days", DESCENDING),
                ("model", ASCENDING),
            ]
        )
        .limit(5)
    )


def run_stream(args: argparse.Namespace) -> None:
    if args.shuffle_partitions < 1:
        raise ValueError("--shuffle-partitions debe ser positivo")
    if args.max_offsets_per_trigger < 1:
        raise ValueError("--max-offsets-per-trigger debe ser positivo")

    mongo = MongoClient(args.mongodb_uri, serverSelectionTimeoutMS=5000)
    spark = None
    try:
        mongo.admin.command("ping")
        collection = mongo[args.database][args.collection]
        create_indexes(collection)

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
        args.checkpoint.mkdir(parents=True, exist_ok=True)
        event_schema = StructType(
            [
                StructField("event_time", TimestampType(), nullable=True),
                StructField("model", StringType(), nullable=True),
                StructField("failure", IntegerType(), nullable=True),
                StructField("weight", DoubleType(), nullable=True),
                StructField("sample_fraction", DoubleType(), nullable=True),
                StructField("run_id", StringType(), nullable=True),
            ]
        )
        spark = (
            SparkSession.builder.appName("BackblazeDriveStatsMongoStreaming")
            .master("local[*]")
            .config("spark.jars.packages", args.packages)
            .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")

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
                from_json(
                    col("value").cast("string"),
                    event_schema,
                    {"mode": "FAILFAST"},
                ).alias("event")
            )
            .select("event.*")
            .withWatermark("event_time", "2 days")
        )
        aggregates = (
            events.groupBy(window("event_time", "7 days"), "run_id", "model")
            .agg(
                spark_sum("weight").alias("drive_days"),
                spark_sum(col("failure") * col("weight")).alias("failures"),
                first("sample_fraction", ignorenulls=False).alias("sample_fraction"),
            )
            .withColumn(
                "afr_percent",
                col("failures") * 365.0 / col("drive_days") * 100.0,
            )
            .select(
                "run_id",
                "model",
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "drive_days",
                "failures",
                "afr_percent",
                "sample_fraction",
            )
        )

        def write_micro_batch(batch: DataFrame, batch_id: int) -> None:
            written = save_batch(batch, collection, batch_id)
            print(
                f"Micro-lote {batch_id}: {written} agregados upsert en "
                f"{args.database}.{args.collection}",
                flush=True,
            )

        query = (
            aggregates.writeStream.outputMode("update")
            .foreachBatch(write_micro_batch)
            .option("checkpointLocation", str(args.checkpoint))
            .start()
        )
        print(
            f"Guardando ventanas de Kafka en MongoDB {args.database}.{args.collection}; "
            "ventanas UTC de 7 días, watermark de 2 días. Ctrl+C para detener.",
            flush=True,
        )
        query.awaitTermination()
    finally:
        if spark is not None:
            spark.stop()
        mongo.close()


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    if args.command == "run":
        run_stream(args)
        return

    if args.min_drive_days <= 0:
        raise ValueError("--min-drive-days debe ser positivo")
    with MongoClient(args.mongodb_uri, serverSelectionTimeoutMS=5000) as mongo:
        mongo.admin.command("ping")
        collection = mongo[args.database][args.collection]
        results = query_top5(
            collection,
            args.run_id,
            args.window_start,
            args.min_drive_days,
        )
        if not results:
            print(
                "No hay modelos que cumplan el mínimo de días-disco para esa "
                "ejecución y ventana."
            )
        else:
            for result in results:
                print(
                    f"{result['model']}: AFR={result['afr_percent']:.4f}%, "
                    f"fallas={result['failures']:.2f}, "
                    f"días-disco={result['drive_days']:.0f}, "
                    f"ventana={result['window_start'].isoformat()} – "
                    f"{result['window_end'].isoformat()}, "
                    f"muestra={result['sample_fraction']:.0%}"
                )
if __name__ == "__main__":
    main()

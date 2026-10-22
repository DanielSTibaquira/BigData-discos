"""Benchmark model aggregation with several Spark partition counts."""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
import time
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, spark_partition_id, sum as spark_sum

from mapreduce_disks import DEFAULT_ARCHIVE, DEFAULT_OUTPUT, extract_sample, sample_dates


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 4, 1))
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--partitions", type=int, nargs="+", default=[2, 8, 32, 200])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def partition_counts(frame, partitions: int) -> list[int]:
    spark = frame.sparkSession
    original_shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.shuffle.partitions", "1")
    try:
        observed = frame.select(
            spark_partition_id().alias("partition_id")
        ).groupBy("partition_id").agg(count("*").alias("rows")).collect()
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", original_shuffle_partitions)
    counts = {
        row["partition_id"]: row["rows"] for row in observed
    }
    return [counts.get(partition_id, 0) for partition_id in range(partitions)]


def distribution(counts: list[int]) -> dict[str, float | int]:
    mean = statistics.mean(counts)
    return {
        "partitions": len(counts),
        "min_rows": min(counts),
        "max_rows": max(counts),
        "mean_rows": mean,
        "max_to_mean": max(counts) / mean if mean else 0.0,
        "coefficient_of_variation": (
            statistics.pstdev(counts) / mean if mean else 0.0
        ),
        "empty_partitions": sum(rows == 0 for rows in counts),
    }


def main() -> None:
    args = parse_args()
    if args.repeats < 2:
        raise ValueError("--repeats debe ser al menos 2")
    if not args.partitions or any(partition < 1 for partition in args.partitions):
        raise ValueError("--partitions debe contener enteros positivos")
    dates = sample_dates(args.start_date, args.days)
    if not args.archive.is_file():
        raise FileNotFoundError(f"No se encuentra el ZIP: {args.archive}")

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    with TemporaryDirectory(prefix="backblaze_q2_2026_") as temporary_directory:
        paths = extract_sample(args.archive, dates, Path(temporary_directory))
        spark = (
            SparkSession.builder.appName("BackblazeDriveStatsLoadBalance")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        daily = None
        try:
            daily = (
                spark.read.option("header", True)
                .csv([str(path) for path in paths])
                .select(
                    "date",
                    "serial_number",
                    "model",
                    col("failure").cast("int").alias("failure"),
                )
                .persist()
            )
            input_rows = daily.count()
            default_parallelism = spark.sparkContext.defaultParallelism

            timings = []
            for partitions in args.partitions:
                spark.conf.set("spark.sql.shuffle.partitions", str(partitions))

                def aggregate() -> int:
                    results = (
                        daily.repartition(partitions)
                        .groupBy("model")
                        .agg(
                            count("*").alias("drive_days"),
                            spark_sum("failure").alias("failures"),
                        )
                        .collect()
                    )
                    return len(results)

                aggregate()
                for repetition in range(1, args.repeats + 1):
                    start = time.perf_counter()
                    model_count = aggregate()
                    elapsed = time.perf_counter() - start
                    timings.append(
                        {
                            "partitions": partitions,
                            "repetition": repetition,
                            "elapsed_seconds": elapsed,
                            "models": model_count,
                        }
                    )

            partition_distribution = {}
            for partitions in args.partitions:
                unkeyed = daily.repartition(partitions)
                keyed = daily.repartition(partitions, col("model"))
                unkeyed_counts = partition_counts(unkeyed, partitions)
                keyed_counts = partition_counts(keyed, partitions)
                partition_distribution[str(partitions)] = {
                    "without_key": {
                        "summary": distribution(unkeyed_counts),
                        "rows_by_partition": unkeyed_counts,
                    },
                    "by_model": {
                        "summary": distribution(keyed_counts),
                        "rows_by_partition": keyed_counts,
                    },
                }

            timing_summary = []
            for partitions in args.partitions:
                samples = [
                    item["elapsed_seconds"]
                    for item in timings
                    if item["partitions"] == partitions
                ]
                timing_summary.append(
                    {
                        "partitions": partitions,
                        "repeats": len(samples),
                        "median_seconds": statistics.median(samples),
                        "min_seconds": min(samples),
                        "max_seconds": max(samples),
                    }
                )

            result = {
                "sample_dates": [sample_date.isoformat() for sample_date in dates],
                "input_rows": input_rows,
                "models_aggregated": timings[-1]["models"],
                "spark_version": spark.version,
                "master": "local[*]",
                "default_parallelism": default_parallelism,
                "timed_aggregation": "DataFrame groupBy, equivalent to part A RDD aggregation",
                "warmups_per_partition_count": 1,
                "timings": timings,
                "timing_summary": timing_summary,
                "partition_distribution": partition_distribution,
            }
            json_path = args.output_dir / "balanceo_carga.json"
            json_path.write_text(
                json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
            )

            csv_path = args.output_dir / "tiempos_particiones.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(
                    output,
                    fieldnames=[
                        "partitions",
                        "repetition",
                        "elapsed_seconds",
                        "models",
                    ],
                )
                writer.writeheader()
                writer.writerows(timings)

            print(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            if daily is not None:
                daily.unpersist()
            spark.stop()


if __name__ == "__main__":
    main()

"""Validate a daily Backblaze sample and compare PySpark RDD/DataFrame results."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import shutil
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from zipfile import ZipFile

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max as spark_max, min as spark_min, sum as spark_sum


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHIVE = ROOT / "data" / "data_Q2_2026.zip"
DEFAULT_OUTPUT = ROOT / "results"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 4, 1))
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def sample_dates(start: date, days: int) -> list[date]:
    if days < 1 or days > 14:
        raise ValueError("--days debe estar entre 1 y 14")
    dates = [start + timedelta(days=offset) for offset in range(days)]
    if dates[0] < date(2026, 4, 1) or dates[-1] > date(2026, 6, 30):
        raise ValueError("La muestra debe estar dentro del trimestre Q2 2026")
    return dates


def extract_sample(archive: Path, dates: list[date], directory: Path) -> list[Path]:
    paths = []
    with ZipFile(archive) as zipped:
        reference_header = None
        for sample_date in dates:
            filename = f"data_Q2_2026/{sample_date.isoformat()}.csv"
            try:
                with zipped.open(filename) as source:
                    header = next(
                        csv.reader(io.TextIOWrapper(source, encoding="utf-8", newline=""))
                    )
            except KeyError as error:
                raise FileNotFoundError(f"Falta el archivo {filename} en {archive}") from error
            required_columns = {"date", "serial_number", "model", "failure"}
            if not required_columns.issubset(header):
                raise ValueError(f"Faltan columnas requeridas en {filename}")
            if reference_header is None:
                reference_header = header
            elif header != reference_header:
                raise ValueError(f"El esquema de {filename} no coincide con el de la muestra")

            path = directory / f"{sample_date.isoformat()}.csv"
            with zipped.open(filename) as source, path.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            paths.append(path)
    return paths


def main() -> None:
    args = parse_args()
    dates = sample_dates(args.start_date, args.days)
    if not args.archive.is_file():
        raise FileNotFoundError(f"No se encuentra el ZIP: {args.archive}")

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="backblaze_q2_2026_") as temporary_directory:
        sample_paths = extract_sample(args.archive, dates, Path(temporary_directory))
        spark = (
            SparkSession.builder.appName("BackblazeDriveStatsMapReduce")
            .master("local[*]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        daily = None
        try:
            daily = (
                spark.read.option("header", True)
                .option("inferSchema", False)
                .csv([str(path) for path in sample_paths])
                .select(
                    "date",
                    "serial_number",
                    "model",
                    col("failure").cast("int").alias("failure"),
                )
                .persist()
            )

            required_nulls = daily.filter(
                col("date").isNull()
                | col("serial_number").isNull()
                | col("model").isNull()
            ).count()
            invalid_failures = daily.filter(
                col("failure").isNull() | ~col("failure").isin(0, 1)
            ).count()
            if required_nulls:
                raise ValueError(f"Hay {required_nulls} filas sin fecha, serial o modelo")
            if invalid_failures:
                raise ValueError(f"Hay {invalid_failures} valores de failure distintos de 0/1")

            daily_counts = (
                daily.groupBy("date")
                .agg(count("*").alias("rows"), spark_sum("failure").alias("failures"))
                .orderBy("date")
                .collect()
            )
            expected_dates = [sample_date.isoformat() for sample_date in dates]
            actual_dates = [row["date"] for row in daily_counts]
            if actual_dates != expected_dates:
                raise ValueError(
                    f"Fechas de las filas no coinciden con la muestra: {actual_dates}"
                )

            rdd_results = (
                daily.select("model", "failure")
                .rdd.map(lambda row: (row["model"], (1, row["failure"])))
                .reduceByKey(
                    lambda left, right: (
                        left[0] + right[0],
                        left[1] + right[1],
                    )
                )
                .collectAsMap()
            )

            dataframe_results = (
                daily.groupBy("model")
                .agg(
                    count("*").alias("drive_days"),
                    spark_sum("failure").alias("failures"),
                )
                .withColumn(
                    "afr_percent",
                    col("failures") * 365.0 / col("drive_days") * 100.0,
                )
                .orderBy(col("afr_percent").desc(), col("model"))
                .collect()
            )
            dataframe_totals = {
                row["model"]: (row["drive_days"], row["failures"])
                for row in dataframe_results
            }
            if rdd_results != dataframe_totals:
                raise AssertionError("Los resultados RDD y DataFrame no coinciden")

            serial_days = (
                daily.groupBy("serial_number")
                .agg(count("*").alias("observed_days"))
                .agg(
                    count("*").alias("serials"),
                    spark_min("observed_days").alias("min_days"),
                    spark_max("observed_days").alias("max_days"),
                    avg("observed_days").alias("mean_days"),
                )
                .first()
            )

            results_path = args.output_dir / "afr_por_modelo.csv"
            with results_path.open("w", newline="", encoding="utf-8") as output:
                writer = csv.writer(output)
                writer.writerow(["model", "drive_days", "failures", "afr_percent"])
                for row in dataframe_results:
                    writer.writerow(
                        [
                            row["model"],
                            row["drive_days"],
                            row["failures"],
                            f"{row['afr_percent']:.8f}",
                        ]
                    )

            validation = {
                "archive": str(args.archive),
                "sample_dates": expected_dates,
                "sample_files": [path.name for path in sample_paths],
                "daily_counts": [
                    {
                        "date": row["date"],
                        "rows": row["rows"],
                        "failures": row["failures"],
                    }
                    for row in daily_counts
                ],
                "models": len(dataframe_results),
                "serial_observations": {
                    "serials": serial_days["serials"],
                    "min_observed_days": serial_days["min_days"],
                    "max_observed_days": serial_days["max_days"],
                    "mean_observed_days": serial_days["mean_days"],
                },
                "rdd_matches_dataframe": True,
                "afr_formula": "(failures * 365 / drive_days) * 100",
                "results_csv": str(results_path),
            }
            validation_path = args.output_dir / "validacion_muestra.json"
            validation_path.write_text(
                json.dumps(validation, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(json.dumps(validation, ensure_ascii=False, indent=2))
            print(f"\nResultados por modelo: {results_path}")
        finally:
            if daily is not None:
                daily.unpersist()
            spark.stop()


if __name__ == "__main__":
    main()

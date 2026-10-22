"""Publish selected Backblaze daily rows to Kafka as JSON events."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from datetime import date, datetime, time as datetime_time, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from kafka import KafkaProducer
from kafka.errors import KafkaError

from mapreduce_disks import DEFAULT_ARCHIVE, extract_sample, sample_dates


ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 4, 1))
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument(
        "--models",
        nargs="+",
        default=[
            "ST8000NM0055",
            "ST12000NM0008",
            "TOSHIBA MG07ACA14TA",
            "WDC WUH721816ALE6L4",
            "ST14000NM001G",
        ],
        help="Entre 3 y 5 modelos exactos como aparecen en el CSV.",
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="backblaze-drives")
    parser.add_argument("--seconds-between-days", type=float, default=10.0)
    parser.add_argument(
        "--sample-fraction",
        type=float,
        default=0.1,
        help="Fracción determinista de filas que se publica; el peso conserva la escala.",
    )
    parser.add_argument("--run-id", help="Identificador reproducible de esta ejecución.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 3 <= len(args.models) <= 5:
        raise ValueError("Selecciona entre 3 y 5 modelos")
    if len(set(args.models)) != len(args.models):
        raise ValueError("La lista de modelos no debe contener duplicados")
    if args.seconds_between_days < 0:
        raise ValueError("--seconds-between-days no puede ser negativo")
    if not 0 < args.sample_fraction <= 1:
        raise ValueError("--sample-fraction debe estar en el intervalo (0, 1]")
    dates = sample_dates(args.start_date, args.days)
    if not args.archive.is_file():
        raise FileNotFoundError(f"No se encuentra el ZIP: {args.archive}")

    with TemporaryDirectory(prefix="backblaze_kafka_") as temporary_directory:
        paths = extract_sample(args.archive, dates, Path(temporary_directory))
        run_id = args.run_id or build_run_id(dates[0], args.days, args.models, args.sample_fraction)
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers.split(","),
            acks="all",
            retries=5,
            value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        )
        total = 0
        models_seen: set[str] = set()
        threshold = int(args.sample_fraction * (1 << 64))
        row_weight = 1.0 / args.sample_fraction
        try:
            for day_index, (sample_date, path) in enumerate(zip(dates, paths)):
                daily_count = 0
                event_time = datetime.combine(
                    sample_date, datetime_time.min, tzinfo=timezone.utc
                ).isoformat().replace("+00:00", "Z")
                with path.open(encoding="utf-8", newline="") as source:
                    for row in csv.DictReader(source):
                        model = row["model"]
                        if model not in args.models:
                            continue
                        sample_key = f"{sample_date.isoformat()}|{row['serial_number']}"
                        sample_value = int.from_bytes(
                            hashlib.sha256(sample_key.encode("utf-8")).digest()[:8],
                            "big",
                        )
                        if sample_value >= threshold:
                            continue
                        failure = int(row["failure"])
                        if failure not in (0, 1):
                            raise ValueError(
                                f"failure={failure} inválido en {path.name}"
                            )
                        producer.send(
                            args.topic,
                            {
                                "event_time": event_time,
                                "model": model,
                                "failure": failure,
                                "weight": row_weight,
                                "sample_fraction": args.sample_fraction,
                                "run_id": run_id,
                            },
                        )
                        models_seen.add(model)
                        daily_count += 1
                producer.flush()
                total += daily_count
                print(
                    f"{sample_date.isoformat()}: publicados {daily_count} eventos "
                    f"(acumulado {total})",
                    flush=True,
                )
                if day_index < len(paths) - 1 and args.seconds_between_days:
                    time.sleep(args.seconds_between_days)
        except KafkaError:
            raise
        finally:
            producer.close()

        missing_models = sorted(set(args.models) - models_seen)
        if missing_models:
            raise ValueError(
                f"Modelos sin registros en la muestra: {', '.join(missing_models)}"
            )
        print(
            json.dumps(
                {
                    "topic": args.topic,
                    "run_id": run_id,
                    "bootstrap_servers": args.bootstrap_servers,
                    "days": len(dates),
                    "events_published": total,
                    "models": sorted(models_seen),
                    "sample_fraction": args.sample_fraction,
                    "event_weight": row_weight,
                },
                ensure_ascii=False,
                indent=2,
            )
        )


def build_run_id(
    start_date: date, days: int, models: list[str], sample_fraction: float
) -> str:
    model_digest = hashlib.sha256(
        "\0".join(sorted(models)).encode("utf-8")
    ).hexdigest()[:8]
    fraction = f"{sample_fraction:g}".replace(".", "p")
    return f"q2-2026-{start_date.isoformat()}-{days}d-{fraction}-{model_digest}"


if __name__ == "__main__":
    main()

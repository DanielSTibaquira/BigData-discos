"""
Reto de Investigación: Análisis de Supervivencia de Discos Duros con Kaplan-Meier
"""

import argparse
import os
import zipfile
import tempfile
import pandas as pd
import matplotlib.pyplot as plt
from lifelines import KaplanMeierFitter
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def extract_sample_csvs(zip_path, start_date_str, days, target_dir):
    start_dt = pd.to_datetime(start_date_str)
    target_dates = [(start_dt + pd.Timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days)]
    extracted_files = []
    
    with zipfile.ZipFile(zip_path, 'r') as zf:
        namelist = zf.namelist()
        for d in target_dates:
            target_name = f"{d}.csv"
            matches = [f for f in namelist if f.endswith(target_name)]
            if matches:
                zf.extract(matches[0], target_dir)
                extracted_files.append(os.path.join(target_dir, matches[0]))
    return extracted_files

def main():
    parser = argparse.ArgumentParser(description="Análisis de supervivencia Kaplan-Meier en Backblaze Drive Stats")
    parser.add_argument("--zip-path", default="data/data_Q2_2026.zip", help="Ruta al ZIP del trimestre")
    parser.add_argument("--start-date", default="2026-04-01", help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=14, help="Número de días a muestrear")
    parser.add_argument("--top-models", type=int, default=4, help="Cantidad de modelos con mayor presencia a analizar")
    args = parser.parse_args()

    os.makedirs("results", exist_ok=True)
    temp_dir = tempfile.mkdtemp(prefix="backblaze_km_")
    
    try:
        print(f"Extrayendo muestra de {args.days} días desde {args.zip_path}...")
        csv_files = extract_sample_csvs(args.zip_path, args.start_date, args.days, temp_dir)
        if not csv_files:
            raise FileNotFoundError("No se pudieron extraer los archivos CSV para las fechas indicadas.")

        print("Iniciando sesión de Spark para agregar duración y fallas por número de serie...")
        spark = SparkSession.builder \
            .appName("KaplanMeierBackblaze") \
            .master("local[*]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        # Lectura de los CSV diarios
        df = spark.read.option("header", "true").csv(csv_files)
        df = df.select(
            F.col("serial_number"),
            F.col("model"),
            F.col("failure").cast("int")
        )

        # Agregación por disco individual:
        # duration (T) = días observados del disco
        # observed_event (E) = 1 si reportó falla en algún momento, 0 si sobrevivió (censurado)
        disk_stats = df.groupBy("serial_number", "model").agg(
            F.count("*").alias("duration"),
            F.max("failure").alias("observed_event")
        )

        # Seleccionar los modelos con mayor presencia para comparar curvas legibles
        top_models = [
            row["model"] for row in disk_stats.groupBy("model")
            .count().orderBy(F.desc("count"))
            .limit(args.top_models).collect()
        ]

        print(f"Modelos seleccionados con mayor exposición: {top_models}")

        # Filtrar y recolectar a Pandas
        pandas_df = disk_stats.filter(F.col("model").isin(top_models)).toPandas()
        spark.stop()

        print("Ajustando estimador Kaplan-Meier...")
        plt.figure(figsize=(10, 6))
        kmf = KaplanMeierFitter()

        summary_results = []
        for model_name in top_models:
            model_data = pandas_df[pandas_df["model"] == model_name]
            T = model_data["duration"]
            E = model_data["observed_event"]
            
            kmf.fit(T, event_observed=E, label=model_name)
            kmf.plot_survival_function(ci_show=True)

            total_disks = len(model_data)
            failed_disks = int(E.sum())
            censored_disks = total_disks - failed_disks
            final_survival = kmf.survival_function_.iloc[-1, 0]

            summary_results.append({
                "model": model_name,
                "total_disks": total_disks,
                "failed_disks": failed_disks,
                "censored_disks": censored_disks,
                "final_survival_prob": round(float(final_survival), 5)
            })

        plt.title(f"Curvas de Supervivencia Kaplan-Meier por Modelo ({args.days} días)")
        plt.xlabel("Tiempo de seguimiento observado (Días)")
        plt.ylabel("Probabilidad de Supervivencia S(t)")
        plt.grid(True, linestyle="--", alpha=0.5)
        plt.ylim(0.98, 1.002)  # Escala ajustada debido a las altas tasas de supervivencia en ventanas cortas
        plt.tight_layout()

        plot_path = os.path.join("results", "curvas_supervivencia_km.png")
        plt.savefig(plot_path, dpi=300)
        plt.close()

        # Guardar resumen numérico
        summary_df = pd.DataFrame(summary_results)
        summary_csv = os.path.join("results", "resumen_supervivencia_km.csv")
        summary_df.to_csv(summary_csv, index=False)

        print("\n--- Resultados del Reto de Investigación ---")
        print(summary_df.to_string(index=False))
        print(f"\nGráfico exportado a: {plot_path}")
        print(f"Resumen numérico exportado a: {summary_csv}")

    finally:
        # Limpieza de temporales
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    main()
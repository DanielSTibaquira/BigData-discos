# Taller: MapReduce, balanceo de carga y Spark Streaming

## Objetivo

Analizar los datos diarios de Backblaze Drive Stats con PySpark y aplicar MapReduce, balanceo de carga y Structured Streaming con Kafka. El indicador principal será la tasa anualizada de fallas por modelo (AFR); no debe interpretarse como la vida útil media de los discos.

## Datos

- Archivo esperado (no incluido en el repositorio): `data/data_Q2_2026.zip`. Descárgalo desde Backblaze y colócalo en esa ruta antes de ejecutar los scripts.
- El ZIP de 2 GB y los materiales docentes de `docs/` no se versionan. Los resultados existentes sí se incluyen como evidencia de las ejecuciones documentadas.
- Contiene 91 CSV diarios, del 1 de abril al 30 de junio de 2026.
- Cada fila representa un disco observado en un día. Las columnas clave son `date`, `serial_number`, `model` y `failure`.
- Para el taller se seleccionarán **14 días consecutivos** (o 7 si los recursos son limitados). La muestra puede leerse desde el ZIP sin extraer el trimestre completo. Catorce archivos ocupan aproximadamente 1–2 GB descomprimidos.
- Mantener la misma muestra en las partes A, B y C para que los resultados sean comparables. Registrar las fechas y los archivos utilizados.
- Citar a Backblaze como fuente y respetar las condiciones de uso indicadas en el ejercicio.

### Indicador

```text
AFR (%) = (fallas × 365 / días-disco) × 100
```

En este ejercicio, `días-disco` es el número de filas observadas. `fallas` es la suma de la columna `failure`. Para cada modelo:

```text
días-disco = count(*)
fallas      = sum(failure)
```

La cifra de AFR es una tasa anualizada a partir de la exposición observada; no demuestra cuánto vivirá un disco. Los discos que siguen activos al final de la muestra están censurados: sabemos que sobrevivieron hasta su última observación, no cuándo fallarán.

## Ruta de trabajo

### Ejecutar las partes 0 y A

Se requiere Python con PySpark y Java 17 o superior. Desde PowerShell, con el entorno configurado:

```powershell
$env:JAVA_HOME = "C:\ruta\a\java-17"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
python src\mapreduce_disks.py --start-date 2026-04-01 --days 14
```

El script extrae temporalmente solo los CSV seleccionados, valida esquema, fechas y valores de `failure`, y elimina la muestra temporal al terminar. Compara MapReduce con RDD frente al `groupBy` de DataFrames; si no coinciden, la ejecución falla explícitamente. Deja `results/validacion_muestra.json` y `results/afr_por_modelo.csv`. Se puede cambiar el inicio o usar `--days 7`.

**Ejecución inicial (1–14 de abril de 2026):** se validaron 4.848.207 filas, 259 fallas y 79 modelos; se observaron 348.619 seriales durante entre 1 y 14 días. Los agregados RDD y DataFrame coincidieron exactamente. Los modelos que encabezan el AFR tienen pocas fallas y poca exposición, así que no se deben interpretar sin un umbral mínimo de días-disco.

### 0. Preparar y validar la muestra

1. Elegir 14 fechas consecutivas del trimestre y anotar la selección; usar 7 días si el equipo no puede procesar 14.
2. Leer solo esos CSV desde el ZIP o extraer únicamente esos miembros.
3. Inspeccionar encabezados, tipos, valores nulos y valores de `failure`; confirmar que las fechas de las filas coinciden con las fechas de los archivos.
4. Contar filas y fallas por día y guardar estos controles para detectar lecturas incompletas o errores de esquema.

**Evidencia:** fechas seleccionadas, conteos de validación y esquema de columnas utilizado.

### A. MapReduce por modelo — obligatorio, ~20 min

1. Leer los CSV y seleccionar `model` y `failure`.
2. En RDD, emitir `(model, (1, failure))` por fila y sumar ambos valores con `reduceByKey`.
3. Calcular `días-disco`, `fallas` y AFR por modelo.
4. Repetir el agregado con `groupBy` de DataFrames y comparar ambos resultados por modelo.
5. Contar los días observados por `serial_number`; explicar la censura y por qué una ventana corta no permite afirmar la vida completa del disco.

**Evidencia y criterio de aceptación:** los agregados RDD y DataFrame coinciden en días-disco y fallas por modelo; la fórmula de AFR está aplicada correctamente y sus límites se explican.

### B. Balanceo de carga — obligatorio, ~30 min

1. Ejecutar el agregado de la parte A con 2, 8, 32 y 200 particiones.
2. Repetir cada medición con la misma muestra, configuración y acción de Spark. Registrar todos los tiempos y resumirlos con la mediana; hacer una ejecución de calentamiento que no se incluya en las mediciones.
3. Comprobar el número real de particiones y documentar los recursos usados (modo local o clúster, núcleos y memoria).
4. Repartir los datos sin clave y por `model`. Contar las filas por partición usando `spark_partition_id()`.
5. Comparar la distribución y explicar si algunos modelos concentran más registros y cómo ese sesgo afecta la tarea más lenta.

**Evidencia y criterio de aceptación:** tabla de tiempos con repeticiones, configuración reproducible y conteos por partición para ambos repartos. La explicación del sesgo debe apoyarse en esos conteos, no solo en una expectativa.

#### Ejecución

```powershell
python src\load_balance_disks.py --start-date 2026-04-01 --days 14 --repeats 3
```

El script calienta cada configuración y mide el agregado DataFrame con el mismo `groupBy` validado contra el RDD en la parte A. Para cada configuración hace tres repeticiones por defecto y compara los conteos reales después de un `repartition` sin clave frente a uno por `model`. Genera `results/balanceo_carga.json` y `results/tiempos_particiones.csv`. Se pueden reducir los días o repeticiones para una prueba rápida.

**Medición inicial:** Spark 4.2.0 en `local[*]` (paralelismo predeterminado: 8), 14 días y dos repeticiones por configuración, luego de un calentamiento. La mediana fue **1,788 s** con 2 particiones, **1,688 s** con 8, **2,933 s** con 32 y **13,075 s** con 200. En esta ejecución local, 8 fue lo más rápido; muchas particiones pequeñas añadieron planificación y shuffle sin ayudar al cálculo.

El reparto sin clave fue casi uniforme: con 200 particiones tuvo entre 24.238 y 24.244 filas por partición. Al repartir por `model`, con 8 particiones la carga máxima fue 1,83 veces el promedio (CV 0,55); con 200, 131 particiones quedaron vacías y la más cargada tuvo 26,33 veces el promedio. Hay 79 modelos: el hash asigna modelos completos y el volumen desigual por modelo deja particiones desbalanceadas. Estos tiempos son de esta máquina y de esta muestra, no una recomendación universal para un clúster.

### C. Kafka y Structured Streaming — obligatorio, ~40 min

1. Crear un productor que publique los registros por fecha en orden cronológico, por ejemplo, un día cada 10 segundos.
2. Para que el productor sea manejable, filtrar de 3 a 5 modelos y conservar los campos necesarios: fecha del evento, modelo y falla (y serial si hace falta para los controles).
3. Publicar en un topic Kafka y comprobar que el consumidor recibe y puede interpretar los mensajes.
4. En Structured Streaming, parsear los mensajes y agrupar por modelo en ventanas de 7 días con watermark de 2 días.
5. Calcular por ventana los días-disco, las fallas y AFR. Probar el flujo de extremo a extremo y observar cómo se actualiza el resultado conforme llegan nuevas fechas.
6. Configurar y conservar un `checkpointLocation` para la consulta.

**Evidencia y criterio de aceptación:** instrucciones para iniciar Kafka, productor y aplicación Spark; salida de al menos una ventana validada con los datos enviados; configuración de ventana, watermark y checkpoint documentada.

#### Ejecución local

Instalar las dependencias Python (`python -m pip install -r requirements.txt`) y tener Java 17 o superior disponible en `JAVA_HOME`. Iniciar el broker de un nodo en una terminal:

```powershell
docker compose up -d kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic backblaze-drives --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

Iniciar Structured Streaming en otra terminal. Spark descarga el conector Kafka compatible con Spark 4.2 y Scala 2.13 al primer uso:

```powershell
python src\stream_disks.py --bootstrap-servers localhost:9092 --topic backblaze-drives
```

Publicar los modelos de ejemplo de la muestra de 14 días en una tercera terminal (el productor espera 10 segundos entre fechas):

```powershell
python src\produce_disks.py --start-date 2026-04-01 --days 14 --bootstrap-servers localhost:9092 --topic backblaze-drives
```

El productor publica una muestra determinista del 10 % por modelo y día para mantener un volumen manejable; cada evento incluye peso inverso (`10`) para escalar `drive_days` y fallas en las agregaciones. Puede ajustarse con `--sample-fraction` (usar `1` para publicar todas las filas). La AFR de esta muestra ponderada es una estimación y puede variar por muestreo.

La salida de Spark muestra agregados por modelo en ventanas de 7 días UTC, con `drive_days`, fallas y AFR. El watermark de 2 días permite datos tardíos; se usa modo `update` para observar también las ventanas aún abiertas mientras avanza el productor. En local se usan 8 particiones de shuffle y un máximo de 50.000 offsets por micro-lote, ajustables con `--shuffle-partitions` y `--max-offsets-per-trigger`. El checkpoint queda en `results/checkpoints/backblaze-q2-2026`. Para repetir desde cero, detener la consulta y usar un directorio de checkpoint nuevo mediante `--checkpoint`; no reutilizar un checkpoint con otra configuración de consulta. Kafka persiste sus datos en el volumen `kafka-data`.

**Prueba de extremo a extremo:** se publicaron 28.754 eventos muestreados de tres modelos, se confirmaron los offsets de las tres particiones y Spark produjo agregados de dos ventanas UTC. La evidencia y las AFR observadas están en [`results/kafka_streaming_smoke.json`](./results/kafka_streaming_smoke.json). Esos agregados usan ponderación inversa del muestreo del 10 %; el AFR alto de una ventana pequeña ilustra por qué hace falta cautela con baja exposición.

### D. MongoDB — opcional, ~20 min

1. Escribir los agregados por modelo y ventana en MongoDB.
2. Consultar los cinco modelos con mayor AFR, exigiendo un mínimo de días-disco para evitar rankings basados en pocas observaciones.
3. Documentar el umbral elegido y verificar que la consulta y los resultados correspondan a los agregados de Spark.

**Evidencia:** consulta reproducible, umbral documentado y resultado de los cinco modelos. Esta actividad no es geoespacial: el dataset de discos no incluye coordenadas.

#### Implementación en Python

Se añadió `src/stream_disks_mongodb.py`. El streaming guarda agregados actualizados por ventana de 7 días en `drive_stats.model_windows`. Cada documento identifica `run_id`, modelo e inicio de ventana; un índice único y `ReplaceOne(..., upsert=True)` impiden duplicados si Spark reintenta un micro-lote. Se guarda `sample_fraction` para dejar claro si el AFR procede de una muestra ponderada. La consulta pide un `run_id` y una ventana concretos y filtra un mínimo configurable de 50.000 días-disco; puede devolver menos de cinco modelos si no hay cinco que cumplan el umbral.

Con Docker Desktop activo, levantar ambos servicios y crear el topic dedicado:

```powershell
docker compose up -d kafka mongodb
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic backblaze-drives-mongo --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
python -m pip install -r requirements.txt
```

En una terminal iniciar el consumidor/stream de Mongo:

```powershell
python src\stream_disks_mongodb.py run --topic backblaze-drives-mongo --checkpoint results\checkpoints\backblaze-q2-mongodb
```

En otra terminal publicar la muestra (los cinco modelos están preseleccionados y existen en el trimestre):

```powershell
python src\produce_disks.py --start-date 2026-04-01 --days 14 --topic backblaze-drives-mongo --run-id q2-2026-14d-demo
```

Una vez que el productor termina y el stream procesa los eventos, consultar una ventana completa, por ejemplo la que empieza el 2 de abril:

```powershell
python src\stream_disks_mongodb.py top5 --run-id q2-2026-14d-demo --window-start 2026-04-02T00:00:00Z --min-drive-days 50000
```

`MONGODB_URI` permite usar una instancia distinta de `mongodb://localhost:27017`. El script valida la conexión explícitamente. Para repetir una ejecución, detener Spark y Kafka, usar un topic/checkpoint limpio o cambiar `run_id`; el índice no permitirá duplicados del mismo run, modelo y ventana. Compass se puede usar para inspeccionar la colección y contrastar los documentos, pero el procesamiento y la consulta solicitada son reproducibles desde Python.

### Reto de investigación — elegir uno

El equipo elegirá una opción y entregará una página con lo aprendido y una integración pequeña que funcione:

- **Apache Iceberg:** consultar la copia de Backblaze en Iceberg desde Spark, compararla con la lectura de CSV y mostrar una consulta o comparación verificable.
- **Kaplan–Meier:** estimar curvas de supervivencia por modelo y explicar cómo se trata la censura. Se necesitan observaciones longitudinales por disco y tiempos de entrada/salida adecuados; no presentar el AFR de una muestra de 7–14 días como vida útil estimada.

**Criterio de aceptación:** evidencia de una integración ejecutada por el equipo, no solo un resumen de documentación.

## Entrega y comprobación final

Entregar:

1. Código o notebooks para las partes realizadas, con instrucciones de ejecución y dependencias.
2. Selección de archivos/fechas y controles de calidad de los datos.
3. Tabla comparativa RDD/DataFrame y resultados de AFR.
4. Tabla de tiempos repetidos y análisis de carga por partición.
5. Evidencia del flujo Kafka–Structured Streaming, incluidas ventanas y watermark.
6. Si se realiza, consulta MongoDB y/o integración del reto de investigación.
7. Fuente de los datos, decisiones, limitaciones y configuración de ejecución.

Antes de presentar, confirmar:

- RDD y DataFrame coinciden en días-disco y fallas.
- AFR se calcula como `(fallas × 365 / días-disco) × 100`.
- Los tiempos se midieron sobre la misma carga y se reportan con repeticiones.
- La explicación del balanceo usa los conteos observados por partición.
- El streaming funciona de extremo a extremo y aplica ventanas de 7 días y watermark de 2 días.
- Las conclusiones distinguen una tasa anualizada de fallas de una estimación de vida útil.

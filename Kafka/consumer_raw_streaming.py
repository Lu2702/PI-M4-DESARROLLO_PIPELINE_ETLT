#!/usr/bin/env python3
"""
Consumidor de Kafka → Parquet (RAW Streaming)
=============================================

Descripción
-----------
Lee mensajes JSON desde un tópico de Kafka (producidos por OpenWeather),
los parsea con un esquema explícito y los escribe en formato Parquet, particionados
por `date` y `city`. Mantiene estado de streaming mediante un directorio de checkpoint.

Modos de ejecución
------------------
- oneshot   : Usa trigger `availableNow=True` → procesa todo lo disponible y termina.
- streaming : Usa trigger `processingTime=15 seconds` → se mantiene corriendo.

Argumentos CLI
--------------
--bootstrap           : Lista `host:port` del/los broker(s) Kafka. (requerido)
--topic               : Nombre del tópico a consumir. (default: openweather-topic)
--out                 : Ruta de salida para Parquet (local, HDFS o s3a://...). (requerido)
--checkpoint          : Ruta para el checkpoint de streaming. (requerido)
--starting-offsets    : "earliest" o "latest" (default: earliest)
--mode                : "oneshot" o "streaming" (default: oneshot)

Entradas / Salidas
------------------
- Input  : Kafka (clave ignorada, `value` es un JSON con el payload).
- Output : Parquet con `partitionBy("date", "city")`.
- Checkpoint: Carpeta donde Spark guarda offsets/estado para tolerancia a fallos.

Esquema esperado (coincide con el del producer)
-----------------------------------------------
source        (string)
city          (string)
lat           (double)
lon           (double)
ts            (long, epoch seconds)
datetime_utc  (string ISO-8601)
temp_c        (double)
humidity      (double)
pressure      (double)
wind_speed    (double)

Requisitos
----------
- Spark con Structured Streaming.
- Conector Kafka para Spark: `org.apache.spark:spark-sql-kafka-0-10_2.12:<spark_ver>`
- (Opcional S3) `org.apache.hadoop:hadoop-aws:3.3.4` + `com.amazonaws:aws-java-sdk-bundle:1.12.262`
- Permisos/credenciales válidas si escribes a `s3a://...`
"""

import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# ====== Argumentos ======
parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", required=True)
parser.add_argument("--topic", default="openweather-topic")
parser.add_argument("--out", required=True)
parser.add_argument("--checkpoint", required=True)
parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
parser.add_argument("--mode", default="oneshot", choices=["oneshot", "streaming"])
args = parser.parse_args()

# ====== Rutas ======
# Normaliza para asegurar terminación con "/"
out_path = args.out.rstrip("/") + "/"
ckpt_path = args.checkpoint.rstrip("/") + "/"

# ====== Esquema ======
# Debe reflejar exactamente la estructura del JSON producido.
schema = StructType([
    StructField("source", StringType()),
    StructField("city", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("ts", LongType()),
    StructField("datetime_utc", StringType()),
    StructField("temp_c", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("wind_speed", DoubleType())
])

# Construcción de la sesión Spark
spark = (SparkSession.builder
         .appName("OW-RAW-Streaming")
         .config("spark.sql.session.timeZone", "UTC")              # Consistencia temporal
         .config("spark.sql.parquet.compression.codec", "snappy")  # Compresión eficiente
         .getOrCreate())

# ====== Lectura desde Kafka ======
# Lee el stream de Kafka con los offsets iniciales definidos por CLI.
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", args.bootstrap)
      .option("subscribe", args.topic)
      .option("startingOffsets", args.starting_offsets)
      .option("failOnDataLoss", "false")
      .load())

# ====== Parseo ======
# - Convierte value (bytes) → string → JSON con esquema.
# - Deriva event_ts a partir de datetime_utc o ts (epoch).
# - Agrega ingest_ts y la partición de fecha.
parsed = (df.selectExpr("CAST(value AS STRING) AS json_str")
           .select(F.from_json("json_str", schema).alias("d"))
           .select("d.*")
           .withColumn("event_ts",
                       F.coalesce(
                           F.to_timestamp("datetime_utc"),
                           F.to_timestamp(F.from_unixtime("ts"))
                       ))
           .withColumn("ingest_ts", F.current_timestamp())
           .withColumn("date", F.to_date("event_ts")))

# ====== Escritura ======
# Escribe en Parquet particionando por (date, city) con modo append.
writer = (parsed.writeStream
          .format("parquet")
          .option("path", out_path)
          .option("checkpointLocation", ckpt_path)
          .partitionBy("date", "city")
          .outputMode("append"))

# Selección de trigger según el modo:
# - oneshot   → availableNow=True (procesa backlog y se detiene).
# - streaming → processingTime="15 seconds" (ciclo periódico).
if args.mode == "oneshot":
    q = writer.trigger(availableNow=True).start()
else:
    q = writer.trigger(processingTime="15 seconds").start()

# Espera a que el query termine (en streaming, hasta que se interrumpa el proceso).
q.awaitTermination()

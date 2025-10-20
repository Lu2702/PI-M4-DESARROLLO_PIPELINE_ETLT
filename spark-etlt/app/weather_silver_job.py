# -*- coding: utf-8 -*-
# =============================================================================
# weather_silver_job.py
# -----------------------------------------------------------------------------
# Propósito:
#   - Estandarizar y enriquecer datos de OpenWeather desde BRONZE → SILVER
#   - Combina dos fuentes:
#       * Airbyte (Parquet particionado por year/month/day)
#       * Manual (JSON históricos por ingest_date)
#   - Genera columnas derivadas necesarias para GOLD (sin normalizar 0–1 aquí)
#   - Escribe en Parquet Snappy particionado por city/year/month/day
#
# Modo de uso (ejemplos):
#   # Upsert del día (desde Airbyte):
#   spark-submit ... weather_silver_job.py \
#     --mode upsert --city Patagonia --date 2025-10-04 \
#     --bronze s3a://etlt-datalake-dev-us-east-1-bronze \
#     --silver s3a://etlt-datalake-dev-us-east-1-silver
#
#   # Backfill manual (desde JSON históricos):
#   spark-submit ... weather_silver_job.py \
#     --mode backfill-manual --city Riohacha --ingest-date 2025-10-04 \
#     --bronze s3a://etlt-datalake-dev-us-east-1-bronze \
#     --silver s3a://etlt-datalake-dev-us-east-1-silver
#
# Notas de ejecución en tu EC2:
#   - Si dejaste "solo master" en Docker, ejecuta en local:  --master local[*]
#   - El acceso a S3 usa el rol IAM de la instancia (InstanceProfile).
# =============================================================================
# BRONZE (Airbyte/Manual)  ->  SILVER (Parquet Snappy particionado)
# Particiones: city / event_year / event_month / event_day
# - Corre en Docker (Bitnami Spark) sobre EC2 con credenciales vía InstanceProfile.
# - Cálculo de hora local estable con dt + timezone (segundos).
# =============================================================================

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

# ------------------------------ Constantes -----------------------------------
CITY_PREFIX = {
    "Patagonia": "city-patagonia",
    "Riohacha":  "city-riohacha",
}

KEEP = [
    "city", "event_ts", "event_date", "hour",
    "timezone_off", "local_ts", "local_hour", "is_daylight",
    "event_year", "event_month", "event_day",
    "lat", "lon",
    "temp", "feels_like", "humidity", "pressure",
    "wind_speed", "wind_deg", "clouds_pct",
    "rain_1h", "snow_1h", "precip_1h",
    "weather_main", "weather_desc",
    "uv_index",
    "solar_uv_proxy", "solar_cloud_proxy", "solar_potential_raw",
    "air_density", "wind_power_wm2",
    "cloud_bin", "wind_bin", "humidity_bin",
    "src_system", "etl_run_ts",
]

PARTITION_COLS = ["city","event_year","event_month","event_day"]

# ------------------------------ Spark Session (SIN CAMBIOS) ------------------
def build_spark(app_name="weather-silver-job", shuffle_parts=8):
    """
    Construye Spark para correr local en el contenedor.
    La configuración de S3A/credenciales/timeouts ya viene de spark-defaults.conf.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", str(shuffle_parts))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.warehouse.dir", "/opt/etlt/app/spark-warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Logging breve (útil para debug)
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    print("[Spark] master                         =", spark.sparkContext.master)
    print("[Spark] fs.s3a.impl                    =", hconf.get("fs.s3a.impl"))
    print("[Spark] fs.s3a.endpoint                =", hconf.get("fs.s3a.endpoint"))
    print("[Spark] fs.s3a.credentials.provider    =", hconf.get("fs.s3a.aws.credentials.provider"))
    print("[Spark] shuffle.partitions             =", spark.conf.get("spark.sql.shuffle.partitions"))
    return spark

# ------------------------------ Lecturas -------------------------------------
def read_airbyte_city_day(sp, bronze_root, city, y, m, d):
    path = f"{bronze_root}/airbyte/openweather/{CITY_PREFIX[city]}/year={y}/month={m:02d}/day={d:02d}/"
    return sp.read.parquet(path).withColumn("src_system", F.lit("airbyte"))

def read_manual_city(sp, bronze_root, city, ingest_date):
    path = f"{bronze_root}/manual/{CITY_PREFIX[city]}/ingest_date={ingest_date}/*.json"
    df = sp.read.option("multiLine", True).json(path)

    # si el JSON es array raíz → explotar
    if len(df.schema.fields) == 1 and isinstance(df.schema.fields[0].dataType, T.ArrayType):
        root = df.schema.fields[0].name
        df = df.select(F.explode(F.col(root)).alias("obj")).select("obj.*")
    return df.withColumn("src_system", F.lit("manual"))

# ------------------------------ Helpers --------------------------------------
def one_hour_precip(df, root_col: str):
    field = next((f for f in df.schema.fields if f.name == root_col), None)
    if field and isinstance(field.dataType, T.StructType):
        names = {f.name for f in field.dataType.fields}
        if "1h" in names:
            return F.col(root_col).getField("1h").cast("double")
        if "_1h" in names:
            return F.col(root_col).getField("_1h").cast("double")
    return F.lit(0.0)

def bin_col(col, edges):
    return (
        F.when(col.isNull(), F.lit(None))
         .when(col < edges[0], F.lit(f"<{edges[0]}"))
         .when(col < edges[1], F.lit(f"{edges[0]}-{edges[1]}"))
         .when(col < edges[2], F.lit(f"{edges[1]}-{edges[2]}"))
         .when(col < edges[3], F.lit(f"{edges[2]}-{edges[3]}"))
         .otherwise(F.lit(f"{edges[3]}+"))
    )

# ------------------------------ Normalización (fechas robustas) --------------
def normalize_to_silver(df, job_city):
    cols = set(df.columns)

    # lat/lon
    lat_expr = F.col("coord.lat").cast("double") if "coord" in cols else F.col("lat").cast("double")
    lon_expr = F.col("coord.lon").cast("double") if "coord" in cols else F.col("lon").cast("double")

    # weather array (1-based en Spark SQL)
    if "weather" in cols:
        w_main = F.element_at(F.col("weather"), 1).getField("main")
        w_desc = F.element_at(F.col("weather"), 1).getField("description")
    else:
        w_main = F.lit(None).cast("string")
        w_desc = F.lit(None).cast("string")

    rain_1h_raw = one_hour_precip(df, "rain")
    snow_1h_raw = one_hour_precip(df, "snow")
    uv_index_expr = F.col("uv_index").cast("double") if "uv_index" in cols else F.lit(None).cast("double")

    base = (
        df.withColumn("lat", lat_expr)
          .withColumn("lon", lon_expr)
          .withColumn("city", F.lit(job_city))
          .withColumn("src_system", F.coalesce(F.col("src_system"), F.lit("openweather")))
    )

    # dt y timezone en segundos (enteros)
    dt_sec = F.col("dt").cast("long")
    tz_sec = F.coalesce(F.col("timezone").cast("int"), F.lit(0))

    silver = (
        base
        # tiempo base UTC a partir de epoch (segundos)
        .withColumn("event_ts",   F.to_timestamp(F.from_unixtime(dt_sec)))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("hour",       F.hour("event_ts"))

        # métricas
        .withColumn("temp",        F.col("main.temp").cast("double"))
        .withColumn("feels_like",  F.col("main.feels_like").cast("double"))
        .withColumn("humidity",    F.col("main.humidity").cast("int"))
        .withColumn("pressure",    F.col("main.pressure").cast("int"))
        .withColumn("wind_speed",  F.col("wind.speed").cast("double"))
        .withColumn("wind_deg",    F.col("wind.deg").cast("int"))
        .withColumn("clouds_pct",  F.col("clouds.all").cast("int"))
        .withColumn("rain_1h",     F.coalesce(rain_1h_raw, F.lit(0.0)))
        .withColumn("snow_1h",     F.coalesce(snow_1h_raw, F.lit(0.0)))
        .withColumn("precip_1h",   (F.col("rain_1h") + F.col("snow_1h")).cast("double"))
        .withColumn("weather_main", w_main)
        .withColumn("weather_desc", w_desc)

        # tiempo local (dt + timezone, ambos en segundos)
        .withColumn("timezone_off", tz_sec)
        .withColumn("local_ts",     F.to_timestamp(F.from_unixtime(dt_sec + tz_sec)))
        .withColumn("local_hour",   F.hour("local_ts"))
        .withColumn("is_daylight",  ((F.col("local_hour") >= 7) & (F.col("local_hour") <= 19)).cast("int"))

        # derivados
        .withColumn("uv_index",           uv_index_expr)
        .withColumn("solar_uv_proxy",     F.col("uv_index"))
        .withColumn("solar_cloud_proxy",
                    F.when(F.col("uv_index").isNull(),
                           (F.lit(1.0) - (F.col("clouds_pct")/100.0)) * F.col("is_daylight")))
        .withColumn("solar_potential_raw", F.coalesce(F.col("solar_uv_proxy"), F.col("solar_cloud_proxy")))
        .withColumn("air_density",
                    F.when(F.col("pressure").isNotNull() & F.col("temp").isNotNull(),
                           (F.col("pressure")*F.lit(100.0)) / (F.lit(287.058)*(F.col("temp")+F.lit(273.15))))
                     .otherwise(F.lit(None)).cast("double"))
        .withColumn("wind_power_wm2",
                    (F.lit(0.5) * F.col("air_density") * F.pow(F.col("wind_speed"), 3)).cast("double"))

        .withColumn("cloud_bin",    bin_col(F.col("clouds_pct"), [20,40,60,80]))
        .withColumn("wind_bin",     bin_col(F.col("wind_speed"), [3.0,6.0,9.0,12.0]))
        .withColumn("humidity_bin", bin_col(F.col("humidity"),   [40,60,80,90]))

        .withColumn("etl_run_ts", F.current_timestamp())
        .drop("main","wind","clouds","weather","rain","snow","coord",
              "city_name","name","dt_iso","base","sys","visibility","id","cod","timezone")
        .dropDuplicates(["city","event_ts"])
        .withColumn("event_year",  F.year("event_date").cast("int"))
        .withColumn("event_month", F.month("event_date").cast("int"))
        .withColumn("event_day",   F.dayofmonth("event_date").cast("int"))
    )

    return silver.select([c for c in KEEP if c in silver.columns])

# ------------------------------ Escritura (sin HIVE default) -----------------
def write_silver(df_lite, silver_root):
    # 1) Bloquear particiones nulas para evitar __HIVE_DEFAULT_PARTITION__
    required = ["event_ts"] + PARTITION_COLS
    before = df_lite.count()
    df_clean = df_lite.na.drop(subset=required)
    after = df_clean.count()
    dropped = before - after
    print(f"[DQ] Registros originales: {before} | sin particiones nulas: {after} | descartados: {dropped}")

    # 2) Escribir
    (df_clean
     .repartition(*PARTITION_COLS)
     .write.mode("overwrite")
     .partitionBy(*PARTITION_COLS)
     .option("compression","snappy")
     .parquet(f"{silver_root}/openweather"))

def read_silver_partition(sp, silver_root, city, y, m, d):
    path = f"{silver_root}/openweather/city={city}/event_year={y}/event_month={m}/event_day={d}/"
    try:
        return sp.read.parquet(path)
    except Exception:
        return None

# ------------------------------ Modos ----------------------------------------
def backfill_manual(sp, bronze_root, silver_root, city, ingest_date):
    df = read_manual_city(sp, bronze_root, city, ingest_date)
    write_silver(normalize_to_silver(df, city), silver_root)

def upsert_airbyte_day(sp, bronze_root, silver_root, city, y, m, d):
    new_df = normalize_to_silver(read_airbyte_city_day(sp, bronze_root, city, y, m, d), city)
    old_df = read_silver_partition(sp, silver_root, city, y, m, d)
    merged = old_df.unionByName(new_df, allowMissingColumns=True).dropDuplicates(["city","event_ts"]) if old_df is not None else new_df
    write_silver(merged, silver_root)

# ------------------------------ CLI ------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETLT OpenWeather: BRONZE → SILVER")
    parser.add_argument("--mode", choices=["backfill-manual","upsert"], required=True)
    parser.add_argument("--city", choices=list(CITY_PREFIX.keys()), required=True)
    parser.add_argument("--date", help="YYYY-MM-DD (para --mode upsert)", default=None)
    parser.add_argument("--ingest-date", help="YYYY-MM-DD (para --mode backfill-manual)", default=None)
    parser.add_argument("--bronze", default="s3a://etlt-datalake-dev-us-east-1-bronze")
    parser.add_argument("--silver", default="s3a://etlt-datalake-dev-us-east-1-silver")
    parser.add_argument("--shuffle-partitions", type=int, default=8)
    args = parser.parse_args()

    spark = build_spark(shuffle_parts=args.shuffle_partitions)

    if args.mode == "backfill-manual":
        if not args.ingest_date:
            raise SystemExit("--ingest-date es requerido para backfill-manual")
        backfill_manual(spark, args.bronze, args.silver, args.city, args.ingest_date)

    elif args.mode == "upsert":
        if not args.date:
            raise SystemExit("--date es requerido para upsert")
        y, m, d = map(int, args.date.split("-"))
        upsert_airbyte_day(spark, args.bronze, args.silver, args.city, y, m, d)

    print(f"[OK] weather_silver_job terminado: mode={args.mode} city={args.city}")
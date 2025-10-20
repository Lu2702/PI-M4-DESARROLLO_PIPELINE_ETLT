# -*- coding: utf-8 -*-
# =============================================================================
# weather_gold_job.py  (v2.10)
# =============================================================================
# Descripción:
#   Construye la CAPA GOLD desde SILVER (modelo Kimball) para el dominio
#   openweather. Genera DIMs, FACTs y respuestas Q1–Q7.
#
# NOTA EC2/DOCKER:
#   - Este script asume que se ejecuta con `spark-submit` en una instancia EC2,
#     idealmente con un IAM Role adjunto que tenga permisos S3 (GetObject,
#     PutObject, ListBucket) sobre los buckets silver/gold. En Docker, el
#     contenedor hereda credenciales vía Instance Metadata (IMDS) o variables
#     de entorno.
#
#
# Paths GOLD (S3):
#   s3a://etlt-datalake-dev-us-east-1-gold/openweather/
#     ├─ dim_*
#     ├─ fact_*
#     └─ answers/
#        ├─ q1_solar_hour_by_month/
#        ├─ q2_wind_patterns/
#        ├─ q3_weather_main/                 
#        ├─ q4_today_vs_last_year/date=YYYY-MM-DD/
#        ├─ q5_best_days_topk/
#        ├─ q5_worst_days_topk/
#        ├─ q6_wind_sector_topk/
#        └─ q7_temp_extremes/{hottest_topk,coldest_topk}/
#
# Notas clave:
#   - RPS (potencial renovable normalizado 0–1):
#       Día: 0.6*solar_norm + 0.4*wind_norm
#       Noche: wind_norm
#   - Normalización por percentiles p5–p95 con *fallbacks*:
#       2024 por ciudad/mes  →  todos los años por ciudad/mes  →  global por mes
#       y con epsilon para evitar p95≈p5.
#   - Q3 usa solo días con cobertura != 'low' para evitar sesgos.
#   - DIM joins con broadcast (map-side join) para reducir shuffles.
#
# OPTIMIZACIÓN:
#   - Cache + materialización de DIMs tras construirlas (cache(); count()) para
#     evitar relecturas a S3 durante overwrite y estabilizar el pipeline.
#   - Unpersist de DIMs al final para liberar memoria.
# =============================================================================
import argparse
from pyspark.sql import SparkSession, functions as F, Window as W

# ------------------------------ Constantes ----------------------------------- #
SILVER = "s3a://etlt-datalake-dev-us-east-1-silver/openweather"
GOLDEN = "s3a://etlt-datalake-dev-us-east-1-gold/openweather"

COVERAGE_MIN_DAYS = 7
DAYLIGHT_MIN = 8.0
DAYLIGHT_MAX = 16.0

# ------------------------- Spark Session ------------------------ #
def build_spark(app_name="weather-gold-job", shuffle_parts=16):
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
    return spark

# -------------------------------- Utils -------------------------------------- #
def clamp01(col):
    return F.when(col < 0, 0.0).when(col > 1, 1.0).otherwise(col)

def safe_norm(num, lo, hi):
    denom = (hi - lo)
    return F.when(denom.isNull() | (F.abs(denom) < 1e-9), F.lit(0.0)) \
            .otherwise((num - lo)/denom)

def write_parquet(df, path, parts=None, max_records_per_file=None):
    w = df.write.mode("overwrite").option("compression","snappy")
    if parts: w = w.partitionBy(*parts)
    if max_records_per_file: w = w.option("maxRecordsPerFile", str(max_records_per_file))
    w.parquet(path)

def quality_from_pct(pct_col):
    return (F.when(pct_col >= 0.6, "good")
             .when(pct_col >= 0.2, "partial")
             .otherwise("low"))

# -------------------------------- Main --------------------------------------- #
def main(args):
    spark = build_spark(shuffle_parts=args.shuffle_partitions)

    # ---------- 1) Load SILVER ---------- #
    silver = spark.read.parquet(SILVER)
    if args.years_keep or args.months_keep or args.cities:
        cond = F.lit(True)
        if args.years_keep:
            years = [int(x.strip()) for x in args.years_keep.split(",") if x.strip()]
            cond = cond & (F.col("event_year").isin(years))
        if args.months_keep:
            months = [int(x.strip()) for x in args.months_keep.split(",") if x.strip()]
            cond = cond & (F.col("event_month").isin(months))
        if args.cities:
            cities = [c.strip() for c in args.cities.split(",") if c.strip()]
            cond = cond & (F.col("city").isin(cities))
        silver = silver.where(cond)
    silver = silver.cache()

    # ---------- 2) DIMs ---------- #
    dim_date = (
        silver.select(F.to_date("event_date").alias("date"))
              .dropna().distinct()
              .withColumn("date_key", F.date_format("date","yyyyMMdd").cast("int"))
              .withColumn("year",  F.year("date"))
              .withColumn("month", F.month("date"))
              .withColumn("day",   F.dayofmonth("date"))
              .select("date_key","date","year","month","day")
    )

    dim_city = (
        silver.select(F.col("city").alias("city_name"),"lat","lon")
              .dropDuplicates(["city_name"])
              .withColumn("city_key",(F.abs(F.hash("city_name")) % F.lit(1000000)).cast("int"))
              .select("city_key","city_name","lat","lon")
    )

    dim_weather_condition = (
        silver.select("weather_main","weather_desc")
              .fillna({"weather_main":"Unknown","weather_desc":"Unknown"})
              .dropDuplicates()
              .withColumn("weather_key",(F.abs(F.hash("weather_main","weather_desc")) % F.lit(1000000)).cast("int"))
              .select("weather_key","weather_main","weather_desc")
    )

    # Materializar DIMs 
    dim_date = dim_date.cache(); _ = dim_date.count()
    dim_city = dim_city.cache(); _ = dim_city.count()
    dim_weather_condition = dim_weather_condition.cache(); _ = dim_weather_condition.count()

    write_parquet(dim_date, f"{GOLDEN}/dim_date", max_records_per_file=args.max_records_per_file)
    write_parquet(dim_city, f"{GOLDEN}/dim_city", max_records_per_file=args.max_records_per_file)
    write_parquet(dim_weather_condition, f"{GOLDEN}/dim_weather_condition",
                  max_records_per_file=args.max_records_per_file)

    d_city  = F.broadcast(dim_city.select("city_key","city_name"))
    d_wx    = F.broadcast(dim_weather_condition.select("weather_key","weather_main","weather_desc"))
    d_wsect = F.broadcast(
        spark.createDataFrame(
            [(0,"N",  -22.5,  22.5),
             (1,"NE",  22.5,  67.5),
             (2,"E",   67.5, 112.5),
             (3,"SE", 112.5, 157.5),
             (4,"S",  157.5, 202.5),
             (5,"SW", 202.5, 247.5),
             (6,"W",  247.5, 292.5),
             (7,"NW", 292.5, 337.5)],
            "sector_key int, sector_name string, deg_start double, deg_end double"
        ).select("sector_key","sector_name","deg_start","deg_end")
    )
    d_date  = F.broadcast(dim_date.select("date_key","date","year","month","day"))

    # ---------- 3) Normalización p5–p95 con fallbacks ---------- #
    base = silver.select(
        "city","event_date","event_year","event_month","local_hour",
        F.col("is_daylight").cast("int").alias("is_daylight"),
        "solar_potential_raw","wind_power_wm2",
        "wind_speed","wind_deg","temp","feels_like",
        "precip_1h","humidity","clouds_pct",
        "weather_main","weather_desc"
    )

    p24_solar = (base.filter(F.col("event_year")==2024)
                    .groupBy("city","event_month")
                    .agg(F.expr("percentile_approx(solar_potential_raw, array(0.05,0.95), 200) as q")))
    p24_wind  = (base.filter(F.col("event_year")==2024)
                    .groupBy("city","event_month")
                    .agg(F.expr("percentile_approx(wind_power_wm2, array(0.05,0.95), 200) as q")))
    pa_solar  = (base.groupBy("city","event_month")
                    .agg(F.expr("percentile_approx(solar_potential_raw, array(0.05,0.95), 200) as q")))
    pa_wind   = (base.groupBy("city","event_month")
                    .agg(F.expr("percentile_approx(wind_power_wm2, array(0.05,0.95), 200) as q")))

    perc_city = (
        p24_solar.alias("s24").join(p24_wind.alias("w24"), ["city","event_month"], "full_outer")
        .join(pa_solar.alias("sa"), ["city","event_month"], "full_outer")
        .join(pa_wind.alias("wa"), ["city","event_month"], "full_outer")
        .select(
            F.coalesce(F.col("s24.city"), F.col("sa.city")).alias("city"),
            F.col("event_month"),
            F.coalesce(F.element_at("s24.q",1), F.element_at("sa.q",1)).alias("solar_p5_city"),
            F.coalesce(F.element_at("s24.q",2), F.element_at("sa.q",2)).alias("solar_p95_city"),
            F.coalesce(F.element_at("w24.q",1), F.element_at("wa.q",1)).alias("wind_p5_city"),
            F.coalesce(F.element_at("w24.q",2), F.element_at("wa.q",2)).alias("wind_p95_city"),
        )
        .withColumn("solar_p95_city", F.when(F.col("solar_p95_city") <= F.col("solar_p5_city")+F.lit(1e-9),
                                             F.col("solar_p5_city")+F.lit(1e-6)).otherwise(F.col("solar_p95_city")))
        .withColumn("wind_p95_city",  F.when(F.col("wind_p95_city")  <= F.col("wind_p5_city")+F.lit(1e-9),
                                             F.col("wind_p5_city")+F.lit(1e-6)).otherwise(F.col("wind_p95_city")))
    )

    perc_global = (
        base.groupBy("event_month")
            .agg(
                F.expr("percentile_approx(solar_potential_raw, array(0.05,0.95), 200)").alias("qs"),
                F.expr("percentile_approx(wind_power_wm2, array(0.05,0.95), 200)").alias("qw"),
            )
            .select(
                "event_month",
                F.element_at("qs",1).alias("solar_p5_g"),
                F.element_at("qs",2).alias("solar_p95_g"),
                F.element_at("qw",1).alias("wind_p5_g"),
                F.element_at("qw",2).alias("wind_p95_g"),
            )
            .withColumn("solar_p95_g", F.when(F.col("solar_p95_g") <= F.col("solar_p5_g")+F.lit(1e-9),
                                              F.col("solar_p5_g")+F.lit(1e-6)).otherwise(F.col("solar_p95_g")))
            .withColumn("wind_p95_g",  F.when(F.col("wind_p95_g")  <= F.col("wind_p5_g")+F.lit(1e-9),
                                              F.col("wind_p5_g")+F.lit(1e-6)).otherwise(F.col("wind_p95_g")))
    )

    hourly_n = (
        base.join(perc_city, ["city","event_month"], "left")
            .join(perc_global, ["event_month"], "left")
            .withColumn("solar_p5",  F.coalesce(F.col("solar_p5_city"),  F.col("solar_p5_g"),  F.lit(0.0)))
            .withColumn("solar_p95", F.coalesce(F.col("solar_p95_city"), F.col("solar_p95_g"), F.lit(1.0)))
            .withColumn("wind_p5",   F.coalesce(F.col("wind_p5_city"),   F.col("wind_p5_g"),   F.lit(0.0)))
            .withColumn("wind_p95",  F.coalesce(F.col("wind_p95_city"),  F.col("wind_p95_g"),  F.lit(1.0)))
            .withColumn(
                "solar_norm",
                F.when(F.col("is_daylight")==0, 0.0)
                 .otherwise(safe_norm(F.col("solar_potential_raw"), F.col("solar_p5"), F.col("solar_p95")))
            )
            .withColumn(
                "wind_norm",
                clamp01(safe_norm(F.col("wind_power_wm2"), F.col("wind_p5"), F.col("wind_p95")))
            )
            .withColumn("solar_norm", clamp01(F.when(F.isnan("solar_norm") | F.col("solar_norm").isNull(), 0.0)
                                               .otherwise(F.col("solar_norm"))))
            .withColumn("wind_norm",  clamp01(F.when(F.isnan("wind_norm")  | F.col("wind_norm").isNull(),  0.0)
                                               .otherwise(F.col("wind_norm"))))
            .withColumn("rps_hour",
                F.when(F.col("is_daylight")==1, 0.6*F.col("solar_norm")+0.4*F.col("wind_norm"))
                 .otherwise(F.col("wind_norm"))
            )
    )

    # ---------- 4) FACT HOURLY ---------- #
    sector_idx = F.floor(((F.col("wind_deg")+F.lit(22.5)) % 360)/45).cast("int")
    fact_weather_hourly = (
        hourly_n
        .withColumn("event_date", F.to_date("event_date"))
        .withColumn("date_key", F.date_format("event_date","yyyyMMdd").cast("int"))
        .join(d_city, hourly_n.city == d_city.city_name, "left")
        .join(d_wx,   ["weather_main","weather_desc"], "left")
        .withColumn("sector_key", sector_idx)
        .select(
            "date_key","city_key","weather_key","sector_key",
            "event_date","event_year","event_month","local_hour",
            F.col("is_daylight").cast("int").alias("is_daylight"),
            "solar_norm","wind_norm","rps_hour",
            "wind_power_wm2","wind_speed","clouds_pct",
            "precip_1h","humidity","temp","feels_like"
        )
    )
    write_parquet(
        fact_weather_hourly,
        f"{GOLDEN}/fact_weather_hourly",
        parts=["city_key","event_year","event_month","event_date"],
        max_records_per_file=args.max_records_per_file
    )

    # ---------- 5) FACT DAILY ---------- #
    baseline_daylight = (
        fact_weather_hourly.filter(F.col("event_year")==2024)
        .groupBy("city_key","event_month")
        .agg((F.avg(F.col("is_daylight"))*F.lit(24)).alias("expected_daylight_hours"))
    )

    agg_day = (
        fact_weather_hourly.groupBy("city_key","date_key","event_year","event_month","event_date")
        .agg(
            F.count("*").alias("hours_observed"),
            F.sum(F.col("is_daylight")).alias("hours_observed_daylight"),
            F.mean(F.when(F.col("is_daylight")==1, F.col("solar_norm"))).alias("solar_idx_day"),
            F.mean("wind_norm").alias("wind_idx_day"),
            F.mean("rps_hour").alias("rps_day"),
            F.sum("precip_1h").alias("precip_1d_sum"),
            F.sum(F.when(F.col("is_daylight")==1,1).otherwise(0)).alias("hours_daylight"),
            F.mean("clouds_pct").alias("avg_clouds"),
            F.mean("humidity").alias("avg_humidity"),
            F.max("temp").alias("max_temp"),
            F.min("temp").alias("min_temp"),
            F.mean(F.when(F.col("solar_norm")>0.6,1).otherwise(0)).alias("pct_sunny_hours"),
            F.mean(F.when(F.col("wind_norm")>0.6,1).otherwise(0)).alias("pct_windy_hours")
        )
    )

    fact_weather_daily = (
        agg_day.join(F.broadcast(baseline_daylight),["city_key","event_month"],"left")
               .withColumn("expected_daylight_hours", F.coalesce(F.col("expected_daylight_hours"), F.lit(12.0)))
               .withColumn("expected_daylight_hours",
                           F.when(F.col("expected_daylight_hours") <  DAYLIGHT_MIN, F.lit(DAYLIGHT_MIN))
                            .when(F.col("expected_daylight_hours") > DAYLIGHT_MAX, F.lit(DAYLIGHT_MAX))
                            .otherwise(F.col("expected_daylight_hours")))
               .withColumn("day_coverage_pct",
                    clamp01(
                        F.when(F.col("expected_daylight_hours")>0,
                               F.col("hours_observed_daylight")/F.col("expected_daylight_hours"))
                         .otherwise(F.lit(0.0))
                    ))
               .withColumn("coverage_quality", quality_from_pct(F.col("day_coverage_pct")))
    )
    write_parquet(
        fact_weather_daily,
        f"{GOLDEN}/fact_weather_daily",
        parts=["city_key","event_year","event_month"],
        max_records_per_file=args.max_records_per_file
    )

    # ---------- 6) ANSWERS ---------- #
    dim_date_b  = d_date
    dim_city_b  = d_city
    dim_wsect_b = d_wsect
    dim_wx_b    = d_wx

    H = (fact_weather_hourly
         .join(dim_city_b,"city_key","left")
         .join(dim_date_b,"date_key","left"))
    D = (fact_weather_daily
         .join(dim_city_b,"city_key","left")
         .join(dim_date_b,"date_key","left"))

    ym = dim_date_b.select("year","month").dropDuplicates()
    ym = ym.withColumn("first_day", F.to_date(F.concat_ws('-', F.col('year'), F.col('month'), F.lit(1))))
    days_in_month = F.broadcast(ym.select("year","month", F.dayofmonth(F.last_day("first_day")).alias("days_in_month")))

    answers_arg = getattr(args, "only_answers", None)
    answers_wanted = set(
        a.strip().lower() for a in (answers_arg or "q1,q2,q3,q4,q5,q6,q7").split(",") if a.strip()
    )

    # ---- Q1 ----
    if "q1" in answers_wanted:
        days_with_light = (H.where("is_daylight=1")
                             .select("city_name","year","month","local_hour","date_key").dropDuplicates()
                             .groupBy("city_name","year","month","local_hour")
                             .agg(F.count("*").alias("n_days_with_light")))
        p_q1_2024 = (H.filter("year=2024")
                       .groupBy("city_name","month","local_hour")
                       .agg(F.mean("is_daylight").alias("p2024")))
        p_q1_cur  = (H.groupBy("city_name","year","month","local_hour")
                       .agg(F.mean("is_daylight").alias("pcur")))
        p_default = (H.select("local_hour").dropDuplicates()
                       .withColumn("pdef",
                           F.when(F.col("local_hour").between(10,16), 0.9)
                            .when(F.col("local_hour").between(7,9), 0.5)
                            .when(F.col("local_hour").between(17,18), 0.5)
                            .otherwise(0.1)))
        q1_base = (H.where("is_daylight=1")
                     .groupBy("city_name","year","month","local_hour")
                     .agg(F.mean("solar_norm").alias("solar_idx_mean"),
                          F.mean("clouds_pct").alias("avg_clouds"),
                          F.mean("precip_1h").alias("precip_1h_mean")))
        q1 = (q1_base
                .join(days_with_light,["city_name","year","month","local_hour"],"left")
                .join(p_q1_2024, ["city_name","month","local_hour"], "left")
                .join(p_q1_cur,  ["city_name","year","month","local_hour"], "left")
                .join(p_default, ["local_hour"], "left")
                .join(days_in_month, ["year","month"], "left")
                .withColumn("p_guess", F.coalesce(F.col("p2024"), F.col("pcur"), F.col("pdef")))
                .withColumn("target_days",
                            F.greatest(F.lit(1), F.round(F.col("days_in_month")*F.col("p_guess")).cast("int")))
                .withColumn("obs_days", F.col("n_days_with_light"))
                .withColumn("hours_coverage_pct",
                            clamp01(F.when(F.col("target_days")>0, F.col("obs_days")/F.col("target_days"))
                                      .otherwise(F.lit(0.0))))
                .withColumn("coverage_quality_rule", quality_from_pct(F.col("hours_coverage_pct")))
                .withColumn("coverage_quality",
                            F.when(F.col("obs_days") < F.lit(COVERAGE_MIN_DAYS), F.lit("low"))
                             .otherwise(F.col("coverage_quality_rule")))
                .select("city_name","year","month","local_hour",
                        "solar_idx_mean","avg_clouds","precip_1h_mean",
                        "obs_days","target_days","hours_coverage_pct","coverage_quality")
             )
        write_parquet(q1, f"{GOLDEN}/answers/q1_solar_hour_by_month",
                      parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)

    # ---- Q2 ----
    if "q2" in answers_wanted:
        days_with_hour = (H.select("city_name","year","month","local_hour","date_key").dropDuplicates()
                            .groupBy("city_name","year","month","local_hour")
                            .agg(F.count("*").alias("n_days_with_hour")))
        q2_base = (H.groupBy("city_name","year","month","local_hour")
                     .agg(F.mean("wind_norm").alias("wind_idx_mean"),
                          F.mean("wind_power_wm2").alias("wind_power_wm2_mean")))
        q2 = (q2_base
                .join(days_with_hour,["city_name","year","month","local_hour"],"left")
                .join(days_in_month, ["year","month"], "left")
                .withColumn("target_days", F.col("days_in_month"))
                .withColumn("obs_days", F.col("n_days_with_hour"))
                .withColumn("hours_coverage_pct",
                            clamp01(F.when(F.col("target_days")>0, F.col("obs_days")/F.col("target_days"))
                                      .otherwise(F.lit(0.0))))
                .withColumn("coverage_quality_rule", quality_from_pct(F.col("hours_coverage_pct")))
                .withColumn("coverage_quality",
                            F.when(F.col("obs_days") < F.lit(COVERAGE_MIN_DAYS), F.lit("low"))
                             .otherwise(F.col("coverage_quality_rule")))
                .select("city_name","year","month","local_hour",
                        "wind_idx_mean","wind_power_wm2_mean",
                        "obs_days","target_days","hours_coverage_pct","coverage_quality")
             )
        write_parquet(q2, f"{GOLDEN}/answers/q2_wind_patterns",
                      parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)

    # ---- Q3 ----
    if "q3" in answers_wanted:
        D_ok = D.filter("coverage_quality!='low'")
        H_ok = H.join(D_ok.select("city_name","date_key").dropDuplicates(),
                      ["city_name","date_key"], "inner")

        q3_stats = (H_ok
            .join(d_wx.select("weather_key","weather_main"), ["weather_key"], "left")
            .groupBy("city_name","year","month","weather_main")
            .agg(
                F.count("*").alias("n_hours"),
                F.countDistinct("date_key").alias("n_days"),
                F.avg("rps_hour").alias("rps_mean"),
                F.stddev_samp("rps_hour").alias("rps_std")
            )
            .withColumn("rps_std", F.coalesce(F.col("rps_std"), F.lit(0.0)))
            .withColumn("rps_se",  F.when(F.col("n_hours")>1, F.col("rps_std")/F.sqrt(F.col("n_hours"))))
            .withColumn("rps_ci_low",  F.col("rps_mean")  - F.lit(1.96)*F.col("rps_se"))
            .withColumn("rps_ci_high", F.col("rps_mean")  + F.lit(1.96)*F.col("rps_se"))
        )

        baseline_all = (H_ok.groupBy("city_name","year","month")
            .agg(F.avg("rps_hour").alias("rps_baseline")))

        q3_single = q3_stats.join(F.broadcast(baseline_all), ["city_name","year","month"], "left")
        q3_single = q3_single.withColumn("rps_delta", F.col("rps_mean") - F.col("rps_baseline"))
        q3_single = q3_single.withColumn(
            "rps_delta_pct",
            F.when(F.col("rps_baseline").isNull() | (F.abs(F.col("rps_baseline")) < 1e-6), None)
             .otherwise(100.0 * F.col("rps_delta") / F.col("rps_baseline"))
        )
        q3_single = q3_single.withColumn(
            "rps_drop_confident",
            F.when(F.col("rps_ci_high").isNotNull()
                   & F.col("rps_baseline").isNotNull()
                   & (F.col("rps_ci_high") < F.col("rps_baseline")), True).otherwise(False)
        )
        q3_single = q3_single.withColumn("rps_reduction_flag", F.col("rps_delta_pct") < 0)
        q3_single = q3_single.withColumn(
            "rps_reduction_confident",
            (F.col("rps_reduction_flag") & F.col("rps_drop_confident"))
        )
        q3_single = q3_single.select(
            "city_name","year","month","weather_main",
            "n_hours","n_days",
            "rps_mean","rps_baseline","rps_delta","rps_delta_pct",
            "rps_ci_low","rps_ci_high","rps_drop_confident",
            "rps_reduction_flag","rps_reduction_confident"
        )

        write_parquet(
            q3_single, f"{GOLDEN}/answers/q3_weather_main",
            parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file
        )

    # ---- Q4 ----
    if "q4" in answers_wanted and args.compare_date:
        compare = F.to_date(F.lit(args.compare_date))
        today = (D.where(F.col("date") == compare)
                   .select("city_name","date","rps_day","coverage_quality")
                   .withColumnRenamed("rps_day","rps_today"))
        ly_date = F.add_months(compare, -12)
        ly = (D.where(F.col("date") == ly_date)
                .select("city_name", F.col("rps_day").alias("rps_last_year")))
        base4 = today.join(ly, "city_name", "left")
        q4 = (base4
              .withColumn("diff_abs", F.col("rps_today") - F.col("rps_last_year"))
              .withColumn("den_ok", F.when(F.abs(F.col("rps_last_year")) >= 1e-6, F.col("rps_last_year")))
              .withColumn("diff_pct",
                          F.when(F.col("den_ok").isNull(), F.lit(None).cast("double"))
                           .otherwise(100.0 * F.col("diff_abs") / F.col("den_ok")))
              .withColumn(
                  "analysis_note",
                  F.when(F.col("coverage_quality")=="low",
                         F.lit("Cobertura diaria baja hoy; comparación poco confiable."))
                   .when(F.col("den_ok").isNull(),
                         F.lit("Sin base del año pasado o base ~0; % no calculado."))
                   .otherwise(F.lit("Cobertura diaria aceptable."))
              )
              .drop("den_ok")
        )
        write_parquet(
            q4, f"{GOLDEN}/answers/q4_today_vs_last_year/date={args.compare_date}",
            parts=["city_name"], max_records_per_file=args.max_records_per_file
        )

    # ---- Q5 ----
    if "q5" in answers_wanted:
        D_rank = D if args.top_include_low else D.filter("coverage_quality!='low'")
        wdesc = W.partitionBy("city_name","year","month").orderBy(F.col("rps_day").desc())
        wasc  = W.partitionBy("city_name","year","month").orderBy(F.col("rps_day").asc())
        topk  = (D_rank.withColumn("rk", F.row_number().over(wdesc)).where(F.col("rk")<=args.top_k).drop("rk"))
        botk  = (D_rank.withColumn("rk", F.row_number().over(wasc )).where(F.col("rk")<=args.top_k).drop("rk"))
        write_parquet(topk, f"{GOLDEN}/answers/q5_best_days_topk",
                      parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)
        write_parquet(botk, f"{GOLDEN}/answers/q5_worst_days_topk",
                      parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)

    # ---- Q6 ----
    if "q6" in answers_wanted:
        D_ok_dates = D.filter("coverage_quality!='low'").select("city_name","date_key").dropDuplicates()
        H_ok2 = H.join(D_ok_dates, ["city_name","date_key"], "inner")
        q6_full = (H_ok2.join(d_wsect, "sector_key", "left")
            .groupBy("city_name","year","month","sector_name")
            .agg(
                F.count("*").alias("n_hours"),
                F.countDistinct("date_key").alias("n_days"),
                F.avg("wind_power_wm2").alias("wind_power_wm2_avg"),
                F.avg("wind_norm").alias("wind_norm_avg")
            ))
        hours_tot = q6_full.groupBy("city_name","year","month").agg(F.sum("n_hours").alias("n_total"))
        q6_full = (q6_full.join(F.broadcast(hours_tot), ["city_name","year","month"])
                         .withColumn("sector_hours_share", F.col("n_hours")/F.col("n_total"))
                         .drop("n_total"))
        wsect = W.partitionBy("city_name","year","month").orderBy(F.col("sector_hours_share").desc())
        q6_topk = (q6_full.withColumn("rk", F.row_number().over(wsect))
                          .where(F.col("rk")<=args.top_k).drop("rk"))
        write_parquet(q6_topk, f"{GOLDEN}/answers/q6_wind_sector_topk",
                      parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)

    # ---- Q7 ----
    if "q7" in answers_wanted:
        D_rank2 = D if args.top_include_low else D.filter("coverage_quality!='low'")
        whot  = W.partitionBy("city_name","year","month").orderBy(F.col("max_temp").desc())
        wcold = W.partitionBy("city_name","year","month").orderBy(F.col("min_temp").asc())
        hottest = (D_rank2.withColumn("rk", F.row_number().over(whot))
                         .where(F.col("rk")<=args.top_k).drop("rk"))
        coldest = (D_rank2.withColumn("rk", F.row_number().over(wcold))
                         .where(F.col("rk")<=args.top_k).drop("rk"))
        write_parquet(hottest, f"{GOLDEN}/answers/q7_temp_extremes/hottest_topk",
                      parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)
        write_parquet(coldest, f"{GOLDEN}/answers/q7_temp_extremes/coldest_topk",
                      parts=["city_name","year","month"], max_records_per_file=args.max_records_per_file)

    # ---------- liberar DIMs ---------- #
    dim_date.unpersist(False)
    dim_city.unpersist(False)
    dim_weather_condition.unpersist(False)

    print("[OK] GOLDEN construido en:", GOLDEN)

# -------------------------------- CLI ---------------------------------------- #
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--compare-date", default=None, help="YYYY-MM-DD para Q4 (opcional)")
    # Filtros I/O
    ap.add_argument("--years-keep",  default=None, help="p.ej.: 2024,2025")
    ap.add_argument("--months-keep", default=None, help="p.ej.: 10 ó 9,10")
    ap.add_argument("--cities",      default=None, help='p.ej.: "Riohacha,Patagonia"')
    # Subconjunto de answers a construir
    ap.add_argument("--only-answers", default=None,
                    help="Subconjunto de respuestas a construir. Ej.: q1,q3,q5. Si se omite, construye todas.")
    # Top configs
    ap.add_argument("--top-k", type=int, default=5, help="K para Top-K (Q5/Q6/Q7)")
    ap.add_argument("--top-include-low", action="store_true",
                    help="Si se activa, los Top-K incluyen días 'low' (por defecto se excluyen)")
    # Performance
    ap.add_argument("--shuffle-partitions", type=int, default=16)
    ap.add_argument("--max-records-per-file", type=int, default=150000)
    args = ap.parse_args()
    main(args)
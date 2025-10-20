#!/usr/bin/env bash
set -euo pipefail

# ===== Configuración =====
BROKER="172.31.16.122:9092"
TOPIC="openweather-topic"
OUT="s3a://etlt-datalake-dev-us-east-1-raw-streaming/raw-streaming/openweather"
CKPT="s3a://etlt-datalake-dev-us-east-1-raw-streaming/_checkpoints/openweather/app=ow-raw"

# ===== Lógica =====
if aws s3 ls "${CKPT/s3a:/s3:}/" --recursive | grep -q . ; then
  STARTING="latest"
else
  STARTING="earliest"
fi

echo ">>> [run_consumer] Iniciando one-shot (topic=$TOPIC, startingOffsets=$STARTING)"

# ===== Ejecutar Spark job =====
/opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --conf spark.hadoop.fs.s3a.create.directory.marker=false \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.parquet.compression.codec=snappy \
  "$HOME/consumer_raw_streaming.py" \
    --bootstrap "$BROKER" \
    --topic "$TOPIC" \
    --out "$OUT" \
    --checkpoint "$CKPT" \
    --starting-offsets "$STARTING" \
    --mode oneshot

echo ">>> [run_consumer] Finalizado. Últimos objetos en S3:"
aws s3 ls ${OUT/s3a:/s3:} --recursive | sort -k1,1 -k2,2 | tail -n 20 || true

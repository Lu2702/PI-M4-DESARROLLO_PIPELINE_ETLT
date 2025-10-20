# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils import timezone as airflow_tz  

DAG_ID = "weather_etlt_run_date"

# Remoto (EC2 de Spark)
SPARK_CONT = "spark"  # nombre del contenedor Docker con Spark
SPARK_SUBMIT = "/opt/bitnami/spark/bin/spark-submit"
APP_DIR = "/opt/etlt/app"

# Directorios seguros (escritura) dentro del contenedor
TMP_DIR = "/tmp"
ARTIF_DIR = "/tmp/spark-artifacts"
WAREHOUSE_DIR = "/tmp/spark-warehouse"
LOCAL_DIR = "/tmp"

# Ciudades por defecto 
DEFAULT_CITIES = ["Riohacha", "Patagonia"]

# Flags Spark: evitar /opt/etlt/app para artefactos/temp
SPARK_CONFS = (
    "--conf spark.sql.artifact.dir=" + ARTIF_DIR + " "
    + "--conf spark.sql.warehouse.dir=" + WAREHOUSE_DIR + " "
    + "--conf spark.local.dir=" + LOCAL_DIR + " "
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,                                # un reintento
    "retry_delay": timedelta(minutes=5),         # espera 5 min
    "retry_exponential_backoff": True,           # backoff exponencial
}

with DAG(
    dag_id=DAG_ID,
    description="OpenWeather ETLT: SILVER->GOLD por fecha; GOLD con baseline 2024 (artefacts en /tmp)",
    start_date=datetime(2025, 10, 1),
    schedule="0 19 * * *",                 # todos los días a las 19:00 UTC
    catchup=False,
    default_args=default_args,
    params={"cities": DEFAULT_CITIES},
    render_template_as_native_obj=True,
    tags=["etlt", "openweather", "spark", "s3", "silver", "gold"],
    is_paused_upon_creation=False,
    max_active_runs=1,                      #  evita solapes
    
):

    # ------- PRECHECK: contenedor arriba + preparar /tmp ----------
    precheck = SSHOperator(
        task_id="precheck_remote",
        ssh_conn_id="ssh_spark",
        command=(
            "docker exec -i " + SPARK_CONT + " bash -lc '"
            "set -euo pipefail; "
            # Crear dirs y permisos seguros en /tmp
            "mkdir -p " + ARTIF_DIR + " " + WAREHOUSE_DIR + " " + LOCAL_DIR + " ; "
            "chmod 1777 " + ARTIF_DIR + " " + WAREHOUSE_DIR + " " + LOCAL_DIR + " ; "
            # Asegurar spark-submit disponible
            "cd " + TMP_DIR + " ; "
            + SPARK_SUBMIT + " --version >/dev/null 2>&1 || (echo FAIL && exit 1)"
            "'"
        ),
        do_xcom_push=False,
        get_pty=True,
        cmd_timeout=120,
    )

    # -------- SILVER: una tarea por ciudad  --------
    silver_tasks = []
    for city in DEFAULT_CITIES:
        cmd = (
            "{% set run_date = dag_run.conf.get('date', ds) %}"
            "docker exec -i " + SPARK_CONT + " bash -lc '"
            "set -euo pipefail; "
            "cd " + TMP_DIR + " ; "
            + SPARK_SUBMIT + " " + SPARK_CONFS + APP_DIR + "/weather_silver_job.py "
            "--mode upsert --city " + city + " --date \"{{ run_date }}\""
            "'"
        )
        t = SSHOperator(
            task_id="silver_upsert_" + city.lower(),
            ssh_conn_id="ssh_spark",
            command=cmd,
            do_xcom_push=False,
            get_pty=True,
            cmd_timeout=3600,
        )
        precheck >> t
        silver_tasks.append(t)

    # -------- BARRERA --------
    barrier = SSHOperator(
        task_id="after_silver_barrier",
        ssh_conn_id="ssh_spark",
        command="echo 'SILVER completado. Iniciando GOLD.'",
        do_xcom_push=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        get_pty=True,
        cmd_timeout=300,
    )

    # -------- GOLD --------
    gold_cmd = (
        "{% set run_date  = dag_run.conf.get('date', ds) %}"
        "{% set run_year  = run_date[:4] %}"
        "{% set run_month = (run_date[5:7] | int) %}"
        "{% set cities_csv = (dag_run.conf.get('cities', params.cities)) | join(',') %}"
        "docker exec -i " + SPARK_CONT + " bash -lc '"
        "set -euo pipefail; "
        "cd " + TMP_DIR + " ; "
        + SPARK_SUBMIT + " " + SPARK_CONFS + APP_DIR + "/weather_gold_job.py "
        "--years-keep \"2024,{{ run_year }}\" "
        "--months-keep {{ run_month }} "
        "--cities \"{{ cities_csv }}\" "
        "--compare-date {{ run_date }} "
        "--shuffle-partitions 16 "
        "--max-records-per-file 150000"
        "'"
    )

    gold = SSHOperator(
        task_id="gold_build",
        ssh_conn_id="ssh_spark",
        command=gold_cmd,
        do_xcom_push=False,
        get_pty=True,
        cmd_timeout=7200,
    )

    # Dependencias
    for t in silver_tasks:
        t >> barrier
    barrier >> gold



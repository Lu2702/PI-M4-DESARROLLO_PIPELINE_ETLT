# 🌦️ ETLT Diseño e implementación de un pipeline de datos ETLT escalable sobre un Data Lake en AWS 

## Avance 1: Documento técnico del diseño ETLT

- Elaboración de un documento técnico que describa el diseño general del pipeline ETLT a implementar.  
**Archivo:** [`AVANCE_1_PI_M4.pdf`](AVANCE_1_PI_M4.pdf)

**Contenido (resumen):** visión general de la arquitectura **Medallion (Bronze/Silver/Gold)**, justificación del stack (Airbyte, S3, Spark, Airflow, Kafka), fuentes de datos, preguntas de negocio y modelo dimensional (hechos y dimensiones) para la capa Gold, además de lineamientos de gobernanza y ciclo de vida.


## 🌐 🌀 🪣 Fase 1(avance 2): Ingesta con Airbyte → Bronze (S3)

## 🧭 1. Resumen Ejecutivo
Esta primera fase implementa una **ingesta diaria y automatizada** de datos meteorológicos desde la **API de OpenWeather** hacia la **capa Bronze** de un *Data Lake* en **Amazon S3**, utilizando **Airbyte** como herramienta de extracción y carga.

La ejecución está programada **todos los días a las 12:00 PM (hora de México)**, generando archivos crudos (raw) particionados por **fecha** y **ciudad**, que servirán como base para las fases posteriores de estandarización (Silver) y modelado analítico (Gold).

---

## 🎯 2. Objetivos
- Automatizar la captura diaria de datos meteorológicos.
- Mantener una estructura estandarizada en la capa **Bronze**.
- Dejar sentadas las bases para el modelado **Kimball** y la arquitectura **Medallion**.

---

## 📦 3. Alcance
Incluye:
- Despliegue y configuración de **Airbyte**.
- Creación del conector **HTTP (OpenWeather)**.
- Definición del destino **Amazon S3 (Bronze)**.
- Programación de la ingesta diaria mediante **CRON (12:00 PM MX)**.
---

## 🧱 4. Arquitectura de la Fase 1

La capa Bronze constituye el punto de entrada del Data Lake, donde se almacenan los datos en su forma más cruda (raw data), tal como provienen de las fuentes originales, sin transformaciones.
En esta fase, se utilizan dos rutas de ingesta:

Automatizada (Airbyte) → datos provenientes de la API de OpenWeather.

Manual (Backfill) → datos históricos cargados desde archivos JSON locales.

📂 Estructura general del bucket

El bucket de la capa Bronze sigue la siguiente estructura jerárquica:
```text
s3://etlt-datalake-dev-us-east-1-bronze/
├── airbyte/                           # Ingesta automática desde el conector Airbyte
│   └── openweather/                   # Fuente de datos: API de OpenWeather
│       └── city-<nombre_ciudad>/      # Ciudad (por ejemplo: city-patagonia)
│           └── year=<YYYY>/           # Año de la observación
│               └── month=<MM>/        # Mes de la observación
│                   └── day=<DD>/      # Día de la observación
│                       └── *.parquet  # Archivos crudos exportados
└── manual/                            # Ingesta manual (backfill histórico)
    └── city-<nombre_ciudad>/          # Ciudad cargada manualmente (por ejemplo: city-patagonia)
        └── ingest_date=<YYYY-MM-DD>/  # Fecha de carga manual
            └── *.json                 # Archivos originales históricos
```
## ☁️ 5. Configuración en Airbyte Cloud (Conector OpenWeather → S3 Bronze)

Se utiliza **Airbyte Cloud**, lo que simplifica el despliegue y garantiza la ejecución diaria sin necesidad de infraestructura propia.  
A continuación se describen los **pasos exactos** para replicar el conector OpenWeather y su destino en S3 dentro de Airbyte Cloud.

---

### 🔹 A. Fuente (Source) — `city-openweather`

**Tipo de conector:** HTTP API (personalizado)  
**Endpoint:** `https://api.openweathermap.org/data/2.5/weather`  
**Método:** GET  
**Retrieval type:** Synchronous Request

**Parámetros globales:**

| Campo | Descripción | Valor configurado |
|--------|--------------|-------------------|
| `Source name` | Nombre del conector | `city-openweather` |
| `city_riohacha` | Nombre y país de la ciudad | `Riohacha,CO` |
| `api_key` | Clave privada de OpenWeather (secreto) | `***utiliza tu API KEY***` |
| `patagonia_lat` | Latitud Patagonia (AR) | `-41.810147` |
| `patagonia_lon` | Longitud Patagonia (AR) | `-68.906269` |

---

### 🔹 B. Streams configurados

El conector define **dos streams independientes**, uno por ciudad.  
Cada stream realiza una llamada al endpoint con parámetros distintos (`q` o `lat/lon`) según la ciudad.

#### 1️⃣ Stream: `city-riohacha`
- **Endpoint:** `https://api.openweathermap.org/data/2.5/weather`
- **Parámetros:**
  | Key | Value |
  |-----|--------|
  | `q` | `{{ config['city_riohacha'] }}` |
  | `lang` | `es` |
  | `appid` | `{{ config['api_key'] }}` |
  | `units` | `metric` |

---

#### 2️⃣ Stream: `city-patagonia`
- **Endpoint:** `https://api.openweathermap.org/data/2.5/weather`
- **Parámetros:**
  | Key | Value |
  |-----|--------|
  | `lat` | `{{ config['patagonia_lat'] }}` |
  | `lon` | `{{ config['patagonia_lon'] }}` |
  | `lang` | `es` |
  | `appid` | `{{ config['api_key'] }}` |
  | `units` | `metric` |

---

### 🔹 C. Destino (Destination) — `bronze-etlt-s3`

**Tipo:** Amazon S3  
**Versión:** v1.9.3  
**Bucket:** `etlt-datalake-dev-us-east-1-bronze `  
**Región:** `us-east-1`  
**Ruta base:** `airbyte/openweather/`  
**Formato de salida:** `Parquet (Columnar Storage)`  
**Compresión:** `SNAPPY`  
**Tamaño de bloque:** `128 MB`

**Opciones de particionado y nombrado:**

| Campo | Valor configurado |
|--------|-------------------|
| `S3 Path Format` | `${STREAM_NAME}/year=${YEAR}/month=${MONTH}/day=${DAY}/` |
| `File Name Pattern` | `{timestamp}_part_{part_number}.parquet` |

---

### 🔹 D. Conexión (Source → Destination)

| Parámetro | Valor |
|------------|--------|
| **Connection name** | `openweather_to_s3_daily` |
| **Namespace** | `airbyte/openweather/` |
| **Schedule type** | CRON |
| **CRON expression** | `0 0 18 * * ?` |
| **Time zone** | `UTC` |
| **Sync mode** | Full Refresh / Append |

---

Cada corrida diaria sobrescribe o agrega nuevos archivos según el modo de escritura, manteniendo un esquema claro por **ciudad/año/mes/día**.

---

### 💬 Notas adicionales
- El formato **Parquet con compresión Snappy** fue seleccionado para optimizar almacenamiento y lectura desde Spark en la capa Silver.  
- Se mantuvo el idioma **español** (`lang=es`) para los descriptores de clima.  
- Las variables configuradas en `{{ config[...] }}` permiten parametrizar la API Key y coordenadas sin exponerlas directamente en el código.  
- El programador CRON de Airbyte Cloud ejecuta automáticamente el flujo diario sin intervención manual.

---

### ✅ Resultado Final
La configuración completa permite que **Airbyte Cloud** recolecte cada día, a las **12:00 PM hora de México**, los datos meteorológicos de **Patagonia** y **Riohacha**, almacenándolos en la capa **Bronze** de S3 en formato **Parquet**, con compresión eficiente y estructura particionada.

## 🧱🪣📦 Fase 2(avance 3): Procesamiento de datos con spark

🧭 1. Resumen Ejecutivo

En esta segunda fase del proyecto ETLT, se describe el proceso de transformación y modelado de datos dentro del Data Lake, siguiendo los principios de la arquitectura Medallion (Bronze → Silver → Gold).

La solución se implementa completamente en la nube, utilizando los servicios de Amazon Web Services (AWS). En particular, los datos son almacenados en Amazon S3, mientras que el procesamiento se realiza mediante Apache Spark desplegado en un contenedor Docker sobre una instancia EC2, con autenticación segura a través de un IAM Role.

Esta fase incluye los jobs de transformación Silver y Gold, totalmente operativos, junto con una guía reproducible paso a paso para la implementación del entorno desde cero. De esta forma, se garantiza un flujo de datos automatizado, escalable y alineado con las mejores prácticas de ingeniería de datos moderna.

## 🎯 2. Objetivos

- Estandarizar, limpiar y enriquecer los datos provenientes de la API de OpenWeather y cargas manuales históricas, aplicando las reglas de negocio definidas en la capa Silver.
- Diseñar y publicar modelos analíticos en la capa Gold, siguiendo la metodología Kimball, que incluyen tablas de dimensiones, hechos y respuestas analíticas (Q1–Q7) orientadas a indicadores meteorológicos clave.

## 📦 3. Alcance

- Procesamiento y transformación en Spark con los jobs:
    - [`weather_silver_job.py`](spark-etlt\app\weather_silver_job.py) → procesamiento, estandarización y enriquecimiento.
    - [`weather_gold_job.py`](spark-etlt\app\weather_gold_job.py) → modelado dimensional (hechos, dimensiones y métricas).
- Publicación de salidas analíticas:
    - Tablas de dimensiones y hechos.
    - Resultados agregados que responden a las preguntas analíticas Q1–Q7.
- Infraestructura de ejecución completamente en AWS:
    - Amazon S3 (almacenamiento por capas).
    - EC2 + Docker (procesamiento con Spark).
    - IAM Role (autenticación segura sin llaves).

## 🧱 4. Arquitectura y flujo fase 2
```bash
    BRONZE S3 = (Airbyte(.parquet) / Manual (.json)) 
                         |
                         v
        SILVER S3 = parquet snappy, particionado
                         |
                         v
      GOLD S3 = Dims + Facts + Answers (Q1..Q7)
```
- Acceso a S3 con el conector S3A de Hadoop (esquema s3a://).
- El contenedor Spark obtiene credenciales temporales del Instance Metadata Service gracias al IAM Role.

## 🧱 5. Requisitos:

- Cuenta AWS con permisos para:
- EC2: Ubuntu 22.04 (tamaño t3.large o superior recomendado).
- Docker + Contenedor Spark (bitnami/spark base).
- IAM Role asociado a la EC2 con permisos sobre los buckets:
    - s3:ListBucket para los buckets bronze/silver/gold.
    - s3:GetObject, s3:PutObject, s3:DeleteObject en prefijos correspondientes.

## 6. 💻 Preparar la instancia (Ubuntu + Docker)

- Instalar Docker

```bash
# 🔐 Accede por SSH
ssh -i <tu-key.pem> ubuntu@<EC2_PUBLIC_IP>

# 🐳 Instala Docker CE
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

# ⚙️ (Opcional) Usa Docker sin sudo (requiere reiniciar sesión)
sudo usermod -aG docker $USER

# 📦 Instala el plugin Docker Compose V2 (docker compose)
sudo apt-get install -y docker-compose-plugin

# 🔍 Verifica las versiones instaladas
docker --version
docker compose version

```
## 7. 🗂️ Estructura del proyecto

```text
├── app/                                   # Scripts principales de procesamiento en Spark
│   ├── weather_silver_job.py              # Job para transformación y estandarización (capa Silver)
│   └── weather_gold_job.py                # Job para modelado dimensional y métricas (capa Gold)
│
├── conf/                                  # Archivos de configuración
│   └── spark-defaults.conf                # Parámetros por defecto para sesiones de Spark
│
├── logs/                                  # Carpeta donde Spark almacena los registros de ejecución
│
├── Dockerfile                             # Define la imagen base de Spark y dependencias
├── docker-compose.yml                     # Orquesta contenedores (Spark, dependencias, etc.)

```
### Notas: 
- Todos los jobs PySpark se ejecutan dentro del contenedor.
- Los parámetros de conexión y comportamiento de Spark se definen en conf/spark-defaults.conf
- Los logs generados por cada job se guardan en la carpeta logs/, útil para depuración.
- docker-compose.yml permite levantar el entorno completo de Spark.
- ⚙️ conf/[`spark-defaults.conf`](spark-etlt\conf\spark-defaults.conf)
Este archivo define la configuración por defecto de Spark para el proyecto. En el repo encontrarás la versión completa en `conf/spark-defaults.conf`.

Puntos clave:

- UI y event log: habilita la consola progresiva y el event log local para depuración.
- Acceso a S3 con IAM Role: Spark usa InstanceProfileCredentialsProvider (las credenciales las provee el IAM Role de la EC2; no hay llaves en texto plano).
- Rendimiento S3: fast.upload, bytebuffer y mayor connection.maximum para throughput.
- Parquet / SQL: compresión snappy, partitionOverwriteMode=dynamic, particiones de shuffle razonables.
- Timeouts numéricos: valores enteros en milisegundos/segundos (evitamos sufijos tipo 60s/24h para asegurar compatibilidad entre versiones).
- S3Guard desactivado: usamos el S3 nativo; TTLs numéricos coherentes.

Ejemplo: 

```bash
# S3A con IAM Role (sin llaves)
spark.hadoop.fs.s3a.aws.credentials.provider  com.amazonaws.auth.InstanceProfileCredentialsProvider
spark.hadoop.fs.s3a.fast.upload               true
spark.hadoop.fs.s3a.fast.upload.buffer        bytebuffer
spark.hadoop.fs.s3a.connection.maximum        200

# Parquet / SQL
spark.sql.sources.partitionOverwriteMode      dynamic
spark.sql.parquet.compression.codec           snappy
spark.sql.shuffle.partitions                  64

# Timeouts NUMÉRICOS (ms / s)
spark.hadoop.fs.s3a.connection.timeout            60000
spark.hadoop.fs.s3a.connection.establish.timeout  60000
spark.hadoop.fs.s3a.socket.timeout                60000
spark.hadoop.fs.s3a.threads.keepalivetime         60

```
- 🐳 `docker-compose.yml` y `dockerfile`

El [`docker-compose.yml`](spark-etlt\docker-compose.yml) levanta un único servicio Spark (modo master local) basado en la imagen de Bitnami [`dockerfile`](spark-etlt\dockerfile).
El archivo completo está en la raíz del repo; aquí explicamos su comportamiento:

- build: .: usa tu Dockerfile para construir la imagen con dependencias.
- user: "0:0": evita problemas de permisos al escribir logs/volúmenes.
- Variables de entorno: modo master, sin RPC encryption (entorno privado), HADOOP_USER_NAME=spark.
- Puertos: expone 8080 (Spark Web UI) y 4040 (UI de jobs en ejecución).
- Healthcheck: comprueba la UI en http://localhost:8080 para saber si está “healthy”.

Ejemplo:
```bash
services:
  spark:
    build: .
    container_name: spark
    user: "0:0"
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - HADOOP_USER_NAME=spark
    command: ["/opt/bitnami/scripts/spark/run.sh"]
    ports:
      - "8080:8080"   # Spark Web UI
      - "4040:4040"   # Spark app UI
    volumes:
      - ./conf/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf:ro
      - ./app:/opt/etlt/app
      - ./logs:/opt/bitnami/spark/logs
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080"]
      interval: 30s
      timeout: 10s
      retries: 5
```
### ▶️ Cómo usarlo

```bash
# 1) Construir imagen
docker compose build

# 2) Levantar el servicio
docker compose up -d

# 3) Ver la UI de Spark
http://<EC2_PUBLIC_IP>:8080
```

### 🪣Estructura de los buckets

```bash
Particionado por city/event_year/event_month/event_day:

Silver:
s3://etlt-datalake-dev-us-east-1-silver/
└── openweather/
    └── city=<CityName>/                 # Patagonia, Riohacha, etc.
        └── event_year=YYYY/
            └── event_month=MM/
                └── event_day=DD/
                    └── part-*.snappy.parquet
Ejemplo:
.../silver/openweather/city=Patagonia/event_year=2025/event_month=10/event_day=11/part-000*.snappy.parquet

Gold:
gold/openweather/
├── dim_date/
├── dim_city/
├── dim_weather_condition/
├── fact_weather_hourly/ 
├── fact_weather_daily/  
└── answers/
    ├── q1_solar_hour_by_month/
    ├── q2_wind_patterns/
    ├── q3_weather_main/
    ├── q4_today_vs_last_year/date=YYYY-MM-DD/
    ├── q5_best_days_topk/
    ├── q5_worst_days_topk/
    ├── q6_wind_sector_topk/
    └── q7_temp_extremes/{hottest_topk,coldest_topk}/
```
- Notas
* Formato: Parquet (Snappy).
* Modelo: Kimball (dimensiones y hechos).
- Particionado:
* fact_weather_hourly: city_key → event_year → event_month → event_date
* fact_weather_daily: city_key → event_year → event_month
* answers: particiones específicas según la pregunta (año/mes/fecha/ciudad).
* _SUCCESS puede aparecer en algunas salidas como marcador de job completado.

## ▶️ 8. Ejecución manual (pre-Airflow)

- Útil para validar los jobs antes de orquestarlos. Requiere que el contenedor spark esté arriba con docker compose up -d y que la instancia EC2 tenga IAM Role con permisos S3.

- Comandos (ejemplos octubre 2025):

```bash

#SILVER
#Riohacha – upsert 11/10/2025

sudo docker exec -it spark bash -lc '
/opt/bitnami/spark/bin/spark-submit \
  /opt/etlt/app/weather_silver_job.py \
  --mode upsert \
  --city Riohacha \
  --date 2025-10-11 \
  --shuffle-partitions 8
'

#Patagonia – upsert 11/10/2025

sudo docker exec -it spark bash -lc '
/opt/bitnami/spark/bin/spark-submit \
  /opt/etlt/app/weather_silver_job.py \
  --mode upsert \
  --city Patagonia \
  --date 2025-10-11 \
  --shuffle-partitions 8
'

#GOLD

#Patagonia y Rioacha
sudo docker exec -it spark bash -lc '
/opt/bitnami/spark/bin/spark-submit \
  /opt/etlt/app/weather_gold_job.py \
  --years-keep "2024,2025" \
  --months-keep 10 \
  --cities "Riohacha,Patagonia" \
  --compare-date 2025-10-11 \
  --shuffle-partitions 8 \
  --max-records-per-file 50000
'
```
## 🧭 🌀⏱️ Fase 3(avance 4): Orquestación

## 🧭 1. Resumen Ejecutivo

En esta fase se implementa la orquestación completa del flujo ETLT mediante Apache Airflow, desplegado dentro de un contenedor Docker sobre una instancia EC2 en Amazon Web Services (AWS).

El propósito de esta etapa es automatizar la ejecución diaria de los procesos de transformación y modelado desarrollados previamente en Spark, garantizando un flujo continuo de actualización de datos en el Data Lake.

El entorno de Airflow se encuentra dockerizado, facilitando la portabilidad y escalabilidad del pipeline, y se comunica con el contenedor de Spark a través de SSH usando el IAM Role de la instancia para autenticarse de forma segura frente a Amazon S3, sin exponer credenciales.

El DAG principal [`weather_etlt_run_date`](airflow\dags\weather_etlt_run_date.py) coordina la ejecución diaria de los jobs de Spark que transforman los datos en la capa Silver y luego generan los modelos Gold (dimensiones, hechos y respuestas Q1–Q7).

## 🎯 2. Objetivos
- Automatizar la transformación de datos desde la capa Silver hasta la Gold utilizando Airflow como motor de orquestación.
- Programar ejecución diaria a 19:00 UTC (13:00 CDMX).
- Definir dependencias lógicas entre tareas, garantizando que Gold solo se ejecute cuando todas las tareas Silver finalicen correctamente.
- Ejecutar los scripts de Spark (weather_silver_job.py y weather_gold_job.py) remotamente en la EC2, aprovechando IAM Roles para autenticación segura frente a S3.
- Centralizar el monitoreo y logging del flujo desde la interfaz web de Airflow (Gantt, Logs, Graph View).


## 📦 3. Alcance
Esta fase abarca la orquestación de las capas Silver y Gold dentro del Data Lake, sin incluir la ingesta (Airbyte permanece fuera de alcance).
- DAG: weather_etlt_run_date, activo y programado.
- Frecuencia: diaria a a las 19:00 UTC (equivale a 13:00 en CDMX).
- Operador principal: SSHOperator para ejecutar comandos docker exec spark spark-submit remotos.

Dependencias del DAG:
- Tareas Silver (upsert por ciudad): una por cada ciudad definida (Riohacha, Patagonia).
- Barrera de sincronización: espera a que todas las tareas Silver finalicen correctamente.
- Tarea Gold: ejecuta weather_gold_job.py con baseline de 2024 + el año/mes de la fecha de ejecución, y --compare-date = misma fecha.

Entradas/salidas:
- Lee de silver/openweather/...
- Escribe en gold/openweather/... (dimensiones, hechos y answers Q1–Q7).
Seguridad: uso de IAM Role para autenticación a S3 (sin llaves en texto plano).
Conexión necesaria: ssh_spark configurada en Airflow (puerto 22 hacia la EC2 de Spark).

---

## 🧱 4. Infraestructura utilizada

| Componente | Descripción |
|-------------|--------------|
| **EC2 Airflow Project** | Orquestador de tareas ETLT (scheduler, webserver, PostgreSQL) |
| **EC2 Spark Project** | Procesamiento distribuido con PySpark (Silver y Gold jobs) |
| **Airbyte Cloud** | Ingesta automática de datos desde API OpenWeather |
| **AWS S3** | Almacenamiento de las capas medallion (Bronze, Silver, Gold) |
| **AWS Lake Formation** | Gobernanza y control de permisos |
| **Docker & Docker Compose** | Contenerización de servicios |
| **Python 3.11 + PySpark 4.0.0** | Desarrollo y ejecución de los jobs ETLT |

---

## 🚀 5. Preparación de las instancias EC2

### Crear instancia EC2
- **Instancia:** `airflow-project`  
  - Tipo: `t2.large` o `c7i-flex.large`  
  - Sistema: `Ubuntu 22.04 LTS`
  - Puertos abiertos: `22 (SSH)`, `8080 (Airflow UI)`

### Conectarse por SSH
```bash
ssh -i spark-etlt-ec2.pem ubuntu@<ip_publica>
```
🐋 5.1 Instalación de Docker y Docker Compose 

- Docker-compose para Airflow
[`docker-compose.yml`](airflow\docker-compose.yml)
- Dockerfile para Airflow 
[`dockerfile`](airflow\dockerfile)
- Requirements para Airflow
[`requirements.txt`](airflow\requirements.txt)
```bash
sudo apt update -y
sudo apt install -y ca-certificates curl gnupg lsb-release
sudo mkdir -m 0755 -p /etc/apt/keyrings

# Instalar Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker

# Verificar
docker --version
docker compose version
```
## ⚙️ 6.Configuración del contenedor de AIrflow

- 6.1 crear carpeta
```bash
mkdir -p ~/airflow/{dags,logs,plugins,keys}
cd ~/airflow
```
- 6.2 Clona el repo y copia docker-compose.yml (el del repo) en ~/airflow/. 

- 6.3 Copia la clave .pem usada para Spark:

```bash
cp ~/spark-etlt-ec2.pem ~/airflow/keys/spark-etlt-ec2.pem
chmod 600 ~/airflow/keys/spark-etlt-ec2.pem
```
- 6.4 Crea el archivo .env.
```bash
# Imágenes
AIRFLOW_BASE_IMAGE=apache/airflow:2.9.3-python3.11
AIRFLOW_IMAGE=airflow-custom:2.9.3

# Airflow
AIRFLOW_UID=1000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.session,airflow.api.auth.backend.basic_auth
AWS_DEFAULT_REGION=us-east-1
AIRFLOW__CORE__FERNET_KEY=YOUR_FERNET_KEY

# Postgres
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow

# Usuario admin inicial
AIRFLOW_ADMIN_USERNAME=admin123
AIRFLOW_ADMIN_PASSWORD=admin123
AIRFLOW_ADMIN_EMAIL=admin123@example.com
AIRFLOW_ADMIN_FIRSTNAME=abc
AIRFLOW_ADMIN_LASTNAME=abc

# >>> Spark (IP PRIVADA de tu instancia Spark)
SPARK_PRIVATE_IP=ip_privada

# Conexión SSH para SSHOperator (ruta DENTRO del contenedor)
AIRFLOW_CONN_SSH_SPARK=ssh://ubuntu@${SPARK_PRIVATE_IP}?key_file=/opt/airflow/keys/spark-etlt-ec2.pem

Importante: usa la IP privada real de la EC2 Spark.
Airflow crea automáticamente la conexión ssh_spark desde AIRFLOW_CONN_SSH_SPARK
```
- 6.5 Inicializa Airflow y levanta servicios:
```bash
docker compose up airflow-init
docker compose up -d

Abre la UI: http://<EC2_A_public_ip>:8080
Usuario/clave inicial: admin123 / admin123 (del .env).

Copia el DAG del repo en ~/airflow/dags/:

weather_etlt_run_date.py
```

- 6.6 Ejecutar manualmente por fecha
```bash
# 12/oct/2025
docker compose exec scheduler bash -lc \
"airflow dags trigger weather_etlt_run_date --conf '{\"date\":\"2025-10-12\"}'"

# 15/oct/2025
docker compose exec scheduler bash -lc \
"airflow dags trigger weather_etlt_run_date --conf '{\"date\":\"2025-10-15\"}'"

# 16/oct/2025
docker compose exec scheduler bash -lc \
"airflow dags trigger weather_etlt_run_date --conf '{\"date\":\"2025-10-16\"}'"
```

- 6.7 Pasar a automático (diario)

El DAG ya trae:
schedule="0 19 * * *" → 19:00 UTC (≈ 13:00 CDMX todo el año).
Asegúrate de que esté unpaused en la UI (toggle azul).

## 📡🔥🪣 Fase 4(avance 5): Streaming 

En esta fase se incorpora ingesta en tiempo (casi) real del clima desde OpenWeather hacia Apache Kafka y su consumo con Spark Structured Streaming en modo one-shot (micro-lote controlado).
El productor publica eventos JSON en el tópico openweather-topic. El consumidor Spark lee del tópico, parsea y valida los mensajes, y escribe Parquet particionado por date y city en Amazon S3, manteniendo estado/checkpoints también en S3 para garantizar exactly-once semantics a nivel de partición de salida.

La solución corre completamente en AWS sobre EC2 con Docker. El acceso a S3 se realiza vía IAM Role (sin credenciales estáticas). Esta etapa cierra el ciclo Bronze (streaming) → Silver/Gold (batch/orquestado), habilitando pipelines híbridos (batch + streaming).

##  🎯 2. Objetivos

- Ingerir datos de clima desde OpenWeather en Kafka de forma continua (productor HTTP → Kafka).
- Consumir con Spark Structured Streaming (one-shot) y persistir en S3 en formato Parquet con particionado por date y city.
- Asegurar confiabilidad con checkpointing en S3 y escritura idempotente (modo append con control de particiones).
- Estandarizar el esquema de mensajes y validar campos mínimos (timestamp, city, coords, main, wind, etc.).

## 📦 3. Alcance

- Kafka (single-node en Docker) y tópico openweather-topic.
- Productor que consulta la API de OpenWeather y publica JSON en Kafka.
- Consumidor Spark (consumer_raw_streaming.py) que:
- Lee de Kafka (subscribe=openweather-topic).
- Normaliza el JSON y deriva particiones date y city.
- Escribe Parquet/Snappy a s3a://…-raw-streaming/raw-streaming/openweather/…
- Mantiene checkpoints en s3a://…-raw-streaming/_checkpoints/openweather/
- Configuración S3A con IAM Role (sin llaves).
- Modo de ejecución one-shot (útil para “ventanas” cortas, backfills, o disparos programados).

### 4. Crear instancia EC2
- **Instancia:** `kafka-project`  
  - Tipo: `t2.large` o `c7i-flex.large`  
  - Sistema: `Ubuntu 22.04 LTS`
  - Puertos abiertos: `22 (SSH)`

### Conectarse por SSH
```bash
ssh -i spark-etlt-ec2.pem ubuntu@<ip_publica>
```
🐋 4.1 Instalación de Docker y Docker Compose

- Docker-compose para Kafka
[`docker-compose.yml`](Kafka\docker-compose.yml)

```bash
sudo apt update -y
sudo apt install -y ca-certificates curl gnupg lsb-release
sudo mkdir -m 0755 -p /etc/apt/keyrings

# Instalar Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker

# Verificar
docker --version
docker compose version
```
- 4.2 Instalar Spark 3.5.1 (pre-built Hadoop 3)
```bash
SPARK_VER=3.5.1
HADOOP_PROFILE=hadoop3
cd /tmp
wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VER}/spark-${SPARK_VER}-bin-${HADOOP_PROFILE}.tgz
sudo mkdir -p /opt/spark
sudo tar -xzf spark-${SPARK_VER}-bin-${HADOOP_PROFILE}.tgz -C /opt/spark --strip-components=1
/opt/spark/bin/spark-submit --version

No instalamos Hadoop “completo”. Para S3 usamos hadoop-aws + aws-java-sdk-bundle en runtime vía --packages.
```
- 4.3 Repositorio del proyecto
```bash
mkdir -p ~/kafka
cd ~/kafka
```
- 4.4 Clona el repo y copia docker-compose.yml (el del repo) en ~/Kafka/. 

- 4.4 Levantar Kafka
```bash
docker compose up -d
docker ps   
```
### 5. Producer (OpenWeather → Kafka)

- 5.1 Archivos

* [`producer.py`](Kafka\producer.py): app Python que consulta OpenWeather y publica JSON en Kafka.

* [`consumer_raw_streaming.py`](Kafka\consumer_raw_streaming.py): app Python de Spark Structured Streaming (modo one-shot) que lee de Kafka y escribe Parquet a S3 (particionado por date, city) usando un checkpoint en S3.

* [`run_producer.sh`](Kafka\run_producer.sh): “wrapper” para ejecutar el producer con variables y parámetros de prueba (n° de envíos, intervalo).

* [`run_consumer.sh`](Kafka\run_consumer.sh): “wrapper” para ejecutar el consumer en one-shot, resolviendo dependencias de Spark (conectores) y listando el output en S3.

Por diseño, los .py contienen la lógica y los .sh estandarizan la ejecución (parámetros, librerías, paths S3). Esto evita hardcodear rutas/credenciales dentro del código y hace reproducibles las corridas.

### 🌊 6.1 Capa Bronze (Streaming) — Estructura general
```bash
s3://etlt-datalake-dev-us-east-1-raw-streaming/
├── raw-streaming/
│   └── openweather/                                  # Datos escritos por Spark
│       ├── _spark_metadata/                          # Metadatos del File Sink de Spark
│       │   ├── 0
│       │   ├── 1
│       │   └── 2
│       └── date=YYYY-MM-DD/                          # Partición 1 (fecha UTC/local según tu job)
│           └── city=<CityName>/                      # Partición 2 (ciudad)
│               └── part-*.snappy.parquet             # Archivos de datos
│
└── _checkpoints/
    └── openweather/                                  # STATE (estado del streaming)
        ├── app=app=ow-raw/                           # Un “app id” por ejecución/proceso
        │   ├── commits/                              # Log de commits de cada micro-lote
        │   ├── offsets/                              # Offsets de Kafka procesados
        │   ├── sources/                              # Info de fuentes y schema
        │   └── metadata                              # Estado global 
   
```

### ▶️ 7. Ejecución
- Usa OWM_API_KEY (OpenWeather).
- Por defecto envía 5 mensajes cada 120 s (configurable en el .sh).
- Publica en el tópico openweather-topic.

- 7.1 Flujo de uso
```bash
# Levantar Kafka
docker compose up -d
# Crear venv (una sola vez)
python3 -m venv ~/venv-kafka

# Activarlo (cada sesión)
source ~/venv-kafka/bin/activate

# Instalar dependencias Python del producer
pip install --upgrade pip
pip install kafka-python requests

# Producer (5 mensajes cada 2 min)
./run_producer.sh

# Consumer (one-shot, procesa lo nuevo y termina)
./run_consumer.sh

# MAX_RUNS=0 → producer infinito.
#--mode streaming → consumer continuo.
```
## 🟡📊 Visualización Gold (Colab)

- Notebook: [`visualizacion_gold.ipynb`](visualizacion_gold.ipynb)
- Capa: Gold del pipeline ETLT (medallion)
- Objetivo: cargar tablas/answers de la capa Gold y generar visualizaciones (Q1–Q7).

### 📦 Requisitos

- Cuenta de Google para usar Colab.
- Acceso a tu bucket S3 con la capa Gold (por ejemplo: etlt-datalake-dev-us-east-1-gold).
- Credenciales AWS con permiso de solo lectura en las rutas Gold (idealmente un usuario o rol con política mínima para s3:GetObject y s3:ListBucket). Pegar temporalmente las credenciales como secrets en Colab.
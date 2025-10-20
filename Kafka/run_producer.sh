#!/usr/bin/env bash
set -euo pipefail

# ===== Configuración =====
BROKER="172.31.16.122:9092"
TOPIC="openweather-topic"
CITY="Irapuato,mx"

# Pega tu API key aquí ↓↓↓
export OWM_API_KEY=7f005c1390c03699089ebe52a4e1f453

# ===== Modo prueba =====
export RUN_ONCE=false
export MAX_RUNS=5
export POLL_SECS=120

echo ">>> [run_producer] broker=$BROKER topic=$TOPIC city=$CITY runs=$MAX_RUNS cada ${POLL_SECS}s"

export BROKER TOPIC CITY
python3 "$HOME/producer.py"

echo ">>> [run_producer] Terminado."
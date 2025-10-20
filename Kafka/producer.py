#!/usr/bin/env python3
"""
Productor de clima hacia Kafka (OpenWeather → Kafka)

Descripción
-----------
Este script consulta periódicamente la API de OpenWeather para una ciudad (Irapuato, México)
y envía un mensaje JSON con datos meteorológicos a un tópico de Kafka.

Variables de entorno
--------------------
- BROKER:    Host:puerto del broker Kafka (por defecto "172.31.16.122:9092" modifica con tu IP privada).
- TOPIC:     Tópico de destino en Kafka (por defecto "openweather-topic").
- CITY:      Ciudad "Nombre,cc" (por defecto "Irapuato,mx").
- OWM_API_KEY: API Key de OpenWeather (obligatoria).
- POLL_SECS: Segundos entre consultas a la API (por defecto "120").
- MAX_RUNS:  Número máximo de envíos antes de terminar (por defecto "5").
             Si es "0", se ignora el límite y el bucle puede ser infinito
- RUN_ONCE:  Si es "true/1/yes", realiza un solo envío y termina.

Dependencias
------------
- kafka-python
- requests

Ejemplo (bash)
--------------
export OWM_API_KEY="TU_API_KEY"
export BROKER="172.31.16.122:9092"
export TOPIC="openweather-topic"
export CITY="Irapuato,mx"
export POLL_SECS="60"
export MAX_RUNS="0"           # 0 = correr indefinidamente
export RUN_ONCE="false"
python3 producer.py
"""

import os, json, time, requests, sys
from datetime import datetime, timezone
from kafka import KafkaProducer

# ===== Configuración vía variables de entorno =====
BROKER = os.getenv("BROKER", "172.31.16.122:9092")
TOPIC = os.getenv("TOPIC", "openweather-topic")
CITY = os.getenv("CITY", "Irapuato,mx")
API_KEY = os.getenv("OWM_API_KEY")
POLL_SECS = int(os.getenv("POLL_SECS", "120"))
MAX_RUNS = int(os.getenv("MAX_RUNS", "5"))
# Interpreta "1/true/yes" (insensible a mayúsculas) como True
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() in {"1","true","yes"}


def build_producer():
    """
    Construye y retorna un KafkaProducer configurado para:
    - Enviar valores en JSON (UTF-8).
    - Esperar confirmación de todos los réplicas (acks="all") para mayor durabilidad.
    """
    return KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all"
    )


def fetch_weather(city: str):
    """
    Consulta la API 'Current Weather Data' de OpenWeather para la ciudad indicada
    y devuelve un diccionario con los campos relevantes.

    Parámetros
    ----------
    city : str
        Ciudad en formato "Nombre,cc" (ej. "Irapuato,mx").

    Retorna
    -------
    dict
        Estructura JSON lista para serializar y enviar a Kafka.

    Errores
    -------
    - Si falta OWM_API_KEY → sale con código 1.
    - Si la petición HTTP falla → requests.raise_for_status() lanza excepción.
    """
    if not API_KEY:
        print("ERROR: Falta OWM_API_KEY (tu API key de OpenWeather).", file=sys.stderr)
        sys.exit(1)

    # Construcción de URL con unidades métricas y respuesta en español
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric&lang=es"

    # Timeout defensivo para evitar que la petición cuelgue indefinidamente
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    js = r.json()

    # Estructura compacta y consistente para downstream (consumidores)
    return {
        "source": "openweather",
        "city": js.get("name"),
        "lat": js["coord"]["lat"],
        "lon": js["coord"]["lon"],
        "ts": js.get("dt"),
        "datetime_utc": datetime.fromtimestamp(js.get("dt"), tz=timezone.utc).isoformat(),
        "temp_c": js["main"]["temp"],
        "humidity": js["main"]["humidity"],
        "pressure": js["main"]["pressure"],
        "wind_speed": js["wind"]["speed"]
    }


def main():
    """
    Bucle principal:
    - Crea el productor Kafka.
    - En cada iteración consulta OpenWeather y envía un mensaje al tópico.
    - Controla la terminación por RUN_ONCE o por MAX_RUNS (si > 0).
    - Espera POLL_SECS segundos entre envíos cuando aplica.
    """
    producer = build_producer()
    print(f"[producer] broker={BROKER} topic={TOPIC} city={CITY}")

    iteration = 0
    while True:
        try:
            # 1) Obtención de datos
            data = fetch_weather(CITY)

            # 2) Envío al tópico con espera de confirmación (future.get)
            producer.send(TOPIC, data).get(timeout=30)

            # 3) Log para seguimiento en consola
            print(f"[producer] Enviado -> {data['city']}  temp_c={data['temp_c']}  {data['datetime_utc']}")
            iteration += 1

        except Exception as e:
            
            print(f"[producer] ERROR: {e}", file=sys.stderr)

        # Condiciones de salida:
        # - RUN_ONCE: termina tras el primer intento (exitoso o no).
        # - MAX_RUNS: si es > 0 y se alcanzó el conteo, se termina.
        if RUN_ONCE or (MAX_RUNS and iteration >= MAX_RUNS):
            break

        # Pausa entre consultas para no saturar la API
        time.sleep(POLL_SECS)

    print(f"[producer] Terminado tras {iteration} envíos.")


if __name__ == "__main__":
    main()

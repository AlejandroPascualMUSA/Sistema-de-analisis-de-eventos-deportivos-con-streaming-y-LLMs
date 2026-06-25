"""Configuracion central del proyecto.

Este modulo concentra rutas, nombres de topic Kafka y variables de entorno
para que el resto de scripts no tenga valores hardcodeados.
"""

from __future__ import annotations

import os
from pathlib import Path


# Ruta base del proyecto. En Docker suele ser /opt/app; fuera de Docker se infiere automaticamente.
# Rutas base del proyecto. En Docker APP_BASE_PATH apunta normalmente a /opt/app.
PROJECT_ROOT = Path(os.getenv("APP_BASE_PATH", Path(__file__).resolve().parents[1]))
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = Path(os.getenv("OUTPUT_BASE_PATH", PROJECT_ROOT / "output"))
REPORTS_DIR = PROJECT_ROOT / "reports"

# Configuracion Kafka: topic y brokers para ejecucion local/Docker.
# Configuracion de Kafka. El productor local puede usar localhost, Spark en Docker usa kafka:9092.
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "statsbomb_events")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SPARK_KAFKA_BOOTSTRAP_SERVERS = os.getenv("SPARK_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Convencion StatsBomb usada en este proyecto:
# data/events/statsbomb/<match_id>.json
# Cada archivo contiene una lista JSON de eventos. El match_id se toma del nombre del archivo.
# Convencion de entrada: cada fichero JSON representa un partido StatsBomb.
# Ubicacion esperada de los eventos StatsBomb: un JSON por partido.
EVENTS_DIR = DATA_DIR / "events" / "statsbomb"
EVENTS_FILE = EVENTS_DIR

# Tablas estaticas y documentos usados para enriquecer metricas y alimentar RAG.
PLAYERS_FILE = DATA_DIR / "static" / "players.csv"
TEAMS_FILE = DATA_DIR / "static" / "teams.csv"
METRICS_CATALOG_FILE = DATA_DIR / "static" / "metrics_catalog.csv"
DOCS_DIR = DATA_DIR / "docs"
INDEX_FILE = DATA_DIR / "index" / "rag_index.pkl"

"""Productor Kafka de eventos StatsBomb.

Lee eventos desde data/events/statsbomb, los serializa como JSON y los publica
en el topic configurado. Es el punto de entrada del flujo streaming.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

from kafka import KafkaProducer

try:
    from src.config import EVENTS_FILE, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
    from src.lecture_data import generate_sample_statsbomb_events, load_events, write_statsbomb_json
except ModuleNotFoundError:
    from config import EVENTS_FILE, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
    from lecture_data import generate_sample_statsbomb_events, load_events, write_statsbomb_json


# Extrae de forma segura el campo name de objetos anidados de StatsBomb.
# Extrae de forma segura el campo name dentro de un objeto StatsBomb anidado.
def _nested_name(event: dict[str, Any], field: str) -> str:
    value = event.get(field)
    if isinstance(value, dict):
        return str(value.get("name", ""))
    return ""


# Construye el productor Kafka con serializacion JSON UTF-8.
# Inicializa el productor Kafka con serializacion JSON en UTF-8.
def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda event: json.dumps(event, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        acks="all",
        retries=5,
    )


# Envia los eventos al topic respetando el orden y, opcionalmente, una pausa para simular streaming.
# Publica los eventos en Kafka y muestra progreso de envio.
def send_events(
    events: list[dict[str, Any]],
    topic: str,
    bootstrap_servers: str,
    delay: float,
) -> None:
    producer = build_producer(bootstrap_servers)
    try:
        total = len(events)
        for i, event in enumerate(events, start=1):
            match_id = str(event.get("match_id") or "unknown_match")
            event_type = _nested_name(event, "type") or str(event.get("type", ""))
            team = _nested_name(event, "team") or "unknown_team"
            player = _nested_name(event, "player") or "no_player"

            producer.send(topic, key=match_id, value=event)
            producer.flush()
            print(f"[{i}/{total}] match={match_id} type={event_type} team={team} player={player}")
            if delay > 0:
                time.sleep(delay)
    finally:
        producer.close()


# Argumentos configurables: broker, topic, fichero/directorio de eventos y retardo entre mensajes.
# Define los argumentos de linea de comandos para ejecutar este modulo de forma flexible.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send StatsBomb event files to Kafka")
    parser.add_argument(
        "--file",
        default=str(EVENTS_FILE),
        help="Input StatsBomb JSON file or directory. File name is used as match_id.",
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", KAFKA_TOPIC))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--delay", type=float, default=0.03, help="Seconds between events")
    parser.add_argument("--match-id", default=None, help="Optional override for a single input file")
    parser.add_argument(
        "--generate-if-missing",
        action="store_true",
        help="Create a small StatsBomb-shaped sample file if the input path does not exist",
    )
    return parser.parse_args()


# Crea datos de muestra si se ejecuta el producer sin ficheros reales.
# Crea un archivo de ejemplo si no existen eventos reales y se pide generar muestra.
def _create_sample_at(path: Path, match_id: str | None) -> Path:
    if path.suffix.lower() == ".json":
        output_file = path
    else:
        output_file = path / f"{match_id or 'MATCH_001'}.json"
    write_statsbomb_json(generate_sample_statsbomb_events(), output_file)
    return output_file


# Entrada principal del producer: prepara datos y los publica en Kafka.
# Punto de entrada del modulo: conecta las piezas y ejecuta el flujo principal.
def main() -> None:
    args = parse_args()
    input_path = Path(args.file)

    if not input_path.exists() and args.generate_if_missing:
        sample_path = _create_sample_at(input_path, args.match_id)
        input_path = sample_path if sample_path.is_file() else input_path
        print(f"Sample StatsBomb file created at {sample_path}")

    events = load_events(input_path, match_id=args.match_id)
    if not events:
        raise RuntimeError(f"No events found in {input_path}")

    match_ids = sorted({str(event.get("match_id")) for event in events})
    print(f"Sending {len(events)} StatsBomb events from {len(match_ids)} match(es) to topic {args.topic}")
    print(f"Kafka bootstrap servers: {args.bootstrap_servers}")
    send_events(events, args.topic, args.bootstrap_servers, args.delay)
    print("Finished sending events")


if __name__ == "__main__":
    main()

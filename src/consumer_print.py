"""Consumidor Kafka simple para depuracion.

No calcula metricas: solo lee eventos del topic e imprime un resumen para
comprobar que el producer y Kafka estan funcionando.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any

from kafka import KafkaConsumer

try:
    from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
except ModuleNotFoundError:
    from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


# Obtiene nombres de campos anidados para imprimir mensajes legibles.
# Extrae nombres anidados para imprimir una vista compacta de cada evento consumido.
def _name(event: dict[str, Any], field: str) -> str:
    value = event.get(field)
    if isinstance(value, dict):
        return str(value.get("name", ""))
    return ""


# Configura broker, topic y grupo de consumidor para la prueba manual.
# Define los argumentos de linea de comandos para ejecutar este modulo de forma flexible.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Debug consumer for StatsBomb events")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", KAFKA_TOPIC))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", KAFKA_BOOTSTRAP_SERVERS),
    )
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--compact", action="store_true", help="Print one compact line per event instead of full JSON")
    return parser.parse_args()


# Bucle de lectura: imprime los eventos recibidos y confirma que Kafka funciona.
# Punto de entrada del modulo: conecta las piezas y ejecuta el flujo principal.
def main() -> None:
    args = parse_args()
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        group_id="statsbomb-print-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    print(f"Listening to {args.topic} at {args.bootstrap_servers}")
    for message in consumer:
        event = message.value
        if args.compact:
            print(
                "match={match} index={index} minute={minute} type={etype} team={team} player={player}".format(
                    match=event.get("match_id"),
                    index=event.get("index"),
                    minute=event.get("minute"),
                    etype=_name(event, "type"),
                    team=_name(event, "team"),
                    player=_name(event, "player") or "-",
                )
            )
        else:
            print(json.dumps(event, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

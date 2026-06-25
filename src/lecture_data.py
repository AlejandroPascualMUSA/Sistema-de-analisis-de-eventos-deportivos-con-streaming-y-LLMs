"""Lectura y preparacion de eventos StatsBomb.

Este modulo carga ficheros JSON/JSONL, infiere el match_id a partir del
nombre del archivo y prepara eventos independientes para enviarlos por Kafka.
"""

from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path
from typing import Any, Iterable, Iterator


# Carga flexible: acepta tanto listas JSON de StatsBomb como ficheros JSONL linea a linea.
# Carga eventos StatsBomb desde JSON lista o JSONL, validando el formato de entrada.
def _load_json_or_jsonl(path: Path) -> list[dict[str, Any]]:
    """Carga una lista JSON de StatsBomb o un archivo JSONL."""
    if not path.exists():
        raise FileNotFoundError(f"Input events file not found: {path}")

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError(f"Expected a JSON list of events in {path}")
        return data

    events: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON at {path}:{line_number}: {exc}") from exc
        if not isinstance(event, dict):
            raise ValueError(f"Expected JSON object at {path}:{line_number}")
        events.append(event)
    return events


# Extrae el identificador del partido usando el nombre del fichero sin la extension .json.
# Infiere el identificador de partido a partir del nombre del archivo.
def match_id_from_file(path: str | Path) -> str:
    """Los archivos StatsBomb se llaman <match_id>.json; el stem se usa como match_id."""
    return Path(path).stem


# Inserta match_id en cada evento para que cada mensaje Kafka sea autocontenido.
# Anade match_id a cada evento porque StatsBomb no siempre lo incluye dentro del JSON.
def add_match_id(event: dict[str, Any], match_id: str) -> dict[str, Any]:
    """Devuelve una copia del evento con match_id incorporado.

    Los ficheros StatsBomb no siempre repiten el identificador del partido en
    cada evento. Como los mensajes Kafka son independientes, se añade antes
    del envio para que Spark pueda agrupar por partido.
    """
    event_copy = copy.deepcopy(event)
    event_copy.setdefault("match_id", match_id)
    return event_copy


# Punto principal de lectura: puede recibir un fichero concreto o un directorio con muchos partidos.
# Carga uno o varios partidos y devuelve una lista ordenada de eventos preparados.
def load_statsbomb_events(path: str | Path, match_id: str | None = None) -> list[dict[str, Any]]:
    """Carga eventos StatsBomb desde un fichero o desde un directorio.

    Estructura esperada:

    data/events/statsbomb/3895302.json
    data/events/statsbomb/3895303.json

    Cada fichero contiene una lista JSON. El match_id se infiere del nombre del
    archivo, salvo que se pase uno explicito para un fichero concreto.
    """
    path = Path(path)
    if path.is_dir():
        files = sorted(p for p in path.rglob("*.json") if p.is_file())
        if not files:
            raise FileNotFoundError(f"No .json StatsBomb files found in directory: {path}")

        all_events: list[dict[str, Any]] = []
        for file_path in files:
            file_match_id = match_id_from_file(file_path)
            for event in _load_json_or_jsonl(file_path):
                all_events.append(add_match_id(event, file_match_id))
        return sorted(all_events, key=lambda e: (str(e.get("match_id", "")), int(e.get("index", 0) or 0)))

    if path.is_file():
        file_match_id = match_id or match_id_from_file(path)
        return [add_match_id(event, file_match_id) for event in _load_json_or_jsonl(path)]

    raise FileNotFoundError(f"StatsBomb path not found: {path}")


# Nombre compatible con versiones anteriores usado por producer.py.
# Alias mantenido para compatibilidad con producer.py y versiones anteriores del proyecto.
# Alias de compatibilidad usado por el productor Kafka.
def load_events(path: str | Path, match_id: str | None = None) -> list[dict[str, Any]]:
    return load_statsbomb_events(path, match_id=match_id)


# Iterador util cuando se quiere recorrer eventos sin tratar directamente con la lista completa.
# Iterador para recorrer eventos sin exponer la implementacion de carga.
def iter_events(path: str | Path, match_id: str | None = None) -> Iterator[dict[str, Any]]:
    for event in load_statsbomb_events(path, match_id=match_id):
        yield event


# Escritura auxiliar usada para crear un fichero de ejemplo cuando no existen datos reales.
# Escribe una lista de eventos en formato JSON legible para pruebas locales.
def write_statsbomb_json(events: Iterable[dict[str, Any]], path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(list(events), ensure_ascii=False, indent=2), encoding="utf-8")


# Constructor pequeno para objetos StatsBomb del tipo {id, name}.
# Construye objetos StatsBomb simples con id y name para los datos de ejemplo.
def _named(identifier: int, name: str) -> dict[str, Any]:
    return {"id": identifier, "name": name}


# Genera un mini partido con estructura StatsBomb para probar Docker/Kafka/Spark rapidamente.
# Genera una muestra pequena con forma StatsBomb para probar el pipeline sin datos externos.
def generate_sample_statsbomb_events() -> list[dict[str, Any]]:
    """Muestra pequena con forma StatsBomb para pruebas locales.

    Para la entrega final debe sustituirse por ficheros StatsBomb reales.
    """
    athletic = _named(215, "Athletic Club")
    getafe = _named(216, "Getafe")
    other = _named(5, "Other")
    regular = _named(1, "Regular Play")

    return [
        {
            "id": "8e173601-0086-5189-a0f6-ff4007a0cfc0",
            "index": 1,
            "period": 1,
            "timestamp": "00:00:00.000",
            "minute": 0,
            "second": 0,
            "type": _named(35, "Starting XI"),
            "possession": 1,
            "possession_team": getafe,
            "play_pattern": other,
            "team": athletic,
            "duration": 0.0,
            "obv_total_net": None,
            "tactics": {
                "formation": 4231,
                "lineup": [
                    {"player": _named(315482, "Alejandro Padilla Pérez"), "position": _named(1, "Goalkeeper"), "jersey_number": 26},
                    {"player": _named(6670, "Andoni Gorosabel Espinosa"), "position": _named(2, "Right Back"), "jersey_number": 2},
                    {"player": _named(30225, "Oihan Sancet Tirapu"), "position": _named(19, "Center Attacking Midfield"), "jersey_number": 8},
                    {"player": _named(12553, "Gorka Guruzeta Rodríguez"), "position": _named(23, "Center Forward"), "jersey_number": 12},
                ],
            },
        },
        {
            "id": "a9e7f59e-dc7e-519a-aba4-128baa486212",
            "index": 2,
            "period": 1,
            "timestamp": "00:00:00.000",
            "minute": 0,
            "second": 0,
            "type": _named(35, "Starting XI"),
            "possession": 1,
            "possession_team": getafe,
            "play_pattern": other,
            "team": getafe,
            "duration": 0.0,
            "obv_total_net": None,
            "tactics": {
                "formation": 4141,
                "lineup": [
                    {"player": _named(6722, "David Soria Solís"), "position": _named(1, "Goalkeeper"), "jersey_number": 13},
                    {"player": _named(71519, "Juan Antonio Iglesias Sánchez"), "position": _named(2, "Right Back"), "jersey_number": 21},
                    {"player": _named(6634, "Djené Dakonam Ortega"), "position": _named(3, "Right Center Back"), "jersey_number": 2},
                    {"player": _named(12131, "Borja Mayoral Moya"), "position": _named(23, "Center Forward"), "jersey_number": 19},
                ],
            },
        },
        {
            "id": "00000000-0000-0000-0000-000000000003",
            "index": 3,
            "period": 1,
            "timestamp": "00:00:07.200",
            "minute": 0,
            "second": 7,
            "type": _named(30, "Pass"),
            "possession": 2,
            "possession_team": athletic,
            "play_pattern": regular,
            "team": athletic,
            "player": _named(6670, "Andoni Gorosabel Espinosa"),
            "position": _named(2, "Right Back"),
            "location": [35.2, 62.0],
            "duration": 1.3,
            "obv_total_net": 0.012,
            "pass": {
                "recipient": _named(30225, "Oihan Sancet Tirapu"),
                "length": 22.7,
                "angle": -0.3,
                "height": _named(1, "Ground Pass"),
                "end_location": [56.8, 48.0],
            },
        },
        {
            "id": "00000000-0000-0000-0000-000000000004",
            "index": 4,
            "period": 1,
            "timestamp": "00:02:15.900",
            "minute": 2,
            "second": 15,
            "type": _named(42, "Ball Receipt*"),
            "possession": 2,
            "possession_team": athletic,
            "play_pattern": regular,
            "team": athletic,
            "player": _named(30225, "Oihan Sancet Tirapu"),
            "position": _named(19, "Center Attacking Midfield"),
            "location": [58.1, 45.0],
            "duration": 0.0,
            "obv_total_net": 0.004,
        },
        {
            "id": "00000000-0000-0000-0000-000000000005",
            "index": 5,
            "period": 1,
            "timestamp": "00:12:45.100",
            "minute": 12,
            "second": 45,
            "type": _named(16, "Shot"),
            "possession": 5,
            "possession_team": athletic,
            "play_pattern": regular,
            "team": athletic,
            "player": _named(12553, "Gorka Guruzeta Rodríguez"),
            "position": _named(23, "Center Forward"),
            "location": [105.0, 39.8],
            "duration": 0.7,
            "obv_total_net": 0.091,
            "shot": {
                "statsbomb_xg": 0.18,
                "end_location": [120.0, 40.1, 1.2],
                "outcome": _named(100, "Saved"),
                "body_part": _named(40, "Right Foot"),
                "type": _named(87, "Open Play"),
            },
        },
        {
            "id": "00000000-0000-0000-0000-000000000006",
            "index": 6,
            "period": 1,
            "timestamp": "00:18:03.000",
            "minute": 18,
            "second": 3,
            "type": _named(2, "Ball Recovery"),
            "possession": 7,
            "possession_team": getafe,
            "play_pattern": regular,
            "team": getafe,
            "player": _named(6634, "Djené Dakonam Ortega"),
            "position": _named(3, "Right Center Back"),
            "location": [41.5, 21.2],
            "duration": 0.0,
            "obv_total_net": 0.032,
            "ball_recovery": {"offensive": False, "recovery_failure": False},
        },
        {
            "id": "00000000-0000-0000-0000-000000000007",
            "index": 7,
            "period": 1,
            "timestamp": "00:23:11.400",
            "minute": 23,
            "second": 11,
            "type": _named(30, "Pass"),
            "possession": 9,
            "possession_team": getafe,
            "play_pattern": regular,
            "team": getafe,
            "player": _named(71519, "Juan Antonio Iglesias Sánchez"),
            "position": _named(2, "Right Back"),
            "location": [61.0, 70.5],
            "duration": 1.1,
            "obv_total_net": -0.018,
            "pass": {
                "recipient": _named(12131, "Borja Mayoral Moya"),
                "length": 35.2,
                "angle": -0.6,
                "height": _named(2, "Low Pass"),
                "end_location": [92.0, 51.3],
                "outcome": _named(9, "Incomplete"),
            },
        },
        {
            "id": "00000000-0000-0000-0000-000000000008",
            "index": 8,
            "period": 1,
            "timestamp": "00:36:20.200",
            "minute": 36,
            "second": 20,
            "type": _named(16, "Shot"),
            "possession": 13,
            "possession_team": getafe,
            "play_pattern": regular,
            "team": getafe,
            "player": _named(12131, "Borja Mayoral Moya"),
            "position": _named(23, "Center Forward"),
            "location": [109.5, 38.0],
            "duration": 0.8,
            "obv_total_net": 0.425,
            "shot": {
                "statsbomb_xg": 0.31,
                "end_location": [120.0, 39.6, 0.7],
                "outcome": _named(97, "Goal"),
                "body_part": _named(40, "Right Foot"),
                "type": _named(87, "Open Play"),
            },
        },
        {
            "id": "00000000-0000-0000-0000-000000000009",
            "index": 9,
            "period": 1,
            "timestamp": "00:44:19.800",
            "minute": 44,
            "second": 19,
            "type": _named(22, "Foul Committed"),
            "possession": 15,
            "possession_team": athletic,
            "play_pattern": regular,
            "team": athletic,
            "player": _named(30225, "Oihan Sancet Tirapu"),
            "position": _named(19, "Center Attacking Midfield"),
            "location": [72.0, 36.1],
            "duration": 0.0,
            "obv_total_net": -0.021,
            "foul_committed": {"card": None, "penalty": False, "advantage": False},
        },
        {
            "id": "00000000-0000-0000-0000-000000000010",
            "index": 10,
            "period": 2,
            "timestamp": "00:63:33.300",
            "minute": 63,
            "second": 33,
            "type": _named(14, "Dribble"),
            "possession": 22,
            "possession_team": athletic,
            "play_pattern": regular,
            "team": athletic,
            "player": _named(30225, "Oihan Sancet Tirapu"),
            "position": _named(19, "Center Attacking Midfield"),
            "location": [87.0, 42.7],
            "duration": 0.8,
            "obv_total_net": 0.044,
            "dribble": {"outcome": _named(8, "Complete"), "overrun": False, "nutmeg": False},
        },
    ]


# Parametros de linea de comandos para probar la lectura de eventos desde terminal.
# Define los argumentos de linea de comandos para ejecutar este modulo de forma flexible.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read or create StatsBomb-style event files")
    parser.add_argument("--file", default="data/events/statsbomb/MATCH_001.json")
    parser.add_argument("--sample", action="store_true", help="write a small sample StatsBomb JSON file")
    return parser.parse_args()


# Entrada CLI: carga eventos y muestra un resumen basico.
# Punto de entrada del modulo: conecta las piezas y ejecuta el flujo principal.
def main() -> None:
    args = parse_args()
    path = Path(args.file)
    if args.sample:
        write_statsbomb_json(generate_sample_statsbomb_events(), path)
        print(f"Sample StatsBomb events written to {path}")
        return

    events = load_statsbomb_events(path)
    print(f"Loaded {len(events)} events from {path}")
    if events:
        first = events[0]
        event_type = first.get("type", {}).get("name") if isinstance(first.get("type"), dict) else None
        team = first.get("team", {}).get("name") if isinstance(first.get("team"), dict) else None
        print(f"First event: match_id={first.get('match_id')} type={event_type} team={team}")


if __name__ == "__main__":
    main()

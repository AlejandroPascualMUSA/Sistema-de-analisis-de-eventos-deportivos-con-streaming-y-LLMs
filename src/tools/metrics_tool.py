"""Tool de consulta de metricas calculadas por Spark.

Lee los Parquet de output/ y devuelve un resumen estructurado para que
LangGraph pueda usar datos cuantitativos reales del partido.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import unquote
from typing import Any

import pandas as pd
from langchain_core.tools import tool

# La tool puede ejecutarse como paquete (src.) o como script dentro del contenedor.
try:
    from src.config import OUTPUT_DIR
except ModuleNotFoundError:
    from config import OUTPUT_DIR


# Lee directorios Parquet de Spark, incluyendo particiones Hive como match_id=.../team=....
# Lee directorios Parquet de Spark, incluyendo datasets particionados por match_id/team.
def _read_parquet_dir(path: Path) -> pd.DataFrame:
    """Lee un directorio Parquet escrito por Spark.

    Spark puede escribir datasets particionados como:

        processed/lineups/match_id=3946395_events/team=Getafe/part-....parquet

    Leer cada part file de forma independiente pierde las columnas de particion
    (`match_id`, `team`). Primero intentamos leer el directorio como dataset. Si
    falla, leemos archivos individuales y reconstruimos
    las columnas de particion estilo Hive desde la ruta.
    """
    if not path.exists():
        return pd.DataFrame()

    try:
        df = pd.read_parquet(path)
        if not df.empty:
            return df
    except Exception:
        pass

    parquet_files = [p for p in path.rglob("*.parquet") if p.is_file()]
    if not parquet_files:
        return pd.DataFrame()

    frames = []
    for file in parquet_files:
        try:
            frame = pd.read_parquet(file)
            try:
                relative_parts = file.relative_to(path).parts[:-1]
            except ValueError:
                relative_parts = file.parts
            for segment in relative_parts:
                if "=" in segment:
                    key, value = segment.split("=", 1)
                    if key not in frame.columns:
                        frame[key] = unquote(value)
            frames.append(frame)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# Convierte tipos de pandas/numpy a tipos Python serializables en JSON.
# Convierte valores de pandas/numpy a tipos JSON serializables.
def _number(value: Any) -> Any:
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


# Transforma una fila pandas en diccionario limpio para el resumen.
# Convierte una fila de pandas en diccionario limpio.
def _record(row: pd.Series | None) -> dict[str, Any]:
    if row is None:
        return {}
    return {key: _number(value) for key, value in row.to_dict().items()}


# Filtra las tablas por match_id para evitar mezclar partidos.
# Filtra tablas de metricas para devolver solo el partido solicitado.
def _filter_match(df: pd.DataFrame, match_id: str | None) -> pd.DataFrame:
    if df.empty or not match_id or "match_id" not in df.columns:
        return df
    # No se debe volver al dataset completo si no aparece el match_id solicitado.
    # Devolver todos los partidos ocultaria problemas de IDs/nombres y podria mezclar alineaciones
    # de otro partido en el informe final.
    return df[df["match_id"].astype(str) == str(match_id)].copy()


# Convierte columnas a numericas y rellena nulos para poder ordenar/sumar.
# Asegura que las columnas metricas sean numericas antes de ordenar o sumar.
def _numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df


# Carga todas las tablas de metricas generadas por Spark.
# Carga las tablas Parquet generadas por Spark para un partido concreto.
def load_metric_tables(output_base: str | Path | None = None, match_id: str | None = None) -> dict[str, pd.DataFrame]:
    base = Path(output_base or os.getenv("OUTPUT_BASE_PATH", OUTPUT_DIR))
    team_df = _filter_match(_read_parquet_dir(base / "aggregates" / "team_metrics"), match_id)
    player_df = _filter_match(_read_parquet_dir(base / "aggregates" / "player_metrics"), match_id)
    intensity_df = _filter_match(_read_parquet_dir(base / "aggregates" / "intensity"), match_id)
    lineups_df = _filter_match(_read_parquet_dir(base / "processed" / "lineups"), match_id)
    return {
        "team_metrics": team_df,
        "player_metrics": player_df,
        "intensity": intensity_df,
        "lineups": lineups_df,
    }


# Construye el resumen estructurado que usaran los agentes y el informe.
# Construye el resumen completo del partido que usaran el informe y las tools.
def build_match_summary(output_base: str | Path | None = None, match_id: str | None = None) -> dict[str, Any]:
    tables = load_metric_tables(output_base=output_base, match_id=match_id)
    team_df = tables["team_metrics"].copy()
    player_df = tables["player_metrics"].copy()
    intensity_df = tables["intensity"].copy()
    lineups_df = tables["lineups"].copy()

    # Estructura comun que consumen LangGraph y report_generator.py.
    summary: dict[str, Any] = {
        "match_id": match_id or "latest_available",
        "status": "ok",
        "totals": {},
        "top_team_by_recoveries": {},
        "top_team_by_total_events": {},
        "top_team_by_xg": {},
        "top_team_by_pass_success": {},
        "top_player_by_participation": {},
        "top_player_by_contribution": {},
        "top_player_by_xg": {},
        "highest_intensity_interval": {},
        "team_metrics": [],
        "player_metrics_top10": [],
        "intensity_top10": [],
        "lineups_top30": [],
    }

    if team_df.empty and player_df.empty and intensity_df.empty:
        summary["status"] = "no_metrics_found"
        summary["message"] = "No Parquet metric files were found. Run the Spark streaming consumer first."
        return summary

    team_numeric = [
        "total_events",
        "passes",
        "successful_passes",
        "shots",
        "goals",
        "xg_total",
        "fouls_committed",
        "fouls_won",
        "ball_recoveries",
        "pressures",
        "interceptions",
        "clearances",
        "carries",
        "dribbles",
        "successful_dribbles",
        "final_third_events",
        "attacking_third_entries",
        "avg_event_x",
        "event_value_total",
        "obv_total_net",
        "pass_success_rate",
    ]
    player_numeric = [
        "participation_total",
        "passes",
        "successful_passes",
        "shots",
        "goals",
        "xg_total",
        "shot_assists",
        "goal_assists",
        "ball_recoveries",
        "pressures",
        "interceptions",
        "clearances",
        "successful_dribbles",
        "fouls_committed",
        "fouls_won",
        "avg_event_x",
        "game_contribution",
        "obv_total_net",
        "pass_success_rate",
    ]
    intensity_numeric = [
        "minute_interval_start",
        "minute_interval_end",
        "events_in_interval",
        "passes_in_interval",
        "shots_in_interval",
        "goals_in_interval",
        "recoveries_in_interval",
        "pressures_in_interval",
        "xg_in_interval",
        "interval_value",
    ]

    team_df = _numeric(team_df, team_numeric)
    player_df = _numeric(player_df, player_numeric)
    intensity_df = _numeric(intensity_df, intensity_numeric)

    if not team_df.empty:
        summary["totals"] = {
            "events": int(team_df.get("total_events", pd.Series(dtype=float)).sum()),
            "passes": int(team_df.get("passes", pd.Series(dtype=float)).sum()),
            "successful_passes": int(team_df.get("successful_passes", pd.Series(dtype=float)).sum()),
            "shots": int(team_df.get("shots", pd.Series(dtype=float)).sum()),
            "goals": int(team_df.get("goals", pd.Series(dtype=float)).sum()),
            "xg_total": round(float(team_df.get("xg_total", pd.Series(dtype=float)).sum()), 3),
            "fouls_committed": int(team_df.get("fouls_committed", pd.Series(dtype=float)).sum()),
            "ball_recoveries": int(team_df.get("ball_recoveries", pd.Series(dtype=float)).sum()),
            "pressures": int(team_df.get("pressures", pd.Series(dtype=float)).sum()),
            "obv_total_net": round(float(team_df.get("obv_total_net", pd.Series(dtype=float)).sum()), 3),
        }
        if "total_events" in team_df.columns:
            summary["top_team_by_total_events"] = _record(team_df.sort_values("total_events", ascending=False).iloc[0])
        if "ball_recoveries" in team_df.columns:
            summary["top_team_by_recoveries"] = _record(team_df.sort_values("ball_recoveries", ascending=False).iloc[0])
        if "xg_total" in team_df.columns:
            summary["top_team_by_xg"] = _record(team_df.sort_values("xg_total", ascending=False).iloc[0])
        if "pass_success_rate" in team_df.columns:
            summary["top_team_by_pass_success"] = _record(team_df.sort_values("pass_success_rate", ascending=False).iloc[0])
        sort_col = "total_events" if "total_events" in team_df.columns else team_df.columns[0]
        summary["team_metrics"] = [
            {key: _number(value) for key, value in row.items()}
            for row in team_df.sort_values(sort_col, ascending=False).to_dict(orient="records")
        ]

    if not player_df.empty:
        if "participation_total" in player_df.columns:
            summary["top_player_by_participation"] = _record(player_df.sort_values("participation_total", ascending=False).iloc[0])
        if "game_contribution" in player_df.columns:
            summary["top_player_by_contribution"] = _record(player_df.sort_values("game_contribution", ascending=False).iloc[0])
        if "xg_total" in player_df.columns:
            summary["top_player_by_xg"] = _record(player_df.sort_values("xg_total", ascending=False).iloc[0])
        sort_col = "participation_total" if "participation_total" in player_df.columns else player_df.columns[0]
        summary["player_metrics_top10"] = [
            {key: _number(value) for key, value in row.items()}
            for row in player_df.sort_values(sort_col, ascending=False).head(10).to_dict(orient="records")
        ]

    if not intensity_df.empty:
        if "events_in_interval" in intensity_df.columns:
            summary["highest_intensity_interval"] = _record(intensity_df.sort_values("events_in_interval", ascending=False).iloc[0])
        sort_col = "events_in_interval" if "events_in_interval" in intensity_df.columns else intensity_df.columns[0]
        summary["intensity_top10"] = [
            {key: _number(value) for key, value in row.items()}
            for row in intensity_df.sort_values(sort_col, ascending=False).head(10).to_dict(orient="records")
        ]

    if not lineups_df.empty:
        sort_columns = [column for column in ["team", "jersey_number", "player"] if column in lineups_df.columns]
        if sort_columns:
            lineups_df = lineups_df.sort_values(sort_columns)
        summary["lineups_top30"] = [
            {key: _number(value) for key, value in row.items()}
            for row in lineups_df.head(30).to_dict(orient="records")
        ]

    return summary


# Responde a preguntas predefinidas a partir del resumen de metricas.
# Responde preguntas frecuentes sobre las metricas calculadas.
def answer_metric_question(question: str, match_id: str | None = None, output_base: str | Path | None = None) -> dict[str, Any]:
    summary = build_match_summary(output_base=output_base, match_id=match_id)
    question_l = question.lower()

    if summary.get("status") != "ok":
        return summary

    if "recuper" in question_l or "recovery" in question_l:
        return {"answer_type": "top_team_by_recoveries", "data": summary["top_team_by_recoveries"]}
    if "particip" in question_l or "intervino" in question_l:
        return {"answer_type": "top_player_by_participation", "data": summary["top_player_by_participation"]}
    if "contrib" in question_l or "obv" in question_l or "destacado" in question_l:
        return {"answer_type": "top_player_by_contribution", "data": summary["top_player_by_contribution"]}
    if "xg" in question_l or "expected" in question_l:
        return {"answer_type": "top_team_by_xg", "data": summary["top_team_by_xg"], "totals": summary["totals"]}
    if "pase" in question_l or "pass" in question_l:
        return {"answer_type": "top_team_by_pass_success", "data": summary["top_team_by_pass_success"], "totals": summary["totals"]}
    if "intens" in question_l or "tramo" in question_l or "interval" in question_l:
        return {"answer_type": "highest_intensity_interval", "data": summary["highest_intensity_interval"]}
    if "tiro" in question_l or "shot" in question_l:
        return {"answer_type": "total_shots", "data": {"shots": summary["totals"].get("shots", 0), "xg_total": summary["totals"].get("xg_total", 0)}}
    if "falta" in question_l or "foul" in question_l:
        return {"answer_type": "total_fouls_committed", "data": {"fouls_committed": summary["totals"].get("fouls_committed", 0)}}
    if "gol" in question_l or "goal" in question_l:
        return {"answer_type": "total_goals", "data": {"goals": summary["totals"].get("goals", 0)}}
    if "alineacion" in question_l or "lineup" in question_l or "starting" in question_l:
        return {"answer_type": "lineups", "data": summary["lineups_top30"]}

    return {"answer_type": "full_match_summary", "data": summary}


# Tool de LangChain/LangGraph: expone metricas del partido en formato JSON.
@tool
# Tool expuesta a LangGraph para consultar metricas del partido.
def consulta_metricas_partido(question: str, match_id: str = "") -> str:
    """Consulta metricas StatsBomb generadas por Spark y guardadas en Parquet."""
    result = answer_metric_question(question=question, match_id=match_id or None)
    return json.dumps(result, ensure_ascii=False, indent=2)

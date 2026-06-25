"""Generacion del informe Markdown/HTML.

Construye una version estable basada en metricas y RAG, y opcionalmente añade
una interpretacion breve generada con Ollama local.
"""

from __future__ import annotations

import html
import json
import os
import re
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


# Normaliza texto para limpiar y filtrar fragmentos RAG.
# Normaliza texto para comparaciones internas de equipos/contextos.
# Normaliza texto usado por los filtros de contexto del informe.
def _normalize_for_context_filter(text: Any) -> str:
    value = str(text or "").lower()
    value = re.sub(r"[^a-z0-9áéíóúüñç ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


# Alias reutilizado por las funciones de filtrado de equipos.
# Normaliza texto para comparar nombres de equipos y documentos de forma robusta.
def _normalize(text: Any) -> str:
    return _normalize_for_context_filter(text)


TEAM_ALIASES: dict[str, set[str]] = {
    "athletic club": {"athletic club", "athletic"},
    "getafe": {"getafe"},
    "girona": {"girona"},
    "real betis": {"real betis", "betis"},
    "barcelona": {"barcelona", "fc barcelona", "barça", "barca"},
    "real madrid": {"real madrid"},
    "atlético madrid": {"atlético madrid", "atletico madrid", "atlético", "atletico"},
    "real sociedad": {"real sociedad", "sociedad"},
    "sevilla": {"sevilla"},
    "valencia": {"valencia"},
    "villarreal": {"villarreal"},
    "osasuna": {"osasuna"},
    "celta vigo": {"celta vigo", "celta"},
    "rayo vallecano": {"rayo vallecano", "rayo"},
    "mallorca": {"mallorca"},
    "deportivo alavés": {"deportivo alavés", "deportivo alaves", "alavés", "alaves"},
    "las palmas": {"las palmas", "ud las palmas"},
    "espanyol": {"espanyol"},
    "leganés": {"leganés", "leganes"},
    "real valladolid": {"real valladolid", "valladolid"},
    "cádiz": {"cádiz", "cadiz"},
    "granada": {"granada"},
    "almería": {"almería", "almeria"},
}

ALL_TEAM_ALIASES: set[str] = set()
for aliases in TEAM_ALIASES.values():
    ALL_TEAM_ALIASES.update(aliases)


CONTEXT_FALLBACK_SNIPPETS: dict[str, str] = {
    "athletic club": "Documento de contexto sobre Athletic Club. Se usa como apoyo interpretativo para relacionar métricas como presión, recuperaciones, uso de bandas, entradas al último tercio, tiros y xG con posibles patrones de intensidad competitiva.",
    "getafe": "Documento de contexto sobre Getafe. Se usa para interpretar métricas asociadas a duelos, faltas, defensa compacta, recuperaciones, juego directo y fases de presión sin sustituir las cifras calculadas por Spark.",
    "girona": "Documento de contexto sobre Girona. Sirve como apoyo para analizar posesión, circulación, progresión por zonas interiores o laterales, entradas al último tercio, volumen de pases, tiros, xG y fases de dominio territorial.",
    "real betis": "Documento de contexto sobre Real Betis. Ayuda a interpretar posesión, calidad técnica, uso de mediocampo, progresiones, pases, tiros, xG, recuperaciones y el equilibrio entre elaboración y finalización.",
    "barcelona": "Documento de contexto sobre Barcelona. Se utiliza para interpretar métricas de posesión, volumen de pases, presión tras pérdida, entradas al último tercio, producción de xG y dominio territorial frente a generación real de ocasiones.",
    "valencia": "Documento de contexto sobre Valencia. Sirve para contextualizar fases de repliegue, transiciones, duelos, recuperaciones, progresiones por banda, tiros, xG y eficiencia ofensiva o defensiva.",
    "real madrid": "Documento de contexto sobre Real Madrid. Ayuda a interpretar métricas de transiciones, ataques rápidos, calidad de ocasión, xG, tiros, conducción, último tercio y contribución individual.",
    "atlético madrid": "Documento de contexto sobre Atlético Madrid. Apoya la lectura de organización defensiva, duelos, recuperaciones, presión selectiva, ataques directos, balón parado, tiros y xG.",
    "real sociedad": "Documento de contexto sobre Real Sociedad. Permite interpretar presión organizada, circulación, control territorial, recuperaciones, progresión y calidad de ocasiones a partir de xG y tiros.",
    "sevilla": "Documento de contexto sobre Sevilla. Solo debe aparecer si Sevilla participa en el partido; se usa para apoyar la interpretación de intensidad, bandas, centros, duelos y producción ofensiva.",
    "las palmas": "Documento de contexto sobre Las Palmas. Solo debe aparecer si Las Palmas participa en el partido; se usa para contextualizar posesión, salida de balón, ritmo de pase y progresión.",
    "guía de intensidad temporal": "Documento metodológico que ayuda a leer los intervalos de mayor actividad. Un pico de eventos debe interpretarse junto con el tipo de acciones: pases, presiones, recuperaciones, tiros, despejes o faltas.",
    "guia de intensidad temporal": "Documento metodológico que ayuda a leer los intervalos de mayor actividad. Un pico de eventos debe interpretarse junto con el tipo de acciones: pases, presiones, recuperaciones, tiros, despejes o faltas.",
    "guía de calidad de datos": "Documento metodológico sobre validación de datos. Recuerda comprobar nulos, nombres de equipos, jugadores sin posición, eventos sin coordenadas y consistencia entre eventos StatsBomb, Parquet y métricas agregadas.",
    "guia de calidad de datos": "Documento metodológico sobre validación de datos. Recuerda comprobar nulos, nombres de equipos, jugadores sin posición, eventos sin coordenadas y consistencia entre eventos StatsBomb, Parquet y métricas agregadas.",
    "cómo interpretar el informe": "Documento guía que explica la separación entre evidencias cuantitativas y contexto textual. Las cifras proceden de Kafka y Spark Structured Streaming; el RAG aporta contexto complementario, no datos del partido.",
    "como interpretar el informe": "Documento guía que explica la separación entre evidencias cuantitativas y contexto textual. Las cifras proceden de Kafka y Spark Structured Streaming; el RAG aporta contexto complementario, no datos del partido.",
    "guía del informe final": "Documento metodológico que describe la estructura del informe: resumen cuantitativo, métricas por equipo, jugadores destacados, tramos de intensidad, alineaciones, contexto RAG y conclusión técnica.",
    "guia del informe final": "Documento metodológico que describe la estructura del informe: resumen cuantitativo, métricas por equipo, jugadores destacados, tramos de intensidad, alineaciones, contexto RAG y conclusión técnica.",
    "glosario de eventos statsbomb": "Documento de apoyo para interpretar eventos StatsBomb como Pass, Shot, Pressure, Ball Recovery, Foul Committed, Carry, Dribble, Interception y Duel en las métricas del informe.",
    "métricas statsbomb": "Documento metodológico sobre métricas como xG, OBV, presiones, recuperaciones, tiros, pases, faltas, entradas al último tercio e intensidad temporal.",
    "metricas statsbomb": "Documento metodológico sobre métricas como xG, OBV, presiones, recuperaciones, tiros, pases, faltas, entradas al último tercio e intensidad temporal.",
}


# Texto alternativo cuando un fragmento RAG solo contiene encabezados o metadatos.
# Genera un texto de contexto minimo cuando el chunk recuperado no es mostrable.
def _fallback_context_text(title: Any) -> str:
    normalized_title = _normalize_for_context_filter(title)
    if normalized_title in CONTEXT_FALLBACK_SNIPPETS:
        return CONTEXT_FALLBACK_SNIPPETS[normalized_title]

    # Permite coincidencias parciales para titulos como "Guía de calidad de datos.md".
    for key, value in CONTEXT_FALLBACK_SNIPPETS.items():
        if key and (key in normalized_title or normalized_title in key):
            return value

    return (
        "Documento contextual validado por el filtro RAG del partido. Se usa solo como apoyo interpretativo; "
        "las cifras principales del informe proceden de las métricas calculadas con Spark."
    )


# Busca frases completas evitando coincidencias parciales.
# Comprueba si una frase normalizada aparece dentro de un texto normalizado.
def _contains_phrase(text: str, phrase: str) -> bool:
    return bool(re.search(r"(?<![a-z0-9])" + re.escape(phrase) + r"(?![a-z0-9])", text))


# Construye alias de equipos para reconocer documentos relevantes del partido.
# Construye variantes de nombres de equipos para permitir coincidencias flexibles.
def _aliases_for_match_teams(teams: list[str]) -> set[str]:
    allowed: set[str] = set()
    normalized_teams = [_normalize(team) for team in teams if team]
    for team in normalized_teams:
        allowed.add(team)
        for aliases in TEAM_ALIASES.values():
            if team in aliases or any(_contains_phrase(team, alias) or _contains_phrase(alias, team) for alias in aliases):
                allowed.update(aliases)
    return {alias for alias in allowed if alias}


# Detecta menciones a equipos dentro de un contexto recuperado.
# Detecta que equipos conocidos aparecen mencionados en un texto.
def _mentioned_team_aliases(text: str) -> set[str]:
    return {alias for alias in ALL_TEAM_ALIASES if _contains_phrase(text, alias)}


# Determina si un documento es de un equipo especifico.
# Identifica documentos que son especificamente de un equipo.
def _is_explicit_team_document(context: dict[str, Any], normalized_haystack: str) -> bool:
    raw = " ".join(str(context.get(field, "")) for field in ["title", "source", "text", "doc_id"])
    raw_lower = raw.lower()
    title = _normalize(context.get("title", ""))
    source = _normalize(context.get("source", ""))
    doc_id = _normalize(context.get("doc_id", ""))

    if "tipo_documento: equipo" in raw_lower or "tipo documento equipo" in normalized_haystack:
        return True
    if re.search(r"\bequipo\s*[:=]\s*[a-záéíóúüñç]", raw_lower):
        return True
    if title in ALL_TEAM_ALIASES or any(_contains_phrase(source, alias) or _contains_phrase(doc_id, alias) for alias in ALL_TEAM_ALIASES):
        return True
    return False




# Obtiene equipos desde el resumen de metricas usado para el informe.
# Extrae nombres de equipos desde el resumen de metricas del informe.
def _teams_from_metrics(metrics: dict[str, Any]) -> list[str]:
    summary = _get_summary(metrics)
    teams: list[str] = []

    for row in summary.get("team_metrics", []) or []:
        team = row.get("team")
        if isinstance(team, str) and team and team not in teams:
            teams.append(team)

    for row in summary.get("lineups_top30", []) or []:
        team = row.get("team")
        if isinstance(team, str) and team and team not in teams:
            teams.append(team)

    for key in [
        "top_team_by_total_events",
        "top_team_by_recoveries",
        "top_team_by_xg",
        "top_team_by_pass_success",
        "highest_intensity_interval",
    ]:
        row = summary.get(key, {})
        if isinstance(row, dict):
            team = row.get("team")
            if isinstance(team, str) and team and team not in teams:
                teams.append(team)

    return teams


# Aplica el filtro final para no mostrar contexto de equipos ajenos al partido.
# Filtra contextos RAG para que no contaminen el informe con equipos ajenos.
def _filter_contexts_for_match(contexts: list[dict[str, Any]], metrics: dict[str, Any]) -> list[dict[str, Any]]:
    """Remove team-specific RAG chunks that do not belong to the match.

    This final filter is applied immediately before the report is rendered.
    Therefore, even if the retriever returns Sevilla or Las Palmas for a Girona
    vs Real Betis match, those chunks cannot appear in section 6.
    """
    teams = _teams_from_metrics(metrics)
    allowed_aliases = _aliases_for_match_teams(teams)
    generic_terms = [
        "statsbomb",
        "xg",
        "obv",
        "presiones",
        "recuperaciones",
        "kafka",
        "spark",
        "structured streaming",
        "informe",
        "métricas",
        "metricas",
        "calidad de datos",
        "glosario",
        "zonas",
        "intensidad",
        "pipeline",
    ]

    filtered: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in contexts:
        haystack = " ".join(str(item.get(field, "")) for field in ["title", "source", "text", "doc_id"])
        normalized = _normalize_for_context_filter(haystack)
        mentioned_aliases = _mentioned_team_aliases(normalized)

        mentions_current_team = bool(mentioned_aliases & allowed_aliases)
        mentions_other_team = bool(mentioned_aliases - allowed_aliases)
        explicit_team_doc = _is_explicit_team_document(item, normalized)
        is_generic = any(term in normalized for term in generic_terms)

        keep = False
        if mentions_current_team:
            keep = True
        elif mentions_other_team:
            keep = False
        elif explicit_team_doc:
            keep = False
        elif is_generic:
            keep = True

        if keep:
            key = str(item.get("doc_id") or item.get("source") or item.get("title") or item.get("text", "")[:80])
            if key not in seen:
                seen.add(key)
                filtered.append(item)

    return filtered


# Convierte contextos RAG en texto compacto para el prompt de Ollama.
# Prepara el contexto RAG para enviarlo a Ollama o incorporarlo al informe.
def _format_contexts(contexts: list[dict[str, Any]], max_chars: int = 2200) -> str:
    """Return a short RAG context block.

    Ollama running locally is slower than a hosted model. Keeping the context
    small makes the report faster and reduces the risk that the model only
    summarizes the RAG documents and ignores the Spark metrics.
    """
    if not contexts:
        return "No se recuperaron documentos contextuales."

    lines: list[str] = []
    used = 0
    for idx, item in enumerate(contexts, start=1):
        title = str(item.get("title", "documento"))
        source = str(item.get("source", "desconocido"))
        text = str(item.get("text", "")).strip().replace("\n", " ")
        chunk = f"[{idx}] {title} ({source})\n{text}"
        if used + len(chunk) > max_chars:
            remaining = max_chars - used
            if remaining > 120:
                lines.append(chunk[:remaining].rstrip() + "...")
            break
        lines.append(chunk)
        used += len(chunk)
    return "\n\n".join(lines)


# Limpia fragmentos antes de mostrarlos en la seccion RAG del informe.
# Limpia snippets del RAG para ocultar encabezados y metadatos internos.
def _clean_rag_snippet(text: Any) -> str:
    """Prepare a RAG chunk for the final report.

    The index may contain small chunks that are only headings or metadata, for
    example "# Barcelona" or "tipo_documento: equipo". Those chunks are useful
    for retrieval, but they look bad in the final report, so they are removed
    from the displayed snippet.
    """
    raw = str(text or "").replace("\r", "\n")
    cleaned_lines: list[str] = []
    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            continue
        normalized = _normalize_for_context_filter(line)
        if line.startswith("#") and len(normalized.split()) <= 5:
            continue
        if "tipo documento" in normalized or "tipo_documento" in line.lower():
            continue
        if re.fullmatch(r"equipo\s*[:=].*", line.lower()):
            continue
        cleaned_lines.append(line)

    cleaned = " ".join(cleaned_lines)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


# Deduplica y prepara los contextos que apareceran visibles en el informe.
# Selecciona los contextos que se mostraran de forma visible en el informe.
def _contexts_for_display(contexts: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Deduplica fragmentos RAG por titulo y conserva solo extractos utiles."""
    grouped: dict[str, dict[str, str]] = {}
    order: list[str] = []

    for item in contexts:
        title = str(item.get("title") or item.get("source") or item.get("doc_id") or "documento").strip()
        if not title:
            title = "documento"
        key = _normalize_for_context_filter(title)
        if not key:
            key = title.lower()

        snippet = _clean_rag_snippet(item.get("text", ""))

        if key not in grouped:
            grouped[key] = {"title": title, "text": snippet}
            order.append(key)
        else:
            # Conserva el fragmento mas informativo cuando hay varios del mismo documento.
            if len(snippet) > len(grouped[key].get("text", "")):
                grouped[key]["text"] = snippet

    result: list[dict[str, str]] = []
    for key in order:
        item = grouped[key]
        title = item.get("title", "documento")
        text = item.get("text", "").strip()
        if not text:
            text = _fallback_context_text(title)
        result.append({"title": title, "text": text})

    return result


# Prioriza visualmente documentos de los equipos participantes.
# Ordena contextos visibles poniendo antes los equipos reales del partido.
def _context_team_index_for_display(context: dict[str, str], teams: list[str]) -> int | None:
    title = _normalize_for_context_filter(context.get("title", ""))
    text = _normalize_for_context_filter(context.get("text", ""))
    for index, team in enumerate(teams):
        aliases = _aliases_for_match_teams([team])
        if title in aliases:
            return index
        if any(_contains_phrase(title, alias) for alias in aliases):
            return index
        # Los textos de respaldo de equipos tambien contienen el nombre del equipo, pero
        # en condiciones normales basta con que coincida el titulo.
        if any(_contains_phrase(text, alias) for alias in aliases) and title in ALL_TEAM_ALIASES:
            return index
    return None


# Da prioridad a guias generales relevantes para interpretar metricas.
# Prioriza documentos genericos visibles por utilidad interpretativa.
def _generic_display_priority(context: dict[str, str]) -> int:
    title = _normalize_for_context_filter(context.get("title", ""))
    text = _normalize_for_context_filter(context.get("text", ""))
    haystack = f"{title} {text}"
    if any(term in haystack for term in ["métricas statsbomb", "metricas statsbomb", "glosario"]):
        return 0
    if any(term in haystack for term in ["interpretar el informe", "lectura táctica", "lectura tactica"]):
        return 1
    if any(term in haystack for term in ["intensidad", "zonas", "progresión", "progresion"]):
        return 2
    if any(term in haystack for term in ["calidad de datos", "pipeline", "rag"]):
        return 3
    return 4


# Ordena contextos: equipos primero, guias despues.
# Ordena todos los contextos antes de componer la seccion RAG del informe.
def _order_contexts_for_report(contexts: list[dict[str, str]], teams: list[str]) -> list[dict[str, str]]:
    """Muestra primero contextos de equipos y luego algunas guias genericas."""
    def sort_key(item: dict[str, str]) -> tuple[int, int, str]:
        team_index = _context_team_index_for_display(item, teams)
        title = str(item.get("title", ""))
        if team_index is not None:
            return (0, team_index, title)
        return (1, _generic_display_priority(item), title)

    return sorted(contexts, key=sort_key)


# Redondeo seguro de valores numericos para tablas del informe.
# Redondea valores numericos evitando errores con nulls o tipos no esperados.
def _round(value: Any, digits: int = 3) -> Any:
    try:
        return round(float(value), digits)
    except Exception:
        return value


# Localiza el resumen del partido dentro de la estructura de metricas.
# Extrae el resumen principal desde el contenedor de metricas.
def _get_summary(metrics: dict[str, Any]) -> dict[str, Any]:
    if metrics.get("answer_type") == "full_match_summary":
        data = metrics.get("data", {})
        return data if isinstance(data, dict) else {}
    data = metrics.get("data", metrics)
    return data if isinstance(data, dict) else {}


# Reduce las metricas antes de enviarlas a Ollama para mejorar latencia y claridad.
# Reduce las metricas al subconjunto necesario para no saturar el prompt de Ollama.
def _compact_metrics_for_llm(metrics: dict[str, Any]) -> dict[str, Any]:
    """Keep only the metrics needed by the local LLM.

    The deterministic report already contains the full tables. Ollama only gets
    a compact version so it can write a short interpretation without timing out
    or drifting into a generic RAG summary.
    """
    summary = _get_summary(metrics)
    return {
        "match_id": summary.get("match_id"),
        "status": summary.get("status"),
        "totals": summary.get("totals", {}),
        "top_team_by_total_events": summary.get("top_team_by_total_events", {}),
        "top_team_by_recoveries": summary.get("top_team_by_recoveries", {}),
        "top_team_by_xg": summary.get("top_team_by_xg", {}),
        "top_team_by_pass_success": summary.get("top_team_by_pass_success", {}),
        "top_player_by_participation": summary.get("top_player_by_participation", {}),
        "top_player_by_contribution": summary.get("top_player_by_contribution", {}),
        "top_player_by_xg": summary.get("top_player_by_xg", {}),
        "highest_intensity_interval": summary.get("highest_intensity_interval", {}),
        "team_metrics": summary.get("team_metrics", [])[:4],
        "player_metrics_top10": summary.get("player_metrics_top10", [])[:6],
        "intensity_top10": summary.get("intensity_top10", [])[:5],
    }


# Genera el informe estable y controlado con metricas reales y RAG.
# Informe base: siempre funciona y se apoya en datos calculados por Spark.
# Genera un informe estable y determinista basado en metricas y contexto RAG.
def _template_report(metrics: dict[str, Any], contexts: list[dict[str, Any]]) -> str:
    summary = _get_summary(metrics)

    if summary.get("status") == "no_metrics_found":
        return (
            "# Informe automático de partido\n\n"
            "No se encontraron métricas de Spark. Ejecuta primero el consumidor Structured Streaming y después el grafo LangGraph.\n"
        )

    totals = summary.get("totals", {})
    top_events_team = summary.get("top_team_by_total_events", {})
    top_recovery_team = summary.get("top_team_by_recoveries", {})
    top_xg_team = summary.get("top_team_by_xg", {})
    top_pass_team = summary.get("top_team_by_pass_success", {})
    top_player = summary.get("top_player_by_participation", {})
    top_contribution = summary.get("top_player_by_contribution", {})
    top_player_xg = summary.get("top_player_by_xg", {})
    top_interval = summary.get("highest_intensity_interval", {})
    team_metrics = summary.get("team_metrics", [])
    player_metrics = summary.get("player_metrics_top10", [])
    intensity_rows = summary.get("intensity_top10", [])
    lineups = summary.get("lineups_top30", [])

    match_teams = _teams_from_metrics(metrics)
    display_contexts = _order_contexts_for_report(_contexts_for_display(contexts), match_teams)
    context_titles: list[str] = []
    for item in display_contexts:
        title = item.get("title")
        if title and title not in context_titles:
            context_titles.append(title)

    lines = [
        "# Informe automático de partido con eventos StatsBomb",
        "",
        "## 1. Resumen cuantitativo del stream",
        "",
        (
            f"El pipeline ha procesado **{totals.get('events', 0)} eventos agregados por equipo**. "
            f"En esos eventos aparecen **{totals.get('passes', 0)} pases**, "
            f"**{totals.get('shots', 0)} tiros**, **{totals.get('goals', 0)} goles**, "
            f"**{totals.get('fouls_committed', 0)} faltas cometidas**, "
            f"**{totals.get('ball_recoveries', 0)} recuperaciones** y "
            f"**{totals.get('pressures', 0)} presiones**. "
            f"El xG total agregado es **{_round(totals.get('xg_total', 0))}** y el OBV neto total es "
            f"**{_round(totals.get('obv_total_net', 0))}**."
        ),
        "",
    ]

    if top_events_team:
        lines.append(
            f"El equipo con mayor volumen de eventos fue **{top_events_team.get('team', 'desconocido')}**, con **{top_events_team.get('total_events', 0)} eventos**."
        )
    if top_xg_team:
        lines.append(
            f"El equipo con mayor producción de xG fue **{top_xg_team.get('team', 'desconocido')}**, con **xG={_round(top_xg_team.get('xg_total', 0))}**."
        )
    if top_recovery_team:
        lines.append(
            f"El equipo con más recuperaciones fue **{top_recovery_team.get('team', 'desconocido')}**, con **{top_recovery_team.get('ball_recoveries', 0)} recuperaciones**."
        )
    if top_pass_team:
        lines.append(
            f"La mejor tasa de acierto en el pase fue de **{top_pass_team.get('team', 'desconocido')}**, con **{top_pass_team.get('pass_success_rate', 0)}%**."
        )
    if top_player:
        lines.append(
            f"El jugador con mayor participación fue **{top_player.get('player', 'desconocido')}** ({top_player.get('team', 'desconocido')}), con **{top_player.get('participation_total', 0)} eventos**."
        )
    if top_contribution:
        lines.append(
            f"El mayor valor acumulado de contribución lo registró **{top_contribution.get('player', 'desconocido')}**, con **{_round(top_contribution.get('game_contribution', 0))}**."
        )
    if top_player_xg:
        lines.append(
            f"El jugador con más xG fue **{top_player_xg.get('player', 'desconocido')}**, con **xG={_round(top_player_xg.get('xg_total', 0))}**."
        )
    if top_interval:
        start = top_interval.get("minute_interval_start", "?")
        end = top_interval.get("minute_interval_end", "?")
        lines.append(
            f"El tramo de mayor intensidad fue el intervalo **{start}-{end}** de **{top_interval.get('team', 'desconocido')}**, con **{top_interval.get('events_in_interval', 0)} eventos**."
        )

    lines.extend(["", "## 2. Métricas por equipo", ""])
    if team_metrics:
        for team in team_metrics:
            lines.append(
                "- **{team}**: {events} eventos, {passes} pases, {pass_rate}% acierto, "
                "{shots} tiros, {goals} goles, xG={xg}, {recoveries} recuperaciones, "
                "{pressures} presiones, OBV={obv}.".format(
                    team=team.get("team", "desconocido"),
                    events=team.get("total_events", 0),
                    passes=team.get("passes", 0),
                    pass_rate=team.get("pass_success_rate", 0),
                    shots=team.get("shots", 0),
                    goals=team.get("goals", 0),
                    xg=_round(team.get("xg_total", 0)),
                    recoveries=team.get("ball_recoveries", 0),
                    pressures=team.get("pressures", 0),
                    obv=_round(team.get("obv_total_net", 0)),
                )
            )
    else:
        lines.append("No hay métricas por equipo disponibles.")

    lines.extend(["", "## 3. Top jugadores", ""])
    if player_metrics:
        for player in player_metrics[:10]:
            lines.append(
                "- **{player}** ({team}, {position}): {events} eventos, {passes} pases, "
                "{shots} tiros, {goals} goles, xG={xg}, contribución={contribution}.".format(
                    player=player.get("player", "desconocido"),
                    team=player.get("team", "desconocido"),
                    position=player.get("position", "posición desconocida"),
                    events=player.get("participation_total", 0),
                    passes=player.get("passes", 0),
                    shots=player.get("shots", 0),
                    goals=player.get("goals", 0),
                    xg=_round(player.get("xg_total", 0)),
                    contribution=_round(player.get("game_contribution", 0)),
                )
            )
    else:
        lines.append("No hay métricas por jugador disponibles.")

    lines.extend(["", "## 4. Tramos de intensidad", ""])
    if intensity_rows:
        for row in intensity_rows[:5]:
            lines.append(
                "- **{team}**, minuto {start}-{end}: {events} eventos, {passes} pases, {shots} tiros, "
                "{recoveries} recuperaciones, {pressures} presiones, xG={xg}.".format(
                    team=row.get("team", "equipo desconocido"),
                    start=row.get("minute_interval_start", "?"),
                    end=row.get("minute_interval_end", "?"),
                    events=row.get("events_in_interval", 0),
                    passes=row.get("passes_in_interval", 0),
                    shots=row.get("shots_in_interval", 0),
                    recoveries=row.get("recoveries_in_interval", 0),
                    pressures=row.get("pressures_in_interval", 0),
                    xg=_round(row.get("xg_in_interval", 0)),
                )
            )
    else:
        lines.append("No hay métricas de intensidad disponibles.")

    if lineups:
        lines.extend(["", "## 5. Alineaciones detectadas en eventos Starting XI", ""])
        for item in lineups[:22]:
            lines.append(
                f"- {item.get('team', 'equipo desconocido')}: {item.get('player', 'jugador desconocido')} - {item.get('position', 'posición desconocida')} #{item.get('jersey_number', '')}"
            )

    lines.extend(["", "## 6. Contexto recuperado por RAG", ""])
    if context_titles:
        lines.append("El módulo RAG recuperó estos documentos: " + ", ".join(context_titles) + ".")
    else:
        lines.append("El módulo RAG no recuperó documentos contextuales.")

    for idx, item in enumerate(display_contexts[:5], start=1):
        text = str(item.get("text", "")).strip().replace("\n", " ")
        if len(text) > 500:
            text = text[:500].rstrip() + "..."
        lines.append(f"- Contexto {idx} - {item.get('title', 'documento')}: {text}")

    lines.extend(["", "## 7. Interpretación base", ""])
    if top_events_team:
        lines.append(
            f"El alto volumen de eventos de **{top_events_team.get('team', 'el equipo principal')}** sugiere mayor presencia en el flujo de acciones registradas."
        )
    if top_xg_team:
        lines.append(
            f"El xG de **{top_xg_team.get('team', 'el equipo con más xG')}** ayuda a separar la cantidad de tiros de la calidad de las ocasiones."
        )
    if top_recovery_team:
        lines.append(
            f"Las recuperaciones de **{top_recovery_team.get('team', 'el equipo con más recuperaciones')}** indican capacidad para volver a intervenir tras pérdida o disputa."
        )
    if top_interval:
        lines.append(
            f"El intervalo **{top_interval.get('minute_interval_start', '?')}-{top_interval.get('minute_interval_end', '?')}** concentra el pico de actividad y puede revisarse en vídeo para explicar cambios de ritmo, presión o ataques consecutivos."
        )

    lines.extend(
        [
            "",
            "## 8. Separación de fuentes",
            "",
            "- Las cifras proceden de eventos StatsBomb consumidos desde Kafka y procesados con Spark Structured Streaming.",
            "- El contexto textual procede de los documentos recuperados por el módulo RAG.",
            "- LangGraph coordina la consulta de métricas, la recuperación documental, la generación y la escritura del informe.",
            "",
            "## 9. Conclusión técnica",
            "",
            "El sistema completa un flujo Big Data de extremo a extremo: los eventos StatsBomb se publican en Kafka, Spark los transforma y agrega en streaming, las métricas quedan persistidas en Parquet y el grafo LangGraph genera un informe enriquecido con RAG y Ollama local.",
        ]
    )
    return "\n".join(lines)


# Valida si la respuesta de Ollama aporta valor y no es generica.
# Valida si la respuesta de Ollama aporta algo util o debe descartarse.
def _content_is_useful(content: str, metrics: dict[str, Any]) -> bool:
    """Rechaza respuestas pobres de Ollama que solo resumen el contexto RAG."""
    text = content.strip()
    if len(text) < 350:
        return False

    lower = text.lower()
    bad_patterns = [
        "el contexto proporcionado",
        "no se proporciona suficiente información",
        "en cuanto a la pregunta específica",
        "algunas posibles preguntas",
        "en resumen, el informe proporciona",
    ]
    if any(pattern in lower for pattern in bad_patterns):
        return False

    # A useful analytical comment should mention at least one numeric value and
    # at least one team/player name from the Spark metrics.
    if not re.search(r"\d", text):
        return False

    compact = _compact_metrics_for_llm(metrics)
    names: list[str] = []
    for key in [
        "top_team_by_total_events",
        "top_team_by_recoveries",
        "top_team_by_xg",
        "top_team_by_pass_success",
        "top_player_by_participation",
        "top_player_by_contribution",
        "top_player_by_xg",
    ]:
        record = compact.get(key, {})
        if isinstance(record, dict):
            for field in ["team", "player"]:
                value = record.get(field)
                if isinstance(value, str) and value:
                    names.append(value.lower())

    return not names or any(name in lower for name in names)


# Llama a Ollama para añadir una interpretacion breve opcional.
# Ollama solo aporta una interpretacion breve; no sustituye las metricas reales.
# Solicita a Ollama una interpretacion breve y controlada del partido.
def _ollama_comment(metrics: dict[str, Any], contexts: list[dict[str, Any]]) -> str | None:
    """Ask Ollama for a short interpretation only.

    The final report is always built from the deterministic metric template, so
    Ollama cannot accidentally replace it with a generic RAG summary.
    """
    base_url = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434").strip().rstrip("/")
    model = os.getenv("OLLAMA_MODEL", "llama3.2:latest").strip()
    use_ollama = os.getenv("USE_OLLAMA", "1").strip().lower() not in {"0", "false", "no"}

    if not model or not use_ollama:
        return None

    try:
        num_ctx = int(os.getenv("OLLAMA_NUM_CTX", "2048"))
    except ValueError:
        num_ctx = 2048

    try:
        timeout = int(os.getenv("OLLAMA_TIMEOUT", "900"))
    except ValueError:
        timeout = 900

    try:
        num_predict = int(os.getenv("OLLAMA_NUM_PREDICT", "550"))
    except ValueError:
        num_predict = 550

    compact_metrics = _compact_metrics_for_llm(metrics)

    system_prompt = """
Eres analista de rendimiento en fútbol. Debes escribir una interpretación breve en español.
No eres un chatbot generalista: no digas "el contexto proporcionado", no propongas preguntas y no expliques qué es un informe.
Tu respuesta debe usar cifras concretas de las métricas Spark y, como apoyo secundario, el contexto RAG.
Si falta un dato, omítelo. No inventes nombres ni cifras.
""".strip()

    user_prompt = f"""
Escribe SOLO la sección titulada "## Interpretación generada con Ollama".

Estructura obligatoria:
- Un párrafo de lectura general del partido con 2-3 cifras.
- Un párrafo sobre equipos, mencionando xG, tiros, recuperaciones o acierto de pase si están disponibles.
- Un párrafo sobre jugadores destacados, mencionando participación, xG o contribución si están disponibles.
- Un párrafo final con una conclusión táctica.

No incluyas introducciones genéricas. No expliques qué es StatsBomb. No digas que no hay información suficiente si existen métricas.

Métricas Spark compactas:
{json.dumps(compact_metrics, ensure_ascii=False, indent=2)}

Contexto RAG resumido:
{_format_contexts(contexts, max_chars=1600)}
""".strip()

    # Peticion HTTP a Ollama: se limita contexto y tokens para evitar respuestas lentas o genericas.
    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "options": {
            "temperature": 0.1,
            "num_ctx": num_ctx,
            "num_predict": num_predict,
        },
        "keep_alive": "15m",
    }

    request = urllib.request.Request(
        url=f"{base_url}/api/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            data = json.loads(raw)
        content = data.get("message", {}).get("content", "").strip()
        if _content_is_useful(content, metrics):
            if not content.startswith("## Interpretación generada con Ollama"):
                content = "## Interpretación generada con Ollama\n\n" + content
            return content
        return None
    except urllib.error.HTTPError:
        return None
    except Exception:
        return None


# Punto unico de generacion: informe base + comentario opcional de Ollama.
# Funcion principal de generacion: plantilla estable mas comentario opcional de Ollama.
def generate_report(metrics: dict[str, Any], contexts: list[dict[str, Any]]) -> str:
    contexts = _filter_contexts_for_match(contexts, metrics)
    base_report = _template_report(metrics, contexts)
    ollama_section = _ollama_comment(metrics, contexts)

    if not ollama_section:
        return base_report + (
            "\n\n---\n\n"
            "**Nota técnica:** la sección generativa de Ollama no se añadió porque el modelo local no respondió a tiempo o produjo una respuesta demasiado genérica. "
            "El informe mantiene las métricas de Spark, el contexto RAG validado para el partido y la ejecución completa de LangGraph."
        )

    return base_report + "\n\n---\n\n" + ollama_section


# Escribe el informe Markdown en reports/.
# Guarda el informe Markdown en reports/.
def write_markdown_report(text: str, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


# Convierte el Markdown simple a HTML para entregar/visualizar.
# Convierte el Markdown a un HTML sencillo para entrega/visualizacion.
def write_html_report(markdown_text: str, output_path: str | Path) -> Path:
    escaped = html.escape(markdown_text)
    body = escaped.replace("\n", "<br>\n")
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    html_text = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <title>Informe automático StatsBomb</title>
  <style>
    body {{ font-family: Arial, sans-serif; max-width: 960px; margin: 40px auto; line-height: 1.5; }}
    .meta {{ color: #666; font-size: 0.9em; }}
  </style>
</head>
<body>
  <p class="meta">Generado en {now}</p>
  <div>{body}</div>
</body>
</html>
"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html_text, encoding="utf-8")
    return path

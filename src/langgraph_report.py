"""Orquestacion multiagente del informe con LangGraph.

Define los nodos metrics_agent, rag_agent, report_agent y persistence_agent
para combinar metricas Spark, contexto RAG y generacion del reporte final.
"""

from __future__ import annotations

import argparse
import json
import re
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

try:
    from src.config import REPORTS_DIR
    from src.report_generator import generate_report, write_html_report, write_markdown_report
    from src.tools.metrics_tool import consulta_metricas_partido
    from src.tools.rag_tool import recuperacion_documental
except ModuleNotFoundError:
    from config import REPORTS_DIR
    from report_generator import generate_report, write_html_report, write_markdown_report
    from tools.metrics_tool import consulta_metricas_partido
    from tools.rag_tool import recuperacion_documental


# Estado compartido entre los nodos de LangGraph.
# Estado compartido que circula por los nodos de LangGraph durante la generacion del informe.
class ReportState(TypedDict, total=False):
    match_id: str
    metrics_raw: str
    metrics_summary: dict[str, Any]
    rag_queries: list[str]
    contexts: list[dict[str, Any]]
    report_markdown: str
    markdown_path: str
    html_path: str


# Normaliza texto para comparar nombres de equipos y titulos sin problemas de acentos/mayusculas.
# Normaliza texto para comparar nombres de equipos y documentos de forma robusta.
def _normalize(text: Any) -> str:
    value = str(text or "").lower()
    value = re.sub(r"[^a-z0-9áéíóúüñç ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


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


# Comprueba frases completas con limites de palabra para evitar falsos positivos.
# Comprueba si una frase normalizada aparece dentro de un texto normalizado.
def _contains_phrase(text: str, phrase: str) -> bool:
    return bool(re.search(r"(?<![a-z0-9])" + re.escape(phrase) + r"(?![a-z0-9])", text))


# Genera alias conocidos de los equipos del partido para filtrar documentos RAG.
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


# Detecta que equipos aparecen mencionados en un documento recuperado.
# Detecta que equipos conocidos aparecen mencionados en un texto.
def _mentioned_team_aliases(text: str) -> set[str]:
    return {alias for alias in ALL_TEAM_ALIASES if _contains_phrase(text, alias)}


# Distingue documentos de equipo de documentos genericos sobre metricas o metodologia.
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



# Extrae el bloque de resumen a partir de la respuesta JSON de la tool de metricas.
# Convierte la respuesta JSON de la tool de metricas en un diccionario manejable.
def _summary_from_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    if metrics.get("answer_type") == "full_match_summary":
        data = metrics.get("data", {})
        return data if isinstance(data, dict) else {}
    data = metrics.get("data", metrics)
    return data if isinstance(data, dict) else {}


# Obtiene los equipos reales del partido desde las metricas calculadas por Spark.
# Obtiene los equipos reales del partido desde las metricas de Spark.
def _extract_teams(summary: dict[str, Any]) -> list[str]:
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


# Identifica documentos genericos que son validos para cualquier partido.
# Decide si un documento generico de metricas/metodologia puede acompanar al informe.
def _is_generic_statsbomb_context(text: str) -> bool:
    normalized = _normalize(text)
    generic_terms = [
        "statsbomb",
        "xg",
        "obv",
        "presiones",
        "recuperaciones",
        "structured streaming",
        "kafka",
        "spark",
        "informe",
        "métricas",
        "metricas",
    ]
    return any(term in normalized for term in generic_terms)


# Filtro principal: deja pasar equipos del partido y documentos genericos, bloquea otros equipos.
# Filtra contextos para evitar documentos de equipos que no juegan el partido.
def _context_matches_match(context: dict[str, Any], teams: list[str]) -> bool:
    """Mantiene solo contexto seguro para el partido actual.

    Reglas:
    - Los documentos de equipo se permiten solo si el equipo participa en el partido.
    - Los documentos de otros equipos se rechazan aunque mencionen StatsBomb.
    - Los documentos metodologicos genericos se permiten si no mencionan otro equipo.
    """
    haystack = " ".join(
        str(context.get(field, "")) for field in ["title", "source", "text", "doc_id"]
    )
    normalized_haystack = _normalize(haystack)
    allowed_aliases = _aliases_for_match_teams(teams)
    mentioned_aliases = _mentioned_team_aliases(normalized_haystack)

    if mentioned_aliases & allowed_aliases:
        return True

    if mentioned_aliases - allowed_aliases:
        return False

    if _is_explicit_team_document(context, normalized_haystack):
        return False

    generic_terms = [
        "statsbomb",
        "xg",
        "obv",
        "presiones",
        "recuperaciones",
        "structured streaming",
        "kafka",
        "spark",
        "informe",
        "métricas",
        "metricas",
        "calidad de datos",
        "glosario",
        "zonas",
        "intensidad",
    ]
    return any(term in normalized_haystack for term in generic_terms)


# Elimina contextos duplicados para que el informe no repita documentos.
# Elimina contextos repetidos antes de mostrarlos o pasarlos al generador.
def _dedupe_contexts(contexts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for item in contexts:
        key = str(item.get("doc_id") or item.get("source") or item.get("title") or item.get("text")[:80])
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


# Asigna prioridad a documentos de equipo segun el orden de equipos detectados.
# Devuelve la posicion del equipo del partido al que corresponde un contexto.
def _context_team_index(context: dict[str, Any], teams: list[str]) -> int | None:
    """Devuelve la posicion del equipo del partido si el contexto es de equipo."""
    title = _normalize(context.get("title", ""))
    source = _normalize(context.get("source", ""))
    doc_id = _normalize(context.get("doc_id", ""))
    text = _normalize(context.get("text", ""))
    haystack = f"{title} {source} {doc_id} {text}"

    for index, team in enumerate(teams):
        aliases = _aliases_for_match_teams([team])
        if title in aliases:
            return index
        if any(_contains_phrase(source, alias) or _contains_phrase(doc_id, alias) for alias in aliases):
            return index
        # La comprobacion sobre el texto es mas debil y solo se usa despues de titulo/ruta/doc_id,
        # para que un documento generico que mencione un equipo no supere a un documento de equipo.
        if any(_contains_phrase(text, alias) for alias in aliases) and _is_explicit_team_document(context, haystack):
            return index
    return None


# Prioriza documentos genericos mas utiles: metricas, interpretacion, intensidad, etc.
# Asigna prioridad a documentos genericos segun su utilidad para el informe.
def _generic_context_priority(context: dict[str, Any]) -> int:
    title = _normalize(context.get("title", ""))
    text = _normalize(context.get("text", ""))
    haystack = f"{title} {text}"
    if any(term in haystack for term in ["métricas statsbomb", "metricas statsbomb", "glosario", "eventos statsbomb"]):
        return 0
    if any(term in haystack for term in ["interpretar el informe", "lectura táctica", "lectura tactica"]):
        return 1
    if any(term in haystack for term in ["intensidad", "zonas", "progresión", "progresion"]):
        return 2
    if any(term in haystack for term in ["calidad de datos", "pipeline", "rag"]):
        return 3
    return 4


# Ordena el contexto RAG: primero equipos del partido, despues guias generales utiles.
# Ordena los contextos: equipos del partido primero y guias metodologicas despues.
def _prioritize_contexts(contexts: list[dict[str, Any]], teams: list[str]) -> list[dict[str, Any]]:
    """Coloca primero documentos de equipo y despues guias genericas utiles."""
    def sort_key(item: dict[str, Any]) -> tuple[int, int, float, str]:
        team_index = _context_team_index(item, teams)
        score = float(item.get("score", 0.0) or 0.0)
        title = str(item.get("title") or "")
        if team_index is not None:
            return (0, team_index, -score, title)
        return (1, _generic_context_priority(item), -score, title)

    return sorted(contexts, key=sort_key)


# Agente 1: consulta las metricas Parquet producidas por Spark.
# Nodos del grafo: cada agente recibe y devuelve el estado compartido.
# Agente que consulta la tool de metricas y guarda el resumen en el estado de LangGraph.
def metrics_agent(state: ReportState) -> ReportState:
    match_id = state.get("match_id", "")
    metrics_raw = consulta_metricas_partido.invoke(
        {"question": "resumen completo del partido", "match_id": match_id}
    )
    metrics_parsed = json.loads(metrics_raw)
    return {
        **state,
        "metrics_raw": metrics_raw,
        "metrics_summary": metrics_parsed,
    }


# Agente 2: formula consultas y recupera contexto documental filtrado por partido.
# Agente que recupera contexto documental filtrado y priorizado para el partido.
def rag_agent(state: ReportState) -> ReportState:
    metrics = state.get("metrics_summary", {})
    summary = _summary_from_metrics(metrics)

    teams = _extract_teams(summary)
    top_team = summary.get("top_team_by_total_events", {}).get("team", "")
    top_player = summary.get("top_player_by_participation", {}).get("player", "")
    top_recovery_team = summary.get("top_team_by_recoveries", {}).get("team", "")

    queries: list[str] = []

    # Las consultas de equipos deben ir primero. Si no, TF-IDF puede devolver varios documentos genericos
    # guide documents before the team documents, and section 6 of the report
    # ends up hiding the most useful context.
    for team in teams:
        queries.extend(
            [
                f"{team}",
                f"{team} contexto tactico analisis partido StatsBomb xG presiones recuperaciones",
            ]
        )

    if top_team:
        queries.append(f"estilo tactico {top_team} StatsBomb")
    if top_recovery_team and top_recovery_team != top_team:
        queries.append(f"recuperaciones presion contexto tactico {top_recovery_team}")
    if top_player:
        queries.append(f"perfil jugador {top_player} StatsBomb")

    queries.extend(
        [
            "metricas StatsBomb xG OBV presiones recuperaciones tiros pases",
            "intensidad temporal zonas progresion lectura tactica StatsBomb",
            "como interpretar informe partido datos StatsBomb Kafka Spark RAG",
        ]
    )

    contexts: list[dict[str, Any]] = []
    for query in queries:
        raw = recuperacion_documental.invoke({"query": query, "k": 5})
        try:
            retrieved = json.loads(raw)
        except json.JSONDecodeError:
            retrieved = []
        for item in retrieved:
            if _context_matches_match(item, teams):
                contexts.append(item)

    contexts = _prioritize_contexts(_dedupe_contexts(contexts), teams)

    return {
        **state,
        "rag_queries": queries,
        "contexts": contexts,
    }


# Agente 3: combina metricas y contexto para construir el texto del informe.
# Agente que combina metricas y contexto para generar el informe en Markdown.
def report_agent(state: ReportState) -> ReportState:
    report = generate_report(
        metrics=state.get("metrics_summary", {}),
        contexts=state.get("contexts", []),
    )
    return {
        **state,
        "report_markdown": report,
    }


# Agente 4: guarda el informe en Markdown y HTML.
# Agente que persiste el informe en Markdown y HTML dentro de reports/.
def persistence_agent(state: ReportState) -> ReportState:
    match_id = state.get("match_id") or "latest"
    md_path = REPORTS_DIR / f"report_{match_id}.md"
    html_path = REPORTS_DIR / f"report_{match_id}.html"
    write_markdown_report(state.get("report_markdown", ""), md_path)
    write_html_report(state.get("report_markdown", ""), html_path)
    return {
        **state,
        "markdown_path": str(md_path),
        "html_path": str(html_path),
    }


# Define el grafo de ejecucion secuencial de LangGraph.
# Construye el grafo secuencial de LangGraph con los agentes del proyecto.
def build_graph():
    graph = StateGraph(ReportState)
    graph.add_node("metrics_agent", metrics_agent)
    graph.add_node("rag_agent", rag_agent)
    graph.add_node("report_agent", report_agent)
    graph.add_node("persistence_agent", persistence_agent)

    graph.set_entry_point("metrics_agent")
    graph.add_edge("metrics_agent", "rag_agent")
    graph.add_edge("rag_agent", "report_agent")
    graph.add_edge("report_agent", "persistence_agent")
    graph.add_edge("persistence_agent", END)
    return graph.compile()


# Argumentos de ejecucion para generar informes por match_id.
# Define los argumentos de linea de comandos para ejecutar este modulo de forma flexible.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate final report with LangGraph")
    parser.add_argument("--match-id", default="MATCH_001", help="StatsBomb match id, normally the JSON filename without .json")
    return parser.parse_args()


# Entrada CLI: invoca el grafo completo para un partido.
# Punto de entrada del modulo: conecta las piezas y ejecuta el flujo principal.
def main() -> None:
    args = parse_args()
    graph = build_graph()
    final_state = graph.invoke({"match_id": args.match_id})
    print("LangGraph execution finished")
    print(f"Markdown report: {final_state.get('markdown_path')}")
    print(f"HTML report: {final_state.get('html_path')}")


if __name__ == "__main__":
    main()

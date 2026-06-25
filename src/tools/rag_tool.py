"""Tool de recuperacion documental para LangGraph.

Envuelve la funcion retrieve_context y expone el RAG como herramienta invocable
dentro del flujo multiagente.
"""

from __future__ import annotations

import json
from langchain_core.tools import tool

try:
    from src.rag_index import retrieve_context
except ModuleNotFoundError:
    from rag_index import retrieve_context


# Tool de LangChain/LangGraph: recupera documentos relevantes del indice RAG local.
@tool
# Tool que devuelve contexto RAG en formato JSON para el agente documental.
def recuperacion_documental(query: str, k: int = 4) -> str:
    """Recupera documentos contextuales de futbol usando el indice RAG local."""
    results = retrieve_context(query=query, k=k)
    return json.dumps(results, ensure_ascii=False, indent=2)

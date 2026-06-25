#!/usr/bin/env bash
set -euo pipefail

# Build the local TF-IDF RAG index and then run the LangGraph report.
# The report text is generated with Ollama when OLLAMA_MODEL is available.
docker compose run --rm app python -m src.rag_index --force

docker compose run --rm app python -m src.langgraph_report --match-id "${MATCH_ID:-MATCH_001}"

#!/usr/bin/env bash
set -euo pipefail

MODEL="${OLLAMA_MODEL:-llama3.2:3b}"
docker compose exec ollama ollama pull "$MODEL"
docker compose exec ollama ollama list

#!/usr/bin/env bash
set -euo pipefail

# Put real StatsBomb files in data/events/statsbomb/<match_id>.json.
# The producer attaches match_id from the filename before sending each event to Kafka.
docker compose run --rm app python -m src.producer \
  --bootstrap-servers kafka:9092 \
  --topic "${KAFKA_TOPIC:-statsbomb_events}" \
  --file "${EVENTS_PATH:-/opt/app/data/events/statsbomb}" \
  --generate-if-missing \
  --delay "${EVENT_DELAY:-0}"

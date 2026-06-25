#!/usr/bin/env bash
set -euo pipefail

docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
  /opt/app/src/consumer_spark.py \
  --bootstrap-servers kafka:9092 \
  --topic "${KAFKA_TOPIC:-statsbomb_events}" \
  --base-path /opt/app \
  --output-base /opt/app/output \
  --starting-offsets "${STARTING_OFFSETS:-earliest}"

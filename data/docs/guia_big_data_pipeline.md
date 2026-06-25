# Guía del pipeline Big Data

El flujo combina eventos JSON, Kafka, Spark Structured Streaming y almacenamiento Parquet. Kafka desacopla la ingesta del procesamiento; Spark consume el topic, aplana el esquema StatsBomb, calcula agregados y guarda resultados consultables.

Los checkpoints permiten a Spark mantener estado entre micro-batches. Los archivos Parquet reducen coste de lectura y facilitan que las tools consulten métricas por equipo, jugador e intervalo temporal.

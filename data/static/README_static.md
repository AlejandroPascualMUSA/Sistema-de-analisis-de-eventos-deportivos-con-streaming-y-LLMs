# Datos estáticos del proyecto

Estos CSV no son el stream principal. Funcionan como tablas de referencia para enriquecer eventos y métricas.

- `teams.csv`: contexto estructurado de equipos. Spark lo cruza por la columna `team`.
- `players.csv`: contexto estructurado de jugadores. Spark lo cruza por `player` y `team`.
- `metrics_catalog.csv`: glosario de métricas calculadas.
- `zones.csv`: interpretación de zonas derivadas de coordenadas StatsBomb.
- `event_type_catalog.csv`: interpretación de tipos de evento StatsBomb.

Para que `teams.csv` y `players.csv` afecten a los Parquet ya generados, hay que reejecutar Spark y el producer, porque el enriquecimiento se hace dentro de `consumer_spark.py`. Los documentos de `data/docs/` solo requieren regenerar el índice RAG.

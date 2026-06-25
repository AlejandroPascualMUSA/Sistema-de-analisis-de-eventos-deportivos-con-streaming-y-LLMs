# Proyecto final Big Data Processing II

## Análisis de eventos StatsBomb con Kafka, Spark Structured Streaming, RAG, LangGraph y Ollama

Este proyecto implementa un pipeline completo para procesar eventos de fútbol en formato StatsBomb. Los eventos se leen desde ficheros JSON, se publican en Kafka, se consumen con Spark Structured Streaming, se agregan en métricas deportivas y se utilizan para generar informes automáticos mediante tools, RAG, LangGraph y Ollama local.

El flujo general es:

```text
StatsBomb JSON
-> Producer Python
-> Kafka topic statsbomb_events
-> Spark Structured Streaming
-> Parquet en output/
-> Tools de métricas
-> RAG con documentos locales
-> LangGraph
-> Informe Markdown/HTML
-> Ollama local opcional
```

## Estructura del proyecto

```text
laliga_statsbomb/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── data/
│   ├── events/
│   │   └── statsbomb/
│   │       ├── 3946395_events.json
│   │       └── 3946396_events.json
│   ├── static/
│   │   ├── teams.csv
│   │   ├── players.csv
│   │   ├── metrics_catalog.csv
│   │   ├── zones.csv
│   │   └── event_type_catalog.csv
│   ├── docs/
│   │   └── documentos usados por el RAG
│   └── index/
├── src/
│   ├── lecture_data.py
│   ├── producer.py
│   ├── consumer_print.py
│   ├── consumer_spark.py
│   ├── rag_index.py
│   ├── langgraph_report.py
│   ├── report_generator.py
│   └── tools/
│       ├── metrics_tool.py
│       └── rag_tool.py
├── output/
│   ├── processed/
│   ├── aggregates/
│   └── checkpoints/
└── reports/
```

## Servicios Docker

El proyecto se ejecuta con Docker Compose. Los servicios principales son:

```text
laliga-kafka          broker Kafka en modo KRaft
laliga-spark-master   master de Spark
laliga-spark-worker   worker de Spark
laliga-app            contenedor Python del proyecto
laliga-ollama         servidor local de Ollama
```

No es necesario instalar Kafka, Spark ni Java localmente. Esos componentes se ejecutan dentro de contenedores.

## Nota importante sobre Ollama

Ollama guarda los modelos descargados en un volumen Docker. Por eso no se debe usar:

```powershell
docker compose down -v --remove-orphans
```

La opción `-v` borra los volúmenes y elimina el modelo descargado. Para parar los contenedores se debe usar:

```powershell
docker compose down --remove-orphans
```

Para limpiar una ejecución anterior se eliminan manualmente las carpetas `output/`, `reports/` y `data/index/`.

# Ejecución completa desde cero

## 1. Entrar en la carpeta del proyecto

Usar la ruta real del proyecto:

```powershell
cd "C:\Users\..."
```

## 2. Parar contenedores sin borrar volúmenes

```powershell
docker compose down --remove-orphans
```

No usar `down -v` si se quiere conservar Ollama.

## 3. Limpiar ejecuciones anteriores

```powershell
Remove-Item -Recurse -Force .\output\checkpoints -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\output\processed -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\output\aggregates -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\reports -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\data\index -ErrorAction SilentlyContinue
```

## 4. Levantar Docker

```powershell
docker compose up -d --build --force-recreate
```

Comprobar servicios:

```powershell
docker compose ps
```

Deben aparecer:

```text
laliga-app
laliga-kafka
laliga-spark-master
laliga-spark-worker
laliga-ollama
```

## 5. Preparar Ollama

Comprobar si el modelo ya está descargado:

```powershell
docker compose exec ollama ollama list
```

Si no aparece `llama3.2:latest`, descargarlo:

```powershell
docker compose exec ollama ollama pull llama3.2
```

Volver a comprobar:

```powershell
docker compose exec ollama ollama list
```

Debe aparecer algo parecido a:

```text
llama3.2:latest
```

## 6. Crear el topic Kafka

Para evitar datos duplicados, se puede borrar el topic si existe:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --delete `
  --if-exists `
  --topic statsbomb_events
```

Después se crea el topic:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --create `
  --if-not-exists `
  --topic statsbomb_events `
  --partitions 1 `
  --replication-factor 1
```

Comprobar que existe:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --list
```

Debe aparecer:

```text
statsbomb_events
```

## 7. Comprobar partidos disponibles

```powershell
Get-ChildItem .\data\events\statsbomb\*.json | Select-Object BaseName
```

En este proyecto los identificadores de partido son el nombre del fichero sin `.json`. Por ejemplo:

```text
3946395_events
3946396_events
```

# Procesamiento en streaming

## Terminal 1: lanzar Spark Structured Streaming

Abrir una PowerShell nueva y ejecutar:

```powershell
cd "C:\Users\..."
```

Después:

```powershell
docker compose exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 `
  /opt/app/src/consumer_spark.py `
  --bootstrap-servers kafka:9092 `
  --topic statsbomb_events `
  --base-path /opt/app `
  --output-base /opt/app/output `
  --starting-offsets earliest
```

Esta terminal queda abierta porque Spark está escuchando Kafka.

## Terminal 2: lanzar el producer

Abrir otra PowerShell y ejecutar:

```powershell
cd "C:\Users\..."
```

Después:

```powershell
docker compose run --rm app python -m src.producer `
  --bootstrap-servers kafka:9092 `
  --topic statsbomb_events `
  --file /opt/app/data/events/statsbomb `
  --generate-if-missing `
  --delay 0
```

El producer lee los JSON de `data/events/statsbomb/` y envía los eventos a Kafka.

## 8. Comprobar métricas generadas

Cuando el producer termine, comprobar las salidas:

```powershell
Get-ChildItem .\output\aggregates\ -Recurse
```

Deben aparecer archivos Parquet en:

```text
output/aggregates/team_metrics
output/aggregates/player_metrics
output/aggregates/intensity
```

Cuando los resultados estén escritos, parar Spark con `Ctrl + C` en la terminal donde se lanzó `spark-submit`.

Si Spark no se detiene con `Ctrl + C`, usar desde otra terminal:

```powershell
docker stop laliga-spark-master laliga-spark-worker
```

# RAG e informes

## 9. Regenerar el índice RAG

```powershell
Remove-Item -Recurse -Force .\data\index -ErrorAction SilentlyContinue
```

```powershell
docker compose run --rm app python -m src.rag_index --force
```

El RAG utiliza los documentos de `data/docs/`. El sistema prioriza los documentos de los equipos reales del partido y documentos genéricos útiles sobre métricas, zonas, intensidad, calidad de datos y pipeline.

## 10. Generar informes

### Opción recomendada para entrega: informe estable

Esta opción evita que Ollama genere texto irrelevante y usa un informe controlado con métricas Spark y contexto RAG.

```powershell
docker compose run --rm -e USE_OLLAMA=0 app python -m src.langgraph_report --match-id 3946395_events
```

```powershell
docker compose run --rm -e USE_OLLAMA=0 app python -m src.langgraph_report --match-id 3946396_events
```

### Opción con Ollama activado

Usar esta opción para incluir generación textual local con Ollama:

```powershell
docker compose run --rm `
  -e OLLAMA_MODEL=llama3.2:latest `
  -e OLLAMA_TIMEOUT=900 `
  -e OLLAMA_NUM_CTX=2048 `
  app python -m src.langgraph_report --match-id 3946395_events
```

```powershell
docker compose run --rm `
  -e OLLAMA_MODEL=llama3.2:latest `
  -e OLLAMA_TIMEOUT=900 `
  -e OLLAMA_NUM_CTX=2048 `
  app python -m src.langgraph_report --match-id 3946396_events
```

## 11. Abrir informes

```powershell
Invoke-Item .\reports\report_3946395_events.html
```

```powershell
Invoke-Item .\reports\report_3946396_events.html
```

También se puede listar la carpeta:

```powershell
Get-ChildItem .\reports\
```

# Secuencia corta si ya existen los Parquet

Si ya existen estas carpetas:

```text
output/aggregates/team_metrics
output/aggregates/player_metrics
output/aggregates/intensity
```

no hace falta repetir Kafka, Spark ni producer. Basta con regenerar el índice RAG y los informes:

```powershell
cd "C:\Users\..."

docker compose up -d app ollama kafka

docker compose exec ollama ollama list

Remove-Item -Recurse -Force .\data\index -ErrorAction SilentlyContinue

docker compose run --rm app python -m src.rag_index --force

docker compose run --rm -e USE_OLLAMA=0 app python -m src.langgraph_report --match-id 3946395_events

docker compose run --rm -e USE_OLLAMA=0 app python -m src.langgraph_report --match-id 3946396_events

Invoke-Item .\reports\report_3946395_events.html

Invoke-Item .\reports\report_3946396_events.html
```

# Validaciones útiles

## Comprobar los equipos de un partido

```powershell
docker compose run --rm app python -c "import json; from src.tools.metrics_tool import consulta_metricas_partido; d=json.loads(consulta_metricas_partido.invoke({'question':'resumen completo del partido','match_id':'3946395_events'})); print([r['team'] for r in d['data']['team_metrics']])"
```

## Comprobar qué documentos recupera el RAG

```powershell
docker compose run --rm app python -c "from src.langgraph_report import metrics_agent, rag_agent; s=metrics_agent({'match_id':'3946395_events'}); s=rag_agent(s); print([c.get('title') for c in s['contexts'][:8]])"
```

## Comprobar los nodos de LangGraph

```powershell
@'
from src.langgraph_report import build_graph

graph = build_graph()

for step in graph.stream({"match_id": "3946395_events"}):
    print(step.keys())
'@ | docker compose run --rm -T app python -
```

Deben aparecer nodos como:

```text
metrics_agent
rag_agent
report_agent
persistence_agent
```

# Limpieza final

Para parar contenedores sin borrar modelos:

```powershell
docker compose down --remove-orphans
```

No usar:

```powershell
docker compose down -v --remove-orphans
```

porque borra los volúmenes de Docker y puede eliminar el modelo de Ollama.

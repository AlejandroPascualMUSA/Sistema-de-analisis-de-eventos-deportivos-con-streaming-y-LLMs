# Memoria técnica

## Proyecto final Big Data Processing II

## 1. Objetivo del proyecto

El objetivo del proyecto es construir un sistema de análisis de eventos deportivos en tiempo casi real utilizando tecnologías de Big Data. El caso de uso elegido es el análisis de eventos de fútbol en formato StatsBomb.

El sistema procesa eventos de partido desde ficheros JSON, los publica en Kafka, los consume con Spark Structured Streaming, calcula métricas deportivas y genera informes automáticos mediante una arquitectura de tools, RAG, LangGraph y Ollama local.

El flujo completo es:

```text
StatsBomb JSON
-> Kafka producer
-> Kafka topic statsbomb_events
-> Spark Structured Streaming
-> limpieza y agregación
-> Parquet
-> tools de métricas
-> RAG documental
-> LangGraph
-> informe Markdown/HTML
```

## 2. Arquitectura general

La arquitectura está compuesta por los siguientes módulos:

```text
1. Ingesta de eventos
2. Broker de mensajería Kafka
3. Procesamiento streaming con Spark
4. Persistencia en Parquet
5. Datos estáticos y documentos RAG
6. Tools de consulta
7. Orquestación multiagente con LangGraph
8. Generación de informe con plantilla estable y Ollama opcional
```

Los servicios se ejecutan mediante Docker Compose:

```text
laliga-kafka          broker Kafka en modo KRaft
laliga-spark-master   nodo master de Spark
laliga-spark-worker   nodo worker de Spark
laliga-app            contenedor Python para producer, tools, RAG y LangGraph
laliga-ollama         servidor local de Ollama
```

La decisión de usar Docker evita instalar Java, Kafka o Spark directamente en la máquina local.

## 3. Datos utilizados

El proyecto utiliza eventos StatsBomb almacenados en:

```text
data/events/statsbomb/
```

Cada fichero JSON representa un partido. El nombre del fichero sin extensión se utiliza como `match_id`. Por ejemplo:

```text
3946395_events.json -> match_id = 3946395_events
3946396_events.json -> match_id = 3946396_events
```

Los eventos StatsBomb tienen una estructura anidada. Por ejemplo:

```json
{
  "id": "...",
  "minute": 12,
  "type": {"name": "Pass"},
  "team": {"name": "Barcelona"},
  "player": {"name": "..."},
  "location": [34.2, 52.1]
}
```

El pipeline convierte esta estructura en columnas analíticas:

```text
id                  -> event_id
nombre del fichero  -> match_id
type.name           -> event_type
team.name           -> team
player.name         -> player
minute              -> minute
second              -> second
location[0]         -> x
location[1]         -> y
shot.statsbomb_xg   -> shot_xg
obv_total_net       -> obv_total_net
```

## 4. Ingesta con Kafka

El productor Python lee los eventos StatsBomb y los envía a Kafka. El topic utilizado es:

```text
statsbomb_events
```

Kafka desacopla la lectura de los ficheros del procesamiento posterior. Esto permite simular un flujo de eventos como si fueran llegando en tiempo real.

El productor se lanza con:

```powershell
docker compose run --rm app python -m src.producer `
  --bootstrap-servers kafka:9092 `
  --topic statsbomb_events `
  --file /opt/app/data/events/statsbomb `
  --generate-if-missing `
  --delay 0
```

## 5. Procesamiento con Spark Structured Streaming

Spark Structured Streaming consume los eventos desde Kafka, aplica un esquema explícito, aplana el JSON de StatsBomb y calcula métricas agregadas.

El consumidor principal es:

```text
src/consumer_spark.py
```

Este módulo realiza:

```text
lectura del topic Kafka
conversión de JSON a columnas
limpieza de nulos y tipos
extracción de campos anidados
normalización de eventos
cálculo de métricas
persistencia en Parquet
```

Spark se lanza desde el contenedor `spark-master`:

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

El proceso de Spark no termina solo porque es streaming. Se detiene manualmente cuando el producer ha terminado y los Parquet ya se han escrito.

## 6. Métricas calculadas

El sistema calcula métricas por equipo, por jugador e intensidad temporal.

### Métricas por equipo

```text
total de eventos
pases
pases completados
porcentaje de acierto en pase
tiros
goles
xG
faltas cometidas
faltas recibidas
recuperaciones
presiones
intercepciones
despejes
conducciones
regates
entradas al último tercio
OBV neto
```

### Métricas por jugador

```text
participación total
eventos ofensivos
eventos defensivos
pases
tiros
xG
OBV
recuperaciones
presiones
```

### Métricas de intensidad

Se agrupan los eventos por intervalos temporales para detectar fases de mayor actividad.

Ejemplo:

```text
match_id
team
minute_interval
events_in_interval
interval_value
```

## 7. Persistencia

Los resultados se guardan en formato Parquet:

```text
output/processed/events
output/processed/lineups
output/aggregates/team_metrics
output/aggregates/player_metrics
output/aggregates/intensity
output/checkpoints
```

El uso de Parquet permite almacenar datos en formato columnar, eficiente para consultas posteriores desde las tools de métricas.

## 8. Datos estáticos

El proyecto incluye datos estáticos en:

```text
data/static/
```

Estos datos actúan como tablas de referencia. Incluyen información de equipos, jugadores, métricas, zonas del campo y tipos de evento.

La diferencia entre eventos y datos estáticos es:

```text
data/events/statsbomb/ -> datos dinámicos del partido
data/static/           -> datos de referencia o enriquecimiento
data/docs/             -> documentos textuales para RAG
```

Los datos estáticos complementan las métricas, pero no sustituyen al stream principal.

## 9. RAG documental

El módulo RAG utiliza documentos locales almacenados en:

```text
data/docs/
```

Los documentos incluyen:

```text
contexto de equipos
métricas StatsBomb
zonas del campo
intensidad temporal
calidad de datos
lectura táctica
pipeline Big Data
glosario de eventos
uso seguro del RAG
```

Durante las pruebas se observó que un RAG puramente semántico podía recuperar documentos de equipos que no participaban en el partido. Para corregirlo, el agente RAG aplica reglas de filtrado y prioridad:

```text
1. Priorizar documentos de los equipos reales del partido.
2. Permitir documentos genéricos útiles.
3. Excluir documentos de equipos que no participan en el partido.
4. Mostrar primero equipos y después guías metodológicas.
```

Así, si el partido es Barcelona-Valencia, el RAG prioriza Barcelona y Valencia. Si el partido es Girona-Real Betis, prioriza Girona y Real Betis.

El RAG se usa como apoyo interpretativo. Las conclusiones principales se basan en las métricas calculadas por Spark.

## 10. Tools

El proyecto incorpora tools reutilizables:

```text
src/tools/metrics_tool.py
src/tools/rag_tool.py
```

La tool de métricas consulta los Parquet generados por Spark. Permite obtener:

```text
resumen del partido
métricas por equipo
ranking de jugadores
tramo de mayor intensidad
alineaciones
```

La tool RAG recupera documentos relevantes del índice construido a partir de `data/docs/`.

## 11. LangGraph

La orquestación se realiza con LangGraph. El grafo está formado por nodos especializados:

```text
metrics_agent
rag_agent
report_agent
persistence_agent
```

Funciones de cada agente:

```text
metrics_agent       consulta las métricas de Spark
rag_agent           recupera contexto documental relevante
report_agent        genera el informe con métricas y contexto
persistence_agent   guarda el informe en Markdown y HTML
```

Esta separación evita que el modelo generativo invente datos. Las métricas proceden de Spark, el contexto procede del RAG y el informe se genera de forma controlada.

## 12. Ollama como modelo generativo local

Para evitar costes de API externa, se utiliza Ollama como modelo generativo local dentro de Docker Compose.

El servicio es:

```text
laliga-ollama
```

El modelo utilizado es:

```text
llama3.2:latest
```

Ollama se puede descargar con:

```powershell
docker compose exec ollama ollama pull llama3.2
```

La generación local puede ser más lenta que una API externa porque depende del hardware local, Docker Desktop, CPU, RAM y posible disponibilidad de GPU.

Por robustez, el sistema permite dos modos:

```text
USE_OLLAMA=0      informe estable con plantilla, métricas y RAG
Ollama activado   informe con sección generativa local
```

La opción estable se recomienda para la entrega cuando se quiere evitar que el modelo local genere texto genérico o irrelevante.

## 13. Ejecución final

La ejecución final se realiza desde PowerShell.

### 13.1 Entrar en el proyecto

```powershell
cd "C:\Users\..."
```

### 13.2 Parar sin borrar volúmenes

```powershell
docker compose down --remove-orphans
```

No se debe usar:

```powershell
docker compose down -v --remove-orphans
```

porque borra el volumen donde Ollama guarda los modelos.

### 13.3 Limpiar resultados

```powershell
Remove-Item -Recurse -Force .\output\checkpoints -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\output\processed -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\output\aggregates -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\reports -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force .\data\index -ErrorAction SilentlyContinue
```

### 13.4 Levantar servicios

```powershell
docker compose up -d --build --force-recreate
```

### 13.5 Preparar Ollama

```powershell
docker compose exec ollama ollama list
```

Si no aparece el modelo:

```powershell
docker compose exec ollama ollama pull llama3.2
```

### 13.6 Crear topic Kafka

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --delete `
  --if-exists `
  --topic statsbomb_events
```

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh `
  --bootstrap-server kafka:9092 `
  --create `
  --if-not-exists `
  --topic statsbomb_events `
  --partitions 1 `
  --replication-factor 1
```

### 13.7 Lanzar Spark

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

### 13.8 Lanzar producer

```powershell
docker compose run --rm app python -m src.producer `
  --bootstrap-servers kafka:9092 `
  --topic statsbomb_events `
  --file /opt/app/data/events/statsbomb `
  --generate-if-missing `
  --delay 0
```

### 13.9 Comprobar métricas

```powershell
Get-ChildItem .\output\aggregates\ -Recurse
```

### 13.10 Regenerar RAG

```powershell
Remove-Item -Recurse -Force .\data\index -ErrorAction SilentlyContinue
```

```powershell
docker compose run --rm app python -m src.rag_index --force
```

### 13.11 Generar informes

Modo estable recomendado:

```powershell
docker compose run --rm -e USE_OLLAMA=0 app python -m src.langgraph_report --match-id 3946395_events
```

```powershell
docker compose run --rm -e USE_OLLAMA=0 app python -m src.langgraph_report --match-id 3946396_events
```

Modo con Ollama:

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

## 14. Validación del sistema

La validación se realiza comprobando:

```text
1. Servicios Docker levantados.
2. Topic Kafka creado.
3. Producer enviando eventos.
4. Spark generando Parquet.
5. Tools leyendo métricas.
6. RAG recuperando documentos adecuados.
7. LangGraph ejecutando los agentes.
8. Informes generados en Markdown y HTML.
```

Comprobación de nodos LangGraph:

```powershell
@'
from src.langgraph_report import build_graph

graph = build_graph()

for step in graph.stream({"match_id": "3946395_events"}):
    print(step.keys())
'@ | docker compose run --rm -T app python -
```

## 15. Limitaciones

La principal limitación es el rendimiento de Ollama en local. Al ejecutarse dentro de Docker y depender del hardware disponible, la generación puede ser lenta. Por eso se mantiene un modo estable sin Ollama para garantizar que el informe sea coherente.

Otra limitación es que la calidad del RAG depende de los documentos disponibles. Para mitigarlo, se añadieron documentos específicos de equipos y guías metodológicas, y se implementó un filtrado para evitar contexto de equipos que no participan en el partido.

## 16. Conclusión

El proyecto cumple el objetivo de construir un pipeline Big Data aplicado al análisis de eventos StatsBomb. Kafka gestiona la ingesta, Spark Structured Streaming procesa y agrega los eventos, Parquet conserva los resultados, las tools permiten consultarlos y LangGraph orquesta la generación de informes con apoyo de RAG y Ollama local.

La solución final evita dependencias de APIs de pago, mantiene trazabilidad entre datos y conclusiones, y genera informes reproducibles en Markdown y HTML.

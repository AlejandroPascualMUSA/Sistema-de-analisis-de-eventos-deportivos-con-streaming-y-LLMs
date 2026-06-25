# Informe automático de partido con eventos StatsBomb

## 1. Resumen cuantitativo del stream

El pipeline ha procesado **2660 eventos agregados por equipo**. En esos eventos aparecen **741 pases**, **16 tiros**, **2 goles**, **30 faltas cometidas**, **70 recuperaciones** y **286 presiones**. El xG total agregado es **1.015** y el OBV neto total es **2.422**.

El equipo con mayor volumen de eventos fue **Athletic Club**, con **1629 eventos**.
El equipo con mayor producción de xG fue **Getafe**, con **xG=0.766**.
El equipo con más recuperaciones fue **Athletic Club**, con **37 recuperaciones**.
La mejor tasa de acierto en el pase fue de **Athletic Club**, con **77.29%**.
El jugador con mayor participación fue **Aitor Paredes Casamichana** (Athletic Club), con **171 eventos**.
El mayor valor acumulado de contribución lo registró **Alejandro Padilla Pérez**, con **1.009**.
El jugador con más xG fue **Carles Aleña Castillo**, con **xG=0.454**.
El tramo de mayor intensidad fue el intervalo **40-45** de **Athletic Club**, con **157 eventos**.

## 2. Métricas por equipo

- **Athletic Club**: 1629 eventos, 502 pases, 77.29% acierto, 7 tiros, 1 goles, xG=0.249, 37 recuperaciones, 112 presiones, OBV=1.002.
- **Getafe**: 1031 eventos, 239 pases, 59.41% acierto, 9 tiros, 1 goles, xG=0.766, 33 recuperaciones, 174 presiones, OBV=1.42.

## 3. Top jugadores

- **Aitor Paredes Casamichana** (Athletic Club, Left Center Back): 171 eventos, 54 pases, 0 tiros, 0 goles, xG=0.0, contribución=0.26.
- **Yuri Berchiche Izeta** (Athletic Club, Left Back): 155 eventos, 66 pases, 1 tiros, 0 goles, xG=0.023, contribución=0.294.
- **Yeray Álvarez López** (Athletic Club, Right Center Back): 151 eventos, 50 pases, 0 tiros, 0 goles, xG=0.0, contribución=0.27.
- **Chrisantus Ugonna Uche** (Getafe, Center Forward): 136 eventos, 11 pases, 1 tiros, 1 goles, xG=0.077, contribución=0.247.
- **Mikel Vesga Arruti** (Athletic Club, Left Defensive Midfield): 114 eventos, 39 pases, 0 tiros, 0 goles, xG=0.0, contribución=0.673.
- **Alejandro Padilla Pérez** (Athletic Club, Goalkeeper): 112 eventos, 44 pases, 0 tiros, 0 goles, xG=0.0, contribución=1.009.
- **Iñaki Williams Arthuer** (Athletic Club, Right Wing): 112 eventos, 27 pases, 2 tiros, 0 goles, xG=0.082, contribución=0.449.
- **Alejandro Berenguer Remiro** (Athletic Club, Left Wing): 107 eventos, 23 pases, 0 tiros, 0 goles, xG=0.0, contribución=0.169.
- **Beñat Prados Díaz** (Athletic Club, Right Defensive Midfield): 106 eventos, 30 pases, 0 tiros, 0 goles, xG=0.0, contribución=0.233.
- **Gorka Guruzeta Rodríguez** (Athletic Club, Center Forward): 101 eventos, 20 pases, 1 tiros, 0 goles, xG=0.059, contribución=-0.239.

## 4. Tramos de intensidad

- **Athletic Club**, minuto 40-45: 157 eventos, 53 pases, 0 tiros, 3 recuperaciones, 5 presiones, xG=0.0.
- **Athletic Club**, minuto 65-70: 155 eventos, 52 pases, 0 tiros, 1 recuperaciones, 2 presiones, xG=0.0.
- **Getafe**, minuto 45-50: 154 eventos, 39 pases, 1 tiros, 5 recuperaciones, 17 presiones, xG=0.082.
- **Athletic Club**, minuto 30-35: 151 eventos, 49 pases, 0 tiros, 3 recuperaciones, 5 presiones, xG=0.0.
- **Athletic Club**, minuto 90-95: 140 eventos, 45 pases, 1 tiros, 5 recuperaciones, 0 presiones, xG=0.023.

## 5. Alineaciones detectadas en eventos Starting XI

- Athletic Club: Andoni Gorosabel Espinosa - Right Back #2
- Athletic Club: Aitor Paredes Casamichana - Left Center Back #4
- Athletic Club: Yeray Álvarez López - Right Center Back #5
- Athletic Club: Mikel Vesga Arruti - Left Defensive Midfield #6
- Athletic Club: Alejandro Berenguer Remiro - Left Wing #7
- Athletic Club: Oihan Sancet Tirapu - Center Attacking Midfield #8
- Athletic Club: Iñaki Williams Arthuer - Right Wing #9
- Athletic Club: Gorka Guruzeta Rodríguez - Center Forward #12
- Athletic Club: Yuri Berchiche Izeta - Left Back #17
- Athletic Club: Beñat Prados Díaz - Right Defensive Midfield #24
- Athletic Club: Alejandro Padilla Pérez - Goalkeeper #26
- Getafe: Djené Dakonam Ortega - Right Center Back #2
- Getafe: Luis Milla Manzanares - Right Center Midfield #5
- Getafe: Chrisantus Ugonna Uche - Center Forward #6
- Getafe: Álex Sola López Ocaña - Left Midfield #7
- Getafe: Mauro Wilney Arambarri Rosa - Left Center Midfield #8
- Getafe: David Soria Solís - Goalkeeper #13
- Getafe: Omar Federico Alderete Fernández - Left Center Back #15
- Getafe: Diego Rico Salguero - Left Back #16
- Getafe: Carles Pérez Sayol - Right Midfield #17
- Getafe: Juan Antonio Iglesias Sánchez - Right Back #21
- Getafe: Nabil Aberdin - Center Defensive Midfield #27

## 6. Contexto recuperado por RAG

El módulo RAG recuperó estos documentos: Athletic Club, Getafe, Guía de métricas StatsBomb, Cómo interpretar el informe, Guía de lectura táctica, Guía de intensidad temporal, Guía de zonas y progresión, Guía de calidad de datos, Guía de uso seguro del RAG, Guía del pipeline Big Data.
- Contexto 1 - Athletic Club: Athletic Club se interpreta como un equipo asociado a ritmo alto, intensidad competitiva, uso de bandas y presencia física en duelos. En StatsBomb conviene observar presiones, recuperaciones, entradas al último tercio, centros, tiros y xG para separar agresividad territorial de generación real de ocasiones.
- Contexto 2 - Getafe: Si Getafe acumula muchas faltas o despejes, el informe debe distinguir entre resistencia defensiva y falta de control. Si genera xG con pocos tiros, puede indicar ocasiones aisladas pero de buena calidad, normalmente asociadas a transiciones o balón parado.
- Contexto 3 - Guía de métricas StatsBomb: Las métricas StatsBomb permiten separar volumen de actividad y calidad de ocasión. Los pases y eventos totales describen participación, mientras que tiros, xG y OBV ayudan a valorar amenaza ofensiva y valor añadido de las acciones.
- Contexto 4 - Cómo interpretar el informe: El informe combina evidencias cuantitativas y contexto textual. Las cifras proceden de eventos StatsBomb publicados en Kafka y procesados con Spark Structured Streaming. El contexto procede de documentos recuperados con RAG.
- Contexto 5 - Guía de lectura táctica: La lectura táctica debe cruzar varios indicadores. Posesión y pases describen control, presiones y recuperaciones describen comportamiento sin balón, tiros y xG describen amenaza, y OBV aproxima valor añadido.

## 7. Interpretación base

El alto volumen de eventos de **Athletic Club** sugiere mayor presencia en el flujo de acciones registradas.
El xG de **Getafe** ayuda a separar la cantidad de tiros de la calidad de las ocasiones.
Las recuperaciones de **Athletic Club** indican capacidad para volver a intervenir tras pérdida o disputa.
El intervalo **40-45** concentra el pico de actividad y puede revisarse en vídeo para explicar cambios de ritmo, presión o ataques consecutivos.

## 8. Separación de fuentes

- Las cifras proceden de eventos StatsBomb consumidos desde Kafka y procesados con Spark Structured Streaming.
- El contexto textual procede de los documentos recuperados por el módulo RAG.
- LangGraph coordina la consulta de métricas, la recuperación documental, la generación y la escritura del informe.

## 9. Conclusión técnica

El sistema completa un flujo Big Data de extremo a extremo: los eventos StatsBomb se publican en Kafka, Spark los transforma y agrega en streaming, las métricas quedan persistidas en Parquet y el grafo LangGraph genera un informe enriquecido con RAG y Ollama local.

---

## Interpretación generada con Ollama

Análisis del contexto RAG resumido:

El contexto proporciona una guía para interpretar los datos de StatsBomb en relación con el rendimiento de dos equipos, Athletic Club y Getafe. La guía se divide en cinco puntos que ofrecen orientación sobre cómo analizar las métricas de cada equipo.

**Puntos clave:**

1. **Athletic Club**: El equipo se caracteriza por un ritmo alto y una intensidad competitiva, con un uso significativo de bandas y presencia física en duelos. Es importante observar presiones, recuperaciones, entradas al último tercio, centros, tiros y xG para entender su juego.
2. **Athletic Club**: Un alto número de recuperaciones o presiones puede indicar fases de presión alta o capacidad para disputar segundas jugadas. La lectura debe compararse con tiros, xG y localización de eventos para evitar concluir dominio solo por intensidad.
3. **Getafe**: El equipo se caracteriza como competitivo y fuerte en duelos, capaz de alternar defensa compacta con ataques directos. Las métricas útiles son faltas, duelos, recuperaciones, pases largos, despejes, tiros y xG.
4. **Getafe**: Si Getafe acumula muchas faltas o despejes, es importante distinguir entre resistencia defensiva y falta de control. Si genera xG con pocos tiros, puede indicar ocasiones aisladas pero de buena calidad, normalmente asociadas a transiciones o balón parado.
5. **Guía de métricas StatsBomb**: Las métricas StatsBomb permiten separar volumen de actividad y calidad de ocasión. Los pases y eventos totales describen participación, mientras que tiros, xG y OBV ayudan a valorar amenaza ofensiva y valor añadido de las acciones.

**Conclusión:**

El contexto RAG resumido ofrece una guía clara para interpretar los datos de StatsBomb en relación con el rendimiento de dos equipos. Al entender las características de cada equipo y cómo analizar las métricas, se puede obtener una visión más completa del juego y tomar decisiones informadas sobre la estrategia y el análisis de los datos.
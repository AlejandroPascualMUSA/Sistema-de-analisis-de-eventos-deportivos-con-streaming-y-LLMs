"""Consumidor principal con Spark Structured Streaming.

Consume eventos StatsBomb desde Kafka, aplana el esquema anidado, limpia datos,
enriquece con tablas estaticas y escribe metricas Parquet para el informe.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    coalesce,
    col,
    count,
    element_at,
    explode,
    floor,
    from_json,
    lit,
    lower,
    round as spark_round,
    sum as spark_sum,
    trim,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# Schema reutilizable para objetos StatsBomb con forma {id, name}.
# Esquema reutilizable para objetos StatsBomb con id y name.
NAMED_OBJECT_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]
)

LINEUP_PLAYER_SCHEMA = StructType(
    [
        StructField("player", NAMED_OBJECT_SCHEMA, True),
        StructField("position", NAMED_OBJECT_SCHEMA, True),
        StructField("jersey_number", IntegerType(), True),
    ]
)

# Schema principal del evento StatsBomb. Se declaran campos anidados para evitar inferencia fragil.
# Esquema explicito del evento StatsBomb anidado que llega desde Kafka.
EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("match_id", StringType(), True),
        StructField("index", IntegerType(), True),
        StructField("period", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("minute", IntegerType(), True),
        StructField("second", IntegerType(), True),
        StructField("type", NAMED_OBJECT_SCHEMA, True),
        StructField("possession", IntegerType(), True),
        StructField("possession_team", NAMED_OBJECT_SCHEMA, True),
        StructField("play_pattern", NAMED_OBJECT_SCHEMA, True),
        StructField("team", NAMED_OBJECT_SCHEMA, True),
        StructField("player", NAMED_OBJECT_SCHEMA, True),
        StructField("position", NAMED_OBJECT_SCHEMA, True),
        StructField("location", ArrayType(DoubleType()), True),
        StructField("duration", DoubleType(), True),
        StructField("obv_for_after", DoubleType(), True),
        StructField("obv_for_before", DoubleType(), True),
        StructField("obv_for_net", DoubleType(), True),
        StructField("obv_against_after", DoubleType(), True),
        StructField("obv_against_before", DoubleType(), True),
        StructField("obv_against_net", DoubleType(), True),
        StructField("obv_total_net", DoubleType(), True),
        StructField(
            "tactics",
            StructType(
                [
                    StructField("formation", IntegerType(), True),
                    StructField("lineup", ArrayType(LINEUP_PLAYER_SCHEMA), True),
                ]
            ),
            True,
        ),
        StructField(
            "pass",
            StructType(
                [
                    StructField("recipient", NAMED_OBJECT_SCHEMA, True),
                    StructField("length", DoubleType(), True),
                    StructField("angle", DoubleType(), True),
                    StructField("height", NAMED_OBJECT_SCHEMA, True),
                    StructField("end_location", ArrayType(DoubleType()), True),
                    StructField("body_part", NAMED_OBJECT_SCHEMA, True),
                    StructField("type", NAMED_OBJECT_SCHEMA, True),
                    StructField("outcome", NAMED_OBJECT_SCHEMA, True),
                    StructField("technique", NAMED_OBJECT_SCHEMA, True),
                    StructField("cross", BooleanType(), True),
                    StructField("switch", BooleanType(), True),
                    StructField("cut_back", BooleanType(), True),
                    StructField("through_ball", BooleanType(), True),
                    StructField("deflected", BooleanType(), True),
                    StructField("shot_assist", BooleanType(), True),
                    StructField("goal_assist", BooleanType(), True),
                    StructField("assisted_shot_id", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "shot",
            StructType(
                [
                    StructField("statsbomb_xg", DoubleType(), True),
                    StructField("end_location", ArrayType(DoubleType()), True),
                    StructField("key_pass_id", StringType(), True),
                    StructField("outcome", NAMED_OBJECT_SCHEMA, True),
                    StructField("body_part", NAMED_OBJECT_SCHEMA, True),
                    StructField("type", NAMED_OBJECT_SCHEMA, True),
                    StructField("technique", NAMED_OBJECT_SCHEMA, True),
                    StructField("first_time", BooleanType(), True),
                    StructField("one_on_one", BooleanType(), True),
                    StructField("open_goal", BooleanType(), True),
                    StructField("follows_dribble", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "carry",
            StructType([StructField("end_location", ArrayType(DoubleType()), True)]),
            True,
        ),
        StructField(
            "dribble",
            StructType(
                [
                    StructField("outcome", NAMED_OBJECT_SCHEMA, True),
                    StructField("overrun", BooleanType(), True),
                    StructField("nutmeg", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "duel",
            StructType(
                [
                    StructField("type", NAMED_OBJECT_SCHEMA, True),
                    StructField("outcome", NAMED_OBJECT_SCHEMA, True),
                ]
            ),
            True,
        ),
        StructField(
            "foul_committed",
            StructType(
                [
                    StructField("card", NAMED_OBJECT_SCHEMA, True),
                    StructField("type", NAMED_OBJECT_SCHEMA, True),
                    StructField("penalty", BooleanType(), True),
                    StructField("advantage", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "foul_won",
            StructType(
                [
                    StructField("defensive", BooleanType(), True),
                    StructField("penalty", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "interception",
            StructType([StructField("outcome", NAMED_OBJECT_SCHEMA, True)]),
            True,
        ),
        StructField(
            "ball_receipt",
            StructType([StructField("outcome", NAMED_OBJECT_SCHEMA, True)]),
            True,
        ),
        StructField(
            "ball_recovery",
            StructType(
                [
                    StructField("offensive", BooleanType(), True),
                    StructField("recovery_failure", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "clearance",
            StructType(
                [
                    StructField("body_part", NAMED_OBJECT_SCHEMA, True),
                    StructField("aerial_won", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "goalkeeper",
            StructType(
                [
                    StructField("type", NAMED_OBJECT_SCHEMA, True),
                    StructField("outcome", NAMED_OBJECT_SCHEMA, True),
                    StructField("position", NAMED_OBJECT_SCHEMA, True),
                    StructField("technique", NAMED_OBJECT_SCHEMA, True),
                ]
            ),
            True,
        ),
    ]
)

# Schemas de tablas estaticas usadas como dimensiones para enriquecer eventos.
# Esquemas de CSV estaticos usados como tablas dimensionales.
TEAM_STATIC_SCHEMA = StructType(
    [
        StructField("team", StringType(), True),
        StructField("coach", StringType(), True),
        StructField("tactical_style", StringType(), True),
        StructField("historical_context", StringType(), True),
    ]
)

PLAYER_STATIC_SCHEMA = StructType(
    [
        StructField("player", StringType(), True),
        StructField("team", StringType(), True),
        StructField("position_group", StringType(), True),
        StructField("player_profile", StringType(), True),
    ]
)


# Parametros necesarios para conectar Spark con Kafka y definir rutas de salida.
# Define los argumentos de linea de comandos para ejecutar este modulo de forma flexible.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming consumer for StatsBomb events")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "statsbomb_events"))
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("SPARK_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument("--base-path", default=os.getenv("APP_BASE_PATH", "/opt/app"))
    parser.add_argument("--output-base", default=os.getenv("OUTPUT_BASE_PATH", "/opt/app/output"))
    parser.add_argument("--starting-offsets", default="latest", choices=["earliest", "latest"])
    parser.add_argument("--max-offsets-per-trigger", default="1000")
    parser.add_argument("--trigger-seconds", default="10")
    return parser.parse_args()


# Crea la sesion Spark con parametros pensados para ejecucion local en Docker.
# Crea la sesion Spark con parametros adecuados para streaming y agregaciones.
def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("StatsBombSparkStructuredStreaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider",
        )
        .getOrCreate()
    )


# Devuelve un DataFrame vacio con schema conocido cuando falta una tabla estatica.
# Crea un DataFrame vacio con el esquema esperado cuando falta un CSV estatico.
def _empty_df(spark: SparkSession, schema: StructType):
    return spark.createDataFrame([], schema)


# Asegura que existan columnas esperadas, rellenando con null si faltan.
# Garantiza que un DataFrame tenga todas las columnas necesarias, rellenando las ausentes con null.
def _select_or_null(df, columns: list[str]):
    for column_name in columns:
        if column_name not in df.columns:
            df = df.withColumn(column_name, lit(None).cast(StringType()))
    return df.select(*columns)


# Lee CSV estaticos de equipos y jugadores para enriquecer el stream.
# Carga las tablas estaticas de equipos y jugadores para enriquecer los eventos del stream.
def read_static_data(spark: SparkSession, base_path: Path):
    teams_path = base_path / "data" / "static" / "teams.csv"
    players_path = base_path / "data" / "static" / "players.csv"

    if teams_path.exists():
        teams_df = spark.read.option("header", True).option("inferSchema", True).csv(str(teams_path))
        teams_df = _select_or_null(teams_df, ["team", "coach", "tactical_style", "historical_context"])
    else:
        teams_df = _empty_df(spark, TEAM_STATIC_SCHEMA)

    if players_path.exists():
        players_df = spark.read.option("header", True).option("inferSchema", True).csv(str(players_path))
        players_df = _select_or_null(players_df, ["player", "team", "position_group", "player_profile"])
    else:
        players_df = _empty_df(spark, PLAYER_STATIC_SCHEMA)

    return teams_df, players_df


# Writer para agregados: sobrescribe el snapshot de cada micro-batch en Parquet.
# Devuelve una funcion foreachBatch que sobrescribe una fotografia de metricas agregadas.
def foreach_overwrite(path: str, partition_cols: list[str] | None = None):
    def _writer(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        writer = batch_df.coalesce(1).write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.parquet(path)
        print(f"Batch {batch_id} written to {path}")

    return _writer


# Convierte condiciones booleanas de Spark en enteros 0/1 para poder sumar eventos.
# Convierte condiciones booleanas en indicadores enteros 0/1 para poder sumarlos en agregaciones.
def _flag(condition):
    return when(condition, lit(1)).otherwise(lit(0))


# Pipeline streaming completo: Kafka -> limpieza -> metricas -> Parquet.
# Punto de entrada del modulo: conecta las piezas y ejecuta el flujo principal.
def main() -> None:
    args = parse_args()
    base_path = Path(args.base_path)
    output_base = Path(args.output_base)

    # Rutas de salida dentro de /opt/app/output; en Windows se ven como .\output.
    processed_path = str(output_base / "processed" / "events")
    lineups_path = str(output_base / "processed" / "lineups")
    team_metrics_path = str(output_base / "aggregates" / "team_metrics")
    player_metrics_path = str(output_base / "aggregates" / "player_metrics")
    intensity_path = str(output_base / "aggregates" / "intensity")
    checkpoint_base = str(output_base / "checkpoints")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Carga datos estaticos antes de unirlos al stream.
    teams_df, players_df = read_static_data(spark, base_path)

    # Lectura streaming desde Kafka: cada value es un JSON de evento StatsBomb.
    # Fuente streaming: Spark consume el topic Kafka donde escribe producer.py.
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .load()
    )

    # Convierte el valor JSON de Kafka en columnas segun EVENT_SCHEMA.
    statsbomb_df = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), EVENT_SCHEMA).alias("event"))
        .select("event.*")
    )

    # Extrae alineaciones de los eventos Starting XI y explota cada jugador como fila.
    # Extrae alineaciones desde eventos Starting XI y las guarda como dataset separado.
    lineup_df = (
        statsbomb_df.withColumn("event_type", lower(trim(col("type.name"))))
        .filter(col("event_type") == "starting xi")
        .select(
            trim(col("match_id")).alias("match_id"),
            col("team.id").cast("int").alias("team_id"),
            trim(col("team.name")).alias("team"),
            col("tactics.formation").cast("int").alias("formation"),
            explode(col("tactics.lineup")).alias("lineup_player"),
        )
        .select(
            "match_id",
            "team_id",
            "team",
            "formation",
            col("lineup_player.player.id").cast("int").alias("player_id"),
            trim(col("lineup_player.player.name")).alias("player"),
            col("lineup_player.position.id").cast("int").alias("position_id"),
            trim(col("lineup_player.position.name")).alias("position"),
            col("lineup_player.jersey_number").cast("int").alias("jersey_number"),
        )
    )

    # Normaliza coordenadas finales, que pueden venir en pass, carry o shot.
    end_location_col = coalesce(col("pass.end_location"), col("carry.end_location"), col("shot.end_location"))

    # Aplanado, limpieza y creacion de flags numericos para agregaciones.
    # Aplana el evento StatsBomb, limpia tipos y crea indicadores para las metricas.
    clean_df = (
        statsbomb_df.withColumn("event_id", trim(col("id")))
        .withColumn("match_id", trim(col("match_id")))
        .withColumn("event_index", col("index").cast("int"))
        .withColumn("event_type", lower(trim(col("type.name"))))
        .withColumn("event_type_raw", trim(col("type.name")))
        .withColumn("team_id", col("team.id").cast("int"))
        .withColumn("team", trim(col("team.name")))
        .withColumn("player_id", col("player.id").cast("int"))
        .withColumn("player", trim(col("player.name")))
        .withColumn("position_id", col("position.id").cast("int"))
        .withColumn("position", trim(col("position.name")))
        .withColumn("possession_team_id", col("possession_team.id").cast("int"))
        .withColumn("possession_team", trim(col("possession_team.name")))
        .withColumn("play_pattern", trim(col("play_pattern.name")))
        .withColumn("x", element_at(col("location"), 1).cast("double"))
        .withColumn("y", element_at(col("location"), 2).cast("double"))
        .withColumn("end_location", end_location_col)
        .withColumn("end_x", element_at(col("end_location"), 1).cast("double"))
        .withColumn("end_y", element_at(col("end_location"), 2).cast("double"))
        .withColumn("location_zone", when(col("x") < 40, lit("defensive_third")).when(col("x") < 80, lit("middle_third")).when(col("x").isNotNull(), lit("final_third")).otherwise(lit("unknown")))
        .withColumn("pass_outcome", lower(trim(col("pass.outcome.name"))))
        .withColumn("pass_height", trim(col("pass.height.name")))
        .withColumn("pass_type", trim(col("pass.type.name")))
        .withColumn("pass_recipient", trim(col("pass.recipient.name")))
        .withColumn("pass_length", col("pass.length").cast("double"))
        .withColumn("pass_angle", col("pass.angle").cast("double"))
        .withColumn("pass_shot_assist", when(col("pass.shot_assist") == lit(True), lit(True)).otherwise(lit(False)))
        .withColumn("pass_goal_assist", when(col("pass.goal_assist") == lit(True), lit(True)).otherwise(lit(False)))
        .withColumn("shot_xg", col("shot.statsbomb_xg").cast("double"))
        .withColumn("shot_outcome", lower(trim(col("shot.outcome.name"))))
        .withColumn("shot_body_part", trim(col("shot.body_part.name")))
        .withColumn("shot_type", trim(col("shot.type.name")))
        .withColumn("dribble_outcome", lower(trim(col("dribble.outcome.name"))))
        .withColumn("duel_type", trim(col("duel.type.name")))
        .withColumn("duel_outcome", trim(col("duel.outcome.name")))
        .withColumn("foul_card", trim(col("foul_committed.card.name")))
        .withColumn("interception_outcome", trim(col("interception.outcome.name")))
        .withColumn("ball_receipt_outcome", trim(col("ball_receipt.outcome.name")))
        .withColumn("keeper_type", trim(col("goalkeeper.type.name")))
        .withColumn("keeper_outcome", trim(col("goalkeeper.outcome.name")))
        .withColumn("obv_total_net", col("obv_total_net").cast("double"))
        .withColumn("event_value", coalesce(col("obv_total_net"), col("shot_xg"), when(col("event_type") == "ball recovery", lit(0.10)), when(col("event_type") == "interception", lit(0.15)), when(col("event_type") == "foul committed", lit(-0.05)), lit(0.0)))
        .withColumn("is_pass", _flag(col("event_type") == "pass"))
        .withColumn("is_successful_pass", _flag((col("event_type") == "pass") & col("pass_outcome").isNull()))
        .withColumn("is_shot", _flag(col("event_type") == "shot"))
        .withColumn("is_goal", _flag(((col("event_type") == "shot") & (col("shot_outcome") == "goal")) | (col("event_type") == "own goal against")))
        .withColumn("is_foul_committed", _flag(col("event_type") == "foul committed"))
        .withColumn("is_foul_won", _flag(col("event_type") == "foul won"))
        .withColumn("is_ball_recovery", _flag(col("event_type") == "ball recovery"))
        .withColumn("is_pressure", _flag(col("event_type") == "pressure"))
        .withColumn("is_interception", _flag(col("event_type") == "interception"))
        .withColumn("is_clearance", _flag(col("event_type") == "clearance"))
        .withColumn("is_carry", _flag(col("event_type") == "carry"))
        .withColumn("is_dribble", _flag(col("event_type") == "dribble"))
        .withColumn("is_successful_dribble", _flag((col("event_type") == "dribble") & (col("dribble_outcome") == "complete")))
        .withColumn("is_duel", _flag(col("event_type") == "duel"))
        .withColumn("is_final_third_event", _flag(col("x") >= 80))
        .withColumn("is_attacking_third_entry", _flag((col("event_type").isin("pass", "carry")) & (col("end_x") >= 80)))
        .withColumn("is_shot_assist", _flag((col("event_type") == "pass") & (col("pass_shot_assist") == lit(True))))
        .withColumn("is_goal_assist", _flag((col("event_type") == "pass") & (col("pass_goal_assist") == lit(True))))
        .filter(col("event_id").isNotNull())
        .filter(col("match_id").isNotNull())
        .filter(col("team").isNotNull())
        .filter(col("event_type").isNotNull())
        .filter(col("minute").between(0, 130))
        .select(
            "event_id",
            "match_id",
            "event_index",
            "period",
            "timestamp",
            "minute",
            "second",
            "event_type",
            "event_type_raw",
            "possession",
            "possession_team_id",
            "possession_team",
            "play_pattern",
            "team_id",
            "team",
            "player_id",
            "player",
            "position_id",
            "position",
            "x",
            "y",
            "end_x",
            "end_y",
            "location_zone",
            "duration",
            "pass_outcome",
            "pass_height",
            "pass_type",
            "pass_recipient",
            "pass_length",
            "pass_angle",
            "pass_shot_assist",
            "pass_goal_assist",
            "shot_xg",
            "shot_outcome",
            "shot_body_part",
            "shot_type",
            "dribble_outcome",
            "duel_type",
            "duel_outcome",
            "foul_card",
            "interception_outcome",
            "ball_receipt_outcome",
            "keeper_type",
            "keeper_outcome",
            "obv_total_net",
            "event_value",
            "is_pass",
            "is_successful_pass",
            "is_shot",
            "is_goal",
            "is_foul_committed",
            "is_foul_won",
            "is_ball_recovery",
            "is_pressure",
            "is_interception",
            "is_clearance",
            "is_carry",
            "is_dribble",
            "is_successful_dribble",
            "is_duel",
            "is_final_third_event",
            "is_attacking_third_entry",
            "is_shot_assist",
            "is_goal_assist",
        )
    )

    # Enriquecimiento con datos estaticos de equipos y jugadores mediante joins broadcast.
    # Enriquece el stream con tablas estaticas de equipo y jugador.
    enriched_df = (
        clean_df.join(broadcast(teams_df), on="team", how="left")
        .join(broadcast(players_df), on=["player", "team"], how="left")
        .withColumn("coach", coalesce(col("coach"), lit("unknown")))
        .withColumn("tactical_style", coalesce(col("tactical_style"), lit("unknown")))
        .withColumn("historical_context", coalesce(col("historical_context"), lit("unknown")))
        .withColumn("position_group", coalesce(col("position_group"), lit("unknown")))
        .withColumn("player_profile", coalesce(col("player_profile"), lit("unknown")))
    )

    # Agregados por equipo: volumen, calidad ofensiva, recuperaciones, presiones y OBV.
    # Agregacion por equipo: volumen, precision de pase, xG, presion, recuperaciones y OBV.
    team_metrics_df = (
        enriched_df.groupBy("match_id", "team", "coach", "tactical_style")
        .agg(
            count("*").alias("total_events"),
            spark_sum("is_pass").alias("passes"),
            spark_sum("is_successful_pass").alias("successful_passes"),
            spark_sum("is_shot").alias("shots"),
            spark_sum("is_goal").alias("goals"),
            spark_round(spark_sum(coalesce(col("shot_xg"), lit(0.0))), 3).alias("xg_total"),
            spark_sum("is_foul_committed").alias("fouls_committed"),
            spark_sum("is_foul_won").alias("fouls_won"),
            spark_sum("is_ball_recovery").alias("ball_recoveries"),
            spark_sum("is_pressure").alias("pressures"),
            spark_sum("is_interception").alias("interceptions"),
            spark_sum("is_clearance").alias("clearances"),
            spark_sum("is_carry").alias("carries"),
            spark_sum("is_dribble").alias("dribbles"),
            spark_sum("is_successful_dribble").alias("successful_dribbles"),
            spark_sum("is_final_third_event").alias("final_third_events"),
            spark_sum("is_attacking_third_entry").alias("attacking_third_entries"),
            spark_round(avg("x"), 2).alias("avg_event_x"),
            spark_round(spark_sum("event_value"), 3).alias("event_value_total"),
            spark_round(spark_sum(coalesce(col("obv_total_net"), lit(0.0))), 3).alias("obv_total_net"),
        )
        .withColumn(
            "pass_success_rate",
            when(col("passes") > 0, spark_round(col("successful_passes") / col("passes") * lit(100.0), 2)).otherwise(lit(0.0)),
        )
    )

    # Agregados por jugador: participacion, tiros, pases, presion y contribucion.
    # Agregacion por jugador: participacion y contribucion ofensiva/defensiva.
    player_metrics_df = (
        enriched_df.filter(col("player").isNotNull())
        .groupBy("match_id", "team", "player", "position", "position_group", "player_profile")
        .agg(
            count("*").alias("participation_total"),
            spark_sum("is_pass").alias("passes"),
            spark_sum("is_successful_pass").alias("successful_passes"),
            spark_sum("is_shot").alias("shots"),
            spark_sum("is_goal").alias("goals"),
            spark_round(spark_sum(coalesce(col("shot_xg"), lit(0.0))), 3).alias("xg_total"),
            spark_sum("is_shot_assist").alias("shot_assists"),
            spark_sum("is_goal_assist").alias("goal_assists"),
            spark_sum("is_ball_recovery").alias("ball_recoveries"),
            spark_sum("is_pressure").alias("pressures"),
            spark_sum("is_interception").alias("interceptions"),
            spark_sum("is_clearance").alias("clearances"),
            spark_sum("is_successful_dribble").alias("successful_dribbles"),
            spark_sum("is_foul_committed").alias("fouls_committed"),
            spark_sum("is_foul_won").alias("fouls_won"),
            spark_round(avg("x"), 2).alias("avg_event_x"),
            spark_round(spark_sum("event_value"), 3).alias("game_contribution"),
            spark_round(spark_sum(coalesce(col("obv_total_net"), lit(0.0))), 3).alias("obv_total_net"),
        )
        .withColumn(
            "pass_success_rate",
            when(col("passes") > 0, spark_round(col("successful_passes") / col("passes") * lit(100.0), 2)).otherwise(lit(0.0)),
        )
    )

    # Intensidad temporal: ventanas de 5 minutos por equipo.
    # Agregacion temporal en ventanas de cinco minutos para medir intensidad.
    intensity_df = (
        enriched_df.withColumn("minute_interval_start", floor(col("minute") / 5) * 5)
        .withColumn("minute_interval_end", col("minute_interval_start") + lit(5))
        .groupBy("match_id", "team", "minute_interval_start", "minute_interval_end")
        .agg(
            count("*").alias("events_in_interval"),
            spark_sum("is_pass").alias("passes_in_interval"),
            spark_sum("is_shot").alias("shots_in_interval"),
            spark_sum("is_goal").alias("goals_in_interval"),
            spark_sum("is_ball_recovery").alias("recoveries_in_interval"),
            spark_sum("is_pressure").alias("pressures_in_interval"),
            spark_round(spark_sum(coalesce(col("shot_xg"), lit(0.0))), 3).alias("xg_in_interval"),
            spark_round(spark_sum("event_value"), 3).alias("interval_value"),
        )
    )

    # Escritura del stream limpio: salida append particionada por partido y equipo.
    # Escritura continua de eventos enriquecidos y datasets agregados con checkpoints independientes.
    processed_query = (
        enriched_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", processed_path)
        .option("checkpointLocation", f"{checkpoint_base}/processed_events")
        .partitionBy("match_id", "team")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    lineups_query = (
        lineup_df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", lineups_path)
        .option("checkpointLocation", f"{checkpoint_base}/lineups")
        .partitionBy("match_id", "team")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    # Escritura de snapshots agregados. outputMode complete mantiene la foto actual de metricas.
    team_query = (
        team_metrics_df.writeStream.outputMode("complete")
        .foreachBatch(foreach_overwrite(team_metrics_path))
        .option("checkpointLocation", f"{checkpoint_base}/team_metrics")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    player_query = (
        player_metrics_df.writeStream.outputMode("complete")
        .foreachBatch(foreach_overwrite(player_metrics_path))
        .option("checkpointLocation", f"{checkpoint_base}/player_metrics")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    intensity_query = (
        intensity_df.writeStream.outputMode("complete")
        .foreachBatch(foreach_overwrite(intensity_path))
        .option("checkpointLocation", f"{checkpoint_base}/intensity")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    # A partir de aqui Spark queda vivo esperando micro-batches hasta Ctrl+C.
    print("Spark streaming queries started")
    print(f"Processed StatsBomb events: {processed_path}")
    print(f"Lineups: {lineups_path}")
    print(f"Team metrics: {team_metrics_path}")
    print(f"Player metrics: {player_metrics_path}")
    print(f"Intensity metrics: {intensity_path}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

"""
Spark Structured Streaming — Transit Delay Detection & Cascade Prediction
Reads from Kafka, computes rolling delay aggregations, flags cascade events,
and sinks to Delta Lake + HBase.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType, TimestampType
)

# ── Schema definitions ───────────────────────────────────────────────────────
VEHICLE_POS_SCHEMA = StructType([
    StructField("vehicle_id",    StringType()),
    StructField("trip_id",       StringType()),
    StructField("route_id",      StringType()),
    StructField("latitude",      DoubleType()),
    StructField("longitude",     DoubleType()),
    StructField("bearing",       DoubleType()),
    StructField("speed_mph",     DoubleType()),
    StructField("current_stop",  StringType()),
    StructField("stop_sequence", IntegerType()),
    StructField("timestamp",     StringType()),
    StructField("agency",        StringType()),
])

TRIP_UPDATE_SCHEMA = StructType([
    StructField("trip_id",   StringType()),
    StructField("route_id",  StringType()),
    StructField("vehicle_id",StringType()),
    StructField("start_date",StringType()),
    StructField("stop_time_updates", ArrayType(StructType([
        StructField("stop_id",                 StringType()),
        StructField("stop_sequence",           IntegerType()),
        StructField("arrival_delay_sec",       IntegerType()),
        StructField("departure_delay_sec",     IntegerType()),
        StructField("schedule_relationship",   StringType()),
    ]))),
    StructField("timestamp", StringType()),
    StructField("agency",    StringType()),
])

# ── Thresholds ───────────────────────────────────────────────────────────────
DELAY_SEVERE_SEC    = 300   # 5 min — "severe" individual delay
DELAY_CASCADE_SEC   = 180   # 3 min — threshold for cascade detection
CASCADE_ROUTE_PCT   = 0.35  # 35% of trips on a route delayed = cascade
SPEED_LOW_MPH       = 5.0   # below this = stuck / major incident


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TransitDelayDetection")
        .config("spark.sql.extensions",              "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",   "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions",      "8")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, topic: str, broker: str = "localhost:9092"):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe",               topic)
        .option("startingOffsets",         "latest")
        .option("failOnDataLoss",          "false")
        .load()
    )


# ── Stream 1: Vehicle positions → speed anomalies ────────────────────────────
def process_vehicle_positions(spark: SparkSession):
    raw = read_kafka_stream(spark, "gtfs.vehicle_positions")

    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), VEHICLE_POS_SCHEMA).alias("d"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .select("d.*", "kafka_ts")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withWatermark("event_ts", "2 minutes")
    )

    # Rolling 5-min average speed per route
    speed_agg = (
        parsed
        .groupBy(
            F.window("event_ts", "5 minutes", "1 minute"),
            "route_id",
            "agency",
        )
        .agg(
            F.avg("speed_mph").alias("avg_speed_mph"),
            F.min("speed_mph").alias("min_speed_mph"),
            F.count("*").alias("vehicle_count"),
            F.stddev("speed_mph").alias("speed_stddev"),
        )
        .withColumn("is_slow_route",
            F.col("avg_speed_mph") < SPEED_LOW_MPH
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    # Sink: Delta Lake
    (
        speed_agg.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/delta/checkpoints/speed_agg")
        .option("path",               "/delta/tables/route_speed_agg")
        .trigger(processingTime="30 seconds")
        .start()
    )

    return parsed


# ── Stream 2: Trip updates → delay scoring & cascade detection ───────────────
def process_trip_updates(spark: SparkSession):
    raw = read_kafka_stream(spark, "gtfs.trip_updates")

    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), TRIP_UPDATE_SCHEMA).alias("d"),
        )
        .select("d.*")
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withWatermark("event_ts", "5 minutes")
        # Explode stop-level updates
        .withColumn("stu", F.explode("stop_time_updates"))
        .select(
            "trip_id", "route_id", "vehicle_id", "agency", "event_ts",
            "stu.stop_id",
            "stu.arrival_delay_sec",
            "stu.departure_delay_sec",
            "stu.schedule_relationship",
        )
    )

    # Per-trip max delay in rolling 10-min window
    trip_delay = (
        parsed
        .groupBy(
            F.window("event_ts", "10 minutes", "2 minutes"),
            "trip_id", "route_id", "vehicle_id", "agency",
        )
        .agg(
            F.max("arrival_delay_sec").alias("max_arrival_delay_sec"),
            F.avg("arrival_delay_sec").alias("avg_arrival_delay_sec"),
            F.count("stop_id").alias("stops_reporting"),
        )
        .withColumn("delay_severity",
            F.when(F.col("max_arrival_delay_sec") >= DELAY_SEVERE_SEC,  "SEVERE")
             .when(F.col("max_arrival_delay_sec") >= DELAY_CASCADE_SEC, "MODERATE")
             .when(F.col("max_arrival_delay_sec") >  0,                 "MINOR")
             .otherwise("ON_TIME")
        )
        .withColumn("window_start", F.col("window.start"))
        .drop("window")
    )

    # Cascade detection: % of trips on a route with ≥ 3-min delay
    cascade_flags = (
        trip_delay
        .groupBy("window_start", "route_id", "agency")
        .agg(
            F.count("*").alias("total_trips"),
            F.sum(
                F.when(F.col("max_arrival_delay_sec") >= DELAY_CASCADE_SEC, 1).otherwise(0)
            ).alias("delayed_trips"),
        )
        .withColumn("delayed_pct",
            F.col("delayed_trips") / F.col("total_trips")
        )
        .withColumn("is_cascade",
            F.col("delayed_pct") >= CASCADE_ROUTE_PCT
        )
        .withColumn("cascade_score",
            # 0-100 severity score
            F.least(F.lit(100.0),
                F.col("delayed_pct") * 100 * (F.col("total_trips") / 10)
            ).cast("integer")
        )
    )

    # Sink 1: Delta Lake for historical analysis
    (
        trip_delay.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/delta/checkpoints/trip_delay")
        .option("path",               "/delta/tables/trip_delay_events")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Sink 2: Delta Lake for cascade events
    (
        cascade_flags.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/delta/checkpoints/cascade_flags")
        .option("path",               "/delta/tables/cascade_flags")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Sink 3: HBase for real-time lookups (via Spark-HBase connector)
    def write_to_hbase(batch_df, batch_id):
        """Write latest route delay state to HBase for sub-100ms lookups."""
        (
            batch_df.write
            .format("org.apache.hadoop.hbase.spark")
            .option("hbase.columns.mapping",
                "route_id STRING :key, "
                "metrics:delayed_pct FLOAT, "
                "metrics:is_cascade BOOLEAN, "
                "metrics:cascade_score INTEGER, "
                "metrics:total_trips INTEGER, "
                "metrics:delayed_trips INTEGER, "
                "metrics:window_start TIMESTAMP"
            )
            .option("hbase.table", "transit:route_delay_state")
            .save()
        )

    (
        cascade_flags.writeStream
        .outputMode("update")
        .foreachBatch(write_to_hbase)
        .trigger(processingTime="15 seconds")
        .start()
    )

    return trip_delay, cascade_flags


# ── Stream 3: ML scoring — predict cascade propagation ─────────────────────
def score_cascade_propagation(spark: SparkSession, trip_delay):
    """
    Feature engineering + ML scoring for cascade propagation prediction.
    Uses a pre-trained MLlib pipeline loaded from HDFS.
    Features: current delay, time of day, route load, historical delay profile.
    """
    from pyspark.ml import PipelineModel

    model = PipelineModel.load("/models/cascade_propagation_v2")

    features = (
        trip_delay
        .withColumn("hour_of_day",   F.hour("window_start"))
        .withColumn("is_rush_hour",
            F.col("hour_of_day").between(7, 9) | F.col("hour_of_day").between(16, 19)
        )
        .withColumn("delay_norm",
            F.col("max_arrival_delay_sec") / 3600.0
        )
    )

    scored = model.transform(features)

    (
        scored.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/delta/checkpoints/ml_scores")
        .option("path",               "/delta/tables/cascade_predictions")
        .trigger(processingTime="60 seconds")
        .start()
    )


if __name__ == "__main__":
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    vp_stream             = process_vehicle_positions(spark)
    trip_delay, cascades  = process_trip_updates(spark)
    # score_cascade_propagation(spark, trip_delay)  # enable once model is trained

    spark.streams.awaitAnyTermination()

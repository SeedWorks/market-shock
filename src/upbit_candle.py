"""
Spark Structured Streaming - Kafka에서 실시간 데이터 처리
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg,
    current_timestamp, to_timestamp, explode, max_by, min_by
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, DateType, FloatType, LongType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "market-candle"  # kafka에서 지정한 토픽으로 바꿔주기

CHECKPOINT_BASE = "/data/checkpoint"
SILVER_BASE = "/data/silver"

def create_spark_session() -> SparkSession:
    spark = ( SparkSession.builder \
        .appName("MetroStreaming") \
        .master("spark://spark-master:7077") \
        .config("spark.cores.max", "1") \
        .config("spark.executor.memory", "512m") \
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_message_schema():
    return StructType([
        StructField("exchange", StringType(), True),
        StructField("type", StringType(), True),
        StructField("market", StringType(), True),
        StructField("asset", StringType(), True),
        StructField("acc_vol_24h", DoubleType(), True),
        StructField("opening_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("trade_price", DoubleType(), True),
        StructField("candle_acc_trade_volume", DoubleType(), True),
        StructField("candle_acc_trade_price", DoubleType(), True),
    ])

def read_from_kafka(spark: SparkSession):
    kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

    return kafka_df

def parse_messages(kafka_df, schema):
    string_df = kafka_df.selectExpr("CAST(value as STRING) as value")

    parsed_df = string_df.select(
        from_json(col("value"),  schema).alias("data")
    ).select("data.*")

    base = (
        parsed_df.withColumn("event_ts", to_timestamp((col("event_time_ms") / 1000).cast("double")))
        .select("exchange", "market", "asset", "price", "acc_vol_24h", "signed_change_rate", "event_time_ms", "event_ts")
    )
    return base

def compute_return_10s(base):
    w = (
        base
        .withWatermark("event_ts", "30 seconds")
        .groupBy(
            col("asset"),
            col("market"),
            window(col("event_ts"), "10 seconds").alias("win")
        )
        .agg(
            min_by(col("price"), col("event_ts")).alias("first_price"),
            max_by(col("price"), col("event_ts")).alias("last_price")
        )
        .withColumn("return_10s", (col("last_price") - col("first_price")) / col("first_price"))
        .select(
            col("asset"),
            col("market"),
            col("win.start").alias("window_start"),
            col("win.end").alias("window_end"),
            col("first_price"),
            col("last_price"),
            col("return_10s")
        )
    )
    return w

def compute_return_1m(base):
    w = (
        base
        .withWatermark("event_ts", "2 minutes")
        .groupBy(
            col("asset"),
            col("market"),
            window(col("event_ts"), "1 minute").alias("win")
        )
        .agg(
            min_by(col("price"), col("event_ts")).alias("first_price_1m"),
            max_by(col("price"), col("event_ts")).alias("last_price_1m")
        )
        .withColumn("return_1m", (col("last_price_1m") - col("first_price_1m")) / col("first_price_1m"))
        .select(
            col("asset"),
            col("market"),
            col("win.start").alias("minute_start"),
            col("win.end").alias("minute_end"),
            col("return_1m"),
            col("last_price_1m").alias("close_1m")
        )
    )
    return w
#콘솔로 확인
def write_console(df):
    return (
        df.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("update")
        .start()
    )

def main():
    spark = create_spark_session()
    schema = get_message_schema()

    kafka_df = read_from_kafka(spark)
    base_df = parse_messages(kafka_df, schema)

    ret10s = compute_return_10s(base_df)
    ret1m = compute_return_1m(base_df)

    q1 = write_console(ret10s)
    q2 = write_console(ret1m)
    
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
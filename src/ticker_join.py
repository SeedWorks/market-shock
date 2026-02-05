
"""
Spark Structured Streaming - Kafka에서 실시간 이벤트 후보/확정 분리 발행
- events.spike_candidate : ticker 기반 빠른 후보
- events.spike_decision : candle 기반 확정/취소
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, avg, sum as fsum, abs as fabs,
    current_timestamp, to_timestamp, explode, max_by, min_by, date_trunc, coalesce, lit
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, DateType, FloatType, LongType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TICKER = "market-ticker"
TOPIC_CANDLE = "market-candle-1s"  


def create_spark_session() -> SparkSession:
    spark = ( SparkSession.builder \
        .appName("MetroStreaming") \
        .master("spark://spark-master:7077") \
        .config("spark.cores.max", "2") \
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.executor.memory", "512m") \
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def ticker_schema():
    return StructType([
        StructField("exchange", StringType(), True),
        StructField("market", StringType(), True),
        StructField("asset", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_time_ms", LongType(), True),
    ])

def candle_schema():
    return StructType([
        StructField("exchange", StringType(), True),
        StructField("type", StringType(), True),
        StructField("market", StringType(), True),
        StructField("asset", StringType(), True),
        StructField("candle_acc_trade_volume", DoubleType(), True),
        StructField("candle_acc_trade_price", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ])

def read_topic(spark, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value as STRING) as value")
    )

# -------- ticker pipeline (price_spike) --------
def parse_ticker(df):
    parsed = df.select(from_json(col("value"), ticker_schema()).alias("d")).select("d.*")
    return (
        parsed
        .withColumn("event_ts", to_timestamp((col("event_time_ms") / 1000).cast("double")))
        .select("market", "asset", "price", "event_ts")
    )

def ticker_10s_features(base):
    w = (
        base.withWatermark("event_ts", "30 seconds")
        .groupBy("asset", "market", window(col("event_ts"), "10 seconds").alias("win"))
        .agg(
            min_by(col("price"), col("event_ts")).alias("first_price"),
            max_by(col("price"), col("event_ts")).alias("last_price"),
        )
        .withColumn("return_10s", (col("last_price") - col("first_price")) / col("first_price"))
        .withColumn("abs_return_10s", fabs(col("return_10s")))
        .withColumn("window_start", col("win.start"))
        .withColumn("window_end", col("win.end"))
        .withColumn("minute_ts", date_trunc("minute", col("win.start")))
        .withColumn("price_spike", col("abs_return_10s") >= lit(0.003))
        .select(
            "asset", "market",
            col("win.start").alias("window_start"),
            col("win.end").alias("window_end"),
            "minute_ts", 
            "first_price", "last_price", "return_10s", "abs_return_10s", "price_spike", "minute_ts"
        )
    )
    return w

# -------- candle pipeline (volume_confirm) --------
def parse_candle(df):
    parsed = df.select(from_json(col("value"), candle_schema()).alias("d")).select("d.*")
    return (
        parsed
        .withColumn("event_ts", to_timestamp((col("timestamp") / 1000).cast("double")))
        .select("market", "asset", "event_ts", "candle_acc_trade_volume", "candle_acc_trade_price")
    )

def candle_10s_features(base):
    w10 = (
        base.withWatermark("event_ts", "30 seconds")
        .groupBy("asset", "market", window(col("event_ts"), "10 seconds").alias("win"))
        .agg(
            fsum(col("candle_acc_trade_volume")).alias("vol_10s"),
            fsum(col("candle_acc_trade_price")).alias("notional_10s"),
        )
        .withColumn("minute_ts", date_trunc("minute", col("win.start")))
        .select(
            "asset", "market", "minute_ts",
            col("win.start").alias("window_start"),
            col("win.end").alias("window_end"),
            "vol_10s", "notional_10s"
        )
    )
    return w10

def candle_baseline_1m(base):
    b1m = (
        base.withWatermark("event_ts", "2 minutes")
        .groupBy("asset", "market", window(col("event_ts"), "1 minute").alias("minwin"))
        .agg(
            fsum(col("candle_acc_trade_volume")).alias("baseline_vol_1m"),
            fsum(col("candle_acc_trade_price")).alias("baseline_notional_1m"),
        )
        .withColumn("minute_ts", col("minwin.start"))
        .select("asset", "market", "minute_ts", "baseline_vol_1m", "baseline_notional_1m")
    )
    return b1m

def candle_confirm(c10s, b1m):
    return (
        c10s.join(b1m, on=["asset", "market", "minute_ts"], how='inner')
        .withColumn("baseline_vol_1m", coalesce(col("baseline_vol_1m"), lit(0.0)))
        .withColumn("baseline_notional_1m", coalesce(col("baseline_notional_1m"), lit(0.0)))
        .withColumn(
            "volume_confirm",
            (col("baseline_vol_1m")>0) & (col("vol_10s") >= col("baseline_vol_1m") * lit(0.3))
        )
        .select(
            "asset", "market", "window_start", "window_end",
            "vol_10s", "notional_10s", "baseline_vol_1m", "baseline_notional_1m", "volume_confirm", "minute_ts"
        )
    )

def join_spike(t10s, confirm):
    out = (
        t10s.join(confirm, on=["asset", "market", "minute_ts"], how='inner')
        .withColumn("is_spike", col("price_spike") & col("volume_confirm"))
    )
    return out

def write_console(df):
    return (
        df.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
        .option("checkpointLocation", "/data/checkpoint/ticker_join")
        .start()
    )

def main():
    spark = create_spark_session()

    ticker_raw = read_topic(spark, TOPIC_TICKER)
    candle_raw = read_topic(spark, TOPIC_CANDLE)

    tbase = parse_ticker(ticker_raw)
    cbase = parse_candle(candle_raw)
    q_dbg_tbase = (
        tbase.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", "/data/checkpoint/_dbg_tbase")
        .start()
    )

    q_dbg_cbase = (
        cbase.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", "/data/checkpoint/_dbg_cbase")
        .start()
    )

    t10s = ticker_10s_features(tbase)
    c10s = candle_10s_features(cbase)
    b1m = candle_baseline_1m(cbase)
    confirm = candle_confirm(c10s, b1m)

    out = join_spike(t10s, confirm)

    q = write_console(out)
    spark.streams.awaitAnyTermination()

if __name__ == '__main__':
    main()
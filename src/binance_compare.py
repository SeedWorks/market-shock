# binance_spike_events.py
"""
Ticker-only Spike Events (Binance)
- candle 사용 안 함
- 10초 윈도우에서 급등/급락 이벤트만 출력
  - return_10s = (last-first)/first  => 방향(UP/DOWN) + pct_10s
  - range_10s  = (max-min)/min       => 중간 튐/원복도 잡힘 (옵션)
- spike 조건:
  - abs(return_10s) >= TH_RETURN  OR  range_10s >= TH_RANGE

+ 추가:
- binance로 들어오는 원본 ticker도 콘솔에 같이 출력
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, window, to_timestamp,
    min_by, max_by, min as fmin, max as fmax,
    abs as fabs, lit, expr, current_timestamp,
    round as fround
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


# ======================
# Config
# ======================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TICKER = os.getenv("TOPIC_TICKER", "market-ticker")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")  # 운영은 latest 추천
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/data/checkpoint")

WM_TICKER = os.getenv("WM_TICKER", "30 seconds")

TH_RETURN = float(os.getenv("TH_RETURN", "0.001"))  # 기본 0.1%
USE_RANGE = os.getenv("USE_RANGE", "true").lower() == "true"
TH_RANGE  = float(os.getenv("TH_RANGE", str(TH_RETURN)))

ONLY_BINANCE = os.getenv("ONLY_BINANCE", "true").lower() == "true"

N_EVENTS = int(os.getenv("N_EVENTS", "50"))
N_RAW    = int(os.getenv("N_RAW", "30"))   # ✅ raw 콘솔 출력 행수


# ======================
# Spark
# ======================
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("binance-spike-events")
        .master("spark://spark-master:7077")
        .config("spark.cores.max", os.getenv("SPARK_CORES_MAX", "1"))
        .config("spark.executor.cores", os.getenv("SPARK_EXECUTOR_CORES", "1"))
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "3"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "512m"))
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ======================
# Schema
# ======================
def ticker_schema() -> StructType:
    return StructType([
        StructField("exchange", StringType(), True),
        StructField("market", StringType(), True),
        StructField("asset", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_time_ms", LongType(), True),
    ])


# ======================
# Kafka read / Parse
# ======================
def read_ticker(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_TICKER)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
    )


def parse_ticker(kafka_df: DataFrame) -> DataFrame:
    parsed = kafka_df.select(from_json(col("value"), ticker_schema()).alias("d")).select("d.*")
    return (
        parsed
        .withColumn("event_ts", to_timestamp((col("event_time_ms") / 1000).cast("double")))
        .select(
            col("exchange"),
            col("market"),
            col("asset"),
            col("price").cast("double").alias("price"),
            col("event_ts")
        )
        .filter(col("event_ts").isNotNull())
        .filter(col("price").isNotNull())
    )


# ======================
# Spike Events
# ======================
def build_spike_events(base: DataFrame) -> DataFrame:
    feat = (
        base.withWatermark("event_ts", WM_TICKER)
        .groupBy("exchange", "asset", "market", window(col("event_ts"), "10 seconds").alias("w"))
        .agg(
            min_by(col("price"), col("event_ts")).alias("first_price"),
            max_by(col("price"), col("event_ts")).alias("last_price"),
            fmin(col("price")).alias("min_price"),
            fmax(col("price")).alias("max_price"),
        )
        .withColumn("window_start", col("w.start"))
        .withColumn("window_end", col("w.end"))
        .drop("w")
        .withColumn(
            "return_10s",
            expr("""
                CASE WHEN first_price IS NULL OR first_price = 0 THEN NULL
                     ELSE (last_price - first_price) / first_price
                END
            """)
        )
        .withColumn(
            "range_10s",
            expr("""
                CASE WHEN min_price IS NULL OR min_price = 0 THEN NULL
                     ELSE (max_price - min_price) / min_price
                END
            """)
        )
        .withColumn("abs_return_10s", fabs(col("return_10s")))
        .withColumn("pct_10s", col("return_10s") * lit(100.0))
        .withColumn("pct_10s_round", fround(col("pct_10s"), 3))
        .withColumn("direction", expr("CASE WHEN return_10s >= 0 THEN 'UP' ELSE 'DOWN' END"))
    )

    if USE_RANGE:
        spike_cond = (col("abs_return_10s") >= lit(TH_RETURN)) | (col("range_10s") >= lit(TH_RANGE))
    else:
        spike_cond = (col("abs_return_10s") >= lit(TH_RETURN))

    events = (
        feat.filter(spike_cond)
        .withColumn("event_type", lit("spike_ticker_only"))
        .withColumn("event_created_at", current_timestamp())
        .select(
            "event_type", "event_created_at",
            "exchange", "market", "asset",
            "window_start", "window_end",
            "first_price", "last_price",
            "min_price", "max_price",
            "pct_10s_round", "direction",
            "abs_return_10s", "range_10s"
        )
    )
    return events


# ======================
# Console writers
# ======================
def write_raw_console(base: DataFrame, ckpt: str):
    """
    ✅ 들어오는 binance ticker 원본을 계속 출력
    """
    return (
        base.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", N_RAW)
        .option("checkpointLocation", ckpt)
        .start()
    )


def write_spike_console(events: DataFrame, ckpt: str):
    def _show(batch_df: DataFrame, batch_id: int):
        print("\n" + "=" * 120)
        print(
            f"[SPIKE_EVENTS] batch_id={batch_id} rows={batch_df.count()} | "
            f"TH_RETURN={TH_RETURN} | USE_RANGE={USE_RANGE} | TH_RANGE={TH_RANGE}"
        )
        print("=" * 120)
        (batch_df
         .orderBy(col("window_start").desc())
         .show(N_EVENTS, truncate=False))

    return (
        events.writeStream
        .foreachBatch(_show)
        .outputMode("append")
        .option("checkpointLocation", ckpt)
        .start()
    )


# ======================
# Main
# ======================
def main():
    spark = create_spark_session()

    raw = read_ticker(spark)
    base = parse_ticker(raw)

    if ONLY_BINANCE:
        base = base.filter(col("exchange") == "binance")

    # ✅ 1) 들어오는 binance ticker 원본 콘솔 출력 추가
    _ = write_raw_console(
        base.select("exchange", "market", "asset", "price", "event_ts"),
        ckpt=f"{CHECKPOINT_BASE}/raw_binance_ticker_console"
    )

    # ✅ 2) spike 이벤트 콘솔 출력 (기존 그대로)
    events = build_spike_events(base)
    _ = write_spike_console(
        events,
        ckpt=f"{CHECKPOINT_BASE}/spike_events_binance_ticker_only"
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

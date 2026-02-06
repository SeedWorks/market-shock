# binance_spike_events_with_candle.py
"""
Ticker + Candle Spike Events (Binance)
- ticker(가격) 기반 10초 spike 이벤트 생성
- candle(거래량/거래대금) 10초 집계 후 spike 이벤트에 조인 (market 통일 전제)
- 출력:
  1) RAW_TICKER 콘솔
  2) RAW_CANDLE 콘솔
  3) SPIKE_EVENTS (ticker spike + candle vol/notional 붙여서)
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, window, to_timestamp,
    min_by, max_by, min as fmin, max as fmax,
    sum as fsum,
    abs as fabs, lit, expr, current_timestamp,
    round as fround, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


# ======================
# Config
# ======================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_TICKER = os.getenv("TOPIC_TICKER", "market-ticker")
TOPIC_CANDLE = os.getenv("TOPIC_CANDLE", "market-candle-1s")

STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
CHECKPOINT_BASE  = os.getenv("CHECKPOINT_BASE", "/data/checkpoint")

WM_TICKER = os.getenv("WM_TICKER", "30 seconds")
WM_CANDLE = os.getenv("WM_CANDLE", "30 seconds")

# spike threshold
TH_RETURN = float(os.getenv("TH_RETURN", "0.001"))  # 0.1%
USE_RANGE = os.getenv("USE_RANGE", "true").lower() == "true"
TH_RANGE  = float(os.getenv("TH_RANGE", str(TH_RETURN)))

ONLY_BINANCE = os.getenv("ONLY_BINANCE", "true").lower() == "true"

N_EVENTS = int(os.getenv("N_EVENTS", "50"))
N_RAW    = int(os.getenv("N_RAW", "30"))


# ======================
# Spark
# ======================
def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("binance-spike-events-with-candle")
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
# Schemas
# ======================
def ticker_schema() -> StructType:
    return StructType([
        StructField("exchange", StringType(), True),
        StructField("market", StringType(), True),
        StructField("asset", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_time_ms", LongType(), True),
    ])


def candle_schema() -> StructType:
    return StructType([
        StructField("exchange", StringType(), True),
        StructField("type", StringType(), True),
        StructField("market", StringType(), True),
        StructField("asset", StringType(), True),
        StructField("candle_acc_trade_volume", DoubleType(), True),
        StructField("candle_acc_trade_price", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ])


# ======================
# Kafka read
# ======================
def read_topic(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", STARTING_OFFSETS)
        .load()
        .selectExpr("CAST(value AS STRING) AS value")
    )


# ======================
# Parse
# ======================
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


def parse_candle(kafka_df: DataFrame) -> DataFrame:
    parsed = kafka_df.select(from_json(col("value"), candle_schema()).alias("d")).select("d.*")
    return (
        parsed
        .withColumn("event_ts", to_timestamp((col("timestamp") / 1000).cast("double")))
        .select(
            col("exchange"),
            col("market"),
            col("asset"),
            col("event_ts"),
            col("candle_acc_trade_volume").cast("double").alias("candle_acc_trade_volume"),
            col("candle_acc_trade_price").cast("double").alias("candle_acc_trade_price"),
        )
        .filter(col("event_ts").isNotNull())
    )


# ======================
# Candle 10s aggregation
# ======================
def candle_10s_agg(cbase: DataFrame) -> DataFrame:
    agg = (
        cbase.withWatermark("event_ts", WM_CANDLE)
        .groupBy("exchange", "asset", "market", window(col("event_ts"), "10 seconds").alias("w"))
        .agg(
            fsum(col("candle_acc_trade_volume")).alias("vol_10s"),
            fsum(col("candle_acc_trade_price")).alias("notional_10s"),
        )
        .withColumn("window_start", col("w.start"))
        .withColumn("window_end", col("w.end"))
        .drop("w")
        .select("exchange", "asset", "market", "window_start", "window_end", "vol_10s", "notional_10s")
    )

    # ✅ 핵심: 집계 결과의 window_start에 다시 watermark를 걸어야 stream-stream join이 안정적
    return agg.withWatermark("window_start", "2 minutes")


# ======================
# Ticker spike events (10s)
# ======================
def build_ticker_spike_events(tbase: DataFrame) -> DataFrame:
    feat = (
        tbase.withWatermark("event_ts", WM_TICKER)
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
        .withColumn("event_type", lit("spike_ticker_10s"))
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

    # ✅ 핵심: spike 이벤트도 window_start에 watermark
    return events.withWatermark("window_start", "2 minutes")


# ======================
# Join spike + candle(10s)
# ======================
def join_spike_with_candle(spike_events: DataFrame, c10s: DataFrame) -> DataFrame:
    # ✅ 조인 키는 window_start(시간) + 심볼키만. window_end는 출력용으로만 둠
    joined = (
        spike_events.alias("s")
        .join(
            c10s.alias("c"),
            on=[
                col("s.exchange") == col("c.exchange"),
                col("s.asset") == col("c.asset"),
                col("s.market") == col("c.market"),
                col("s.window_start") == col("c.window_start"),
            ],
            how="left"
        )
        .select(
            col("s.event_type"),
            col("s.event_created_at"),
            col("s.exchange"),
            col("s.market"),
            col("s.asset"),
            col("s.window_start"),
            col("s.window_end"),
            col("s.first_price"),
            col("s.last_price"),
            col("s.min_price"),
            col("s.max_price"),
            col("s.pct_10s_round"),
            col("s.direction"),
            col("s.abs_return_10s"),
            col("s.range_10s"),
            coalesce(col("c.vol_10s"), lit(0.0)).alias("vol_10s"),
            coalesce(col("c.notional_10s"), lit(0.0)).alias("notional_10s"),
        )
    )
    return joined


# ======================
# Console writers
# ======================
def write_raw_console(df: DataFrame, ckpt: str, title: str):
    return (
        df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", N_RAW)
        .option("checkpointLocation", ckpt)
        .queryName(title)
        .start()
    )


def write_spike_console(events: DataFrame, ckpt: str):
    def _show(batch_df: DataFrame, batch_id: int):
        print("\n" + "=" * 120)
        print(
            f"[SPIKE_EVENTS + CANDLE] batch_id={batch_id} rows={batch_df.count()} | "
            f"TH_RETURN={TH_RETURN} | USE_RANGE={USE_RANGE} | TH_RANGE={TH_RANGE}"
        )
        print("=" * 120)
        (batch_df.orderBy(col("window_start").desc()).show(N_EVENTS, truncate=False))

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

    raw_ticker = read_topic(spark, TOPIC_TICKER)
    raw_candle = read_topic(spark, TOPIC_CANDLE)

    tbase = parse_ticker(raw_ticker)
    cbase = parse_candle(raw_candle)

    if ONLY_BINANCE:
        tbase = tbase.filter(col("exchange") == "binance")
        cbase = cbase.filter(col("exchange") == "binance")

    # raw 콘솔(티커/캔들)
    _ = write_raw_console(
        tbase.select("exchange", "market", "asset", "price", "event_ts"),
        ckpt=f"{CHECKPOINT_BASE}/raw_binance_ticker_console",
        title="RAW_BINANCE_TICKER"
    )

    _ = write_raw_console(
        cbase.select("exchange", "market", "asset", "candle_acc_trade_volume", "candle_acc_trade_price", "event_ts"),
        ckpt=f"{CHECKPOINT_BASE}/raw_binance_candle_console",
        title="RAW_BINANCE_CANDLE"
    )

    # candle 10초 집계
    c10s = candle_10s_agg(cbase)

    # ticker spike 이벤트
    spike = build_ticker_spike_events(tbase)

    # spike + candle(10s) 조인
    out = join_spike_with_candle(spike, c10s)

    _ = write_spike_console(
        out,
        ckpt=f"{CHECKPOINT_BASE}/spike_events_binance_ticker_with_candle_10s"
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

#KRW-BTc, KRW-ENSO, KRW-ETH, KRW-SOL, 



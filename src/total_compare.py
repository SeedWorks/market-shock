import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, expr, max_by
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import from_unixtime, floor

# =========================
# ENV
# =========================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_MARKET_TICKER = os.getenv("TOPIC_MARKET_TICKER", "market-ticker")

CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/data/checkpoint")
APP_NAME = os.getenv("APP_NAME", "CompareUpbitBinanceReturns")

# watermark, timestamp format
WM = os.getenv("WM", "10 minutes")
MINUTE_TS_FMT = os.getenv("MINUTE_TS_FMT", "yyyy-MM-dd'T'HH:mm:ssXXX")  # 필요시 .SSSX로 변경

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# =========================
# Schema (ticker)
# =========================
ticker_schema = StructType([
    StructField("exchange", StringType(), True),          # "upbit" / "binance"
    StructField("market", StringType(), True),
    StructField("asset", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("acc_vol_24h", DoubleType(), True),
    StructField("signed_change_rate", DoubleType(), True),
    StructField("event_time_ms", LongType(), True),
    StructField("event_time", StringType(), True),
    StructField("minute_ts", StringType(), True),         # ISO string
])

def read_ticker(topic: str):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
           .select(from_json(col("json_str"), ticker_schema).alias("v"))
           .select("v.*")
           # ✅ minute_ts 파싱 (null 방지)
           .withColumn("minute_ts", to_timestamp(col("minute_ts"), MINUTE_TS_FMT))
    )
    return parsed

# =========================
# 1) read market-ticker (하나만)
# =========================
ticker_all = read_ticker(TOPIC_MARKET_TICKER)

ticker_all = ticker_all.withColumn(
    "minute_ts",
    to_timestamp(from_unixtime(floor(col("event_time_ms")/1000/60)*60))
)

# ✅ aggregation 전에 watermark
ticker_all_wm = ticker_all.withWatermark("minute_ts", WM)

# =========================
# 2) 1분 close (exchange, asset, minute_ts)
# =========================
close_1m = (
    ticker_all_wm
    .where(col("minute_ts").isNotNull() & col("asset").isNotNull() & col("exchange").isNotNull())
    .groupBy("exchange", "asset", "minute_ts")
    .agg(max_by(col("price"), col("event_time_ms")).alias("close_price"))
)

# =========================
# 3) 이전 1분 close 조인 -> ret_1m
# =========================
curr = close_1m.withWatermark("minute_ts", WM)

prev = (
    close_1m
    .select(
        col("exchange"),
        col("asset"),
        col("minute_ts").alias("prev_minute_ts"),
        col("close_price").alias("prev_close")
    )
    .withWatermark("prev_minute_ts", WM)
)

curr_with_prev_key = (
    curr
    .withColumn("prev_minute_ts", expr("minute_ts - INTERVAL 1 MINUTE"))
    .withWatermark("prev_minute_ts", WM)
)

ret_1m = (
    curr_with_prev_key
    .join(
        prev,
        on=[
            curr_with_prev_key.exchange == prev.exchange,
            curr_with_prev_key.asset == prev.asset,
            curr_with_prev_key.prev_minute_ts == prev.prev_minute_ts
        ],
        how="inner"
    )
    .select(
        curr_with_prev_key.exchange.alias("exchange"),
        curr_with_prev_key.asset.alias("asset"),
        curr_with_prev_key.minute_ts.alias("minute_ts"),
        curr_with_prev_key.close_price.alias("close_price"),
        prev.prev_close.alias("prev_close"),
        (col("close_price") / col("prev_close") - expr("1.0")).alias("ret_1m")
    )
    .where(col("prev_close").isNotNull() & (col("prev_close") != 0))
)

# =========================
# 4) upbit vs binance compare
# =========================
bin_ret = (
    ret_1m.where(col("exchange") == "binance")
    .select(
        col("asset"),
        col("minute_ts"),
        col("ret_1m").alias("binance_ret_1m"),
        col("close_price").alias("binance_close")
    )
)

up_ret = (
    ret_1m.where(col("exchange") == "upbit")
    .select(
        col("asset"),
        col("minute_ts"),
        col("ret_1m").alias("upbit_ret_1m"),
        col("close_price").alias("upbit_close")
    )
)

bin_ret_w = bin_ret.withWatermark("minute_ts", WM)
up_ret_w  = up_ret.withWatermark("minute_ts", WM)

compare = (
    up_ret_w.join(bin_ret_w, on=["asset", "minute_ts"], how="inner")
    .select(
        col("asset"),
        col("minute_ts"),
        col("upbit_ret_1m"),
        col("binance_ret_1m"),
        ((col("upbit_ret_1m") - col("binance_ret_1m")) * expr("100.0")).alias("ret_gap_pp"),
        col("upbit_close"),
        col("binance_close")
    )
)

# =========================
# 5) sink
# =========================

q = (close_1m.writeStream
     .format("console")
     .outputMode("update")
     .option("truncate","false")
     .start())


query = (
    compare.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/compare_ret_1m")
    .start()
)

spark.streams.awaitAnyTermination()

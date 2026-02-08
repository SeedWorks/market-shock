import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, sum as fsum, abs as fabs,
    to_timestamp, max_by, min_by, date_trunc, coalesce, lit,
    expr, round as fround, from_unixtime, floor
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# os.environ['JAVA_HOME'] = '/opt/java/openjdk' 
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# ======================
# 1. Config & InfluxDB Setup
# ======================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INFLUX_URL = os.getenv("INFLUXDB_URL", "http://marketshock-influxdb:9999")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token")
INFLUX_ORG = os.getenv("INFLUXDB_ORG", "my-org")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "market-data")
WM = "10 minutes" #

# ======================
# 2. Schemas
# ======================
def ticker_schema():
    return StructType([
        StructField("exchange", StringType(), True),
        StructField("market", StringType(), True),
        StructField("asset", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("event_time_ms", LongType(), True),
        StructField("minute_ts", StringType(), True),
    ]) #

# ======================
# 3. InfluxDB Save Function
# ======================
# def save_to_influx(batch_df, batch_id):
#     client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
#     write_api = client.write_api(write_options=SYNCHRONOUS)
    
#     rows = batch_df.collect()
#     points = []
    
#     for row in rows:
#         # 1. 비교 데이터 포인트 (Upbit vs Binance)
#         if hasattr(row, 'upbit_close'):
#             p = Point("market_comparison") \
#                 .tag("asset", row.asset) \
#                 .field("upbit_price", float(row.upbit_close)) \
#                 .field("binance_price", float(row.binance_close)) \
#                 .field("upbit_ret", float(row.upbit_ret_1m)) \
#                 .field("binance_ret", float(row.binance_ret_1m)) \
#                 .field("ret_gap", float(row.ret_gap_pp)) \
#                 .time(row.minute_ts, WritePrecision.NS)
#             points.append(p)
            
#     if points:
#         write_api.write(bucket=INFLUX_BUCKET, record=points)
#         print(f"Batch {batch_id}: Saved {len(points)} comparison rows to InfluxDB")
#     client.close()

# def save_to_influx(batch_df, batch_id):
#     # [디버깅 추가] 이번 배치에 조인 성공한 데이터가 있는지 확인
#     row_count = batch_df.count()
#     print(f"DEBUG: Batch {batch_id} - Join 성공 건수: {row_count}")

#     if row_count == 0:
#         # 데이터가 없으면 InfluxDB 연결조차 안 하고 종료
#         return

#     # 데이터가 있을 때만 실행되는 구간
#     client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
#     write_api = client.write_api(write_options=SYNCHRONOUS)
    
#     # ... 기존 저장 로직 ...
#     print(f"SUCCESS: Batch {batch_id} - {row_count}건 InfluxDB 저장 완료!")
#     client.close()

def save_to_influx(batch_df, batch_id):
    row_count = batch_df.count()
    print(f"DEBUG: Batch {batch_id} - Join 성공 건수: {row_count}", flush=True)

    if row_count == 0:
        return

    # 1. InfluxDB 클라이언트 설정
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    # 2. 데이터를 Python 객체로 가져오기
    rows = batch_df.collect()
    points = []
    
    for row in rows:
        # [주의] 현재 조인된 컬럼명이 'up_p', 'bin_p'인지 'upbit_close'인지 확인 필요!
        # 아래는 'up_p', 'bin_p'라고 가정했을 때의 예시입니다.
        p = Point("market_comparison") \
            .tag("asset", row.asset) \
            .field("upbit_price", float(row.up_price)) \
            .field("binance_price", float(row.bin_price)) \
            .time(row.minute_ts, WritePrecision.NS)
        points.append(p)
            
    # 3. 실제 쓰기 작업
    if points:
        try:
            write_api.write(bucket=INFLUX_BUCKET, record=points)
            print(f"SUCCESS: Batch {batch_id} - {len(points)}건 InfluxDB 전송 완료!", flush=True)
        except Exception as e:
            print(f"ERROR: InfluxDB 쓰기 실패 - {e}", flush=True)
    
    client.close()

# ======================
# 4. Processing Logic (Integrated)
# ======================
# def main():
#     spark = SparkSession.builder \
#             .appName("UnifiedInfluxSaver") \
#             .config("spark.sql.streaming.checkpointLocation", "/data/checkpoint/influx_saver_unique") \
#             .getOrCreate()
    
#     # Kafka 데이터 읽기
#     ticker_raw = spark.readStream.format("kafka") \
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
#         .option("subscribe", "market-ticker") \
#         .load()

#     # 데이터 파싱 및 1분 단위 정규화
#     ticker_all = ticker_raw.selectExpr("CAST(value AS STRING) AS json_str") \
#         .select(from_json(col("json_str"), ticker_schema()).alias("v")).select("v.*") \
#         .withColumn("minute_ts", to_timestamp(from_unixtime(floor(col("event_time_ms")/1000/60)*60))) \
#         .withWatermark("minute_ts", WM)

#     # 1. 거래소/자산별 1분 종가 계산
#     close_1m = ticker_all.groupBy("exchange", "asset", "minute_ts") \
#         .agg(max_by(col("price"), col("event_time_ms")).alias("close_price"))

#     # 2. 수익률(Return) 계산을 위한 Self-Join
#     curr = close_1m.withWatermark("minute_ts", WM)
#     prev = close_1m.select(col("exchange"), col("asset"), 
#                            col("minute_ts").alias("prev_ts"), 
#                            col("close_price").alias("prev_close")) \
#                   .withWatermark("prev_ts", WM)

#     ret_1m = curr.join(prev, (curr.exchange == prev.exchange) & (curr.asset == prev.asset) & 
#                        (curr.minute_ts == prev.prev_ts + expr("INTERVAL 1 MINUTE"))) \
#         .select(curr.exchange, curr.asset, curr.minute_ts, curr.close_price, 
#                 (curr.close_price / prev.prev_close - 1.0).alias("ret_1m"))

#     # 3. Upbit vs Binance 비교 Join
#     up_ret = ret_1m.where(col("exchange") == "upbit").select("asset", "minute_ts", "ret_1m", "close_price")
#     bin_ret = ret_1m.where(col("exchange") == "binance").select("asset", "minute_ts", "ret_1m", "close_price")

#     compare = up_ret.alias("u").join(bin_ret.alias("b"), ["asset", "minute_ts"]) \
#         .select(
#             col("asset"), col("minute_ts"),
#             col("u.close_price").alias("upbit_close"),
#             col("b.close_price").alias("binance_close"),
#             col("u.ret_1m").alias("upbit_ret_1m"),
#             col("b.ret_1m").alias("binance_ret_1m"),
#             ((col("u.ret_1m") - col("b.ret_1m")) * 100.0).alias("ret_gap_pp")
#         )

#     # InfluxDB 저장 시작
#     query = compare.writeStream \
#             .foreachBatch(save_to_influx) \
#             .option("checkpointLocation", "/data/checkpoint/influx_saver_unique") \
#             .start()

#     query.awaitTermination()

def main():
    # 1. Spark 세션 시간대를 UTC로 강제 고정 (매우 중요!)
    spark = SparkSession.builder \
        .appName("UnifiedInfluxSaver") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # 2. Kafka 데이터 읽기 (시작 지점 earliest로 변경해서 과거 데이터 확인)
    ticker_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "market-ticker") \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. 데이터 파싱 (프로듀서가 이미 minute_ts를 주므로 활용)
    ticker_all = ticker_raw.selectExpr("CAST(value AS STRING) AS json_str") \
        .select(from_json(col("json_str"), ticker_schema()).alias("v")).select("v.*") \
        .withColumn("minute_ts", to_timestamp(col("minute_ts"))) \
        .withWatermark("minute_ts", "1 minute")

    # 4. 수익률 없이 '가격'만 먼저 조인해서 데이터가 흐르는지 확인
    upbit_df = ticker_all.filter(col("exchange") == "upbit") \
        .selectExpr("asset", "minute_ts", "price as up_price")
    
    binance_df = ticker_all.filter(col("exchange") == "binance") \
        .selectExpr("asset", "minute_ts", "price as bin_price")

    # [수정] 조인 조건을 asset으로만 한정
    compare = upbit_df.join(binance_df, on=["asset", "minute_ts"], how="inner")

    # 5. 저장 로직 (디버깅용 로그 포함)
    def debug_save(batch_df, batch_id):
        count = batch_df.count()
        print(f">>> Batch {batch_id}: 조인 성공 {count}건", flush=True)
        if count > 0:
            batch_df.show(5)
            save_to_influx(batch_df, batch_id)

    query = compare.writeStream \
        .foreachBatch(debug_save) \
        .option("checkpointLocation", "/data/checkpoint/influx_saver_debug_v2") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()


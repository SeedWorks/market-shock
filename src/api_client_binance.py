import websocket
import threading
import json
import time
from datetime import datetime, timezone
from kafka import KafkaProducer


class BinanceTickerWS:
    """
    Binance WebSocket Ticker Client
    - 단일 심볼로 시작 → symbols 리스트 늘리면 멀티스트림으로 확장
    - 프로젝트 표준 스키마로 정규화 후 Kafka로 전송
    """

    def __init__(self, symbols=None):
        # 예: ["btcusdt"] 또는 ["btcusdt", "ethusdt"]
        self.symbols = [s.lower() for s in (symbols or ["btcusdt"])]
        self.ws = None

        self.ticker_topic = "market-ticker"
        self.candle_topic = "market-candle-1s"

        self.producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=5,
            linger_ms=5
        )

    def build_ws_url(self) -> str:
        """
            symbols 개수에 따라 단일(/ws) vs 멀티(/stream) URL 자동 선택
            ✅ ticker + kline(1s) 동시 구독
        """
        base = "wss://stream.binance.com:9443"

        interval = "1s"  # 필요하면 "1m" 등으로 변경

        if len(self.symbols) == 1:
            s = self.symbols[0]
            # ✅ 단일 스트림에서도 combined stream으로 받는 게 관리 쉬움
            streams = f"{s}@ticker/{s}@kline_{interval}"
            return f"{base}/stream?streams={streams}"
        else:
            streams = []
            for s in self.symbols:
                streams.append(f"{s}@ticker")
                streams.append(f"{s}@kline_{interval}")
            return f"{base}/stream?streams={'/'.join(streams)}"

    def on_open(self, ws):
        print("Binance WebSocket connected")
        print("URL:", self.build_ws_url())

    def on_message(self, ws, message):
        try:
            if isinstance(message, (bytes, bytearray)):
                message = message.decode("utf-8")

            msg = json.loads(message)

            # ✅ combined stream wrapper {stream, data}
            data = msg.get("data", msg)

            etype = data.get("e")
            if etype == "24hrTicker":
                self.parse_ticker(data)
            elif etype == "kline":
                self.parse_candle_1s(data)   # ✅ 새로 추가
            else:
                return

        except Exception as e:
            print("Parse error:", e)
            print("Raw message:", message)


    def parse_ticker(self, data: dict):
        """
        binance 24hrTicker -> 프로젝트 표준 스키마로 정규화
        주요 필드:
        - s: symbol (BTCUSDT)
        - c: last price
        - v: base asset volume (24h)
        - P: price change percent (24h)  (예: "0.43")
        - E: event time (ms)
        """
        symbol = data["s"]                  # "BTCUSDT"
        price = float(data["c"])
        acc_vol_24h = float(data["v"])
        signed_change_rate = float(data["P"]) / 100.0
        event_time_ms = int(data["E"])

        ticker = {
            "symbol": symbol,
            "price": price,
            "acc_vol_24h": acc_vol_24h,
            "signed_change_rate": signed_change_rate,
            "timestamp": event_time_ms
        }

        self.handle_ticker(ticker)

    def parse_candle_1s(self, data):

        """
        binance kline -> 업비트 candle_message와 동일 스키마로 정규화
        """
        symbol = data["s"]          # BTCUSDT
        k = data["k"]
        interval = k["i"]           # "1s", "1m", ...

        candle = {
            "exchange": "binance",
            "type": f"kline.{interval}",
            "market": symbol,
            "asset": symbol.replace("USDT", ""),  # 단일 USDT 기준 (필요시 확장)
            "opening_price": float(k["o"]),
            "high_price": float(k["h"]),
            "low_price": float(k["l"]),
            "trade_price": float(k["c"]),
            "candle_acc_trade_volume": float(k["v"]),  # base volume
            "candle_acc_trade_price": float(k["q"]),   # quote volume
            "timestamp": int(data["E"])                # event time (ms)
        }

        self.handle_candle_1s(candle)


    def handle_ticker(self, data: dict):
        event_dt = datetime.fromtimestamp(data["timestamp"] / 1000, tz=timezone.utc)
        minute_dt = event_dt.replace(second=0, microsecond=0)

        symbol = data["symbol"]  # BTCUSDT

        # ✅ 단일 시작이면 하드코딩/간이 파싱 OK
        # 나중에 확장할 때는 exchangeInfo로 base/quote 안전하게 매핑 추천
        if symbol.endswith("USDT"):
            base, quote = symbol[:-4], "USDT"
        elif symbol.endswith("USDC"):
            base, quote = symbol[:-4], "USDC"
        else:
            # fallback
            base, quote = symbol, "UNKNOWN"

        ticker_message = {
            "exchange": "binance",
            "market": f"{quote}-{base}",  # 예: "USDT-BTC"
            "asset": base,
            "price": data["price"],
            "acc_vol_24h": data["acc_vol_24h"],
            "signed_change_rate": data["signed_change_rate"],
            "event_time_ms": data["timestamp"],
            "event_time": event_dt.isoformat(),
            "minute_ts": minute_dt.isoformat()
        }

        self.producer.send(
            topic=self.ticker_topic,
            key=ticker_message["market"],   # ✅ market 기준 파티셔닝
            value=ticker_message
        )

        print("[KAFKA_binance_ticker SENT]", ticker_message)

    def handle_candle_1s(self, candle_message: dict):
        """
        비즈니스 로직 처리 지점 
        - Kafka Producer 전송 (candle)
        - 로그 저장
        - Spark Streaming 입력 등으로 확장 가능
        """      

        self.producer.send(
            topic=self.candle_topic,
            key=candle_message["market"],
            value=candle_message
        )
        
        print("[KAFKA_binance_candle_1s SENT]", candle_message)
        # print("candle_1s:",data)

    def on_error(self, ws, error):
        print("Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        print("Closed", close_status_code, close_msg)

    def start(self):
        url = self.build_ws_url()

        self.ws = websocket.WebSocketApp(
            url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        self.ws.run_forever(ping_interval=30)


if __name__ == "__main__":
    # 1) ✅ 단일로 시작
    # client = BinanceTickerWS(symbols=["btcusdt"])

    # 2) ✅ 여러 마켓으로 확장(바로 멀티스트림 구독형태)
    client = BinanceTickerWS(symbols=["btcusdt", "ethusdt", "xrpusdt","zilusdt","dogeusdt","solusdt","auctionusdt","ensousdt"])

    ws_thread = threading.Thread(target=client.start, daemon=True)
    ws_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Ctrl+C detected. Closing WebSocket...")

        if client.ws:
            client.ws.close()
        if client.producer:
            client.producer.flush()
            client.producer.close()

        ws_thread.join()
        print("[SHUTDOWN] WebSocket closed. Bye 👋")

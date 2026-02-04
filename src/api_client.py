import websocket
import threading
import uuid
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer
import json


class UpbitTickerWS:
    """
    Upbit WebSocket Ticker Client
    - 실시간 시세(ticker) 데이터를 수신
    - 필요한 필드만 정규화하여 downstream(Kafka, Spark 등)으로 전달
    """

    def __init__(self, codes):
        # 구독할 마켓 코드 (기본: KRW-BTC)
        self.codes = codes or ["KRW-BTC"]
        self.ws = None

        # Kafka 설정
        self.ticker_topic = "market-ticker"
        self.candle_topic = "market-candle-1s"

        self.producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,                 #안정성, 충분한 성능
            max_in_flight_requests_per_connection=5, #5이하로 제한해야 순서 보장
            linger_ms=5
        )


    def on_open(self, ws):
        """
        WebSocket 연결이 열리면 호출
        - Upbit WebSocket 구독 메시지 전송
        """
        print("WebSocket connected")

        subscribe_message = [
            # 연결 식별용 ticket (임의 UUID)
            {"ticket": str(uuid.uuid4())},
            {
                # ticker: 현재가, 변동률, 누적 거래량 등 '시장 상태' 데이터
                "type": "ticker",
                "codes": self.codes,
                # 실시간 데이터만 수신 (스냅샷 제외)
                "isOnlyRealtime": True
            },
            # 2️⃣ 1초봉 캔들 구독
            {   
                # 실시간 1초봉 캔들 스트림
                "type": "candle.1s",
                "codes": self.codes
            }
        ]

        # 구독 메시지를 JSON 형태로 전송
        ws.send(json.dumps(subscribe_message))

    def on_message(self, ws, message):
        """
        WebSocket으로부터 메시지를 수신할 때마다 호출
        - 원본 JSON 파싱
        - 프로젝트에서 사용할 필드만 추출 및 타입 정리
        - on_message
        ├─ type == "ticker"     → ticker 파싱 → handle_ticker
        └─ type == "candle.1s"  → candle 파싱 → handle_candle_1s
        └─ type == "candle.1m"  → candle 파싱 → handle_candle_1m
        """
        try:
            # 수신 데이터(JSON 문자열 → dict)
            if isinstance(message, (bytes, bytearray)):
                message = message.decode("utf-8") # upbit가 기본적으로 bytes로 들어와서 
            data = json.loads(message)
            msg_type = data.get("type")

            if msg_type == "ticker":
                self.parse_ticker(data)

            elif msg_type == "candle.1s":
                self.parse_candle_1s(data)
            
            else:
                print("Unknown message type:", msg_type)


        except Exception as e:
            # 파싱 실패 또는 필드 누락 시
            print("Parse error:", e)
            print("Raw message:", message)

    # Upbit 원본 필드 → 프로젝트 표준 스키마로 정규화
    def parse_ticker(self, data):
        ticker = {
            "code": data["code"],   # 마켓 코드 (KRW-BTC)
            "trade_price": float(data["trade_price"]),  # 현재가
            "acc_trade_volume_24h": float(data["acc_trade_volume_24h"]), # 24시간 누적 거래량
            "timestamp": int(data["timestamp"]),  # 이벤트 시각 (ms)
            "signed_change_rate": float(data["signed_change_rate"])  # 전일 대비 변동률
        }

        self.handle_ticker(ticker)
    # Upbit 원본 필드 → 프로젝트 표준 스키마로 정규화
    def parse_candle_1s(self, data):
        candle = {
            "type" : data["type"],   # 캔들타입 1s
            "market": data["code"],   #마켓코드
            "asset": data["code"].split("-")[1], #BTC 내부매핑
            "opening_price" : data["opening_price"], #시가
            "high_price" : data["high_price"], #고가
            "low_price" : data["low_price"], #저가
            "trade_price" : data["trade_price"], #종가
            "candle_acc_trade_volume" : data["candle_acc_trade_volume"], #분 거래량
            "candle_acc_trade_price" : data["candle_acc_trade_price"], #분 거래대금
            "timestamp": int(data["timestamp"])
        }

        self.handle_candle_1s(candle)

    def handle_ticker(self, data: dict):
        """
        비즈니스 로직 처리 지점 
        - Kafka Producer 전송 (ticker)
        - 로그 저장
        - Spark Streaming 입력 등으로 확장 가능
        """

        event_dt = datetime.fromtimestamp(
            data["timestamp"] / 1000, tz=timezone.utc
        )

        minute_dt = event_dt.replace(second=0, microsecond=0)
        #카프카에 추가된 파생 컬럼
        ticker_message = {
            "exchange": "upbit",
            "market": data["code"],
            "asset": data["code"].split("-")[1],
            "price": data["trade_price"],
            "acc_vol_24h": data["acc_trade_volume_24h"],
            "signed_change_rate": data["signed_change_rate"],
            "event_time_ms": data["timestamp"],
            "event_time": event_dt.isoformat(),
            "minute_ts": minute_dt.isoformat()
        }
        
        #ticker_topic
        self.producer.send(
            topic=self.ticker_topic,
            key=ticker_message["market"],
            value=ticker_message
        )

        # 디버그 로그
        # print("[KAFKA_ticker SENT]", ticker_message)
        # print("ticker:",data)

    def handle_candle_1s(self, data):
        """
        비즈니스 로직 처리 지점 
        - Kafka Producer 전송 (candle)
        - 로그 저장
        - Spark Streaming 입력 등으로 확장 가능
        """
        event_dt = datetime.fromtimestamp(
            data["timestamp"] / 1000, tz=timezone.utc
        )

        minute_dt = event_dt.replace(second=0, microsecond=0)
        candle_message = {
            "exchange": "upbit",
            "type": data["type"],
            "market": data["market"],
            "asset": data["asset"],
            "opening_price": data["opening_price"],
            "high_price": data["high_price"],
            "low_price": data["low_price"],
            "trade_price": data["trade_price"],
            "candle_acc_trade_volume": data["candle_acc_trade_volume"],
            "candle_acc_trade_price": data["candle_acc_trade_price"],
            "timestamp": int(data["timestamp"])
        }

        self.producer.send(
            topic=self.candle_topic,
            key=candle_message["market"],
            value=candle_message
        )
        # print("[KAFKA_candle_1s SENT]", candle_message)
        # print("candle_1s:",data)

    def on_error(self, ws, error):
        """
        WebSocket 에러 발생 시 호출
        """
        print("Error:", error)

    def on_close(self, ws, close_status_code, close_msg):
        """
        WebSocket 연결 종료 시 호출
        """
        print("Closed")

    def start(self):
        """
        WebSocket 클라이언트 실행
        - 이벤트 핸들러(on_open, on_message 등) 등록
        - ping 설정으로 연결 유지
        """
        self.ws = websocket.WebSocketApp(
            "wss://api.upbit.com/websocket/v1",
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        # 30초마다 ping 전송하여 연결 유지
        self.ws.run_forever(ping_interval=30)


if __name__ == "__main__":
    # Ticker WebSocket 클라이언트 생성
    CODES = ["KRW-BTC","KRW-ETH","KRW-XRP","KRW-ZIL","KRW-USDT","KRW-POKT","KRW-DOGE","KRW-AUCTION","KRW-F"]
    client = UpbitTickerWS(CODES)

    # 메인 스레드 블로킹 방지를 위해 별도 스레드에서 실행
    ws_thread = threading.Thread(target=client.start,daemon=True)
    ws_thread.start()
    
    try:
        # 프로그램 종료 방지용 루프
        while True:
            time.sleep(1)
            #
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Ctrl+C detected. Closing WebSocket...")
        
        #명시적으로 ws.close()를 호출해야 안전하다
        # WebSocket 정상 종료
        if client.ws:
            client.ws.close()
        # producer 정상 종료
        if client.producer:
            client.producer.flush()
            client.producer.close()

        # WebSocket 스레드 종료 대기
        ws_thread.join()

        print("[SHUTDOWN] WebSocket closed. Bye 👋")
    

        

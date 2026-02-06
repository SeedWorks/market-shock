import requests
from pyspark.sql import SparkSession

def send_discord_alert(webhook_url, message):
    """디스코드 웹후크를 통해 메시지를 보냅니다."""
    payload = {
        "username": "Spark Bot", # 봇 이름 설정
        "content": message       # 보낼 메시지 내용
    }
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status() # 에러 발생 시 예외 처리
        print("Discord notification sent successfully")
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Discord notification: {e}")

# Spark 세션 시작
spark = SparkSession.builder.appName("DiscordAlertApp").getOrCreate()

# 1. 웹후크 URL 설정
DISCORD_WEBHOOK_URL = "YOUR_DISCORD_WEBHOOK_URL_HERE"

# 2. 작업 처리 (예시)
try:
    # ... Spark 작업 코드 (df.write, ml.fit 등) ...
    
    # 3. 성공 알림
    send_discord_alert(DISCORD_WEBHOOK_URL, "✅ Spark 작업이 성공적으로 완료되었습니다!")
except Exception as e:
    # 4. 실패 알림
    send_discord_alert(DISCORD_WEBHOOK_URL, f"❌ Spark 작업 실패: {str(e)}")
finally:
    spark.stop()

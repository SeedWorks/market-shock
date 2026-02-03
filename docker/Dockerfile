FROM python 3:11-slim
ENV PYTHONUNBUFFERED=1 \
TZ = Asia/Seoul

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cahe-dir -r requirements.txt

CMD ["bash"]


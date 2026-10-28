FROM python:3.12-slim
WORKDIR /app
COPY requirements-kafka.txt .
RUN pip install --no-cache-dir -r requirements-kafka.txt
COPY transit ./transit
COPY dashboard ./dashboard
RUN mkdir /data
CMD ["python", "-m", "transit.app", "kafka", "--host", "0.0.0.0", "--db", "/data/transit.db"]

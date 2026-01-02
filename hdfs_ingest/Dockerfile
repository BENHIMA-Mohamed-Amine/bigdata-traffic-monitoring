FROM python:3.9-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

COPY hdfs_consumer.py .

CMD ["python", "hdfs_consumer.py"]

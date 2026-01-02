FROM python:3.9-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

COPY traffic_generator.py .

CMD ["python", "traffic_generator.py"]

2.  **Processing**: Using Apache Spark to perform batch aggregations (e.g., calculating average traffic per zone) that serve as business indicators.
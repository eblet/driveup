FROM python:3.10-slim AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.10-slim

WORKDIR /app

ENV PYTHONPATH="/app/src" \
    PYTHONUNBUFFERED=1

COPY --from=builder /install /usr/local

COPY . .


CMD ["python", "main.py"]
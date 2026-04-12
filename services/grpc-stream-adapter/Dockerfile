FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/generated:/app

COPY services/proto ./proto
COPY services/grpc-stream-adapter/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/generated && \
    python -m grpc_tools.protoc \
      -I /app/proto \
      --python_out=/app/generated \
      --grpc_python_out=/app/generated \
      /app/proto/ecg_ingestion.proto

COPY services/grpc-stream-adapter/main.py ./app/main.py

CMD ["python", "-m", "app.main"]
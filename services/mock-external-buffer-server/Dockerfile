FROM python:3.12-slim

WORKDIR /app

ENV pythonunbuffered=1
ENV pythonpath=/app

COPY services/mock-external-buffer-server/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY services/mock-external-buffer-server/main.py ./app/main.py

CMD ["python", "-m", "app.main"]
FROM python:3.11-slim

WORKDIR /app

COPY fog/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common /app/common
COPY fog /app/fog

ENV PYTHONPATH=/app

CMD ["python", "-m", "fog.app"]

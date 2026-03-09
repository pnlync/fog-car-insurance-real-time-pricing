FROM python:3.11-slim

WORKDIR /app

COPY edge/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common /app/common
COPY edge /app/edge
COPY data /app/data

ENV PYTHONPATH=/app

CMD ["python", "-m", "edge.app"]

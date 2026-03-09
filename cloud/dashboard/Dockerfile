FROM python:3.11-slim

WORKDIR /app

COPY cloud/dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY common /app/common
COPY cloud /app/cloud

ENV PYTHONPATH=/app
ENV PORT=8080

CMD ["python", "-m", "cloud.dashboard.app"]

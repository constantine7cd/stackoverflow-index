FROM python:3.11.9-slim

WORKDIR /app

COPY . /app

RUN apt-get update \
    && apt-get -y install libpq-dev \
    && apt-get install -y --no-install-recommends git \
    && apt-get purge -y --auto-remove \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "app.py"]

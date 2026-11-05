FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgssapi-krb5-2 libltdl7 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
ENV PYTHONUNBUFFERED=1

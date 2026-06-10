FROM registry.cern.ch/cern-sis/base-images/apache/airflow:3.1.7-python3.11

ENV PYTHONUNBUFFERED=0
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=INFO

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    libssl-dev \
    kstart \
    curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

FROM registry.cern.ch/cern-sis/base-images/apache/airflow:3.1.7-python3.11

ENV PYTHONBUFFERED=0
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=INFO

ENV PYTHONPATH="/opt/airflow/refactory:${PYTHONPATH}"

USER root
RUN apt-get update \
 && apt-get install -y kstart \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*


USER airflow

ARG AIRFLOW_VERSION=3.1.7
ARG PYTHON_VERSION=3.11

COPY airflow/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


COPY airflow/dags ./dags


COPY refactory ./refactory

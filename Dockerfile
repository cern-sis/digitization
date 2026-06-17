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


COPY airflow/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


COPY airflow/dags ./dags


COPY refactory ./refactory

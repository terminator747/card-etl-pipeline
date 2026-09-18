FROM apache/airflow:3.3.2

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==3.3.2" \
    "pyspark==4.0.1"

COPY --chown=airflow:root spark_jobs /opt/airflow/spark_jobs
COPY --chown=airflow:root data/raw /opt/airflow/data/raw
COPY --chown=airflow:root config /opt/airflow/config
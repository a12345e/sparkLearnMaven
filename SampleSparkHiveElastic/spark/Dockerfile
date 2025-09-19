FROM bitnami/spark:3.5.3

USER root
RUN apt-get update && apt-get install -y wget curl && rm -rf /var/lib/apt/lists/*

COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
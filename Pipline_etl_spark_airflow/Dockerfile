FROM apache/airflow:2.8.0-python3.11

USER root

# Installer Java pour Spark
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Variables d'environnement Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Installer PySpark et dépendances
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
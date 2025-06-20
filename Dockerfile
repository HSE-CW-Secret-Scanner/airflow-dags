# Install Git and dependencies
FROM apache/airflow:2.5.3-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=${AIRFLOW_HOME}

# Install Python requirements
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
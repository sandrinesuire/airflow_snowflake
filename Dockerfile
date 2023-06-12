# Extend over the airflow base image
FROM apache/airflow:2.5.3
USER root
# Install any root level apt packages
RUN apt-get update && \
  apt-get install -y --no-install-recommends build-essential
# Run remaining commands as the airflow runtime user
USER airflow
# Install airflow related files onto the host
COPY ./plugins /opt/airflow/plugins
COPY ./dags /data/airflow/dags
# Install pip packages into the container that DAG code needs
COPY ./requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip
RUN pip install -r /tmp/requirements.txt
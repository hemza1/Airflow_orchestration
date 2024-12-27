FROM apache/airflow:2.10.4
COPY requirements.txt /
RUN pip install --no-deps --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
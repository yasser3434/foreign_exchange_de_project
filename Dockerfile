FROM apache/airflow:3.1.7

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

FROM apache/airflow:slim-3.3.0

USER airflow
COPY ./requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY ./dbt_requirements.txt /
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /dbt_requirements.txt

CMD [ "standalone" ]
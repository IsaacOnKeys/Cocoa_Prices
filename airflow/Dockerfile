FROM apache/airflow:2.10.2-python3.11@sha256:7c68d8ac3cf1b2c69eeb91da64c1578dc8376d33abf34267bf6e547d071cae39

COPY airflow/requirements.txt /requirements.txt
COPY airflow/constraints-python311.txt /constraints-python311.txt
RUN python -m pip install --no-cache-dir -r /requirements.txt -c /constraints-python311.txt \
    && python -m pip check

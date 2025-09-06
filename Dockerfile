FROM apache/airflow:2.9.1

# Instala dependências do requirements.txt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

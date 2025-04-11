FROM apache/airflow:2.7.3-python3.11

USER root

# Instalando dependências do sistema necessárias
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        default-libmysqlclient-dev \
        pkg-config \
        postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copiando requirements.txt
COPY --chown=airflow:root requirements.txt /requirements.txt

# Instalando dependências Python
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

# Definindo variáveis de ambiente do Airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
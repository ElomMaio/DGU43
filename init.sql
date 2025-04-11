-- Criar usuário e banco de dados para o Airflow
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Conectar ao banco de dados Airflow
\c airflow

-- Configurações específicas (opcional)
CREATE SCHEMA IF NOT EXISTS airflow AUTHORIZATION airflow;
GRANT ALL ON SCHEMA airflow TO airflow;
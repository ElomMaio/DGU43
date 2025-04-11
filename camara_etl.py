
from datetime import datetime, timedelta
import os
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from utils.api_cliente import CamaraApiClient
from utils.transformacoes import (
    clean_deputies_data, 
    clean_propositions_data, 
    process_votes_data,
    create_analytical_view
)
from utils.check_qualidade import (
    check_deputies_data,
    check_propositions_data,
    check_votes_data,
    check_analytical_view
)

# Configurações
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_DIR = '/opt/airflow/data'
RAW_DIR = f'{DATA_DIR}/raw'
PROCESSED_DIR = f'{DATA_DIR}/processed'
FINAL_DIR = f'{DATA_DIR}/final'

# Funções para os operadores
def extract_deputies(**kwargs):
    """Extrai dados de deputados da API da Câmara."""
    client = CamaraApiClient()
    deputies_df = client.get_deputies()
    
    if deputies_df is not None:
        # Criar diretório se não existir
        os.makedirs(RAW_DIR, exist_ok=True)
        
        # Salvar dados brutos
        file_path = f"{RAW_DIR}/deputados_{datetime.now().strftime('%Y%m%d')}.csv"
        deputies_df.to_csv(file_path, index=False)
        
        logging.info(f"Dados de deputados salvos em {file_path}")
        return file_path
    else:
        raise ValueError("Falha ao extrair dados de deputados")

def extract_propositions(**kwargs):
    """Extrai dados de proposições da API da Câmara."""
    client = CamaraApiClient()
    # Extrair proposições do ano atual
    current_year = datetime.now().year
    propositions_df = client.get_propositions(year=current_year)
    
    if propositions_df is not None:
        # Criar diretório se não existir
        os.makedirs(RAW_DIR, exist_ok=True)
        
        # Salvar dados brutos
        file_path = f"{RAW_DIR}/proposicoes_{current_year}_{datetime.now().strftime('%Y%m%d')}.csv"
        propositions_df.to_csv(file_path, index=False)
        
        logging.info(f"Dados de proposições salvos em {file_path}")
        return file_path
    else:
        raise ValueError("Falha ao extrair dados de proposições")

def extract_votes(**kwargs):
    """Extrai dados de votações da API da Câmara."""
    client = CamaraApiClient()
    ti = kwargs['ti']
    
    # Obter caminho do arquivo de proposições
    propositions_file = ti.xcom_pull(task_ids='extract_propositions')
    
    import pandas as pd
    # Ler arquivo de proposições
    propositions_df = pd.read_csv(propositions_file)
    
    # Extrair IDs das 10 primeiras proposições (para demonstração)
    proposition_ids = propositions_df['id'].head(10).tolist()
    
    all_votes = []
    for prop_id in proposition_ids:
        votes_df = client.get_votes(prop_id)
        if votes_df is not None and not votes_df.empty:
            votes_df['proposicaoId'] = prop_id
            all_votes.append(votes_df)
    
    if all_votes:
        # Combinar resultados
        combined_votes = pd.concat(all_votes, ignore_index=True)
        
        # Criar diretório se não existir
        os.makedirs(RAW_DIR, exist_ok=True)
        
        # Salvar dados brutos
        file_path = f"{RAW_DIR}/votacoes_{datetime.now().strftime('%Y%m%d')}.csv"
        combined_votes.to_csv(file_path, index=False)
        
        logging.info(f"Dados de votações salvos em {file_path}")
        return file_path
    else:
        logging.warning("Nenhum dado de votação encontrado")
        return None

def transform_deputies(**kwargs):
    """Transforma dados brutos de deputados."""
    ti = kwargs['ti']
    
    # Obter caminho do arquivo de deputados
    deputies_file = ti.xcom_pull(task_ids='extract_deputies')
    
    import pandas as pd
    # Ler arquivo de deputados
    deputies_df = pd.read_csv(deputies_file)
    
    # Aplicar transformações
    transformed_df = clean_deputies_data(deputies_df)
    
    # Verificar qualidade
    if not check_deputies_data(transformed_df):
        raise ValueError("Falha na verificação de qualidade dos dados de deputados")
    
    # Criar diretório se não existir
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    # Salvar dados processados
    file_path = f"{PROCESSED_DIR}/deputados_processados_{datetime.now().strftime('%Y%m%d')}.csv"
    transformed_df.to_csv(file_path, index=False)
    
    logging.info(f"Dados de deputados processados salvos em {file_path}")
    return file_path

def transform_propositions(**kwargs):
    """Transforma dados brutos de proposições."""
    ti = kwargs['ti']
    
    # Obter caminho do arquivo de proposições
    propositions_file = ti.xcom_pull(task_ids='extract_propositions')
    
    import pandas as pd
    # Ler arquivo de proposições
    propositions_df = pd.read_csv(propositions_file)
    
    # Aplicar transformações
    transformed_df = clean_propositions_data(propositions_df)
    
    # Verificar qualidade
    if not check_propositions_data(transformed_df):
        raise ValueError("Falha na verificação de qualidade dos dados de proposições")
    
    # Criar diretório se não existir
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    
    # Salvar dados processados
    file_path = f"{PROCESSED_DIR}/proposicoes_processadas_{datetime.now().strftime('%Y%m%d')}.csv"
    transformed_df.to_csv(file_path, index=False)
    
    logging.info(f"Dados de proposições processados salvos em {file_path}")
    return file_path

def create_analytics(**kwargs):
    """Cria visão analítica combinando dados processados."""
    ti = kwargs['ti']
    
    # Obter caminhos dos arquivos processados
    deputies_file = ti.xcom_pull(task_ids='transform_deputies')
    propositions_file = ti.xcom_pull(task_ids='transform_propositions')
    
    import pandas as pd
    # Ler arquivos processados
    deputies_df = pd.read_csv(deputies_file)
    propositions_df = pd.read_csv(propositions_file)
    
    # Tentar obter dados de votações
    votes_file = ti.xcom_pull(task_ids='extract_votes')
    votes_df = None
    if votes_file:
        votes_df = pd.read_csv(votes_file)
    
    # Criar visão analítica
    analytical_view = create_analytical_view(deputies_df, propositions_df, votes_df)
    
    # Verificar qualidade
    if not check_analytical_view(analytical_view):
        raise ValueError("Falha na verificação de qualidade da visão analítica")
    
    # Criar diretório se não existir
    os.makedirs(FINAL_DIR, exist_ok=True)
    
    # Salvar visão analítica
    file_path = f"{FINAL_DIR}/visao_analitica_{datetime.now().strftime('%Y%m%d')}.csv"
    analytical_view.to_csv(file_path, index=False)
    
    logging.info(f"Visão analítica salva em {file_path}")
    return file_path

# Definição da DAG
with DAG(
    'camara_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para dados da Câmara dos Deputados',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['camara', 'etl', 'portfolio'],
) as dag:
    
    # Início do pipeline
    start_pipeline = DummyOperator(
        task_id='start_pipeline',
    )
    
    # Extração de dados
    extract_deputies_task = PythonOperator(
        task_id='extract_deputies',
        python_callable=extract_deputies,
    )
    
    extract_propositions_task = PythonOperator(
        task_id='extract_propositions',
        python_callable=extract_propositions,
    )
    
    extract_votes_task = PythonOperator(
        task_id='extract_votes',
        python_callable=extract_votes,
    )
    
    # Transformação de dados
    transform_deputies_task = PythonOperator(
        task_id='transform_deputies',
        python_callable=transform_deputies,
    )
    
    transform_propositions_task = PythonOperator(
        task_id='transform_propositions',
        python_callable=transform_propositions,
    )
    
    # Criação de análises
    create_analytics_task = PythonOperator(
        task_id='create_analytics',
        python_callable=create_analytics,
    )

    # Fim do pipeline
    end_pipeline = DummyOperator(
        task_id='end_pipeline',
    )
    
    # Definir dependências entre tarefas
    start_pipeline >> [extract_deputies_task, extract_propositions_task]
    
    extract_deputies_task >> transform_deputies_task
    extract_propositions_task >> transform_propositions_task
    extract_propositions_task >> extract_votes_task
    
    [transform_deputies_task, transform_propositions_task, extract_votes_task] >> create_analytics_task
    
    create_analytics_task >> end_pipeline
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def check_deputies_data(df):
   
    if df is None or df.empty:
        logger.error("DataFrame de deputados vazio ou None")
        return False
    
    required_columns = ['id', 'nome', 'siglaPartido', 'siglaUf']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Colunas obrigatórias faltando: {missing_columns}")
        return False
    
    critical_fields = ['id', 'nome']
    null_counts = df[critical_fields].isnull().sum()
    
    if null_counts.sum() > 0:
        logger.error(f"Campos críticos com valores nulos: {null_counts[null_counts > 0].to_dict()}")
        return False
    
    duplicate_ids = df[df.duplicated('id')]['id'].tolist()
    
    if duplicate_ids:
        logger.error(f"IDs duplicados encontrados: {duplicate_ids}")
        return False
    
    valid_states = ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 
                   'MA', 'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 
                   'RJ', 'RN', 'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO']
    
    invalid_states = df[~df['siglaUf'].isin(valid_states)]['siglaUf'].unique().tolist()
    
    if invalid_states:
        logger.error(f"Estados inválidos encontrados: {invalid_states}")
        return False
    
    logger.info("Verificação de qualidade dos dados de deputados: PASSOU")
    return True

def check_propositions_data(df):
    
    if df is None or df.empty:
        logger.error("DataFrame de proposições vazio ou None")
        return False
    
    
    required_columns = ['id', 'siglaTipo', 'numero', 'ano']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Colunas obrigatórias faltando: {missing_columns}")
        return False
    
    
    critical_fields = ['id', 'siglaTipo', 'numero']
    null_counts = df[critical_fields].isnull().sum()
    
    if null_counts.sum() > 0:
        logger.error(f"Campos críticos com valores nulos: {null_counts[null_counts > 0].to_dict()}")
        return False
    
    
    duplicate_ids = df[df.duplicated('id')]['id'].tolist()
    
    if duplicate_ids:
        logger.error(f"IDs duplicados encontrados: {duplicate_ids}")
        return False
    
    
    current_year = pd.Timestamp.now().year
    invalid_years = df[(df['ano'] < 1900) | (df['ano'] > current_year)]['ano'].unique().tolist()
    
    if invalid_years:
        logger.error(f"Anos inválidos encontrados: {invalid_years}")
        return False
    
    logger.info("Verificação de qualidade dos dados de proposições: PASSOU")
    return True

def check_votes_data(df):
   
    if df is None or df.empty:
        logger.error("DataFrame de votações vazio ou None")
        return False
    
    
    required_columns = ['id', 'data', 'proposicaoObjeto']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Colunas obrigatórias faltando: {missing_columns}")
        return False
    
    
    critical_fields = ['id']
    if 'critical_fields' in df.columns:
        null_counts = df[critical_fields].isnull().sum()
        
        if null_counts.sum() > 0:
            logger.error(f"Campos críticos com valores nulos: {null_counts[null_counts > 0].to_dict()}")
            return False
    
    logger.info("Verificação de qualidade dos dados de votações: PASSOU")
    return True

def check_analytical_view(df):
    
    if df is None or df.empty:
        logger.error("DataFrame de visão analítica vazio ou None")
        return False
    
    # Verificação 1: DataFrame não vazio
    if len(df) == 0:
        logger.error("Visão analítica sem registros")
        return False
    
    # Verificação 2: Valores numéricos válidos
    numeric_columns = df.select_dtypes(include=['number']).columns
    
    for col in numeric_columns:
        if df[col].isnull().any():
            logger.error(f"Coluna numérica {col} contém valores nulos")
            return False
    
    logger.info("Verificação de qualidade da visão analítica: PASSOU")
    return True
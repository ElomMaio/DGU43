from typing import Optional
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

REGION_MAP = {
    'AC': 'Norte', 'AM': 'Norte', 'AP': 'Norte', 'PA': 'Norte', 
    'RO': 'Norte', 'RR': 'Norte', 'TO': 'Norte',
    'AL': 'Nordeste', 'BA': 'Nordeste', 'CE': 'Nordeste', 
    'MA': 'Nordeste', 'PB': 'Nordeste', 'PE': 'Nordeste', 
    'PI': 'Nordeste', 'RN': 'Nordeste', 'SE': 'Nordeste',
    'DF': 'Centro-Oeste', 'GO': 'Centro-Oeste', 'MS': 'Centro-Oeste', 'MT': 'Centro-Oeste',
    'ES': 'Sudeste', 'MG': 'Sudeste', 'RJ': 'Sudeste', 'SP': 'Sudeste',
    'PR': 'Sul', 'RS': 'Sul', 'SC': 'Sul'
}

def validate_dataframe(df: pd.DataFrame, name: str) -> bool:
    if df is None or df.empty:
        logger.warning(f"DataFrame {name} vazio ou None")
        return False
    return True

def convert_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    date_cols = [col for col in df.columns if "data" in col.lower()]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def clean_deputies_data(df: pd.DataFrame) -> pd.DataFrame:
    if not validate_dataframe(df, "deputados"):
        return pd.DataFrame()
    
    result = df.copy()
    result = convert_date_columns(result)
    
    if 'siglaUf' in result.columns:
        result['regiao'] = result['siglaUf'].map(REGION_MAP)
    
    result['data_processamento'] = datetime.now()
    
    logger.info(f"Dados de deputados processados: {len(result)} registros")
    return result

def clean_propositions_data(df: pd.DataFrame) -> pd.DataFrame:
    if not validate_dataframe(df, "proposições"):
        return pd.DataFrame()
    
    result = df.copy()
    result = convert_date_columns(result)
    
    if 'dataApresentacao' in result.columns:
        result['anoApresentacao'] = result['dataApresentacao'].dt.year
    
    if all(col in result.columns for col in ['siglaTipo', 'numero']):
        result['identificacao'] = result.apply(
            lambda x: f"{x['siglaTipo']} {str(x['numero'])}", axis=1
        )
    
    result['data_processamento'] = datetime.now()
    
    logger.info(f"Dados de proposições processados: {len(result)} registros")
    return result

@dataclass
class VoteAnalysis:
    total_votos: int
    coesao_percentual: float

def process_votes_data(votes_df: pd.DataFrame, vote_details_df: pd.DataFrame) -> pd.DataFrame:
    if not all(validate_dataframe(df, name) for df, name in [
        (votes_df, "votações"), 
        (vote_details_df, "detalhes de votos")
    ]):
        return pd.DataFrame()
    
    required_cols = ['siglaPartido', 'voto']
    if not all(col in vote_details_df.columns for col in required_cols):
        logger.error(f"Colunas necessárias ausentes: {required_cols}")
        return pd.DataFrame()
    
    party_votes = vote_details_df.groupby(['siglaPartido', 'voto']).size().unstack(fill_value=0)
    party_totals = party_votes.sum(axis=1)
    party_cohesion = party_votes.max(axis=1) / party_totals * 100
    
    return pd.DataFrame({
        'total_votos': party_totals,
        'coesao_percentual': party_cohesion
    })

def create_analytical_view(deputies_df: pd.DataFrame, propositions_df: pd.DataFrame, 
                         votes_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    if not all(validate_dataframe(df, name) for df, name in [
        (deputies_df, "deputados"), 
        (propositions_df, "proposições")
    ]):
        return pd.DataFrame()
    
    required_cols = {
        'deputies': ['siglaPartido'],
        'propositions': ['autor']
    }
    
    if not all(col in deputies_df.columns for col in required_cols['deputies']):
        logger.error(f"Colunas necessárias ausentes em deputies_df: {required_cols['deputies']}")
        return pd.DataFrame()
        
    if not all(col in propositions_df.columns for col in required_cols['propositions']):
        logger.error(f"Colunas necessárias ausentes em propositions_df: {required_cols['propositions']}")
        return pd.DataFrame()
    
    return pd.DataFrame({
        'data_processamento': datetime.now(),
        'total_deputados': len(deputies_df),
        'total_proposicoes': len(propositions_df)
    }, index=[0])
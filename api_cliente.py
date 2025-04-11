"""
Cliente para API de Dados Abertos da Câmara dos Deputados
"""
import requests
import pandas as pd
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CamaraApiClient:
    """Cliente para a API de Dados Abertos da Câmara dos Deputados."""
    
    def __init__(self):
        self.base_url = "https://dadosabertos.camara.leg.br/api/v2"
    
    def get_data(self, endpoint, params=None):
        
        url = f"{self.base_url}/{endpoint}"
        logger.info(f"Fazendo requisição para: {url}")
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Erro HTTP: {e}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na requisição: {e}")
            return None
        except json.JSONDecodeError:
            logger.error("Erro ao decodificar JSON")
            return None
    
    def get_deputies(self, status="exercise"):
        
        params = {
            "ordem": "ASC", 
            "ordenarPor": "nome"
        }
        
        if status:
            params["siglaSituacao"] = status
            
        data = self.get_data("deputados", params)
        
        if data and "dados" in data:
            return pd.DataFrame(data["dados"])
        return None
    
    def get_deputy_details(self, deputy_id):
        """Obtém detalhes de um deputado específico."""
        data = self.get_data(f"deputados/{deputy_id}")
        if data and "dados" in data:
            return data["dados"]
        return None
    
    def get_propositions(self, year=None, proposition_type=None, limit=100):
        
        params = {"itens": limit}
        
        if year:
            params["ano"] = year
            
        if proposition_type:
            params["siglaTipo"] = proposition_type
            
        data = self.get_data("proposicoes", params)
        
        if data and "dados" in data:
            return pd.DataFrame(data["dados"])
        return None
    
    def get_votes(self, proposition_id):
        
        data = self.get_data(f"proposicoes/{proposition_id}/votacoes")
        if data and "dados" in data:
            return pd.DataFrame(data["dados"])
        return None
    
    def get_vote_details(self, vote_id):
        
        data = self.get_data(f"votacoes/{vote_id}/votos")
        if data and "dados" in data:
            return pd.DataFrame(data["dados"])
        return None
    
    def get_parties(self):
        
        data = self.get_data("partidos")
        if data and "dados" in data:
            return pd.DataFrame(data["dados"])
        return None
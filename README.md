# DGU43
# Projeto: Análise de Dados da Câmara dos Deputados
Este projeto consiste em um pipeline de engenharia de dados completo para extrair, processar, analisar e visualizar dados públicos da Câmara dos Deputados do Brasil. Utilizamos a API de Dados Abertos da Câmara para obter informações sobre deputados, proposições legislativas e votações.
###Arquitetura
O projeto segue uma arquitetura de engenharia de dados moderna:
- Extração de Dados: API REST dos Dados Abertos da Câmara
- Processamento: Apache Airflow para orquestração de pipelines
- Armazenamento: Sistema de arquivos (CSV) com possibilidade de expansão para bancos de dados
- Análise: Python (Pandas) para transformação e análise
- Visualização: Streamlit para dashboard interativo e/ou Apache Superset
###Componentes
1. Extração de Dados
Utilizamos a API de Dados Abertos da Câmara (https://dadosabertos.camara.leg.br/swagger/api.html) para extrair:

Perfis dos deputados em exercício
Proposições legislativas
Votações
Partidos políticos

2. Pipeline de Dados
Implementamos dois DAGs no Apache Airflow:

camara_etl_pipeline: Extração, transformação e carregamento de dados
camara_analysis_pipeline: Análises e geração de relatórios

3. Estrutura de Dados
Organizamos os dados em diferentes estágios:

Raw: Dados brutos extraídos da API
Processed: Dados limpos e transformados
Final: Visões analíticas consolidadas
Reports: Relatórios e visualizações

4. Análises Implementadas

Distribuição de deputados por partido
Distribuição de deputados por região
Análise de proposições por tipo
Evolução temporal de proposições

5. Visualizações
Criamos um dashboard interativo utilizando Streamlit com:

Filtros dinâmicos (partido, estado)
Gráficos interativos (barras, pizza, linhas)
Tabelas de dados detalhados
Métricas resumidas

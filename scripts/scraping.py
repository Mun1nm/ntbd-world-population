
"""
scraping.py

Este módulo contém funções para buscar e tratar dados populacionais e históricos
do site Worldometers. Ele realiza o download das tabelas, extrai os links dos países,
e processa os dados (conversões de tipos, renomeação de colunas, cálculo de populações)
de forma assíncrona.
"""

import pandas as pd
import re
import asyncio
import nest_asyncio
import numpy as np
import logging
import sys
import warnings
from requests_html import AsyncHTMLSession
from bs4 import BeautifulSoup
from io import StringIO

# Ignora warnings para manter a saída mais limpa 
warnings.filterwarnings("ignore")

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler('./logs/scraping.log')]
)
logger = logging.getLogger(__name__)

# Permite o uso de asyncio em ambientes como Jupyter Notebook
nest_asyncio.apply()

async def fetch_population_table(session):
    """
    Obtém a página principal de "Population by Country" e extrai a tabela via pandas.
    Em paralelo, utiliza BeautifulSoup para capturar o link de cada país.
    Retorna um DataFrame com uma coluna adicional "Country_URL".
    """
    url = "https://www.worldometers.info/world-population/population-by-country/"
    try:
        response = await session.get(url)
        # Extrai a tabela com pd.read_html
        tables = pd.read_html(StringIO(response.text))
        pop_table = None
        for table in tables:
            table.columns = [re.sub(r'\s+', ' ', str(col)).strip() for col in table.columns]
            if any("2024" in col and "Population" in col for col in table.columns):
                pop_table = table
                break
        if pop_table is None:
            logger.error("Tabela de população não encontrada na página principal.")
            return None

        # Extraindo os links via BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        table_html = soup.find("table", {"id": "example2"})
        country_links_dict = {}
        if table_html:
            body_rows = table_html.find("tbody").find_all("tr", recursive=False)
            for row in body_rows:
                cols = row.find_all("td", recursive=False)
                if len(cols) < 2:
                    continue
                a_tag = cols[1].find("a", href=True)
                if a_tag:
                    country_name = a_tag.text.strip()
                    relative_url = a_tag['href'].strip()
                    full_url = "https://www.worldometers.info" + relative_url
                    country_links_dict[country_name] = full_url
        else:
            logger.warning("Tabela HTML com id='example2' não foi localizada; os links não serão inseridos.")

        # Renomeia a coluna com o nome do país para facilitar o merge
        original_country_col = "Country (or dependency)"
        if original_country_col in pop_table.columns:
            pop_table.rename(columns={original_country_col: "Country"}, inplace=True)
        else:
            logger.warning(f"Coluna '{original_country_col}' não encontrada.")

        pop_table["Country_URL"] = pop_table["Country"].map(country_links_dict)
        return pop_table

    except Exception as e:
        logger.error(f"Erro ao obter a tabela de população: {str(e)}")
        return None

async def process_population_data():
    """
    Processa a tabela principal para gerar um DataFrame com:
      - Country
      - Populacao_Total (da coluna "Population (2024)")
      - Taxa_Crescimento (da coluna "Yearly Change")
      - Urban_Percent (da coluna "Urban Pop %")
      - Populacao_Urbana e Populacao_Rural (calculadas)
      - Country_URL (link para a página do país)
    """
    session = AsyncHTMLSession()
    try:
        pop_table = await fetch_population_table(session)
        if pop_table is None:
            return None
        
        logger.info(f"Tabela principal extraída com {len(pop_table)} linhas e colunas: {list(pop_table.columns)}")
        
        pop_table.rename(columns={
            "Population (2024)": "Populacao_Total",
            "Yearly Change": "Taxa_Crescimento",
            "Urban Pop %": "Urban_Percent"
        }, inplace=True, errors='ignore')
        
        if "Populacao_Total" in pop_table.columns:
            pop_table["Populacao_Total"] = (
                pop_table["Populacao_Total"]
                .astype(str)
                .str.replace(",", "", regex=False)
                .astype(int)
            )
        
        def process_numeric(val):
            val_str = str(val).strip()
            if val_str.upper() in ["N.A.", "NA", "NAN", ""]:
                return np.nan
            try:
                return float(val_str.replace("%", ""))
            except Exception:
                return np.nan
        
        if "Taxa_Crescimento" in pop_table.columns:
            pop_table["Taxa_Crescimento"] = pop_table["Taxa_Crescimento"].apply(process_numeric)
        if "Urban_Percent" in pop_table.columns:
            pop_table["Urban_Percent"] = pop_table["Urban_Percent"].apply(process_numeric)
        
        if "Populacao_Total" in pop_table.columns and "Urban_Percent" in pop_table.columns:
            pop_table["Populacao_Urbana"] = (
                (pop_table["Populacao_Total"] * pop_table["Urban_Percent"] / 100)
                .round(0)
                .astype("Int64")
            )
            pop_table["Populacao_Rural"] = pop_table["Populacao_Total"] - pop_table["Populacao_Urbana"]
        
        cols_desejadas = ["Country", "Populacao_Total", "Taxa_Crescimento", 
                          "Urban_Percent", "Populacao_Urbana", "Populacao_Rural", "Country_URL"]
        df_population = pop_table[cols_desejadas].copy()
        return df_population
    except Exception as e:
        logger.error(f"Erro crítico no processamento dos dados populacionais: {str(e)}")
        return None
    finally:
        await session.close()

def process_historical_table(df, country_name):
    """
    Dado o DataFrame extraído da página histórica de um país,
    seleciona (se existirem) apenas as colunas:
      - Year
      - Population (população total)
      - (Taxa de) Crescimento – coluna que contenha "Change" e "%" (taxa de crescimento)
      - Urban_Percent – coluna que contenha "Urban" e "%" (percentual de população urbana)
      - Urban_Population – coluna que contenha "Urban" e "Population" (população urbana)
    Calcula ainda:
      - Rural_Population = Population - Urban_Population
    Retorna um DataFrame com as colunas:
       Country, Year, Population, Growth_Rate, Urban_Percent, Urban_Population, Rural_Population
    """
    # Normaliza os nomes das colunas
    df.columns = [re.sub(r'\s+', ' ', str(col)).strip() for col in df.columns]
    
    col_year = None
    col_population = None
    col_growth = None
    col_urban_percent = None
    col_urban_pop = None
    
    for col in df.columns:
        col_lower = col.lower()
        if "year" in col_lower and col_year is None:
            col_year = col
        elif "population" in col_lower and "urban" not in col_lower and "world" not in col_lower and col_population is None:
            col_population = col
        elif "change" in col_lower and "%" in col_lower and col_growth is None:
            col_growth = col
        elif "urban" in col_lower and "%" in col_lower and col_urban_percent is None:
            col_urban_percent = col
        elif "urban" in col_lower and "population" in col_lower and "%" not in col_lower and col_urban_pop is None:
            col_urban_pop = col
            
    if not all([col_year, col_population, col_growth, col_urban_percent, col_urban_pop]):
        logger.warning(f"Para o país {country_name}, nem todas as colunas foram identificadas: "
                       f"Year: {col_year}, Population: {col_population}, Growth: {col_growth}, "
                       f"Urban Percent: {col_urban_percent}, Urban Pop: {col_urban_pop}.")
    
    cols_to_keep = {}
    if col_year:
        cols_to_keep["Year"] = col_year
    if col_population:
        cols_to_keep["Population"] = col_population
    if col_growth:
        cols_to_keep["Growth_Rate"] = col_growth
    if col_urban_percent:
        cols_to_keep["Urban_Percent"] = col_urban_percent
    if col_urban_pop:
        cols_to_keep["Urban_Population"] = col_urban_pop
        
    df_out = pd.DataFrame()
    for new_col, old_col in cols_to_keep.items():
        if old_col in df.columns:
            df_out[new_col] = df[old_col]
        else:
            df_out[new_col] = np.nan
    
    df_out["Country"] = country_name
    
    # Converte as colunas numéricas removendo vírgulas e símbolos
    for col in ["Population", "Urban_Population"]:
        if col in df_out.columns:
            df_out[col] = (df_out[col]
                           .astype(str)
                           .str.replace(",", "", regex=False)
                           .str.strip())
            df_out[col] = pd.to_numeric(df_out[col], errors='coerce')
    
    for col in ["Urban_Percent", "Growth_Rate"]:
        if col in df_out.columns:
            df_out[col] = (df_out[col]
                           .astype(str)
                           .str.replace("%", "", regex=False)
                           .str.strip())
            df_out[col] = pd.to_numeric(df_out[col], errors='coerce')
    
    if "Year" in df_out.columns:
        df_out["Year"] = pd.to_numeric(df_out["Year"], errors='coerce').astype("Int64")
    
    if "Population" in df_out.columns and "Urban_Population" in df_out.columns:
        df_out["Rural_Population"] = df_out["Population"] - df_out["Urban_Population"]
    else:
        df_out["Rural_Population"] = np.nan
        
    desired_order = ["Country", "Year", "Population", "Growth_Rate", "Urban_Percent", "Urban_Population", "Rural_Population"]
    df_out = df_out[[col for col in desired_order if col in df_out.columns]]
    
    return df_out

async def fetch_country_historical_data(session, country_name, country_url):
    """
    Acessa a página individual do país e extrai a tabela “Population of <país> (2025 and historical)”.
    Em seguida, processa essa tabela para manter somente as colunas de interesse.
    Retorna um DataFrame com os dados históricos (um registro por ano) e com a coluna 'Country'.
    """
    try:
        response = await session.get(country_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Procura por um <h2> cujo texto contenha "Population of" e "historical"
        target_table = None
        for h2 in soup.find_all("h2"):
            txt = h2.get_text().strip()
            if "Population of" in txt and "historical" in txt:
                target_table = h2.find_next("table", {"class": "table-list"})
                break
        
        if target_table is None:
            logger.error(f"Tabela histórica não encontrada para {country_name} ({country_url}).")
            return None
        
        # Lê a tabela com pandas
        df_raw = pd.read_html(str(target_table))[0]
        # Processa a tabela para extrair apenas as colunas desejadas
        df_processed = process_historical_table(df_raw, country_name)
        return df_processed
    except Exception as e:
        logger.error(f"Erro ao extrair dados históricos para {country_name}: {e}")
        return None

async def fetch_all_historical_data():
    """
    A partir do DataFrame principal (obtido via process_population_data),
    acessa o link de cada país e extrai a tabela histórica.
    Retorna um DataFrame consolidado em que cada linha é um registro (Country, Year, ...)
    contendo os dados extraídos (Population, Growth_Rate, Urban_Percent, Urban_Population e Rural_Population).
    """
    session = AsyncHTMLSession()
    try:
        main_df = await process_population_data()
        if main_df is None or main_df.empty:
            logger.error("DataFrame principal com os links dos países está vazio.")
            return None
        
        logger.info(f"Foram encontrados {len(main_df)} países para processar dados históricos.")
        tasks = []
        for _, row in main_df.iterrows():
            country_name = row["Country"]
            country_url = row.get("Country_URL", None)
            if pd.isna(country_url) or not country_url:
                logger.warning(f"URL para {country_name} não encontrada; pulando.")
                continue
            tasks.append(fetch_country_historical_data(session, country_name, country_url))
        
        results = await asyncio.gather(*tasks)
        df_list = [df for df in results if df is not None and not df.empty]
        if df_list:
            df_all = pd.concat(df_list, ignore_index=True)
            return df_all
        else:
            logger.error("Nenhum dado histórico foi extraído.")
            return None
    except Exception as e:
        logger.error(f"Erro crítico ao extrair dados históricos: {e}")
        return None
    finally:
        await session.close()

async def scrape():
    """
    Função principal que:
      - Extrai o DataFrame principal com os links dos países.
      - Para cada país, acessa seu link e extrai os dados históricos (por ano) 
        mantendo somente as colunas: Population, Growth_Rate, Urban_Percent, Urban_Population;
        calcula Rural_Population (Population - Urban_Population) e inclui o Country.
      - Retorna um dicionário com os DataFrames:
            'Fato_Populacao': dados principais com resumo (incluindo Country_URL)
            'Historico_Pais': dados históricos consolidados (uma linha por país/ano)
    """
    session = AsyncHTMLSession()
    try:
        logger.info("Iniciando coleta dos dados do Worldometers")
        df_population = await process_population_data()
        logger.info("Dados principais extraídos.")
        
        historical_df = await fetch_all_historical_data()
        
        return {
            'Fato_Populacao': df_population,
            'Historico_Pais': historical_df
        }
    except Exception as e:
        logger.critical(f"Erro crítico: {e}")
        return {
            'Fato_Populacao': pd.DataFrame(),
            'Historico_Pais': pd.DataFrame()
        }
    finally:
        await session.close()

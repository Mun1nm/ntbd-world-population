"""
dados_fetch.py

Este módulo contém funções para buscar e tratar incipientemente de diversas fontes.
As funções aqui definidas realizam o download dos dados, o tratamento necessário
(e.g., conversões de tipos, renomeações de colunas, remoção de valores ausentes).

"""

import pandas as pd
import requests

# Função para buscar e tratar dados do PIB per capita
def fetch_pib_per_capita():
    df_pib_per_capita = pd.read_csv(
        "https://ourworldindata.org/grapher/gdp-per-capita-worldbank.csv?v=1&csvType=full&useColumnShortNames=true",
        storage_options={'User-Agent': 'Our World In Data data fetch/1.0'}
    )
    metadata = requests.get(
        "https://ourworldindata.org/grapher/gdp-per-capita-worldbank.metadata.json?v=1&csvType=full&useColumnShortNames=true"
    ).json()
    df_pib_per_capita.rename(columns={'ny_gdp_pcap_pp_kd': 'PIB_Per_Capita'}, inplace=True)
    df_pib_per_capita['Year'] = df_pib_per_capita['Year'].astype(int)
    df_pib_per_capita['PIB_Per_Capita'] = pd.to_numeric(df_pib_per_capita['PIB_Per_Capita'], errors='coerce')
    df_pib_per_capita.dropna(subset=['PIB_Per_Capita'], inplace=True)
    return df_pib_per_capita

# Função para buscar e tratar dados de acesso à educação
def fetch_acesso_educacao():
    df_acesso_educacao = pd.read_csv(
        "https://ourworldindata.org/grapher/learning-outcomes-vs-gdp-per-capita.csv?v=1&csvType=full&useColumnShortNames=true",
        storage_options={'User-Agent': 'Our World In Data data fetch/1.0'}
    )
    metadata = requests.get(
        "https://ourworldindata.org/grapher/learning-outcomes-vs-gdp-per-capita.metadata.json?v=1&csvType=full&useColumnShortNames=true"
    ).json()
    df_acesso_educacao['Year'] = df_acesso_educacao['Year'].astype(int)
    df_acesso_educacao['harmonized_test_scores'] = pd.to_numeric(df_acesso_educacao['harmonized_test_scores'], errors='coerce')
    df_acesso_educacao.dropna(subset=['harmonized_test_scores'], inplace=True)
    min_score = df_acesso_educacao['harmonized_test_scores'].min()
    max_score = df_acesso_educacao['harmonized_test_scores'].max()
    df_acesso_educacao['Acesso_Educacao'] = ((df_acesso_educacao['harmonized_test_scores'] - min_score) / (max_score - min_score)) * 100
    df_acesso_educacao = df_acesso_educacao[['Entity', 'Code', 'Year', 'Acesso_Educacao']]
    return df_acesso_educacao

# Função para buscar e tratar dados de expectativa de vida
def fetch_expectativa_vida():
    df_expectativa_vida = pd.read_csv(
        "https://ourworldindata.org/grapher/life-expectancy.csv?v=1&csvType=full&useColumnShortNames=true",
        storage_options={'User-Agent': 'Our World In Data data fetch/1.0'}
    )
    metadata = requests.get(
        "https://ourworldindata.org/grapher/life-expectancy.metadata.json?v=1&csvType=full&useColumnShortNames=true"
    ).json()
    df_expectativa_vida.rename(columns={'life_expectancy_0__sex_total__age_0': 'Expectativa_Vida'}, inplace=True)
    df_expectativa_vida['Year'] = df_expectativa_vida['Year'].astype(int)
    df_expectativa_vida['Expectativa_Vida'] = pd.to_numeric(df_expectativa_vida['Expectativa_Vida'], errors='coerce')
    df_expectativa_vida.dropna(subset=['Expectativa_Vida'], inplace=True)

    df_expectativa_vida.to_csv("./data/expectativa.csv", index=False)

    return df_expectativa_vida

# Função para buscar e tratar dados de taxa de mortalidade
def fetch_taxa_mortalidade():
    df_taxa_mortalidade = pd.read_csv('./data/taxa_mortalidade.csv')
    df_taxa_mortalidade = df_taxa_mortalidade[
        (df_taxa_mortalidade['Sex'] == 'All') &
        (df_taxa_mortalidade['Age group code'] == 'Age_all')
    ]
    df_taxa_mortalidade = df_taxa_mortalidade[['Country Name', 'Year', 'Death rate per 100 000 population']].copy()
    df_taxa_mortalidade.columns = ['Entity', 'Year', 'Taxa_Mortalidade']

    return df_taxa_mortalidade

# Função para buscar e tratar dados de médicos por habitante
def fetch_medicos_por_habitante():
    df_medicos_por_habitante = pd.read_csv('./data/medicos_por_habitante.csv')
    df_medicos_por_habitante['Period'] = df_medicos_por_habitante['Period'].astype(int)
    df_medicos_por_habitante['Medicos_Por_Habitante'] = pd.to_numeric(df_medicos_por_habitante['Value'], errors='coerce')
    df_medicos_por_habitante.dropna(subset=['Medicos_Por_Habitante'], inplace=True)
    df_medicos_por_habitante = df_medicos_por_habitante[['Location', 'Period', 'Medicos_Por_Habitante']]
    return df_medicos_por_habitante

# Função para buscar e tratar dados de conflitos armados
def fetch_em_conflito():
    df_em_conflito = pd.read_csv(
        "https://ourworldindata.org/grapher/civilian-and-combatant-deaths-in-armed-conflicts-based-on-where-they-occurred.csv?v=1&csvType=full&useColumnShortNames=true",
        storage_options={'User-Agent': 'Our World In Data data fetch/1.0'}
    )
    metadata = requests.get(
        "https://ourworldindata.org/grapher/civilian-and-combatant-deaths-in-armed-conflicts-based-on-where-they-occurred.metadata.json?v=1&csvType=full&useColumnShortNames=true"
    ).json()
    df_em_conflito['total_deaths'] = (
        df_em_conflito['number_deaths_civilians__conflict_type_all'] +
        df_em_conflito['number_deaths_unknown__conflict_type_all'] +
        df_em_conflito['number_deaths_combatants__conflict_type_all']
    )
    def conflict_level(deaths):
        if deaths < 100:
            return 'Baixo'
        elif deaths < 1000:
            return 'Médio'
        else:
            return 'Alto'
    df_em_conflito['Em_Conflito'] = df_em_conflito['total_deaths'].apply(conflict_level)
    return df_em_conflito

# Função para buscar, tratar e unificar dados de religião
def fetch_religiao():
    df_fetch_religiao = pd.read_csv("./data/national.csv")

    # Dicionário de mapeamento: coluna de religião -> classificação
    religiao_mapping = {
        "christianity_protestant": "Cristão",
        "christianity_romancatholic": "Cristão",
        "christianity_easternorthodox": "Cristão",
        "christianity_anglican": "Cristão",
        "christianity_other": "Cristão",
        "christianity_all": "Cristão",
        "judaism_orthodox": "Judaico",
        "judaism_conservative": "Judaico",
        "judaism_reform": "Judaico",
        "judaism_other": "Judaico",
        "judaism_all": "Judaico",
        "islam_sunni": "Muçulmano",
        "islam_shi’a": "Muçulmano",
        "islam_ibadhi": "Muçulmano",
        "islam_nationofislam": "Muçulmano",
        "islam_alawite": "Muçulmano",
        "islam_ahmadiyya": "Muçulmano",
        "islam_other": "Muçulmano",
        "islam_all": "Muçulmano",
        "buddhism_mahayana": "Budista",
        "buddhism_theravada": "Budista",
        "buddhism_other": "Budista",
        "buddhism_all": "Budista",
        "zoroastrianism_all": "Outros",
        "hinduism_all": "Hindu",
        "sikhism_all": "Outros",
        "shinto_all": "Tradicionais",
        "baha’i_all": "Outros",
        "taoism_all": "Tradicionais",
        "jainism_all": "Tradicionais",
        "confucianism_all": "Tradicionais",
        "syncretism_all": "Outros",
        "animism_all": "Tradicionais",
        "noreligion_all": "Secular/Não Religioso",
        "otherreligion_all": "Outros"
    }

    # Lista de colunas de religiões a serem "derretidas"
    colunas_religiao = list(religiao_mapping.keys())

    df_religiao = df_fetch_religiao.melt(
        id_vars=["year", "state"],
        value_vars=colunas_religiao,
        var_name="religiao",
        value_name="quantidade"
    )

    df_religiao["quantidade"] = pd.to_numeric(df_religiao["quantidade"], errors="coerce").fillna(0)
    df_religiao["classificacao"] = df_religiao["religiao"].map(religiao_mapping)
    df_religiao.rename(columns={"state": "nome do pais", "year": "ano"}, inplace=True)

    # Agregação: para cada país e ano, identificar a linha (religião) com maior quantidade
    # e pegar sua classificação
    idx = df_religiao.groupby(["ano", "nome do pais"])["quantidade"].idxmax()
    df_predominante = df_religiao.loc[idx, ["ano", "nome do pais", "religiao"]].copy()
    df_predominante.rename(columns={"religiao": "Religiao_Predominante"}, inplace=True)

    df_religiao = df_religiao.merge(df_predominante, on=["ano", "nome do pais"], how="left")

    return df_religiao


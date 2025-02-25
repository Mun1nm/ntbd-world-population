"""
integration.py

Este módulo contém funções para integrar, transformar e unificar dados oriundos de diversas fontes,
como dados históricos populacionais, PIB per capita, acesso à educação, expectativa de vida, taxa de mortalidade,
médicos por habitante, informações de conflito e dados de religião.
A função principal, run_pipeline, realiza o seguinte:
  - Padronização e tratamento inicial dos DataFrames de cada fonte (e.g., conversões de tipos, renomeação de colunas,
    remoção de valores ausentes).
  - Integração dos dados por meio de merges para construir um DataFrame de fato consolidado.
  - Criação de dimensões (tempo, local e religião).
  - Salvamento dos DataFrames finais em arquivos CSV para posterior análise.
"""

import pandas as pd
import numpy as np

def padroniza_nome(nome):
    return "" if pd.isna(nome) else nome.strip().lower()

def run_pipeline(df_historico_pais, df_pib_per_capita, df_acesso_educacao,
                 df_expectativa_vida, df_taxa_mortalidade, df_medicos_por_habitante,
                 df_em_conflito, df_religiao_final):
    """
    Executa o pipeline completo de transformação dos dados e monta os DataFrames
    finais (fato e dimensões).

    Parâmetros:
        df_historico_pais: DataFrame com os dados históricos (contém colunas Country, Population, Growth_Rate, Urban_Population, Rural_Population)
        df_pib_per_capita: DataFrame com dados do PIB per capita
        df_acesso_educacao: DataFrame com dados de acesso à educação
        df_expectativa_vida: DataFrame com dados de expectativa de vida
        df_taxa_mortalidade: DataFrame com dados de taxa de mortalidade (colunas location_name e year serão renomeadas)
        df_medicos_por_habitante: DataFrame com dados de médicos por habitante (colunas Location e Period serão renomeadas)
        df_em_conflito: DataFrame com dados da coluna Em_Conflito
        df_religiao_final: DataFrame unificado com os dados de religião (deve conter colunas Entity, Year e Religiao_Predominante)
        
    Retorna:
        Um dicionário com os DataFrames finais: df_fact_final, dim_tempo, dim_local e dim_religiao.
    """

    # 1. Leitura e Tratamento dos DataFrames de Origem

    # 1.1 - Dados Históricos
    df_hist = df_historico_pais.copy()
    df_hist.rename(columns={
        'Country': 'Entity',
        'Population': 'Populacao_Total',
        'Growth_Rate': 'Taxa_Crescimento',
        'Urban_Population': 'Populacao_Urbana',
        'Rural_Population': 'Populacao_Rural'
    }, inplace=True)
    df_hist['Entity'] = df_hist['Entity'].str.lower().str.strip()
    df_hist['Year'] = df_hist['Year'].astype(int)
    df_hist['Populacao_Total'] = df_hist['Populacao_Total'].fillna(0).astype(int)
    df_hist['Taxa_Crescimento'] = df_hist['Taxa_Crescimento'].fillna(0.0)
    df_hist['Populacao_Urbana'] = df_hist['Populacao_Urbana'].fillna(0).astype(int)
    df_hist['Populacao_Rural'] = df_hist['Populacao_Rural'].fillna(0).astype(int)

    def padroniza_nome(nome):
        return "" if pd.isna(nome) else nome.strip().lower()

    # 1.2 - PIB Per Capita
    df_pib_per_capita['Entity'] = df_pib_per_capita['Entity'].apply(padroniza_nome)
    df_pib_per_capita['Year'] = df_pib_per_capita['Year'].astype(int)
    df_pib_per_capita['PIB_Per_Capita'] = df_pib_per_capita['PIB_Per_Capita'].fillna(0.0)

    # 1.3 - Acesso à Educação
    df_acesso_educacao['Entity'] = df_acesso_educacao['Entity'].apply(padroniza_nome)
    df_acesso_educacao['Year'] = df_acesso_educacao['Year'].astype(int)
    df_acesso_educacao['Acesso_Educacao'] = df_acesso_educacao['Acesso_Educacao'].fillna(0.0)

    # 1.4 - Expectativa de Vida
    df_expectativa_vida['Entity'] = df_expectativa_vida['Entity'].apply(padroniza_nome)
    df_expectativa_vida['Year'] = df_expectativa_vida['Year'].astype(int)
    df_expectativa_vida['Expectativa_Vida'] = df_expectativa_vida['Expectativa_Vida'].fillna(0.0)

    # 1.5 - Taxa de Mortalidade
    df_taxa_mortalidade.rename(columns={'location_name': 'Entity', 'year': 'Year'}, inplace=True)
    df_taxa_mortalidade['Entity'] = df_taxa_mortalidade['Entity'].apply(padroniza_nome)
    df_taxa_mortalidade['Year'] = df_taxa_mortalidade['Year'].astype(int)
    df_taxa_mortalidade['Taxa_Mortalidade'] = df_taxa_mortalidade['Taxa_Mortalidade'].fillna(0.0)

    # 1.6 - Médicos por Habitante
    df_medicos_por_habitante.rename(columns={'Location': 'Entity', 'Period': 'Year'}, inplace=True)
    df_medicos_por_habitante['Entity'] = df_medicos_por_habitante['Entity'].apply(padroniza_nome)
    df_medicos_por_habitante['Year'] = df_medicos_por_habitante['Year'].astype(int)
    df_medicos_por_habitante['Medicos_Por_Habitante'] = df_medicos_por_habitante['Medicos_Por_Habitante'].fillna(0.0)

    # 1.7 - Em Conflito
    df_em_conflito['Entity'] = df_em_conflito['Entity'].apply(padroniza_nome)
    df_em_conflito['Year'] = df_em_conflito['Year'].astype(int)
    df_em_conflito['Em_Conflito'] = df_em_conflito['Em_Conflito'].fillna('Baixo')

    # 1.8 - Religião
    df_religiao = df_religiao_final.copy()
    df_religiao.rename(columns={'nome do pais': 'Entity', 'ano': 'Year', 'religiao': 'Nome_Religiao', 'classificacao': 'Classificacao'}, inplace=True)
    df_religiao['Entity'] = df_religiao['Entity'].apply(padroniza_nome)
    df_religiao['Year'] = df_religiao['Year'].astype(int)
    df_religiao['Religiao_Predominante'] = df_religiao['Religiao_Predominante'].fillna('Não Informado')
    df_religiao_fato = df_religiao[['Year', 'Entity', 'Religiao_Predominante']].drop_duplicates().reset_index(drop=True)

    # 2. Montar o DataFrame temporário de Fato (para posterior inserção das chaves)

    df_fato_temp = df_hist.copy()
    df_fato_temp = df_fato_temp.merge(df_pib_per_capita[['Entity','Year','PIB_Per_Capita']], on=['Entity','Year'], how='left')
    df_fato_temp = df_fato_temp.merge(df_acesso_educacao[['Entity','Year','Acesso_Educacao']], on=['Entity','Year'], how='left')
    df_fato_temp = df_fato_temp.merge(df_expectativa_vida[['Entity','Year','Expectativa_Vida']], on=['Entity','Year'], how='left')
    df_fato_temp = df_fato_temp.merge(df_taxa_mortalidade[['Entity','Year','Taxa_Mortalidade']], on=['Entity','Year'], how='left')
    df_fato_temp = df_fato_temp.merge(df_medicos_por_habitante[['Entity','Year','Medicos_Por_Habitante']], on=['Entity','Year'], how='left')
    df_fato_temp = df_fato_temp.merge(df_em_conflito[['Entity','Year','Em_Conflito']], on=['Entity','Year'], how='left')
    df_fato_temp['Em_Conflito'] = df_fato_temp['Em_Conflito'].fillna('Baixo')
    df_fato_temp = df_fato_temp.merge(df_religiao_fato[['Entity', 'Year', 'Religiao_Predominante']], on=['Entity', 'Year'], how='left')

    # 3. Criação das Dimensões

    # 3.1 - Dim_Tempo
    dim_tempo = pd.DataFrame({'Ano': sorted(df_fato_temp['Year'].unique())})
    dim_tempo['Decada'] = dim_tempo['Ano'].apply(lambda x: (x // 10)*10)
    dim_tempo['ID_Tempo'] = np.arange(1, len(dim_tempo)+1)

    # 3.2 - Dim_Local
    dim_local = df_fato_temp[['Entity']].drop_duplicates().copy()
    dim_local.rename(columns={'Entity': 'Pais'}, inplace=True)
    pais_to_continente = {
        "india": "Asia",
        "china": "Asia",
        "united states": "north america",
        "indonesia": "asia",
        "pakistan": "asia",
        "nigeria": "africa",
        "brazil": "south america",
        "bangladesh": "asia",
        "russia": "europe/asia",
        "ethiopia": "africa",
        "mexico": "north america",
        "japan": "asia",
        "egypt": "africa",
        "philippines": "asia",
        "dr congo": "africa",
        "vietnam": "asia",
        "iran": "asia",
        "turkey": "europe/asia",
        "germany": "europe",
        "thailand": "asia",
        "united kingdom": "europe",
        "tanzania": "africa",
        "france": "europe",
        "south africa": "africa",
        "italy": "europe",
        "kenya": "africa",
        "myanmar": "asia",
        "colombia": "south america",
        "south korea": "asia",
        "sudan": "africa",
        "uganda": "africa",
        "spain": "europe",
        "algeria": "africa",
        "iraq": "asia",
        "argentina": "south america",
        "afghanistan": "asia",
        "yemen": "asia",
        "canada": "north america",
        "poland": "europe",
        "morocco": "africa",
        "angola": "africa",
        "ukraine": "europe",
        "uzbekistan": "asia",
        "malaysia": "asia",
        "mozambique": "africa",
        "ghana": "africa",
        "peru": "south america",
        "saudi arabia": "asia",
        "madagascar": "africa",
        "côte d'ivoire": "africa",
        "nepal": "asia",
        "cameroon": "africa",
        "venezuela": "south america",
        "niger": "africa",
        "australia": "oceania",
        "north korea": "asia",
        "syria": "asia",
        "mali": "africa",
        "burkina faso": "africa",
        "taiwan": "asia",
        "sri lanka": "asia",
        "malawi": "africa",
        "zambia": "africa",
        "kazakhstan": "europe/asia",
        "chad": "africa",
        "chile": "south america",
        "romania": "europe",
        "somalia": "africa",
        "senegal": "africa",
        "guatemala": "central america",
        "netherlands": "europe",
        "ecuador": "south america",
        "cambodia": "asia",
        "zimbabwe": "africa",
        "guinea": "africa",
        "benin": "africa",
        "rwanda": "africa",
        "burundi": "africa",
        "bolivia": "south america",
        "tunisia": "africa",
        "south sudan": "africa",
        "haiti": "central america",
        "belgium": "europe",
        "jordan": "asia",
        "dominican republic": "central america",
        "united arab emirates": "asia",
        "cuba": "central america",
        "honduras": "central america",
        "czech republic (czechia)": "europe",
        "sweden": "europe",
        "tajikistan": "asia",
        "papua new guinea": "oceania",
        "portugal": "europe",
        "azerbaijan": "asia",
        "greece": "europe",
        "hungary": "europe",
        "togo": "africa",
        "israel": "asia",
        "austria": "europe",
        "belarus": "europe",
        "switzerland": "europe",
        "sierra leone": "africa",
        "laos": "asia",
        "turkmenistan": "asia",
        "hong kong": "asia",
        "libya": "africa",
        "kyrgyzstan": "asia",
        "paraguay": "south america",
        "nicaragua": "central america",
        "bulgaria": "europe",
        "serbia": "europe",
        "el salvador": "central america",
        "congo": "africa",
        "denmark": "europe",
        "singapore": "asia",
        "lebanon": "asia",
        "finland": "europe",
        "liberia": "africa",
        "norway": "europe",
        "slovakia": "europe",
        "state of palestine": "asia",
        "central african republic": "africa",
        "oman": "asia",
        "ireland": "europe",
        "new zealand": "oceania",
        "mauritania": "africa",
        "costa rica": "central america",
        "kuwait": "asia",
        "panama": "central america",
        "croatia": "europe",
        "georgia": "europe/asia",
        "eritrea": "africa",
        "mongolia": "asia",
        "uruguay": "south america",
        "puerto rico": "central america",
        "bosnia and herzegovina": "europe",
        "qatar": "asia",
        "moldova": "europe",
        "namibia": "africa",
        "armenia": "europe/asia",
        "lithuania": "europe",
        "jamaica": "central america",
        "albania": "europe",
        "gambia": "africa",
        "gabon": "africa",
        "botswana": "africa",
        "lesotho": "africa",
        "guinea-bissau": "africa",
        "slovenia": "europe",
        "equatorial guinea": "africa",
        "latvia": "europe",
        "north macedonia": "europe",
        "bahrain": "asia",
        "trinidad and tobago": "central america",
        "timor-leste": "asia",
        "estonia": "europe",
        "cyprus": "europe",
        "mauritius": "africa",
        "eswatini": "africa",
        "djibouti": "africa",
        "fiji": "oceania",
        "réunion": "africa",
        "comoros": "africa",
        "guyana": "south america",
        "solomon islands": "oceania",
        "bhutan": "asia",
        "macao": "asia",
        "luxembourg": "europe",
        "montenegro": "europe",
        "suriname": "south america",
        "western sahara": "africa",
        "malta": "europe",
        "maldives": "asia",
        "micronesia": "oceania",
        "cabo verde": "africa",
        "brunei": "asia",
        "belize": "central america",
        "bahamas": "central america",
        "iceland": "europe",
        "guadeloupe": "central america",
        "martinique": "central america",
        "vanuatu": "oceania",
        "mayotte": "africa",
        "french guiana": "south america",
        "new caledonia": "oceania",
        "barbados": "central america",
        "french polynesia": "oceania",
        "sao tome & principe": "africa",
        "samoa": "oceania",
        "curaçao": "central america",
        "saint lucia": "central america",
        "guam": "oceania",
        "kiribati": "oceania",
        "seychelles": "africa",
        "grenada": "central america",
        "aruba": "south america",
        "tonga": "oceania",
        "st. vincent & grenadines": "central america",
        "antigua and barbuda": "central america",
        "u.s. virgin islands": "central america",
        "isle of man": "europe",
        "andorra": "europe",
        "cayman islands": "central america",
        "dominica": "central america",
        "bermuda": "central america",
        "greenland": "north america",
        "faeroe islands": "europe",
        "saint kitts & nevis": "central america",
        "american samoa": "oceania",
        "turks and caicos": "central america",
        "northern mariana islands": "oceania",
        "sint maarten": "central america",
        "liechtenstein": "europe",
        "british virgin islands": "central america",
        "gibraltar": "europe",
        "monaco": "europe",
        "marshall islands": "oceania",
        "san marino": "europe",
        "caribbean netherlands": "central america",
        "saint martin": "central america",
        "palau": "oceania",
        "anguilla": "central america",
        "cook islands": "oceania",
        "nauru": "oceania",
        "wallis & futuna": "oceania",
        "saint barthelemy": "central america",
        "tuvalu": "oceania",
        "saint pierre & miquelon": "north america",
        "saint helena": "africa",
        "montserrat": "central america",
        "falkland islands": "south america",
        "tokelau": "oceania",
        "niue": "oceania",
        "holy see": "europe"
    }
    dim_local['Continente'] = dim_local['Pais'].apply(lambda p: pais_to_continente.get(p, 'desconhecido'))
    dim_local['ID_Local'] = np.arange(1, len(dim_local)+1)

    # 3.3 - Dim_Religiao
    dim_religiao = df_religiao[['Nome_Religiao', 'Classificacao']].drop_duplicates().reset_index(drop=True)
    dim_religiao = pd.concat([dim_religiao, pd.DataFrame([{'Nome_Religiao': 'nao_informado', 'Classificacao': 'Não Informado'}])], ignore_index=True)
    dim_religiao['ID_Religiao'] = np.arange(1, len(dim_religiao)+1)

    # 4. Transformar o DataFrame temporário em DF Fato com Chaves

    # Join com Dim_Tempo
    df_fato_merge = df_fato_temp.merge(
        dim_tempo[['ID_Tempo','Ano']],
        left_on='Year', right_on='Ano', how='left'
    )

    # Join com Dim_Local
    df_fato_merge = df_fato_merge.merge(
        dim_local[['ID_Local','Pais']],
        left_on='Entity', right_on='Pais', how='left'
    )

    # Join com Dim_Religiao: garanta que ambos os campos estejam em minúsculas para consistência
    df_fato_merge['Religiao_Predominante'] = df_fato_merge['Religiao_Predominante'].fillna('nao_informado')
    df_fato_merge = df_fato_merge.merge(
        dim_religiao[['ID_Religiao','Nome_Religiao']],
        left_on='Religiao_Predominante', right_on='Nome_Religiao', how='left'
    )

    # Monta o DataFrame final de fato
    df_fact_final = pd.DataFrame({
        'Chave_Tempo': df_fato_merge['ID_Tempo'],
        'Chave_Local': df_fato_merge['ID_Local'],
        'Chave_Religiao': df_fato_merge['ID_Religiao'],
        'Em_Conflito': df_fato_merge['Em_Conflito'],
        'Populacao_Total': df_fato_merge['Populacao_Total'],
        'Populacao_Urbana': df_fato_merge['Populacao_Urbana'],
        'Populacao_Rural': df_fato_merge['Populacao_Rural'],
        'Taxa_Crescimento': df_fato_merge['Taxa_Crescimento'],
        'Expectativa_Vida': df_fato_merge['Expectativa_Vida'],
        'Taxa_Mortalidade': df_fato_merge['Taxa_Mortalidade'],
        'PIB_Per_Capita': df_fato_merge['PIB_Per_Capita'],
        'Acesso_Educacao': df_fato_merge['Acesso_Educacao'],
        'Medicos_Por_Habitante': df_fato_merge['Medicos_Por_Habitante']
    })

    # 5. Salva CSVs para Análise

    df_fact_final.to_csv("./arquivos_analise/df_fato_final.csv", index=False, encoding="utf-8-sig")
    dim_tempo.to_csv("./arquivos_analise/dim_tempo.csv", index=False, encoding="utf-8-sig")
    dim_local.to_csv("./arquivos_analise/dim_local.csv", index=False, encoding="utf-8-sig")
    dim_religiao.to_csv("./arquivos_analise/dim_religiao.csv", index=False, encoding="utf-8-sig")

    return {
        'df_fact_final': df_fact_final,
        'dim_tempo': dim_tempo,
        'dim_local': dim_local,
        'dim_religiao': dim_religiao
    }
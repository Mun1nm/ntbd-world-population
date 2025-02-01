CREATE TABLE Dim_Tempo (
    ID_Tempo SERIAL PRIMARY KEY,
    Ano INTEGER NOT NULL,
    Decada INTEGER  -- ou calcular com base em Ano
);

CREATE TABLE Dim_Local (
    ID_Local SERIAL PRIMARY KEY,
    Pais VARCHAR(100) NOT NULL,
    Continente VARCHAR(100) NOT NULL
);

CREATE TABLE Dim_Religiao (
    ID_Religiao SERIAL PRIMARY KEY,
    Nome_Religiao VARCHAR(100) NOT NULL,
    Classificacao VARCHAR(50)
);

CREATE TABLE Fato_Populacao (
    ID_Fato SERIAL PRIMARY KEY,
    Descricao TEXT,
    Chave_Tempo INTEGER NOT NULL,
    Chave_Local INTEGER NOT NULL,
	Chave_Religiao INTEGER NOT NULL,
    Em_Conflito VARCHAR(20),  -- por exemplo, 'Baixo', 'Médio' ou 'Alto'
    Populacao_Total BIGINT,
    Populacao_Urbana BIGINT,
    Populacao_Rural BIGINT,
    Taxa_Crescimento NUMERIC(5,2),
    Expectativa_Vida NUMERIC(4,1),
    Taxa_Mortalidade NUMERIC(5,2),
    PIB_Per_Capita NUMERIC(12,2),
    Acesso_Educacao NUMERIC(5,2),
    Medicos_Por_Habitante NUMERIC(5,2),
    CONSTRAINT fk_tempo FOREIGN KEY (Chave_Tempo) REFERENCES Dim_Tempo(ID_Tempo),
    CONSTRAINT fk_local FOREIGN KEY (Chave_Local) REFERENCES Dim_Local(ID_Local)
);

DROP TABLE Fato_Populacao

SELECT * FROM Fato_Populacao

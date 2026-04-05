-- =============================================================================
-- ATHENA - CAMADA SPEC - 6 Tabelas Analíticas
-- =============================================================================

-- 1. Sintomas por Perfil
CREATE EXTERNAL TABLE IF NOT EXISTS covid.spec_sintomas_por_perfil (
    ano_mes INT,
    mes_desc STRING,
    faixa_etaria STRING,
    sexo_desc STRING,
    regiao STRING,
    total_pessoas BIGINT,
    qtd_febre BIGINT,
    qtd_tosse BIGINT,
    qtd_dor_garganta BIGINT,
    qtd_dificuldade_respirar BIGINT,
    qtd_perda_cheiro_sabor BIGINT,
    qtd_pelo_menos_1_sintoma BIGINT,
    pct_febre DOUBLE,
    pct_tosse DOUBLE,
    pct_dor_garganta DOUBLE,
    pct_dificuldade_respirar DOUBLE,
    pct_perda_cheiro_sabor DOUBLE,
    pct_pelo_menos_1_sintoma DOUBLE
) STORED AS PARQUET 
LOCATION 's3://analise-covid/spec/spec_sintomas_por_perfil/' 
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE covid.spec_sintomas_por_perfil;


-- 2. Busca Atendimento
CREATE EXTERNAL TABLE IF NOT EXISTS covid.spec_busca_atendimento (
    ano_mes INT,
    mes_desc STRING,
    faixa_etaria STRING,
    sexo_desc STRING,
    regiao STRING,
    total_pessoas BIGINT,
    qtd_procurou_saude BIGINT,
    qtd_internado BIGINT,
    qtd_plano_saude BIGINT,
    qtd_sintomaticos BIGINT,
    pct_procurou_saude DOUBLE,
    pct_internado DOUBLE,
    pct_plano_saude DOUBLE,
    pct_sintomaticos DOUBLE,
    taxa_internacao_sintomaticos DOUBLE
) STORED AS PARQUET 
LOCATION 's3://analise-covid/spec/spec_busca_atendimento/' 
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE covid.spec_busca_atendimento;

-- 3. Impacto Econômico
CREATE EXTERNAL TABLE IF NOT EXISTS covid.spec_impacto_economico (
    ano_mes INT,
    mes_desc STRING,
    faixa_etaria STRING,
    sexo_desc STRING,
    cor_raca_desc STRING,
    escolaridade_desc STRING,
    total_pessoas BIGINT,
    qtd_trabalhou BIGINT,
    qtd_trabalho_remoto BIGINT,
    qtd_bolsa_familia BIGINT,
    qtd_auxilio_emergencial BIGINT,
    qtd_sintomaticos BIGINT,
    pct_trabalhou DOUBLE,
    pct_trabalho_remoto DOUBLE,
    pct_bolsa_familia DOUBLE,
    pct_auxilio_emergencial DOUBLE,
    pct_sintomaticos DOUBLE
) STORED AS PARQUET LOCATION 's3://analise-covid/spec/spec_impacto_economico/' 
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE covid.spec_impacto_economico;

-- 4. Comportamento Pandemia
CREATE EXTERNAL TABLE IF NOT EXISTS covid.spec_comportamento_pandemia (
    ano_mes INT,
    mes_desc STRING,
    faixa_etaria STRING,
    sexo_desc STRING,
    regiao STRING,
    escolaridade_desc STRING,
    total_pessoas BIGINT,
    qtd_ficou_em_casa BIGINT,
    qtd_reduziu_contato BIGINT,
    qtd_vida_normal BIGINT,
    qtd_trabalhou BIGINT,
    qtd_trabalho_remoto BIGINT,
    qtd_sintomatico_trabalhando BIGINT,
    pct_ficou_em_casa DOUBLE,
    pct_reduziu_contato DOUBLE,
    pct_vida_normal DOUBLE,
    pct_trabalho_remoto DOUBLE,
    pct_sintomatico_trabalhando DOUBLE
) STORED AS PARQUET 
LOCATION 's3://analise-covid/spec/spec_comportamento_pandemia/' 
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE covid.spec_comportamento_pandemia;

-- 5. Indicadores Regionais
CREATE EXTERNAL TABLE IF NOT EXISTS covid.spec_indicadores_regionais (
    ano_mes INT,
    mes_desc STRING,
    uf INT,
    nome_uf STRING,
    regiao STRING,
    total_pessoas BIGINT,
    qtd_sintomaticos BIGINT,
    qtd_dificuldade_respirar BIGINT,
    qtd_procurou_saude BIGINT,
    qtd_internados BIGINT,
    qtd_plano_saude BIGINT,
    qtd_trabalhou BIGINT,
    qtd_remoto BIGINT,
    qtd_bolsa_familia BIGINT,
    qtd_auxilio BIGINT,
    qtd_ficou_em_casa BIGINT,
    idade_media DOUBLE,
    pct_sintomaticos DOUBLE,
    pct_dificuldade_respirar DOUBLE,
    pct_procurou_saude DOUBLE,
    pct_internados DOUBLE,
    pct_plano_saude DOUBLE,
    pct_trabalhou DOUBLE,
    pct_remoto DOUBLE,
    pct_bolsa_familia DOUBLE,
    pct_auxilio DOUBLE,
    pct_ficou_em_casa DOUBLE
) STORED AS PARQUET 
LOCATION 's3://analise-covid/spec/spec_indicadores_regionais/' 
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE covid.spec_indicadores_regionais;

-- 6. Evolução Mensal
CREATE EXTERNAL TABLE IF NOT EXISTS covid.spec_evolucao_mensal (
    ano_mes INT,
    mes_desc STRING,
    total_pessoas BIGINT,
    qtd_sintomaticos BIGINT,
    qtd_febre BIGINT,
    qtd_tosse BIGINT,
    qtd_dor_garganta BIGINT,
    qtd_dificuldade_respirar BIGINT,
    qtd_perda_cheiro_sabor BIGINT,
    qtd_procurou_saude BIGINT,
    qtd_internados BIGINT,
    qtd_plano_saude BIGINT,
    qtd_trabalhou BIGINT,
    qtd_remoto BIGINT,
    qtd_bolsa_familia BIGINT,
    qtd_auxilio BIGINT,
    qtd_ficou_em_casa BIGINT,
    qtd_reduziu_contato BIGINT,
    qtd_vida_normal BIGINT,
    idade_media DOUBLE,
    pct_sintomaticos DOUBLE,
    pct_febre DOUBLE,
    pct_tosse DOUBLE,
    pct_dor_garganta DOUBLE,
    pct_dificuldade_respirar DOUBLE,
    pct_perda_cheiro_sabor DOUBLE,
    pct_procurou_saude DOUBLE,
    pct_internados DOUBLE,
    pct_plano_saude DOUBLE,
    pct_trabalhou DOUBLE,
    pct_remoto DOUBLE,
    pct_bolsa_familia DOUBLE,
    pct_auxilio DOUBLE,
    pct_ficou_em_casa DOUBLE,
    pct_reduziu_contato DOUBLE,
    pct_vida_normal DOUBLE,
    taxa_internacao_sintomaticos DOUBLE
) STORED AS PARQUET 
LOCATION 's3://analise-covid/spec/spec_evolucao_mensal/' 
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE covid.spec_evolucao_mensal;

-- =============================================================================
-- QUERIES ANALÍTICAS ÚTEIS
-- =============================================================================
-- Evolução mensal dos sintomas
-- SELECT mes_desc, pct_sintomaticos, pct_febre, pct_tosse,
--        pct_dificuldade_respirar, pct_perda_cheiro_sabor,
--        taxa_internacao_sintomaticos
-- FROM covid.spec_evolucao_mensal
-- ORDER BY ano_mes;

-- Comparação regional ao longo dos meses
-- SELECT ano_mes, regiao,
--        SUM(qtd_sintomaticos)*100.0/SUM(total_pessoas) AS pct_sint,
--        SUM(qtd_internados)*100.0/SUM(total_pessoas) AS pct_int
-- FROM covid.spec_indicadores_regionais
-- GROUP BY ano_mes, regiao
-- ORDER BY ano_mes, regiao;
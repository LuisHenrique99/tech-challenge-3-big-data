-- =============================================================================
-- ATHENA - CAMADA SOT - Particionado por ano_mes
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS covid.sot_pnad_covid (
    uf INT,
    mes_pesquisa INT,
    tipo_area INT,
    idade INT,
    sexo INT,
    cor_raca INT,
    escolaridade INT,
    sintoma_febre INT,
    sintoma_tosse INT,
    sintoma_dor_garganta INT,
    sintoma_dificuldade_respirar INT,
    sintoma_perda_cheiro_sabor INT,
    procurou_estabelecimento_saude INT,
    foi_internado INT,
    possui_plano_saude INT,
    fez_restricao_contato INT,
    trabalhou_semana_passada DOUBLE,
    trabalho_remoto DOUBLE,
    recebeu_bolsa_familia INT,
    solicitou_auxilio_emergencial INT,
    nome_uf STRING,
    regiao STRING,
    sexo_desc STRING,
    cor_raca_desc STRING,
    escolaridade_desc STRING,
    tipo_area_desc STRING,
    faixa_etaria STRING,
    mes_desc STRING,
    sintoma_febre_desc STRING,
    sintoma_tosse_desc STRING,
    sintoma_dor_garganta_desc STRING,
    sintoma_dificuldade_respirar_desc STRING,
    sintoma_perda_cheiro_sabor_desc STRING,
    procurou_estabelecimento_saude_desc STRING,
    foi_internado_desc STRING,
    possui_plano_saude_desc STRING,
    fez_restricao_contato_desc STRING,
    trabalhou_semana_passada_desc STRING,
    trabalho_remoto_desc STRING,
    recebeu_bolsa_familia_desc STRING,
    solicitou_auxilio_emergencial_desc STRING
) PARTITIONED BY (ano_mes STRING) 
STORED AS PARQUET LOCATION 's3://analise-covid/sot/sot_pnad_covid/' 
TBLPROPERTIES ('parquet.compression' = 'SNAPPY');

MSCK REPAIR TABLE covid.sot_pnad_covid;

-- Exemplo: evolução mensal
-- SELECT ano_mes, COUNT(*) as total,
--   SUM(CASE WHEN sintoma_febre=1 THEN 1 ELSE 0 END) as febre
-- FROM covid.sot_pnad_covid
-- GROUP BY ano_mes ORDER BY ano_mes;
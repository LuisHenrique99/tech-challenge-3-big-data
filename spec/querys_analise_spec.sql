-- =============================================================================
-- QUERYS DE ANALISE - CAMADA SPEC (ATHENA)
-- =============================================================================
-- Objetivo: validar os principais insights do arquivo insights_spec com consultas reproduziveis.
-- Banco: pnad_covid
-- Tabelas: spec_sintomas_por_perfil, spec_busca_atendimento,
--          spec_impacto_economico, spec_comportamento_pandemia,
--          spec_indicadores_regionais, spec_evolucao_mensal
--
-- Ordem sugerida:
--   1) Sanidade geral
--   2) Evolucao temporal
--   3) Perfil demografico
--   4) Regional
--   5) Atendimento
--   6) Economico
--   7) Comportamento
-- =============================================================================
USE pnad_covid;

-- =============================================================================
-- 1) SANIDADE GERAL
-- =============================================================================
-- 1.1 Conferir se as 6 tabelas existem
SHOW TABLES IN pnad_covid LIKE 'spec_%';

-- 1.2 Contagem de linhas por tabela
SELECT
    'spec_sintomas_por_perfil' AS tabela,
    COUNT(*) AS linhas
FROM
    pnad_covid.spec_sintomas_por_perfil
UNION
ALL
SELECT
    'spec_busca_atendimento',
    COUNT(*)
FROM
    pnad_covid.spec_busca_atendimento
UNION
ALL
SELECT
    'spec_impacto_economico',
    COUNT(*)
FROM
    pnad_covid.spec_impacto_economico
UNION
ALL
SELECT
    'spec_comportamento_pandemia',
    COUNT(*)
FROM
    pnad_covid.spec_comportamento_pandemia
UNION
ALL
SELECT
    'spec_indicadores_regionais',
    COUNT(*)
FROM
    pnad_covid.spec_indicadores_regionais
UNION
ALL
SELECT
    'spec_evolucao_mensal',
    COUNT(*)
FROM
    pnad_covid.spec_evolucao_mensal;

-- 1.3 Checar meses carregados
SELECT
    ano_mes,
    mes_desc,
    total_pessoas
FROM
    pnad_covid.spec_evolucao_mensal
ORDER BY
    ano_mes;

-- =============================================================================
-- 2) EVOLUCAO TEMPORAL (validar retomada em novembro)
-- =============================================================================
-- 2.1 Evolucao de sintomas e gravidade
SELECT
    ano_mes,
    mes_desc,
    total_pessoas,
    pct_sintomaticos,
    pct_febre,
    pct_tosse,
    pct_dor_garganta,
    pct_dificuldade_respirar,
    pct_perda_cheiro_sabor,
    pct_internados,
    taxa_internacao_sintomaticos
FROM
    pnad_covid.spec_evolucao_mensal
ORDER BY
    ano_mes;

-- 2.2 Variacao percentual mes a mes (MoM) de sintomaticos
WITH base AS (
    SELECT
        ano_mes,
        mes_desc,
        pct_sintomaticos,
        LAG(pct_sintomaticos) OVER (
            ORDER BY
                ano_mes
        ) AS pct_mes_anterior
    FROM
        pnad_covid.spec_evolucao_mensal
)
SELECT
    ano_mes,
    mes_desc,
    pct_sintomaticos,
    pct_mes_anterior,
    ROUND((pct_sintomaticos - pct_mes_anterior), 2) AS delta_pp,
    ROUND(
        (
            (pct_sintomaticos - pct_mes_anterior) / NULLIF(pct_mes_anterior, 0)
        ) * 100,
        2
    ) AS variacao_pct
FROM
    base
ORDER BY
    ano_mes;

-- =============================================================================
-- 3) PERFIL DEMOGRAFICO (idade e sexo)
-- =============================================================================
-- 3.1 Taxa de pelo menos 1 sintoma por faixa etaria (trimestre consolidado)
SELECT
    faixa_etaria,
    SUM(total_pessoas) AS total_pessoas,
    SUM(qtd_pelo_menos_1_sintoma) AS qtd_sintomaticos,
    ROUND(
        SUM(qtd_pelo_menos_1_sintoma) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_sintomaticos
FROM
    pnad_covid.spec_sintomas_por_perfil
GROUP BY
    faixa_etaria
ORDER BY
    pct_sintomaticos DESC;

-- 3.2 Taxa de pelo menos 1 sintoma por sexo (trimestre consolidado)
SELECT
    sexo_desc,
    SUM(total_pessoas) AS total_pessoas,
    SUM(qtd_pelo_menos_1_sintoma) AS qtd_sintomaticos,
    ROUND(
        SUM(qtd_pelo_menos_1_sintoma) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_sintomaticos
FROM
    pnad_covid.spec_sintomas_por_perfil
GROUP BY
    sexo_desc
ORDER BY
    pct_sintomaticos DESC;

-- =============================================================================
-- 4) VISAO REGIONAL (validar retomada no Sudeste e Sul)
-- =============================================================================
-- 4.1 Taxa de sintomaticos por regiao e mes
SELECT
    ano_mes,
    mes_desc,
    regiao,
    SUM(total_pessoas) AS total_pessoas,
    SUM(qtd_sintomaticos) AS qtd_sintomaticos,
    ROUND(
        SUM(qtd_sintomaticos) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_sintomaticos
FROM
    pnad_covid.spec_indicadores_regionais
GROUP BY
    ano_mes,
    mes_desc,
    regiao
ORDER BY
    ano_mes,
    regiao;

-- 4.2 Comparar outubro x novembro por regiao (delta em pontos percentuais)
WITH reg AS (
    SELECT
        ano_mes,
        regiao,
        ROUND(
            SUM(qtd_sintomaticos) * 100.0 / SUM(total_pessoas),
            2
        ) AS pct_sint
    FROM
        pnad_covid.spec_indicadores_regionais
    GROUP BY
        ano_mes,
        regiao
),
piv AS (
    SELECT
        regiao,
        MAX(
            CASE
                WHEN ano_mes = '202010' THEN pct_sint
            END
        ) AS pct_out,
        MAX(
            CASE
                WHEN ano_mes = '202011' THEN pct_sint
            END
        ) AS pct_nov
    FROM
        reg
    GROUP BY
        regiao
)
SELECT
    regiao,
    pct_out,
    pct_nov,
    ROUND(pct_nov - pct_out, 2) AS delta_pp
FROM
    piv
ORDER BY
    delta_pp DESC;

-- =============================================================================
-- 5) BUSCA POR ATENDIMENTO E INTERNACAO
-- =============================================================================
-- 5.1 Evolucao de procura por saude e internacao
SELECT
    ano_mes,
    mes_desc,
    ROUND(
        SUM(qtd_procurou_saude) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_procurou_saude,
    ROUND(
        SUM(qtd_internado) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_internado,
    ROUND(
        SUM(qtd_internado) * 100.0 / NULLIF(SUM(qtd_sintomaticos), 0),
        2
    ) AS taxa_internacao_sintomaticos
FROM
    pnad_covid.spec_busca_atendimento
GROUP BY
    ano_mes,
    mes_desc
ORDER BY
    ano_mes;

-- 5.2 Plano de saude por mes
SELECT
    ano_mes,
    mes_desc,
    ROUND(
        SUM(qtd_plano_saude) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_plano_saude
FROM
    pnad_covid.spec_busca_atendimento
GROUP BY
    ano_mes,
    mes_desc
ORDER BY
    ano_mes;

-- =============================================================================
-- 6) IMPACTO ECONOMICO E VULNERABILIDADE
-- =============================================================================
-- 6.1 Trabalho e remoto por escolaridade (trimestre)
SELECT
    escolaridade_desc,
    SUM(total_pessoas) AS total_pessoas,
    ROUND(
        SUM(qtd_trabalhou) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_trabalhou,
    ROUND(
        SUM(qtd_trabalho_remoto) * 100.0 / NULLIF(SUM(qtd_trabalhou), 0),
        2
    ) AS pct_remoto_entre_ocupados,
    ROUND(
        SUM(qtd_auxilio_emergencial) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_auxilio
FROM
    pnad_covid.spec_impacto_economico
GROUP BY
    escolaridade_desc
ORDER BY
    pct_trabalhou DESC;

-- 6.2 Evolucao mensal do trabalho e do auxilio
SELECT
    ano_mes,
    mes_desc,
    pct_trabalhou,
    pct_auxilio,
    pct_bolsa_familia
FROM
    pnad_covid.spec_evolucao_mensal
ORDER BY
    ano_mes;

-- =============================================================================
-- 7) COMPORTAMENTO DURANTE A PANDEMIA
-- =============================================================================
-- 7.1 Evolucao de restricao de contato e sintomatico trabalhando
SELECT
    ano_mes,
    mes_desc,
    pct_ficou_em_casa,
    pct_reduziu_contato,
    pct_vida_normal,
    ROUND(
        SUM(qtd_sintomatico_trabalhando) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_sintomatico_trabalhando
FROM
    pnad_covid.spec_comportamento_pandemia
GROUP BY
    ano_mes,
    mes_desc,
    pct_ficou_em_casa,
    pct_reduziu_contato,
    pct_vida_normal
ORDER BY
    ano_mes;

-- 7.2 Top 10 combinacoes com maior taxa de sintomatico trabalhando
SELECT
    ano_mes,
    mes_desc,
    regiao,
    faixa_etaria,
    sexo_desc,
    escolaridade_desc,
    total_pessoas,
    qtd_sintomatico_trabalhando,
    pct_sintomatico_trabalhando
FROM
    pnad_covid.spec_comportamento_pandemia
ORDER BY
    pct_sintomatico_trabalhando DESC
LIMIT
    10;

-- =============================================================================
-- 8) QUERYS DE SUPORTE EXECUTIVO (dashboard)
-- =============================================================================
-- 8.1 KPI mensal resumido (1 linha por mes)
SELECT
    ano_mes,
    mes_desc,
    total_pessoas,
    pct_sintomaticos,
    pct_internados,
    taxa_internacao_sintomaticos,
    pct_procurou_saude,
    pct_trabalhou,
    pct_auxilio
FROM
    pnad_covid.spec_evolucao_mensal
ORDER BY
    ano_mes;

-- 8.2 Ranking de UFs por taxa de sintomaticos no ultimo mes
SELECT
    nome_uf,
    regiao,
    ROUND(
        SUM(qtd_sintomaticos) * 100.0 / SUM(total_pessoas),
        2
    ) AS pct_sintomaticos
FROM
    pnad_covid.spec_indicadores_regionais
WHERE
    ano_mes = '202011'
GROUP BY
    nome_uf,
    regiao
ORDER BY
    pct_sintomaticos DESC;

-- 8.3 Ranking de UFs por taxa de internacao no ultimo mes
SELECT
    nome_uf,
    regiao,
    ROUND(
        SUM(qtd_internados) * 100.0 / SUM(total_pessoas),
        3
    ) AS pct_internados
FROM
    pnad_covid.spec_indicadores_regionais
WHERE
    ano_mes = '202011'
GROUP BY
    nome_uf,
    regiao
ORDER BY
    pct_internados DESC;
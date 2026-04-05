# Camada SPEC - Geração das 6 tabelas analíticas
# Entrada: Parquet SOT → Saída: 6 Parquets especializados + previews CSV

import os, sys, shutil, tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_ORIGEM = os.path.join(BASE_DIR, "sot", "dados", "pnad_covid_sot.parquet")
DADOS_DIR = os.path.join(BASE_DIR, "spec", "dados")
PREVIEW_DIR = os.path.join(BASE_DIR, "spec", "preview")
TEMP_DIR = os.path.join(tempfile.gettempdir(), "pnad_covid_spark_spec")

os.makedirs(DADOS_DIR, exist_ok=True)
os.makedirs(PREVIEW_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("PNAD-COVID19 - SPEC") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.local.dir", TEMP_DIR) \
    .config("spark.sql.warehouse.dir", os.path.join(TEMP_DIR, "warehouse")) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SPEC - Tabelas Analíticas (3 meses)")
print("=" * 60)

print("\n[0/6] Carregando SOT...")
df_pd = pd.read_parquet(PARQUET_ORIGEM, engine="pyarrow")
df = spark.createDataFrame(df_pd)
print(f"  {df.count():,} registros")

# Condição reutilizável: pessoa com pelo menos 1 sintoma
COND_SINTOMATICO = (
    (F.col("sintoma_febre") == 1) |
    (F.col("sintoma_tosse") == 1) |
    (F.col("sintoma_dor_garganta") == 1) |
    (F.col("sintoma_dificuldade_respirar") == 1) |
    (F.col("sintoma_perda_cheiro_sabor") == 1)
)


def salvar(df_res, nome):
    """Persiste em Parquet e gera CSV de preview."""
    pq = os.path.join(DADOS_DIR, f"{nome}.parquet")
    csv = os.path.join(PREVIEW_DIR, f"{nome}_preview.csv")
    df_pandas = df_res.toPandas()
    df_pandas.to_parquet(pq, engine="pyarrow", compression="snappy", index=False)
    df_pandas.to_csv(csv, index=False, encoding="utf-8-sig")
    print(f"  {nome}: {len(df_pandas)} linhas")
    return df_pandas


# ---- 1. SINTOMAS POR PERFIL ------------------------------------------------
print("\n[1/6] spec_sintomas_por_perfil")

df_sint = df.groupBy("ano_mes", "mes_desc", "faixa_etaria", "sexo_desc", "regiao").agg(
    F.count("*").alias("total_pessoas"),
    F.sum(F.when(F.col("sintoma_febre") == 1, 1).otherwise(0)).alias("qtd_febre"),
    F.sum(F.when(F.col("sintoma_tosse") == 1, 1).otherwise(0)).alias("qtd_tosse"),
    F.sum(F.when(F.col("sintoma_dor_garganta") == 1, 1).otherwise(0)).alias("qtd_dor_garganta"),
    F.sum(F.when(F.col("sintoma_dificuldade_respirar") == 1, 1).otherwise(0)).alias("qtd_dificuldade_respirar"),
    F.sum(F.when(F.col("sintoma_perda_cheiro_sabor") == 1, 1).otherwise(0)).alias("qtd_perda_cheiro_sabor"),
    F.sum(F.when(COND_SINTOMATICO, 1).otherwise(0)).alias("qtd_pelo_menos_1_sintoma")
)

for c in ["febre", "tosse", "dor_garganta", "dificuldade_respirar", "perda_cheiro_sabor", "pelo_menos_1_sintoma"]:
    df_sint = df_sint.withColumn(f"pct_{c}", F.round(F.col(f"qtd_{c}") / F.col("total_pessoas") * 100, 2))

df_sint = df_sint.orderBy("ano_mes", "regiao", "faixa_etaria", "sexo_desc")
salvar(df_sint, "spec_sintomas_por_perfil")


# ---- 2. BUSCA POR ATENDIMENTO ----------------------------------------------
print("\n[2/6] spec_busca_atendimento")

df_atend = df.groupBy("ano_mes", "mes_desc", "faixa_etaria", "sexo_desc", "regiao").agg(
    F.count("*").alias("total_pessoas"),
    F.sum(F.when(F.col("procurou_estabelecimento_saude") == 1, 1).otherwise(0)).alias("qtd_procurou_saude"),
    F.sum(F.when(F.col("foi_internado") == 1, 1).otherwise(0)).alias("qtd_internado"),
    F.sum(F.when(F.col("possui_plano_saude") == 1, 1).otherwise(0)).alias("qtd_plano_saude"),
    F.sum(F.when(COND_SINTOMATICO, 1).otherwise(0)).alias("qtd_sintomaticos"),
)

df_atend = (df_atend
    .withColumn("pct_procurou_saude", F.round(F.col("qtd_procurou_saude") / F.col("total_pessoas") * 100, 2))
    .withColumn("pct_internado", F.round(F.col("qtd_internado") / F.col("total_pessoas") * 100, 2))
    .withColumn("pct_plano_saude", F.round(F.col("qtd_plano_saude") / F.col("total_pessoas") * 100, 2))
    .withColumn("pct_sintomaticos", F.round(F.col("qtd_sintomaticos") / F.col("total_pessoas") * 100, 2))
    .withColumn("taxa_internacao_sintomaticos",
        F.when(F.col("qtd_sintomaticos") > 0,
               F.round(F.col("qtd_internado") / F.col("qtd_sintomaticos") * 100, 2))
         .otherwise(0))
    .orderBy("ano_mes", "regiao", "faixa_etaria", "sexo_desc"))

salvar(df_atend, "spec_busca_atendimento")


# ---- 3. IMPACTO ECONÔMICO --------------------------------------------------
print("\n[3/6] spec_impacto_economico")

df_econ = df.groupBy("ano_mes", "mes_desc", "faixa_etaria", "sexo_desc", "cor_raca_desc", "escolaridade_desc").agg(
    F.count("*").alias("total_pessoas"),
    F.sum(F.when(F.col("trabalhou_semana_passada") == 1, 1).otherwise(0)).alias("qtd_trabalhou"),
    F.sum(F.when(F.col("trabalho_remoto") == 1, 1).otherwise(0)).alias("qtd_trabalho_remoto"),
    F.sum(F.when(F.col("recebeu_bolsa_familia") == 1, 1).otherwise(0)).alias("qtd_bolsa_familia"),
    F.sum(F.when(F.col("solicitou_auxilio_emergencial") == 1, 1).otherwise(0)).alias("qtd_auxilio_emergencial"),
    F.sum(F.when(COND_SINTOMATICO, 1).otherwise(0)).alias("qtd_sintomaticos"),
)

df_econ = (df_econ
    .withColumn("pct_trabalhou", F.round(F.col("qtd_trabalhou") / F.col("total_pessoas") * 100, 2))
    .withColumn("pct_trabalho_remoto",
        F.when(F.col("qtd_trabalhou") > 0,
               F.round(F.col("qtd_trabalho_remoto") / F.col("qtd_trabalhou") * 100, 2))
         .otherwise(0))
    .withColumn("pct_bolsa_familia", F.round(F.col("qtd_bolsa_familia") / F.col("total_pessoas") * 100, 2))
    .withColumn("pct_auxilio_emergencial", F.round(F.col("qtd_auxilio_emergencial") / F.col("total_pessoas") * 100, 2))
    .withColumn("pct_sintomaticos", F.round(F.col("qtd_sintomaticos") / F.col("total_pessoas") * 100, 2))
    .orderBy("ano_mes", "faixa_etaria", "sexo_desc"))

salvar(df_econ, "spec_impacto_economico")


# ---- 4. COMPORTAMENTO NA PANDEMIA ------------------------------------------
print("\n[4/6] spec_comportamento_pandemia")

df_comp = df.groupBy("ano_mes", "mes_desc", "faixa_etaria", "sexo_desc", "regiao", "escolaridade_desc").agg(
    F.count("*").alias("total_pessoas"),
    F.sum(F.when(F.col("fez_restricao_contato") == 1, 1).otherwise(0)).alias("qtd_ficou_em_casa"),
    F.sum(F.when(F.col("fez_restricao_contato") == 2, 1).otherwise(0)).alias("qtd_reduziu_contato"),
    F.sum(F.when(F.col("fez_restricao_contato") == 3, 1).otherwise(0)).alias("qtd_vida_normal"),
    F.sum(F.when(F.col("trabalhou_semana_passada") == 1, 1).otherwise(0)).alias("qtd_trabalhou"),
    F.sum(F.when(F.col("trabalho_remoto") == 1, 1).otherwise(0)).alias("qtd_trabalho_remoto"),
    F.sum(F.when(
        ((F.col("sintoma_febre") == 1) | (F.col("sintoma_tosse") == 1)) &
        (F.col("trabalhou_semana_passada") == 1), 1).otherwise(0)).alias("qtd_sintomatico_trabalhando"),
)

df_comp = (df_comp
    .withColumn("pct_ficou_em_casa", F.round(F.col("qtd_ficou_em_casa") / F.col("total_pessoas") * 100, 2))
    .withColumn("pct_reduziu_contato", F.round(F.col("qtd_reduziu_contato") / F.col("total_pessoas") * 100, 2))
    .withColumn("pct_vida_normal", F.round(F.col("qtd_vida_normal") / F.col("total_pessoas") * 100, 2))
    .withColumn("pct_trabalho_remoto",
        F.when(F.col("qtd_trabalhou") > 0,
               F.round(F.col("qtd_trabalho_remoto") / F.col("qtd_trabalhou") * 100, 2))
         .otherwise(0))
    .withColumn("pct_sintomatico_trabalhando",
        F.round(F.col("qtd_sintomatico_trabalhando") / F.col("total_pessoas") * 100, 2))
    .orderBy("ano_mes", "regiao", "faixa_etaria"))

salvar(df_comp, "spec_comportamento_pandemia")


# ---- 5. INDICADORES REGIONAIS ----------------------------------------------
print("\n[5/6] spec_indicadores_regionais")

df_reg = df.groupBy("ano_mes", "mes_desc", "uf", "nome_uf", "regiao").agg(
    F.count("*").alias("total_pessoas"),
    F.sum(F.when(COND_SINTOMATICO, 1).otherwise(0)).alias("qtd_sintomaticos"),
    F.sum(F.when(F.col("sintoma_dificuldade_respirar") == 1, 1).otherwise(0)).alias("qtd_dificuldade_respirar"),
    F.sum(F.when(F.col("procurou_estabelecimento_saude") == 1, 1).otherwise(0)).alias("qtd_procurou_saude"),
    F.sum(F.when(F.col("foi_internado") == 1, 1).otherwise(0)).alias("qtd_internados"),
    F.sum(F.when(F.col("possui_plano_saude") == 1, 1).otherwise(0)).alias("qtd_plano_saude"),
    F.sum(F.when(F.col("trabalhou_semana_passada") == 1, 1).otherwise(0)).alias("qtd_trabalhou"),
    F.sum(F.when(F.col("trabalho_remoto") == 1, 1).otherwise(0)).alias("qtd_remoto"),
    F.sum(F.when(F.col("recebeu_bolsa_familia") == 1, 1).otherwise(0)).alias("qtd_bolsa_familia"),
    F.sum(F.when(F.col("solicitou_auxilio_emergencial") == 1, 1).otherwise(0)).alias("qtd_auxilio"),
    F.sum(F.when(F.col("fez_restricao_contato") == 1, 1).otherwise(0)).alias("qtd_ficou_em_casa"),
    F.avg("idade").alias("idade_media"),
)

for m in ["sintomaticos", "dificuldade_respirar", "procurou_saude", "internados",
          "plano_saude", "trabalhou", "remoto", "bolsa_familia", "auxilio", "ficou_em_casa"]:
    df_reg = df_reg.withColumn(f"pct_{m}", F.round(F.col(f"qtd_{m}") / F.col("total_pessoas") * 100, 2))

df_reg = df_reg.withColumn("idade_media", F.round("idade_media", 1))
df_reg = df_reg.orderBy("ano_mes", "regiao", "nome_uf")
salvar(df_reg, "spec_indicadores_regionais")


# ---- 6. EVOLUÇÃO MENSAL (visão temporal consolidada) ------------------------
print("\n[6/6] spec_evolucao_mensal")

df_evol = df.groupBy("ano_mes", "mes_desc").agg(
    F.count("*").alias("total_pessoas"),
    F.sum(F.when(COND_SINTOMATICO, 1).otherwise(0)).alias("qtd_sintomaticos"),
    F.sum(F.when(F.col("sintoma_febre") == 1, 1).otherwise(0)).alias("qtd_febre"),
    F.sum(F.when(F.col("sintoma_tosse") == 1, 1).otherwise(0)).alias("qtd_tosse"),
    F.sum(F.when(F.col("sintoma_dor_garganta") == 1, 1).otherwise(0)).alias("qtd_dor_garganta"),
    F.sum(F.when(F.col("sintoma_dificuldade_respirar") == 1, 1).otherwise(0)).alias("qtd_dificuldade_respirar"),
    F.sum(F.when(F.col("sintoma_perda_cheiro_sabor") == 1, 1).otherwise(0)).alias("qtd_perda_cheiro_sabor"),
    F.sum(F.when(F.col("procurou_estabelecimento_saude") == 1, 1).otherwise(0)).alias("qtd_procurou_saude"),
    F.sum(F.when(F.col("foi_internado") == 1, 1).otherwise(0)).alias("qtd_internados"),
    F.sum(F.when(F.col("possui_plano_saude") == 1, 1).otherwise(0)).alias("qtd_plano_saude"),
    F.sum(F.when(F.col("trabalhou_semana_passada") == 1, 1).otherwise(0)).alias("qtd_trabalhou"),
    F.sum(F.when(F.col("trabalho_remoto") == 1, 1).otherwise(0)).alias("qtd_remoto"),
    F.sum(F.when(F.col("recebeu_bolsa_familia") == 1, 1).otherwise(0)).alias("qtd_bolsa_familia"),
    F.sum(F.when(F.col("solicitou_auxilio_emergencial") == 1, 1).otherwise(0)).alias("qtd_auxilio"),
    F.sum(F.when(F.col("fez_restricao_contato") == 1, 1).otherwise(0)).alias("qtd_ficou_em_casa"),
    F.sum(F.when(F.col("fez_restricao_contato") == 2, 1).otherwise(0)).alias("qtd_reduziu_contato"),
    F.sum(F.when(F.col("fez_restricao_contato") == 3, 1).otherwise(0)).alias("qtd_vida_normal"),
    F.avg("idade").alias("idade_media"),
)

pct_cols = [
    "sintomaticos", "febre", "tosse", "dor_garganta",
    "dificuldade_respirar", "perda_cheiro_sabor",
    "procurou_saude", "internados", "plano_saude",
    "trabalhou", "remoto", "bolsa_familia", "auxilio",
    "ficou_em_casa", "reduziu_contato", "vida_normal"
]
for c in pct_cols:
    df_evol = df_evol.withColumn(f"pct_{c}", F.round(F.col(f"qtd_{c}") / F.col("total_pessoas") * 100, 2))

df_evol = df_evol.withColumn("idade_media", F.round("idade_media", 1))
df_evol = df_evol.withColumn("taxa_internacao_sintomaticos",
    F.when(F.col("qtd_sintomaticos") > 0,
           F.round(F.col("qtd_internados") / F.col("qtd_sintomaticos") * 100, 2))
     .otherwise(0))

df_evol = df_evol.orderBy("ano_mes")
evol_pd = salvar(df_evol, "spec_evolucao_mensal")

# ---------------------------------------------------------------------------
# Resumo final
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("RESUMO SPEC")
print("=" * 60)
print("  6 tabelas analíticas geradas:")
print("  1. spec_sintomas_por_perfil")
print("  2. spec_busca_atendimento")
print("  3. spec_impacto_economico")
print("  4. spec_comportamento_pandemia")
print("  5. spec_indicadores_regionais")
print("  6. spec_evolucao_mensal")

print("\n  EVOLUÇÃO MENSAL:")
for _, row in evol_pd.iterrows():
    print(f"    {row['mes_desc']}:")
    print(f"      Pessoas: {int(row['total_pessoas']):,}")
    print(f"      Sintomáticos: {row['pct_sintomaticos']}%")
    print(f"      Trabalhou: {row['pct_trabalhou']}%")
    print(f"      Auxílio: {row['pct_auxilio']}%")

print("=" * 60)

spark.stop()
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
print("\nSPEC finalizado.")
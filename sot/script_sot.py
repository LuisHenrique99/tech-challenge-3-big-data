# -*- coding: utf-8 -*-
# Camada SOT - Tratamento e normalização das 20 variáveis selecionadas
# Entrada: Parquet SOR bruto → Saída: Parquet particionado com tipos corretos e decodificações

import os, sys, shutil, tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_ORIGEM = os.path.join(BASE_DIR, "sor", "dados", "pnad_covid_sor.parquet")
PARQUET_DESTINO = os.path.join(BASE_DIR, "sot", "dados", "pnad_covid_sot.parquet")
PREVIEW_DIR = os.path.join(BASE_DIR, "sot", "preview")
TEMP_DIR = os.path.join(tempfile.gettempdir(), "pnad_covid_spark_sot")

os.makedirs(os.path.dirname(PARQUET_DESTINO), exist_ok=True)
os.makedirs(PREVIEW_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("PNAD-COVID19 - SOT") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.local.dir", TEMP_DIR) \
    .config("spark.sql.warehouse.dir", os.path.join(TEMP_DIR, "warehouse")) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SOT - Tratamento e Normalização (3 meses)")
print("=" * 60)

# Lê dados brutos da SOR
print("\n[1/6] Lendo SOR...")
df_pd_raw = pd.read_parquet(PARQUET_ORIGEM, engine="pyarrow")
df = spark.createDataFrame(df_pd_raw)
print(f"  {df.count():,} registros | {len(df.columns)} colunas")

# ---------------------------------------------------------------------------
# Seleção das 20 variáveis do PNAD-COVID + partição
# ---------------------------------------------------------------------------
print("\n[2/6] Selecionando 20 variáveis de negócio...")

COLUNAS = {
    # Demográficas
    "UF": "uf", "V1013": "mes_pesquisa", "V1022": "tipo_area",
    "A002": "idade", "A003": "sexo", "A004": "cor_raca", "A005": "escolaridade",
    # Saúde / Sintomas
    "B0011": "sintoma_febre", "B0012": "sintoma_tosse",
    "B0013": "sintoma_dor_garganta", "B0014": "sintoma_dificuldade_respirar",
    "B00111": "sintoma_perda_cheiro_sabor",
    "B002": "procurou_estabelecimento_saude", "B006": "foi_internado",
    "B011": "possui_plano_saude", "B005": "fez_restricao_contato",
    # Econômicas
    "C001": "trabalhou_semana_passada", "C011A": "trabalho_remoto",
    "D0021": "recebeu_bolsa_familia", "E001": "solicitou_auxilio_emergencial",
}

select_cols = [F.col(orig).alias(novo) for orig, novo in COLUNAS.items()
               if orig in df.columns]
select_cols.append(F.col("ano_mes"))
df_sot = df.select(select_cols)
print(f"  {len(df_sot.columns)} colunas selecionadas")

# ---------------------------------------------------------------------------
# Tipagem para INT
# ---------------------------------------------------------------------------
print("\n[3/6] Tipagem...")

INT_COLS = [
    "uf", "tipo_area", "idade", "sexo", "cor_raca", "escolaridade",
    "sintoma_febre", "sintoma_tosse", "sintoma_dor_garganta",
    "sintoma_dificuldade_respirar", "sintoma_perda_cheiro_sabor",
    "procurou_estabelecimento_saude", "foi_internado",
    "possui_plano_saude", "fez_restricao_contato",
    "trabalhou_semana_passada", "trabalho_remoto",
    "recebeu_bolsa_familia", "solicitou_auxilio_emergencial",
    "mes_pesquisa"
]
for c in INT_COLS:
    if c in df_sot.columns:
        df_sot = df_sot.withColumn(c, F.col(c).cast(IntegerType()))

# ---------------------------------------------------------------------------
# Decodificação de UF e Região
# ---------------------------------------------------------------------------
print("\n[4/6] Decodificando categorias...")

MAP_UF = {
    11: "Rondônia", 12: "Acre", 13: "Amazonas", 14: "Roraima",
    15: "Pará", 16: "Amapá", 17: "Tocantins", 21: "Maranhão",
    22: "Piauí", 23: "Ceará", 24: "Rio Grande do Norte",
    25: "Paraíba", 26: "Pernambuco", 27: "Alagoas", 28: "Sergipe",
    29: "Bahia", 31: "Minas Gerais", 32: "Espírito Santo",
    33: "Rio de Janeiro", 35: "São Paulo", 41: "Paraná",
    42: "Santa Catarina", 43: "Rio Grande do Sul",
    50: "Mato Grosso do Sul", 51: "Mato Grosso", 52: "Goiás",
    53: "Distrito Federal"
}

MAP_REGIAO = {
    11: "Norte", 12: "Norte", 13: "Norte", 14: "Norte",
    15: "Norte", 16: "Norte", 17: "Norte",
    21: "Nordeste", 22: "Nordeste", 23: "Nordeste", 24: "Nordeste",
    25: "Nordeste", 26: "Nordeste", 27: "Nordeste", 28: "Nordeste", 29: "Nordeste",
    31: "Sudeste", 32: "Sudeste", 33: "Sudeste", 35: "Sudeste",
    41: "Sul", 42: "Sul", 43: "Sul",
    50: "Centro-Oeste", 51: "Centro-Oeste", 52: "Centro-Oeste", 53: "Centro-Oeste"
}

map_uf_expr = F.create_map([F.lit(x) for item in MAP_UF.items() for x in item])
map_reg_expr = F.create_map([F.lit(x) for item in MAP_REGIAO.items() for x in item])

df_sot = df_sot.withColumn("nome_uf", map_uf_expr[F.col("uf")])
df_sot = df_sot.withColumn("regiao", map_reg_expr[F.col("uf")])

# Sexo
df_sot = df_sot.withColumn("sexo_desc",
    F.when(F.col("sexo") == 1, "Masculino")
     .when(F.col("sexo") == 2, "Feminino")
     .otherwise("Não informado"))

# Cor/Raça
df_sot = df_sot.withColumn("cor_raca_desc",
    F.when(F.col("cor_raca") == 1, "Branca")
     .when(F.col("cor_raca") == 2, "Preta")
     .when(F.col("cor_raca") == 3, "Amarela")
     .when(F.col("cor_raca") == 4, "Parda")
     .when(F.col("cor_raca") == 5, "Indígena")
     .otherwise("Ignorado"))

# Escolaridade
df_sot = df_sot.withColumn("escolaridade_desc",
    F.when(F.col("escolaridade") == 1, "Sem instrução")
     .when(F.col("escolaridade") == 2, "Fundamental incompleto")
     .when(F.col("escolaridade") == 3, "Fundamental completo")
     .when(F.col("escolaridade") == 4, "Médio incompleto")
     .when(F.col("escolaridade") == 5, "Médio completo")
     .when(F.col("escolaridade") == 6, "Superior incompleto")
     .when(F.col("escolaridade") == 7, "Superior completo")
     .when(F.col("escolaridade") == 8, "Pós-graduação")
     .otherwise("Não informado"))

# Tipo de área
df_sot = df_sot.withColumn("tipo_area_desc",
    F.when(F.col("tipo_area") == 1, "Urbana")
     .when(F.col("tipo_area") == 2, "Rural")
     .otherwise("Não informado"))

# Faixa etária
df_sot = df_sot.withColumn("faixa_etaria",
    F.when(F.col("idade") < 18, "0-17")
     .when((F.col("idade") >= 18) & (F.col("idade") <= 29), "18-29")
     .when((F.col("idade") >= 30) & (F.col("idade") <= 39), "30-39")
     .when((F.col("idade") >= 40) & (F.col("idade") <= 49), "40-49")
     .when((F.col("idade") >= 50) & (F.col("idade") <= 59), "50-59")
     .when((F.col("idade") >= 60) & (F.col("idade") <= 69), "60-69")
     .when(F.col("idade") >= 70, "70+")
     .otherwise("Não informado"))

# Mês descritivo
df_sot = df_sot.withColumn("mes_desc",
    F.when(F.col("ano_mes") == "202009", F.lit("Setembro/2020"))
     .when(F.col("ano_mes") == "202010", F.lit("Outubro/2020"))
     .when(F.col("ano_mes") == "202011", F.lit("Novembro/2020"))
     .otherwise(F.lit("Desconhecido")))

# Decodifica todos os campos Sim/Não do questionário
SIM_NAO_COLS = [
    "sintoma_febre", "sintoma_tosse", "sintoma_dor_garganta",
    "sintoma_dificuldade_respirar", "sintoma_perda_cheiro_sabor",
    "procurou_estabelecimento_saude", "foi_internado",
    "possui_plano_saude", "fez_restricao_contato",
    "trabalhou_semana_passada", "trabalho_remoto",
    "recebeu_bolsa_familia", "solicitou_auxilio_emergencial"
]

for col_name in SIM_NAO_COLS:
    df_sot = df_sot.withColumn(col_name + "_desc",
        F.when(F.col(col_name) == 1, "Sim")
         .when(F.col(col_name) == 2, "Não")
         .when(F.col(col_name) == 3, "Não sabe")
         .when(F.col(col_name) == 9, "Ignorado")
         .otherwise("Não aplicável"))

# ---------------------------------------------------------------------------
# Tratamento de nulos
# ---------------------------------------------------------------------------
print("\n[5/6] Tratando nulos...")
antes = df_sot.count()
df_sot = df_sot.filter(F.col("idade").isNotNull() & (F.col("idade") >= 0))
depois = df_sot.count()
print(f"  Removidos (idade nula/inválida): {antes - depois}")

df_sot = df_sot.fillna("Não informado", subset=[
    "nome_uf", "regiao", "sexo_desc", "cor_raca_desc",
    "escolaridade_desc", "tipo_area_desc", "faixa_etaria"
])

# ---------------------------------------------------------------------------
# Salva Parquet particionado
# ---------------------------------------------------------------------------
print(f"\n[6/6] Salvando SOT particionado...")
df_pd = df_sot.toPandas()

df_pd.to_parquet(
    PARQUET_DESTINO, engine="pyarrow", compression="snappy",
    index=False, partition_cols=["ano_mes"]
)

# Gera preview CSV por mês
for m in df_pd["ano_mes"].unique():
    prev = os.path.join(PREVIEW_DIR, f"sot_preview_{m}.csv")
    df_pd[df_pd["ano_mes"] == m].head(100).to_csv(prev, index=False, encoding="utf-8-sig")

print(f"  Parquet: {PARQUET_DESTINO}")
print(f"  Total: {len(df_pd):,} registros | {len(df_pd.columns)} colunas")

print("\n" + "=" * 60)
print("RESUMO SOT")
print("=" * 60)
for m in sorted(df_pd["ano_mes"].unique()):
    print(f"  {m}: {len(df_pd[df_pd['ano_mes'] == m]):,} registros")
print(f"  TOTAL: {len(df_pd):,} | Partições: {df_pd['ano_mes'].nunique()}")
print(f"  Colunas: {len(df_pd.columns)}")
print("=" * 60)

spark.stop()
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR, ignore_errors=True)
print("\nSOT finalizado.")

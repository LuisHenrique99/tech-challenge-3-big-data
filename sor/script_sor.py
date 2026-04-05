# Camada SOR - Ingestão dos dados brutos PNAD-COVID-19
# Lê os CSVs de Set/Out/Nov 2020, compatibiliza schemas e salva em Parquet particionado

import os, sys, shutil, tempfile

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DESTINO = os.path.join(BASE_DIR, "sor", "dados", "pnad_covid_sor.parquet")
PREVIEW_DIR = os.path.join(BASE_DIR, "sor", "preview")
TEMP_DIR = os.path.join(tempfile.gettempdir(), "pnad_covid_spark_sor")

# Mapeamento mês -> arquivo CSV
MESES = {
    "202009": os.path.join(BASE_DIR, "arquivos_originais", "202009", "PNAD_COVID_092020.csv"),
    "202010": os.path.join(BASE_DIR, "arquivos_originais", "202010", "PNAD_COVID_102020.csv"),
    "202011": os.path.join(BASE_DIR, "arquivos_originais", "202011", "PNAD_COVID_112020.csv"),
}

# Colunas que só existem em novembro (A006A, A006B, A007A)
COLUNAS_EXTRAS_NOV = ["A006A", "A006B", "A007A"]

os.makedirs(os.path.dirname(PARQUET_DESTINO), exist_ok=True)
os.makedirs(PREVIEW_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("PNAD-COVID19 - SOR") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.local.dir", TEMP_DIR) \
    .config("spark.sql.warehouse.dir", os.path.join(TEMP_DIR, "warehouse")) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SOR - Ingestão de Dados Brutos (3 meses)")
print("=" * 60)

dfs = []

for i, (ano_mes, csv_path) in enumerate(MESES.items(), 1):
    print(f"\n[{i}/3] Lendo {ano_mes}: {os.path.basename(csv_path)}")
    if not os.path.exists(csv_path):
        print(f"  AVISO: arquivo não encontrado, pulando...")
        continue

    df_sp = spark.read.csv(csv_path, header=True, inferSchema=False, sep=",")
    n = df_sp.count()
    print(f"  {n:,} registros | {len(df_sp.columns)} colunas")

    # Adiciona coluna de partição
    df_sp = df_sp.withColumn("ano_mes", F.lit(ano_mes))

    # Compatibiliza schema: meses 09/10 não têm as colunas extras de novembro
    for col_extra in COLUNAS_EXTRAS_NOV:
        if col_extra not in df_sp.columns:
            df_sp = df_sp.withColumn(col_extra, F.lit(None).cast("string"))

    df_pd = df_sp.toPandas()
    dfs.append(df_pd)

    # Salva preview (100 primeiras linhas de cada mês)
    df_pd.head(100).to_csv(
        os.path.join(PREVIEW_DIR, f"sor_preview_{ano_mes}.csv"),
        index=False, encoding="utf-8-sig"
    )

spark.stop()

# Unifica todos os meses
print(f"\nUnificando {len(dfs)} meses...")
df_all = pd.concat(dfs, ignore_index=True)
cols = ["ano_mes"] + [c for c in df_all.columns if c != "ano_mes"]
df_all = df_all[cols]
print(f"Total: {len(df_all):,} registros | {len(df_all.columns)} colunas")

# Persiste como Parquet particionado por ano_mes
df_all.to_parquet(
    PARQUET_DESTINO, engine="pyarrow", compression="snappy",
    index=False, partition_cols=["ano_mes"]
)
print(f"Parquet salvo: {PARQUET_DESTINO}")

print("\n" + "=" * 60)
print("RESUMO SOR")
print("=" * 60)
for m in MESES:
    print(f"  {m}: {len(df_all[df_all['ano_mes'] == m]):,} registros")
print(f"  TOTAL: {len(df_all):,} registros | Partições: 3")
print("=" * 60)

# Limpa temp do Spark
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR, ignore_errors=True)

print("\nSOR finalizado.")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, sha2, coalesce, lit
from functools import reduce
from pyspark.sql import DataFrame
import re

#iniciar spark e o hadoop integrado ao S3
spark = (
    SparkSession.builder
    .appName("NY Taxi Rides Transformation")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.375")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

#definir S3
bucket = "talita-costa-pipeline"
types = ["yellow", "green"]

#normalizar schema para cada tipo, pois os nomes das colunas são diferentes
datetime_cols = {
    "yellow": {
        "pickup": "tpep_pickup_datetime",
        "dropoff": "tpep_dropoff_datetime"
    },
    "green": {
        "pickup": "lpep_pickup_datetime",
        "dropoff": "lpep_dropoff_datetime"
    }
}

#listar as partições existentes no S3
def listar_particoes(path):
    path_obj = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = path_obj.getFileSystem(spark._jsc.hadoopConfiguration())
    lista = []
    try:
        for status in fs.listStatus(path_obj):
            nome = status.getPath().getName()
            if nome.startswith("year="):
                year_path = f"{path}/{nome}"
                year_path_obj = spark._jvm.org.apache.hadoop.fs.Path(year_path)
                for month_status in fs.listStatus(year_path_obj):
                    month_name = month_status.getPath().getName()
                    if month_name.startswith("month="):
                        lista.append(f"{year_path}/{month_name}")
    except Exception as e:
        print(f"⚠️ Erro ao listar partições em {path}: {e}")
    return lista

def extrair_ano_mes(path):
    match = re.search(r'year=(\d+)/month=(\d+)', path)
    if match:
        return int(match.group(1)), int(match.group(2))
    else:
        return None, None

#ler os dados da camada raw por tipo
for t in types:
    print(f"🚕 Iniciando transformação para tipo: {t}")

    pickup_col = datetime_cols[t]["pickup"]
    dropoff_col = datetime_cols[t]["dropoff"]

    raw_base_path = f"s3a://{bucket}/raw/ny/taxi_rides/{t}"
    silver_path = f"s3a://{bucket}/silver/ny/taxi_rides/{t}"

    particoes = listar_particoes(raw_base_path)
    
    dfs = []
    for p in particoes:
        try:
            year, month = extrair_ano_mes(p)
            if year is None or month is None:
                print(f"⚠️ Não foi possível extrair ano/mês do path {p}")
                continue

            df = spark.read.parquet(p) \
                .withColumn("year", lit(year)) \
                .withColumn("month", lit(month))

            dfs.append(df)

        except Exception as e:
            print(f"⚠️ Erro lendo partição {p}: {e}")

    if not dfs:
        print(f"❌ Nenhum dado encontrado para {t}")
        continue

    df_raw = reduce(DataFrame.unionByName, dfs)

    #padronizar o nome das colunas para os 2 tipos
    df_renamed = df_raw \
        .withColumnRenamed(pickup_col, "pickup_datetime") \
        .withColumnRenamed(dropoff_col, "dropoff_datetime") 

    #trasformar os dados com as premissas desejáveis
    df_selected = df_renamed.select(
        col("VendorID").cast("string").alias("vendor_id"),
        coalesce(col("passenger_count").cast("int"), lit(0)).alias("passenger_count"),
        col("total_amount").cast("double"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("type"),
        col("year").cast("int"),
        col("month").cast("int")
    ).withColumn(
        "key",
        sha2(concat_ws("||", col("vendor_id"), col("passenger_count").cast("string"), col("pickup_datetime").cast("string"), col("dropoff_datetime").cast("string"), col("type")), 256)
    )

    #garantir a unicidade dos dados caso na camada raw tenha duplicidade de eventos
    #coluna Key usada como chave primária da tabela
    df_deduplicated = df_selected.dropDuplicates(["key"])

    #validar os dados com as premissas desejáveis
    null_vendor_id = df_deduplicated.filter(col("vendor_id").isNull()).count()
    if null_vendor_id > 0:
        raise ValueError(f"❌ Validação falhou: existem {null_vendor_id} valores nulos na coluna VendorID")

    #escrever os dados no S3, por tipo e período, com escrita particionada
    df_deduplicated.write.mode("append") \
        .partitionBy("year", "month") \
        .parquet(silver_path)

    print("✅ Transformação para camada Silver finalizada com sucesso.")

#encerrar spark
spark.stop()
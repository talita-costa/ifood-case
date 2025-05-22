import os
import requests
from urllib3.exceptions import InsecureRequestWarning
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Desabilitar aviso de certificado inseguro (SSL) / minha maquina estava bloqueando o certificado
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

#iniciar spark integrado ao S3
spark = SparkSession.builder \
    .appName("NY Taxi Rides") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

#configurar S3 e as premissas
bucket = "talita-costa-pipeline"
months = ["01", "02", "03", "04", "05"]
types = ["yellow", "green"]

#baixar os arquivos no formato .parquet
def download_data(url, destino):
    if not os.path.exists(destino):
        print(f"📥 Baixando {url}")
        response = requests.get(url, verify=False, timeout=30)
        response.raise_for_status()
        with open(destino, "wb") as f:
            f.write(response.content)
        print(f"✅ Download concluído: {destino}")
    else:
        print(f"⚠️ Arquivo já existe localmente: {destino}")

#ler, processar e gravar os dados no S3, por tipo e período, com escrita particionada
for t in types:
    s3_path = f"s3a://{bucket}/raw/ny/taxi_rides/{t}"
    for month in months:
        file_name = f"{t}_tripdata_2023-{month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        local_path = f"/tmp/{file_name}"
        print(f"🚀 Iniciando ingestão do arquivo: {url}")
    
        try:
            download_data(url, local_path)

            df = spark.read.parquet(local_path)

            df = df.withColumn("year", lit(2023)) \
                   .withColumn("month", lit(int(month))) \
                   .withColumn("type", lit(t))
            
            df.write.mode("append") \
                .partitionBy("year", "month") \
                .parquet(s3_path)
            
            print(f"✅ Dados de {t} taxi {month}/2023 salvos em {s3_path}")

        except Exception as e:
            print(f"❌ Erro ao processar {t} taxi {month}/2023: {e}")

        #limpar arquivo local
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
                print(f"🧹 Arquivo local removido: {local_path}")

#encerrar spark
spark.stop()

import os
import sys

os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/microsoft-11.jdk/Contents/Home"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as _sum, round

def main():
    # 1. LIGANDO O MOTOR
    print("Iniciando o processo de ETL de Vendas Diárias...")
    spark = SparkSession.builder \
        .appName("Vendas_Diarias_ETL") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    # 2. LENDO OS DADOS
    df_telefonia = spark.read.csv("data/base_telefonia.csv", header=True, inferSchema=True)
    df_pessoas = spark.read.csv("data/base_pessoas.csv", header=True, inferSchema=True)

    # 3. FILTRANDO APENAS VENDAS REAIS
    df_vendas = df_telefonia.filter((col("Valor venda").isNotNull()) & (col("Motivo") == "Venda"))
    df_vendas = df_vendas.withColumn("Data", to_date(col("inicio_ligacao")))

    # 4. DESCOBRINDO QUEM É O CHEFE DE QUEM
    df_cruzado = df_vendas.join(
        df_pessoas.select("Username", col("Líder da Equipe").alias("lideranca")), 
        on="Username", 
        how="inner"
    )
    df_cruzado = df_cruzado.fillna("Sem Lideranca", subset=["lideranca"])

    # 5. CALCULANDO O TOTAL
    df_vendas_diarias = df_cruzado.groupBy("Data", "lideranca") \
        .agg(round(_sum("Valor venda"), 2).alias("Valor_Total_Vendas"))

    # 6. SALVANDO O RESULTADO FINAL NA PASTA CORRETA
    caminho_saida = "Scripts em PySpark/output_vendas_diarias_parquet"
    print(f"Salvando os dados particionados em: {caminho_saida} ...")
    
    df_vendas_diarias.repartition(1) \
        .write \
        .mode("overwrite") \
        .partitionBy("lideranca") \
        .parquet(caminho_saida)

    print(f"✅ Sucesso! Os arquivos Parquet foram gerados e divididos por líder na pasta '{caminho_saida}'.")
    
    # Desliga o motor do Spark
    spark.stop()

if __name__ == "__main__":
    main()

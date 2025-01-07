from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
from pyspark.sql import SparkSession
import os
import boto3
from dotenv import load_dotenv

def salvar_csv_como_json():
    # Configuração do Spark
    spark = SparkSession.builder.appName("CSV_to_JSON").getOrCreate()

    # Caminhos de entrada e saída
    input_path = "dags/municipios-estados/csv/"
    output_path = "dags/municipios-estados/json/"

    # Criar a pasta de saída, se não existir
    os.makedirs(output_path, exist_ok=True)

    # Listar todos os arquivos CSV na pasta
    csv_files = [os.path.join(input_path, file) for file in os.listdir(input_path) if file.endswith('.csv')]

    # Converter cada CSV para JSON
    for csv_file in csv_files:
        df = spark.read.csv(csv_file, header=True, inferSchema=True)
        json_file_name = os.path.basename(csv_file).replace(".csv", ".json")
        df.write.mode("overwrite").json(os.path.join(output_path, json_file_name))

    print(f"Todos os arquivos CSV foram convertidos para JSON em: {output_path}")

def importar_minio():
    load_dotenv()

    # Configuração do MinIO
    MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT')
    MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')
    BUCKET_NAME = "aula-08"

    # Caminho da pasta com os JSONs
    json_folder = "dags/municipios-estados/json/"

    # Conexão com o MinIO
    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Certificar-se de que o bucket existe
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except Exception:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' criado com sucesso!")

    # Enviar os arquivos JSON para o MinIO
    for root, _, files in os.walk(json_folder):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            object_name = os.path.relpath(file_path, json_folder)
            s3_client.upload_file(file_path, BUCKET_NAME, object_name)
            print(f"Arquivo '{file_name}' enviado com sucesso para o bucket '{BUCKET_NAME}'.")

def salvar_parquet():
    # Caminho de entrada e saída
    input_path = "dags/municipios-estados/csv/"
    parquet_output_path = "s3a://aula-08/municipios-parquet/"

    # Configuração do Spark para MinIO
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", os.environ.get('MINIO_SPARK'))
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ.get('MINIO_ACCESS_KEY'))
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ.get('MINIO_SECRET_KEY'))
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.endpoint.region", 'sa-east-1')

    # Converter CSVs para Parquet
    for csv_file in csv_files:
        df = spark.read.csv(csv_file, header=True, inferSchema=True)
        parquet_file_name = os.path.basename(csv_file).replace(".csv", ".parquet")
        df.write.mode("overwrite").parquet(os.path.join(parquet_output_path, parquet_file_name))

    print(f"Todos os arquivos CSV foram convertidos para Parquet e salvos no MinIO.")

with DAG(
    dag_id='aula9',
    description='Exercício de automação do fluxo utilizando Airflow',
    schedule_interval=None,
    start_date=datetime(2024,1,2)
) as dag:
    
    task1 = PythonOperator(
        task_id="salvar_csv_como_json",
        python_callable=salvar_csv_como_json
    )

    task2 = PythonOperator(
        task_id="importar_minio",
        python_callable=importar_minio
    )

    task3 = PythonOperator(
        task_id="salvar_parquet",
        python_callable=salvar_parquet
    )

task1>>task2>>task3
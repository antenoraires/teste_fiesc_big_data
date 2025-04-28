from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import s3fs
from deltalake import write_deltalake

def seccao_pyspark(name_section,minio_client):
    
    # Configuração do Spark
    conf = SparkConf() \
        .setAppName(f"{name_section}") \
        .set("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1,org.apache.hadoop:hadoop-aws:3.3.4") \
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .set("spark.hadoop.fs.s3a.access.key", f"{minio_client._provider.retrieve().access_key}") \
        .set("spark.hadoop.fs.s3a.secret.key", f"{minio_client._provider.retrieve().secret_key}") \
        .set("spark.hadoop.fs.s3a.path.style.access", "true") \
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set("spark.jars", "fiesc_big_data/assents/jars/postgresql-42.6.0.jar") \
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .set("spark.hadoop.fs.s3a.fast.upload", "true") \
        .set("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") \
        .set("spark.hadoop.fs.s3a.multipart.size", "104857600") \
        .set("spark.hadoop.fs.s3a.threads.max", "100") \
        .set("spark.hadoop.fs.s3a.buffer.dir", "/tmp") \
        .set("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .set("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer") 
    # Criar a sessão Spark
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    return spark
    

def delta_lake_minio(minio_client, bucket_name,
                     camada, name_arquive, df):
    
    if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)


    storage_options ={
        "AWS_ACCESS_KEY_ID": f"{minio_client._provider.retrieve().access_key}",
        "AWS_SECRET_ACCESS_KEY": f"{minio_client._provider.retrieve().secret_key}",
        "AWS_ENDPOINT_URL": "http://localhost:9000",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }

    delta_path = f"s3://{bucket_name}/{camada}/{name_arquive}"

    # Salvando no minio
    write_deltalake(
    delta_path,
    df,
    mode = "overwrite",
    storage_options=storage_options,
    )


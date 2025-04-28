from minio import Minio
from fiesc_big_data import Pipeline
from fiesc_big_data.control import seccao_pyspark
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carrega as variáveis do arquivo .env
load_dotenv()

# Acessa as variáveis

access_key = os.getenv('ACESSE_KEY_MINIO')
secret_key = os.getenv('ACESSE_SECRET_MINIO')
url_minio = os.getenv('ACESSE_URL_MINIO')
user_postgres = os.getenv('USER_POSTGRES')
password_postgres = os.getenv('PASSWORD_POSTGRES')


# Configurações do MinIO
minio_client = Minio(
    url_minio,  # Endereço do servidor MinIO atualizado
    access_key=access_key,  # Substitua pelo seu access key
    secret_key=secret_key,  # Substitua pelo seu secret key
    secure=False  # True se estiver usando HTTPS
)

engine = create_engine(f"postgresql://{user_postgres}:{password_postgres}@localhost:5432/meubanco")

spark = seccao_pyspark("Fiesc", minio_client)

# Intanciar a classe Pipeline
pipeline = Pipeline(minio_client, engine, spark)
# Ingestão de dados
pipeline.ingest(pipeline.minio_client, pipeline.config)
# Transformação de dados
pipeline.transform(pipeline.minio_client, pipeline.config)
# Aplicação de regras
pipeline.rules(pipeline.minio_client, pipeline.config, pipeline.spark)
# carregando no DW
pipeline.load(pipeline.engine, pipeline.spark)

# Encerrando sessão do Spark
print("Encerrando sessão do Spark...")
spark.stop()
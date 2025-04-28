import pandas as pd
from io import BytesIO
import re
import unicodedata
from sqlalchemy import text


def read_csv_from_minio(
    minio_client,
    bucket_name,
    caminho_arquivo,
):
    response = minio_client.get_object(bucket_name, caminho_arquivo)
    file = BytesIO(response.read())
    # Lê o CSV diretamente do objeto BytesIO
    df = pd.read_csv(file, encoding="utf-8")
    response.close()
    response.release_conn()
    return df


def clear_text(texto):
    if isinstance(texto, str):
        # Remove acentos
        texto = (
            unicodedata.normalize("NFKD", texto)
            .encode("ASCII", "ignore")
            .decode("ASCII")
        )
        # Remove tudo que não for letra, número ou espaço
        texto = re.sub(r"[^A-Za-z0-9\s]", "", texto)
    return texto


def read_spark_delta(spark, path_delta):
    # 2. Lendo a tabela Delta armazenada no MinIO
    df = spark.read.format("delta").load(f"s3a://{path_delta}")
    return df


def write_spark_delta(df, path_delta):
    # 3. Escrevendo o DataFrame em formato Delta no MinIO
    df.write.format("delta").mode("overwrite").save(f"s3a://{path_delta}")


def execute_rules(table_delta, spark, query, path_delta):
    # Lendo a tabela Delta armazenada no MinIO
    df = read_spark_delta(spark, f"fiesc/silver/{table_delta}")

    df_disciplinas = read_spark_delta(spark, "fiesc/silver/disciplinas")

    df_cursos = read_spark_delta(spark, "fiesc/silver/cursos")

    # Registrar como tabela temporária
    df.createOrReplaceTempView(f"{table_delta}")
    df_disciplinas.createOrReplaceTempView("disciplinas")
    df_cursos.createOrReplaceTempView("cursos")

    # Rule 1
    # Executar uma consulta SQL
    df = spark.sql(query)

    # Insira o DataFrame resultante em na camada gold
    write_spark_delta(df, path_delta)
    print(f"✔️ Tabela {table_delta} salva com sucesso na camada gold.")


def render_sql_template(template_path):
    with open(template_path, "r", encoding="utf-8") as file:
        sql = file.read()
    return sql


def execute_query(engine, path):
    query = render_sql_template(path)
    with engine.connect() as connection:
        result = connection.execute(text(query))
        for row in result:
            print(row)

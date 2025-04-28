from fiesc_big_data.ingest import ingest_sheets, save_csv, save_minio 
from fiesc_big_data.etl import read_csv_from_minio, clear_text, render_sql_template, execute_rules, execute_query
from fiesc_big_data.control import delta_lake_minio
import yaml
import pandas as pd
from fiesc_big_data.etl import render_sql_template, read_spark_delta

from pyspark.sql.functions import col, datediff, current_date, when, floor
from minio import Minio



class Pipeline:
    def __init__(self, engine,minio_client,spark):
        self.spark = spark
        self.minio_client = minio_client
        self.engine = engine

        path = "fiesc_big_data/assents/config.yml"
        # Carregar o arquivo YAML
        with open(path) as file:
            config = yaml.safe_load(file)
        
        self.config = config

    def ingest(self, minio_client,config):

        path_credentials = config['general']['path_credentials']
        name_table = config['general']['name_sheets']
        # Extrair dados do Google Sheets
        print("Extraindo Cursos")
        cursos = ingest_sheets(path_credentials, name_table, 0)
        print("Extraindo Disciplinas")
        disciplinas = ingest_sheets(path_credentials, name_table, 1)
        print("Extraindo Turmas")
        turmas = ingest_sheets(path_credentials, name_table, 2)
        
        # Salvar os dados em CSV
        save_csv(cursos,'cursos')
        save_csv(disciplinas,'disciplinas')
        save_csv(turmas,'turmas')
        print("✔️ Arquivos CSV salvos com sucesso.")

        print("salvando dados do google sheets no minio")
        bucket_name = config['minio']['bucket']
        camada = "bronze"
        fonte_dados_nativos = "nativos"
        pasta_destino_nativos = f"{camada}/{fonte_dados_nativos}/"
        fonte_sheets = "sheets"
        pasta_destino = f"bronze/{fonte_sheets}/"

        # salvando raw no minio
        for key, value in config['raw'].items():
            print(f"Salvando {key} no minio")
            save_minio(minio_client, bucket_name, pasta_destino, value)
        print('✔️ Dados do Google Sheets salvos com sucesso no MinIO.')

        # salvando nativos no minio
        for key, value in config['nativos'].items():
            print(f"Salvando {key} no minio")
            save_minio(minio_client, bucket_name, pasta_destino_nativos, value)
        print('✔️ Dados nativos salvos com sucesso no MinIO.')


    
    def transform(self, minio_client, config):

        bucket_name = config['minio']['bucket']
        pasta_destino_sheets = "bronze/sheets/"
        colunas_cursos = ['id_curso','nome_curso','area_conhecimento']
        colunas_disciplinas = ['id_disciplina','id_curso','nome_disciplina','sigla',
                                'dia_semana','carga_horaria', 'avaliacao']
        colunas_turmas = ['id_turma','nome_turma','data_inicio','data_fim','codigo_curso',]
        pasta_nativos = config['minio']['camadas']['bronze']['nativos']
        camada = config['minio']['camadas']['silver']

        print("Transformando os dados")
        print(100*"=")
        df_cursos = read_csv_from_minio(minio_client, bucket_name,
                                        pasta_destino_sheets + "cursos.csv")
        df_cursos = df_cursos.iloc[:, 0].str.split(';', expand=True)
        df_cursos.columns = colunas_cursos
        df_cursos = df_cursos.applymap(clear_text)

        df_disciplinas = read_csv_from_minio(minio_client, bucket_name,
                                            pasta_destino_sheets + "disciplinas.csv")
        df_disciplinas = df_disciplinas.iloc[:, 0].str.split(';', expand=True)
        df_disciplinas.columns = colunas_disciplinas
        df_disciplinas['carga_horaria'] = df_disciplinas['carga_horaria'].astype(int)
        df_disciplinas['avaliacao'] = df_disciplinas['avaliacao'].replace('','0').astype(int)
        df_disciplinas = df_disciplinas.applymap(clear_text)

        df_turmas = read_csv_from_minio(minio_client, bucket_name,
                                pasta_destino_sheets + "turmas.csv")
        df_turmas = df_turmas.iloc[:, 0].str.split(';', expand=True)
        df_turmas.columns = colunas_turmas
        df_turmas['data_inicio'] = pd.to_datetime(df_turmas['data_inicio'], format="%Y-%m-%d")
        df_turmas['data_fim'] = pd.to_datetime(df_turmas['data_fim'], format="%Y-%m-%d")
        df_turmas = df_turmas.applymap(clear_text)
        
        print("✔️ Dados transformados com sucesso.")
        print(100*"=")

        # Lendo dados nativos do MinIO

        df_matricula_aula = read_csv_from_minio(minio_client, bucket_name,
                                        pasta_nativos + "matricula_aulas.csv")

        df_matriculas_disciplina = read_csv_from_minio(minio_client, bucket_name,
                                                    pasta_nativos + "matricula_disciplinas.csv")

        df_matriculas_notas = read_csv_from_minio(minio_client, bucket_name,
                                                pasta_nativos + "matricula_notas.csv")

        df_turma_avaliacao = read_csv_from_minio(minio_client, bucket_name,
                                                pasta_nativos + "turma_avaliacoes.csv")
        
        print("Salvando os dados no Delta Lake Cursos")
        delta_lake_minio(minio_client, bucket_name,
                        camada,
                        "cursos",
                        df_cursos)
        print("Salvando os dados no Delta Lake Disciplinas")
        delta_lake_minio(minio_client, bucket_name,
                        camada,
                        "disciplinas",
                        df_disciplinas)
        print("Salvando os dados no Delta Lake Turmas")
        delta_lake_minio(minio_client, bucket_name,
                        camada,
                        "turmas",
                        df_turmas)
        print("Salvando os dados no Delta Lake Matricula Aulas")
        delta_lake_minio(minio_client, bucket_name, 
                        camada,
                        "matricula_aulas",
                        df_matricula_aula)
        print("Salvando os dados no Delta Lake Matricula Disciplinas")
        delta_lake_minio(minio_client, bucket_name,
                        camada,
                        "matricula_disciplinas",
                        df_matriculas_disciplina)
        print("Salvando os dados no Delta Lake Matricula Notas")
        delta_lake_minio(minio_client, bucket_name,
                        camada,
                        "matricula_notas",
                        df_matriculas_notas)
        print("Salvando os dados no Delta Lake Turma Avaliacoes")
        delta_lake_minio(minio_client, bucket_name,
                        camada,
                        "turma_avaliacoes",
                        df_turma_avaliacao)
        print("✔️ Dados transformados e salvos com sucesso no Delta Lake.")
        print(100*"=")
    
    def rules(self, minio_client, config, spark):
        rule_notas = config['sql']['notas']
        rule_notas = render_sql_template(rule_notas)
        rule_presenca = config['sql']['rule_presenca']
        rule_presenca = render_sql_template(rule_presenca)

        # Executando regra de notas 
        print("Executando regra de notas")
        delta_table_notas = "matricula_notas"
        path_delta = "fiesc/gold/situacao_notas_alunos"
        execute_rules(delta_table_notas, spark, rule_notas, path_delta)
        print("✔️ Regras de notas executadas com sucesso.")
        print(100*"=")

        # Executando regra de presenca
        print("Executando regra de presenca")
        delta_table_presenca = "matricula_aulas"
        path_delta = "fiesc/gold/situacao_presenca_alunos"
        execute_rules(delta_table_presenca, spark, rule_presenca, path_delta)
        print("✔️ Regras de presença executadas com sucesso.")
        print(100*"=")

    def load(self, engine, spark):
        db_url = "jdbc:postgresql://localhost:5432/meubanco"
        properties = {
            "user": "meuuser",
            "password": "minhasenha",
            "driver": "org.postgresql.Driver"
        }
        path_situacao_notas = "fiesc/gold/situacao_notas_alunos"
        path_situacao_presenca = "fiesc/gold/situacao_presenca_alunos"
        path_situacao_turmas = "fiesc/silver/turma_avaliacoes"
        turma = "fiesc/silver/turmas"
        sql_init = "fiesc_big_data/sql/init.sql"
        rule = "fiesc_big_data/sql/rule.sql"

        # Criando tabelas
        execute_query(engine, sql_init)


        df_situacao_turmas = read_spark_delta(spark, path_situacao_turmas)
        df_situacao_turmas.createOrReplaceTempView("situacao_turmas")

        df_turmas = read_spark_delta(spark,turma)
        df_turmas.createOrReplaceTempView("turmas")

        df_situacao_notas = read_spark_delta(spark, path_situacao_notas)
        df_situacao_notas.createOrReplaceTempView("situacao_notas")

        df_situacao_presenca = read_spark_delta(spark, path_situacao_presenca)
        df_situacao_presenca.createOrReplaceTempView("situacao_presenca")

        # Exemplo: ler dados de uma tabela
        df_alunos = spark.read.jdbc(url=db_url, table="alunos", properties=properties)

        df_alunos_idade = df_alunos.withColumn(
            "idade",
            floor(datediff(current_date(), col("data_nascimento")) / 365.25)
        ).withColumn(
            "idade_valida",
            when((col("idade") >= 18) & (col("idade") <= 90), "sim")
            .otherwise("não")
        )
        df_alunos_idade.createOrReplaceTempView("alunos")

        query = render_sql_template(rule)

        df = spark.sql(query)


        # Escrever no Banco
        df.write \
            .format("jdbc") \
            .option("url", db_url) \
            .option("dbtable", "matriculas") \
            .option("user", "meuuser") \
            .option("password", "minhasenha") \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()

        


        





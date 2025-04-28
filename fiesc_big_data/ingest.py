import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
from pathlib import Path
import os


def ingest_sheets(path_credentials:str,
                  name_table:str,
                  index:int):
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    # Autenticar com o arquivo JSON da conta de serviço
    creds = ServiceAccountCredentials.from_json_keyfile_name(path_credentials, scope)
    client = gspread.authorize(creds)
    # Abrir a planilha pelo nome
    tables = client.open(name_table)

    table = tables.get_worksheet(index).get_all_records()
    print(f"Extraindo da tabela {name_table} o index {index} onde;",
      "\n\t0 - Cursos",
      "\n\t1 - Disciplinas",
      "\n\t2 - Turmas")
    return table
 
def save_csv(data_dict, name:str):

    output_dir = "fiesc_big_data/raw"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    colunas = data_dict[0].keys()
    filepath = Path(output_dir) / f"{name}.csv"

    # Escreve o CSV
    with open(filepath, mode='w', newline='', encoding='utf-8-sig') as arquivo_csv:
        writer = csv.DictWriter(arquivo_csv, fieldnames=colunas, delimiter=';')
        writer.writeheader()
        writer.writerows(data_dict)

    print(f"✔️ Arquivo '{name}.csv' salvo com sucesso.")


def save_minio(minio_client,bucket_name,pasta_destino,arquivo):

    if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
   
    objeto_nome = pasta_destino + os.path.basename(arquivo)
    
    # Faz o upload
    minio_client.fput_object(
        bucket_name,
        objeto_nome,
        arquivo
    )
    print(f"Arquivo {arquivo} enviado com sucesso para {objeto_nome}")
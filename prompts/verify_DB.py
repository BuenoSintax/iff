import duckdb
import boto3
import os
import re
from datetime import datetime
from botocore.exceptions import ClientError

def parse_filename(filename: str):
    """
    Recebe um nome de arquivo no formato:
       TableName_YYYYMMDD.csv ou TableName_YYYYMMDD_batch.csv
    e retorna uma tupla (table, date, batch).
    
    Exemplo:
       "Flavours_20240505_1.csv" -> ("Flavours", "20240505", "1")
       "IngredientsRawMaterial_20240505.csv" -> ("IngredientsRawMaterial", "20240505", None)
    """
    base = filename[:-4] if filename.endswith('.csv') else filename
    parts = base.split('_')
    table = parts[0] if len(parts) > 0 else None
    date = parts[1] if len(parts) > 1 else None
    batch = parts[2] if len(parts) > 2 else None
    return table, date, batch

def list_files_from_s3(bucket_name: str, prefix: str) -> list:
    """
    Lista todos os arquivos (objetos) dentro de um prefixo no bucket S3.
    """
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    
    files = []
    for page in page_iterator:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('/'):
                files.append(key)
    return files

def check_table_exists(con, table_name: str) -> bool:
    """
    Verifica se uma tabela com o nome especificado existe no DuckDB.
    DuckDB converte nomes não citados para minúsculas.
    """
    query = "SELECT count(*) FROM information_schema.tables WHERE table_name = ?"
    result = con.execute(query, (table_name.lower(),)).fetchone()[0]
    return result > 0

if __name__ == "__main__":
    bucket_name = "iffdatatest"    # Nome do bucket S3
    prefix = "landing/"            # Prefixo onde estão os arquivos (ex.: "landing/")
    
    # Lista os arquivos CSV no bucket
    files = list_files_from_s3(bucket_name, prefix)
    csv_files = [f for f in files if f.endswith('.csv')]
    
    directories_set = set()
    total_files = 0

    # Conecta ao DuckDB (usa um arquivo 'mydatabase.duckdb'; pode ser ':memory:' se preferir)
    con = duckdb.connect('mydatabase.duckdb')
    
    for full_key in csv_files:
        # Remove o prefixo e pega apenas o nome do arquivo
        rel_key = full_key[len(prefix):] if full_key.startswith(prefix) else full_key
        directory = os.path.dirname(rel_key)
        if directory:
            directories_set.add(directory)
        filename = os.path.basename(rel_key)
        total_files += 1
        
        # Extrai as variáveis do nome
        table, date, batch = parse_filename(filename)
        print("Arquivo lido:", filename)
        print("  Tabela:", table)
        print("  Data  :", date)
        print("  Batch :", batch)
        
        # Constrói o nome da tabela no DuckDB (apenas o nome da tabela em minúsculas)
        dp_table_name = f"dp_{table.lower()}_staging"
        print("Tabela DuckDB esperada:", dp_table_name)
        
        if check_table_exists(con, dp_table_name):
            print(f"-> Tabela {dp_table_name} existe no DuckDB.\n")
        else:
            print(f"-> Tabela {dp_table_name} NÃO existe no DuckDB.\n")
    
    print("Total de arquivos lidos:", total_files)
    print("Total de pastas distintas:", len(directories_set))

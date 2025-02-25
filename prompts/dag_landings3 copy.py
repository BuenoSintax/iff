from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import duckdb
import boto3
import os

# Nome do arquivo do banco de dados DuckDB
DB_FILE = 'iff_db.duckdb'

# Definição dos campos das tabelas
table_fields = {
    "flavours": [
        ("row_id", "INTEGER PRIMARY KEY"),
        ("flavour_id", "INTEGER"),
        ("name", "VARCHAR"),
        ("description", "VARCHAR"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_value", "INTEGER"),
        ("origin_date", "DATE"),
        ("merge_status", "INTEGER DEFAULT 0")
    ],
    "ingredientsrawmaterial": [
        ("row_id", "INTEGER PRIMARY KEY"),
        ("ingredient_rawmaterial_id", "INTEGER"),
        ("ingredient_id", "INTEGER"),
        ("raw_material_type_id", "INTEGER"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_value", "INTEGER"),
        ("origin_date", "DATE"),
        ("merge_status", "INTEGER DEFAULT 0")
    ],
    "provider": [
        ("row_id", "INTEGER PRIMARY KEY"),
        ("provider_id", "INTEGER"),
        ("name", "VARCHAR"),
        ("location_city", "VARCHAR"),
        ("location_country", "VARCHAR"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_value", "INTEGER"),
        ("origin_date", "DATE"),
        ("merge_status", "INTEGER DEFAULT 0")
    ],
    "rawmaterialtype": [
        ("row_id", "INTEGER PRIMARY KEY"),
        ("raw_material_type_id", "INTEGER"),
        ("name", "VARCHAR"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_value", "INTEGER"),
        ("origin_date", "DATE"),
        ("merge_status", "INTEGER DEFAULT 0")
    ],
    "salestransactions": [
        ("row_id", "INTEGER PRIMARY KEY"),
        ("transaction_id", "INTEGER"),
        ("customer_id", "INTEGER"),
        ("flavour_id", "INTEGER"),
        ("quantity_liters", "DOUBLE"),
        ("transaction_date", "DATE"),
        ("country", "VARCHAR"),
        ("town", "VARCHAR"),
        ("postal_code", "VARCHAR"),
        ("amount_dollar", "DOUBLE"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_value", "INTEGER"),
        ("origin_date", "DATE"),
        ("merge_status", "INTEGER DEFAULT 0")
    ],
    "ingredients": [
        ("row_id", "INTEGER PRIMARY KEY"),
        ("ingredient_id", "INTEGER"),
        ("name", "VARCHAR"),
        ("chemical_formula", "VARCHAR"),
        ("molecular_weight", "DOUBLE"),
        ("cost_per_gram", "DOUBLE"),
        ("provider_id", "INTEGER"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_value", "INTEGER"),
        ("origin_date", "DATE"),
        ("merge_status", "INTEGER DEFAULT 0")
    ],
    "recipes": [
        ("row_id", "INTEGER PRIMARY KEY"),
        ("recipe_id", "INTEGER"),
        ("flavour_id", "INTEGER"),
        ("ingredient_id", "INTEGER"),
        ("quantity_grams", "DOUBLE"),
        ("heat_process", "VARCHAR"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_value", "INTEGER"),
        ("origin_date", "DATE"),
        ("merge_status", "INTEGER DEFAULT 0")
    ],
    "customers": [
        ("row_id", "INTEGER PRIMARY KEY"),
        ("index", "INTEGER"),
        ("customer_id", "INTEGER"),
        ("name", "VARCHAR"),
        ("location_city", "VARCHAR"),
        ("location_country", "VARCHAR"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_value", "INTEGER"),
        ("origin_date", "DATE"),
        ("merge_status", "INTEGER DEFAULT 0")
    ],
    "stocks": [
        ("row_id", "INTEGER PRIMARY KEY"),
        ("stock_id", "INTEGER"),
        ("flavour_id", "INTEGER"),
        ("quantity_liters", "DOUBLE"),
        ("location", "VARCHAR"),
        ("inserted_at", "TIMESTAMP"),
        ("batch_value", "INTEGER"),
        ("origin_date", "DATE"),
        ("merge_status", "INTEGER DEFAULT 0")
    ]
}

# Funções auxiliares

def parse_filename(filename: str):
    """Extrai tabela, data e batch do nome do arquivo."""
    base = filename[:-4] if filename.endswith('.csv') else filename
    parts = base.split('_')
    table = parts[0] if len(parts) > 0 else None
    date = parts[1] if len(parts) > 1 else None
    batch = parts[2] if len(parts) > 2 else 0
    return table, date, batch

def list_files_from_s3(bucket_name: str, prefix: str) -> list:
    """Lista arquivos no S3 com base no bucket e prefixo."""
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
    """Verifica se uma tabela existe no DuckDB."""
    query = "SELECT count(*) FROM information_schema.tables WHERE table_name = ?"
    result = con.execute(query, (table_name.lower(),)).fetchone()[0]
    return result > 0

def list_missing_tables():
    """Lista as tabelas que precisam ser criadas no DuckDB."""
    bucket_name = "iffdatatest"
    prefix = "landing/"
    files = list_files_from_s3(bucket_name, prefix)
    csv_files = [f for f in files if f.endswith('.csv')]
    missing_tables = set()
    con = duckdb.connect(DB_FILE)
    for full_key in csv_files:
        filename = os.path.basename(full_key[len(prefix):] if full_key.startswith(prefix) else full_key)
        table, date, batch = parse_filename(filename)
        dp_table_name = f"dp_{table.lower()}_staging"
        if not check_table_exists(con, dp_table_name):
            missing_tables.add(dp_table_name)
    con.close()
    return list(missing_tables)

def create_table_in_duckdb(con, table_name: str, fields: list) -> None:
    """Cria uma tabela no DuckDB com os campos especificados."""
    columns = ", ".join(f"{col} {dtype}" for col, dtype in fields)
    query = f"CREATE TABLE {table_name} ({columns});"
    con.execute(query)
    print(f"Tabela {table_name} criada com sucesso.")

def create_missing_tables(**kwargs):
    """Cria as tabelas faltantes no DuckDB."""
    ti = kwargs['ti']
    missing_tables = ti.xcom_pull(task_ids='check_missing_tables')
    con = duckdb.connect(DB_FILE)
    for dp_table_name in missing_tables:
        base = dp_table_name[len("dp_"):-len("_staging")]
        if base in table_fields:
            table_exists = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{dp_table_name}'").fetchone()[0] > 0
            if not table_exists:
                seq_name = f"seq_row_id_{base}"
                con.execute(f"CREATE SEQUENCE IF NOT EXISTS {seq_name} START 1;")
                create_table_in_duckdb(con, dp_table_name, table_fields[base])
            else:
                print(f"Tabela {dp_table_name} já existe, skipping")
    con.close()

def insert_csv_data():
    """Insere os dados dos arquivos CSV do S3 no DuckDB."""
    bucket_name = "iffdatatest"
    prefix = "landing/"
    s3_files = list_files_from_s3(bucket_name, prefix)
    csv_files = [f for f in s3_files if f.endswith('.csv')]
    
    con = duckdb.connect(DB_FILE)
    con.execute(f"SET s3_region = '{os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}';")
    con.execute(f"SET s3_access_key_id = '{os.getenv('AWS_ACCESS_KEY_ID')}';")
    con.execute(f"SET s3_secret_access_key = '{os.getenv('AWS_SECRET_ACCESS_KEY')}';")
    
    processed_files = []
    for full_key in csv_files:
        filename = os.path.basename(full_key)
        table, date_str, batch = parse_filename(filename)
        if not table or not date_str:
            continue
        
        staging_table = f"dp_{table.lower()}_staging"
        batch_value = int(batch) if batch else 0
        origin_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}" if len(date_str) == 8 else None
        s3_url = f"s3://{bucket_name}/{full_key}"
        
        csv_columns = [col for col, dtype in table_fields[table.lower()] if col not in ("row_id", "inserted_at", "batch_value", "origin_date", "merge_status")]
        csv_columns_str = ", ".join(csv_columns)
        
        seq_name = f"seq_row_id_{table.lower()}"
        insert_query = f"""
            INSERT INTO {staging_table} (row_id, {csv_columns_str}, inserted_at, batch_value, origin_date, merge_status)
            SELECT NEXTVAL('{seq_name}'),
                   {csv_columns_str},
                   CURRENT_TIMESTAMP as inserted_at,
                   {batch_value} as batch_value,
                   STRPTIME('{origin_date}', '%Y-%m-%d') as origin_date,
                   0 as merge_status
            FROM read_csv_auto('{s3_url}', header=True);
        """
        try:
            con.execute(insert_query)
            print(f"Dados inseridos em {staging_table} a partir de {s3_url}")
            processed_files.append(full_key)
        except Exception as e:
            print(f"Erro ao inserir dados de {s3_url}: {e}")
    
    con.close()
    return processed_files

def ensure_ingested_folder_exists(bucket_name):
    """Garante que a pasta 'ingested' existe no bucket S3."""
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="ingested/", MaxKeys=1)
    if 'Contents' not in response:
        s3_client.put_object(Bucket=bucket_name, Key="ingested/.keep")
        print("Pasta 'ingested/' criada no bucket.")

def move_files_to_ingested(processed_files):
    """Move os arquivos processados de 'landing' para 'ingested' no S3."""
    bucket_name = "iffdatatest"
    ensure_ingested_folder_exists(bucket_name)
    
    s3_client = boto3.client('s3')
    for file_key in processed_files:
        new_key = file_key.replace("landing/", "ingested/", 1)
        try:
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': file_key},
                Key=new_key
            )
            s3_client.delete_object(Bucket=bucket_name, Key=file_key)
            print(f"Arquivo movido de {file_key} para {new_key}")
        except Exception as e:
            print(f"Erro ao mover o arquivo {file_key}: {e}")

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_landings3',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_integrate = BashOperator(
        task_id='run_integrate_py',
        bash_command='python /home/infra/prompts/integrate.py 2>&1'
    )

    check_missing = PythonOperator(
        task_id='check_missing_tables',
        python_callable=list_missing_tables
    )
    create_missing = PythonOperator(
        task_id='create_missing_tables',
        python_callable=create_missing_tables,
        provide_context=True
    )
    insert_csv = PythonOperator(
        task_id='insert_csv_data',
        python_callable=insert_csv_data
    )
    move_files = PythonOperator(
        task_id='move_files_to_ingested',
        python_callable=move_files_to_ingested,
        op_args=[insert_csv.output]
    )
    
run_integrate >> check_missing >> create_missing >> insert_csv >> move_files    
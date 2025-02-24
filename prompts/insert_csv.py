import os
import duckdb
from datetime import datetime

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

def insert_csv_data_directly_from_s3():
    """
    Lê os arquivos CSV diretamente do S3 (usando uma URL s3://) e para cada arquivo:
      - Extrai os valores do nome (tabela, data e batch);
      - Configura os parâmetros de S3 no DuckDB (usando credenciais obtidas via environment variables);
      - Cria uma tabela temporária de staging no DuckDB no formato dp_<tabela>_staging, com os campos do CSV
        acrescidos dos campos:
            inserted_at: CURRENT_TIMESTAMP,
            batch_value: valor extraído (ou 0),
            origin_date: data extraída, formatada como YYYY-MM-DD;
      - Insere os dados do CSV diretamente a partir do S3, usando read_csv_auto;
      - Consulta e imprime uma amostra (LIMIT 5) da tabela;
      - Finalmente, descarta a tabela temporária.
    """
    # Configurações do S3 obtidas das variáveis de ambiente
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    
    # Diretório de extração dos CSVs no bucket S3
    bucket_name = "iffdatatest"
    s3_prefix = "landing/extracted/"  # Supondo que os arquivos CSV estejam armazenados neste prefixo
    # Listar os arquivos do S3 pode ser feito com outra função; neste exemplo, vamos supor que
    # você tenha a lista de arquivos CSV disponíveis. Para fins deste exemplo, usaremos um array fixo:
    # (Na prática, use list_files_from_s3(bucket_name, s3_prefix))
    csv_files = [
        # Exemplos de chaves retornadas do S3:
        "landing/extracted/Flavours_20240505_1.csv",
        "landing/extracted/IngredientsRawMaterial_20240505.csv",
        "landing/extracted/Provider_20240506_1.csv"
    ]
    
    # Conecta ao DuckDB (usando um banco persistente; tabelas TEMP serão descartadas ao fechar a conexão)
    con = duckdb.connect('mydatabase.duckdb')
    
    # Configure os parâmetros de S3 no DuckDB para que o read_csv_auto funcione diretamente do S3
    con.execute(f"SET s3_region = '{region}';")
    con.execute(f"SET s3_access_key_id = '{aws_access_key_id}';")
    con.execute(f"SET s3_secret_access_key = '{aws_secret_access_key}';")
    
    files_processed = 0
    for full_key in csv_files:
        filename = os.path.basename(full_key)
        table, date_str, batch = parse_filename(filename)
        if not table or not date_str:
            print(f"Pulado {filename}: formato inválido.")
            continue
        print("Processando arquivo:", filename)
        print("  Tabela:", table)
        print("  Data:", date_str)
        print("  Batch:", batch)
        
        staging_table = f"dp_{table.lower()}_staging"
        # Valores extraídos do nome
        batch_value = int(batch) if batch is not None else 0
        if len(date_str) == 8:
            origin_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        else:
            origin_date = None
        
        # Cria a tabela temporária de staging
        # A definição deve incluir os campos do CSV mais os campos extras.
        # Assume-se que o dicionário table_fields já inclui inserted_at, batch_value e origin_date.
        # Exemplo: table_fields["flavours"] define todos os campos.
        # Cria a tabela temporária:
        temp_table_def = ", ".join(f"{col} {dtype}" for col, dtype in table_fields[table.lower()])
        create_query = f"CREATE TEMPORARY TABLE {staging_table} ({temp_table_def});"
        print("Criando tabela temporária:", staging_table)
        con.execute(create_query)
        
        # Constrói a URL completa para o arquivo CSV no S3
        s3_url = f"s3://{bucket_name}/{full_key}"
        print("Inserindo dados do arquivo:", s3_url)
        
        # Prepara a lista das colunas do CSV (excluindo os campos extras, se estes já estão no table_fields)
        csv_columns = [col for col, dtype in table_fields[table.lower()] if col not in ("inserted_at", "batch_value", "origin_date")]
        csv_columns_str = ", ".join(csv_columns)
        
        # Monta a query de inserção; os campos extras são adicionados com valores constantes
        insert_query = f"""
            INSERT INTO {staging_table}
            SELECT {csv_columns_str},
                   CURRENT_TIMESTAMP as inserted_at,
                   {batch_value} as batch_value,
                   DATE('{origin_date}') as origin_date
            FROM read_csv_auto('{s3_url}', header=True);
        """
        try:
            con.execute(insert_query)
            files_processed += 1
        except Exception as e:
            print(f"Erro ao inserir dados de {s3_url} na tabela {staging_table}: {e}")
            continue
        
        # Consulta e imprime uma amostra (LIMIT 5)
        try:
            sample = con.execute(f"SELECT * FROM {staging_table} LIMIT 5;").fetchall()
            print(f"Amostra da tabela {staging_table}:")
            for row in sample:
                print(row)
        except Exception as e:
            print(f"Erro ao obter amostra da tabela {staging_table}: {e}")
        
        # Dropar a tabela temporária (opcional, pois tabelas TEMP são descartadas ao fechar a conexão)
        con.execute(f"DROP TABLE IF EXISTS {staging_table};")
        print("-" * 40)
    
    con.close()
    print("Total de arquivos processados:", files_processed)

if __name__ == "__main__":
    insert_csv_data_directly_from_s3()

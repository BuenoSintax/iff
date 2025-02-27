from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
import logging

# Configuração do banco de dados
DATABASE_PATH = 'iff_db.duckdb'

# Configuração do logging
logging.basicConfig(
    filename='data_quality.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Função para criar a tabela silver, se ela não existir
def setup_silver_table():
    conn = duckdb.connect(DATABASE_PATH)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dp_salestransactions_silver (
        transaction_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        flavour_id INTEGER,
        quantity_liters DOUBLE,
        transaction_date DATE,
        country VARCHAR,
        town VARCHAR,
        postal_code VARCHAR,
        amount_dollar DOUBLE,
        load_timestamp TIMESTAMP,
        source_date DATE
    );
    """
    conn.execute(create_table_query)
    logger.info("Tabela dp_salestransactions_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 1 e tratamento de duplicatas
def process_scd1_salestransactions():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Definir a CTE para filtrar duplicatas e selecionar a linha mais relevante
    cte_query = """
    WITH ranked_staging AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id 
                ORDER BY batch_value DESC, inserted_at DESC
            ) AS rn
        FROM dp_salestransactions_staging
        WHERE merge_status = 0
    )
    """
    
    # 1. Inserir novos registros (apenas a linha mais relevante por transaction_id)
    insert_new_records_query = cte_query + """
    INSERT INTO dp_salestransactions_silver
    SELECT 
        transaction_id,
        customer_id,
        flavour_id,
        quantity_liters,
        transaction_date,
        country,
        town,
        postal_code,
        amount_dollar,
        inserted_at AS load_timestamp,
        origin_date AS source_date
    FROM ranked_staging
    WHERE rn = 1
    AND transaction_id NOT IN (SELECT transaction_id FROM dp_salestransactions_silver);
    """
    conn.execute(insert_new_records_query)
    
    # 2. Atualizar registros existentes (usando a linha mais relevante)
    update_existing_records_query = cte_query + """
    UPDATE dp_salestransactions_silver AS sil
    SET 
        customer_id = ranked.customer_id,
        flavour_id = ranked.flavour_id,
        quantity_liters = ranked.quantity_liters,
        transaction_date = ranked.transaction_date,
        country = ranked.country,
        town = ranked.town,
        postal_code = ranked.postal_code,
        amount_dollar = ranked.amount_dollar,
        load_timestamp = ranked.inserted_at,
        source_date = ranked.origin_date
    FROM ranked_staging AS ranked
    WHERE ranked.rn = 1
    AND sil.transaction_id = ranked.transaction_id;
    """
    conn.execute(update_existing_records_query)
    
    # 3. Marcar como processadas apenas as linhas usadas (rn = 1)
    mark_processed_query = cte_query + """
    UPDATE dp_salestransactions_staging AS stg
    SET merge_status = 1
    FROM ranked_staging AS ranked
    WHERE stg.row_id = ranked.row_id
    AND ranked.rn = 1;
    """
    conn.execute(mark_processed_query)
    
    logger.info("Processamento SCD Tipo 1 concluído com sucesso.")
    conn.close()

# Função para verificar a qualidade dos dados e registrar logs
def check_data_quality():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Contar o número total de registros
    total_records_query = "SELECT COUNT(*) FROM dp_salestransactions_silver;"
    total_records = conn.execute(total_records_query).fetchone()[0]
    logger.info(f"Total de registros na tabela dp_salestransactions_silver: {total_records}")
    
    # Verificar valores nulos em campos críticos (transaction_id, customer_id)
    null_transaction_id_query = "SELECT COUNT(*) FROM dp_salestransactions_silver WHERE transaction_id IS NULL;"
    null_transaction_id_count = conn.execute(null_transaction_id_query).fetchone()[0]
    if null_transaction_id_count > 0:
        logger.warning(f"Encontrados {null_transaction_id_count} registros com transaction_id nulo.")
    else:
        logger.info("Nenhum registro com transaction_id nulo encontrado.")
    
    null_customer_id_query = "SELECT COUNT(*) FROM dp_salestransactions_silver WHERE customer_id IS NULL;"
    null_customer_id_count = conn.execute(null_customer_id_query).fetchone()[0]
    if null_customer_id_count > 0:
        logger.warning(f"Encontrados {null_customer_id_count} registros com customer_id nulo.")
    else:
        logger.info("Nenhum registro com customer_id nulo encontrado.")
    
    # Verificar duplicatas em transaction_id (chave primária)
    duplicate_transaction_id_query = """
    SELECT transaction_id, COUNT(*) 
    FROM dp_salestransactions_silver 
    GROUP BY transaction_id 
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(duplicate_transaction_id_query).fetchall()
    if duplicates:
        for duplicate in duplicates:
            logger.warning(f"Duplicata encontrada para transaction_id {duplicate[0]} com {duplicate[1]} registros.")
    else:
        logger.info("Nenhuma duplicata de transaction_id encontrada.")
    
    logger.info("Execução da DAG concluída com sucesso.")
    conn.close()

# Definição do DAG
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': None  # Desativa o agendamento automático
}

with DAG(
    dag_id='dag_bronze_to_silver_salestransactions',
    default_args=default_args,
    description='Pipeline SCD Tipo 1 para dp_salestransactions_silver com tratamento de duplicatas',
    catchup=False,
    schedule_interval=None
) as dag:
    # Tarefa 1: Configurar a tabela silver
    setup_task = PythonOperator(
        task_id='setup_silver_table_salestransactions',
        python_callable=setup_silver_table
    )
    
    # Tarefa 2: Processar os dados com SCD Tipo 1 e tratamento de duplicatas
    process_task = PythonOperator(
        task_id='process_scd1_salestransactions',
        python_callable=process_scd1_salestransactions
    )
    
    # Tarefa 3: Verificar qualidade dos dados e emitir logs
    check_data_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality
    )
    
    # Definir a ordem das tarefas
    setup_task >> process_task >> check_data_quality_task
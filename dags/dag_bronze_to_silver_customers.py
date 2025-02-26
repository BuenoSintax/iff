from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
import logging

# Configuração do banco de dados
DATABASE_PATH = 'iff_db.duckdb'

# Configuração do logger
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
    CREATE TABLE IF NOT EXISTS dp_customers_silver (
        customer_id INTEGER,
        name VARCHAR,
        city VARCHAR,
        country VARCHAR,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        active BOOLEAN,
        load_timestamp TIMESTAMP,
        source_date DATE
    );
    """
    conn.execute(create_table_query)
    logger.info("Tabela dp_customers_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 2
def process_scd2_customers():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Lista para armazenar os customer_id processados
    processed_customer_ids = []
    
    # 1. Inserir novos registros (clientes que ainda não existem na silver)
    new_records_query = """
    INSERT INTO dp_customers_silver
    SELECT 
        customer_id,
        name,
        location_city AS city,
        location_country AS country,
        inserted_at AS valid_from,
        NULL AS valid_to,
        TRUE AS active,
        inserted_at AS load_timestamp,
        origin_date AS source_date
    FROM dp_customers_staging
    WHERE merge_status = 0
    AND customer_id NOT IN (SELECT customer_id FROM dp_customers_silver WHERE active = TRUE);
    """
    conn.execute(new_records_query)
    
    # Obter os customer_id dos novos registros inseridos
    new_customer_ids_query = """
    SELECT DISTINCT customer_id
    FROM dp_customers_staging
    WHERE merge_status = 0
    AND customer_id NOT IN (SELECT customer_id FROM dp_customers_silver WHERE active = TRUE);
    """
    new_customer_ids = [row[0] for row in conn.execute(new_customer_ids_query).fetchall()]
    processed_customer_ids.extend(new_customer_ids)
    
    # 2. Identificar os customer_id que precisam ser atualizados
    identify_updates_query = """
    SELECT 
        stg.customer_id
    FROM dp_customers_staging stg
    JOIN dp_customers_silver sil
        ON stg.customer_id = sil.customer_id
    WHERE stg.merge_status = 0
    AND sil.active = TRUE
    AND (stg.name != sil.name OR stg.location_city != sil.city OR stg.location_country != sil.country);
    """
    updated_customer_ids = [row[0] for row in conn.execute(identify_updates_query).fetchall()]
    processed_customer_ids.extend(updated_customer_ids)
    
    # 3. Fechar registros antigos para os customer_id identificados
    if updated_customer_ids:
        placeholders = ', '.join(['?'] * len(updated_customer_ids))
        close_old_records_query = f"""
        UPDATE dp_customers_silver
        SET valid_to = (
            SELECT MAX(inserted_at)
            FROM dp_customers_staging
            WHERE customer_id = dp_customers_silver.customer_id
            AND merge_status = 0
        ),
            active = FALSE
        WHERE customer_id IN ({placeholders})
        AND active = TRUE;
        """
        conn.execute(close_old_records_query, updated_customer_ids)
        
        # 4. Inserir novas versões para os customer_id identificados
        insert_new_versions_query = f"""
        INSERT INTO dp_customers_silver
        SELECT 
            customer_id,
            name,
            location_city AS city,
            location_country AS country,
            inserted_at AS valid_from,
            NULL AS valid_to,
            TRUE AS active,
            inserted_at AS load_timestamp,
            origin_date AS source_date
        FROM dp_customers_staging
        WHERE customer_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(insert_new_versions_query, updated_customer_ids)
    
    # 5. Atualizar o status na tabela staging apenas para os customer_id processados
    if processed_customer_ids:
        placeholders = ', '.join(['?'] * len(processed_customer_ids))
        mark_processed_query = f"""
        UPDATE dp_customers_staging
        SET merge_status = 1
        WHERE customer_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(mark_processed_query, processed_customer_ids)
    
    logger.info("Processamento SCD Tipo 2 concluído com sucesso.")
    conn.close()

# Função para verificar a qualidade dos dados e adicionar logs de observabilidade
def check_data_quality():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Contar o número total de registros
    total_records_query = "SELECT COUNT(*) FROM dp_customers_silver;"
    total_records = conn.execute(total_records_query).fetchone()[0]
    logger.info(f"Total de registros na tabela dp_customers_silver: {total_records}")
    
    # Verificar registros com customer_id nulo
    null_customer_id_query = "SELECT COUNT(*) FROM dp_customers_silver WHERE customer_id IS NULL;"
    null_customer_id_count = conn.execute(null_customer_id_query).fetchone()[0]
    if null_customer_id_count > 0:
        logger.warning(f"Encontrados {null_customer_id_count} registros com customer_id nulo.")
    else:
        logger.info("Nenhum registro com customer_id nulo encontrado.")
    
    # Verificar duplicatas de customer_id com active = TRUE
    duplicate_active_query = """
    SELECT customer_id, COUNT(*) 
    FROM dp_customers_silver 
    WHERE active = TRUE 
    GROUP BY customer_id 
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(duplicate_active_query).fetchall()
    if duplicates:
        for duplicate in duplicates:
            logger.warning(f"Duplicata encontrada para customer_id {duplicate[0]} com {duplicate[1]} registros ativos.")
    else:
        logger.info("Nenhuma duplicata de customer_id com active = TRUE encontrada.")
    
    # Log de observabilidade indicando o término da DAG
    logger.info("Execução da DAG concluída com sucesso.")
    conn.close()

# Definição do DAG
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': None  # Desativa o agendamento automático
}

with DAG(
    dag_id='dag_bronze_to_silver_customers',
    default_args=default_args,
    description='Pipeline SCD Tipo 2 para dp_customers_silver',
    catchup=False,
    schedule_interval=None
) as dag:
    # Tarefa 1: Configurar a tabela silver
    setup_task = PythonOperator(
        task_id='setup_silver_table',
        python_callable=setup_silver_table
    )
    
    # Tarefa 2: Processar os dados com SCD Tipo 2
    process_task = PythonOperator(
        task_id='process_scd2_customers',
        python_callable=process_scd2_customers
    )
    
    # Tarefa 3: Verificar qualidade dos dados e emitir logs
    check_data_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality
    )
    
    # Definir a ordem das tarefas
    setup_task >> process_task >> check_data_quality_task
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
import logging

# Configuração do banco de dados
DATABASE_PATH = 'iff_db.duckdb'

# Configuração do logger
logging.basicConfig(
    filename='data_quality_stock.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Função para criar a tabela silver, se ela não existir
def setup_silver_table():
    conn = duckdb.connect(DATABASE_PATH)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dp_stocks_silver (
        stock_id INTEGER,
        flavour_id INTEGER,
        quantity_liters DOUBLE,
        location VARCHAR,
        batch_value INTEGER,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        active BOOLEAN,
        load_timestamp TIMESTAMP,
        source_date DATE
    );
    """
    conn.execute(create_table_query)
    logger.info("Tabela dp_stocks_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 2
def process_scd2_stocks():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Lista para armazenar os row_id processados
    processed_row_ids = []
    
    # 1. Inserir novos registros
    new_records_query = """
    INSERT INTO dp_stocks_silver
    SELECT 
        stock_id,
        flavour_id,
        quantity_liters,
        location,
        batch_value,
        inserted_at AS valid_from,
        NULL AS valid_to,
        TRUE AS active,
        inserted_at AS load_timestamp,
        origin_date AS source_date
    FROM dp_stocks_staging stg
    WHERE merge_status = 0
    AND NOT EXISTS (
        SELECT 1
        FROM dp_stocks_silver sil
        WHERE sil.stock_id = stg.stock_id
        AND sil.flavour_id = stg.flavour_id
        AND sil.active = TRUE
    );
    """
    conn.execute(new_records_query)
    
    # Obter os row_id dos novos registros inseridos
    new_row_ids_query = """
    SELECT stg.row_id
    FROM dp_stocks_staging stg
    WHERE stg.merge_status = 0
    AND NOT EXISTS (
        SELECT 1
        FROM dp_stocks_silver sil
        WHERE sil.stock_id = stg.stock_id
        AND sil.flavour_id = stg.flavour_id
        AND sil.active = TRUE
    );
    """
    new_row_ids = [row[0] for row in conn.execute(new_row_ids_query).fetchall()]
    processed_row_ids.extend(new_row_ids)
    
    # 2. Identificar os row_id que precisam ser atualizados
    identify_updates_query = """
    SELECT 
        stg.row_id
    FROM dp_stocks_staging stg
    JOIN dp_stocks_silver sil
        ON stg.stock_id = sil.stock_id
        AND stg.flavour_id = sil.flavour_id
    WHERE stg.merge_status = 0
    AND sil.active = TRUE
    AND (stg.quantity_liters != sil.quantity_liters 
         OR stg.location != sil.location 
         OR stg.batch_value != sil.batch_value);
    """
    updated_row_ids = [row[0] for row in conn.execute(identify_updates_query).fetchall()]
    processed_row_ids.extend(updated_row_ids)
    
    # 3. Fechar registros antigos para os row_id identificados
    if updated_row_ids:
        placeholders = ', '.join(['?'] * len(updated_row_ids))
        close_old_records_query = f"""
        UPDATE dp_stocks_silver
        SET valid_to = (
            SELECT MAX(stg.inserted_at)
            FROM dp_stocks_staging stg
            WHERE stg.stock_id = dp_stocks_silver.stock_id
            AND stg.flavour_id = dp_stocks_silver.flavour_id
            AND stg.row_id IN ({placeholders})
        ),
            active = FALSE
        WHERE (stock_id, flavour_id) IN (
            SELECT stock_id, flavour_id
            FROM dp_stocks_staging
            WHERE row_id IN ({placeholders})
        )
        AND active = TRUE;
        """
        conn.execute(close_old_records_query, updated_row_ids + updated_row_ids)
        
        # 4. Inserir novas versões para os row_id identificados
        insert_new_versions_query = f"""
        INSERT INTO dp_stocks_silver
        SELECT 
            stock_id,
            flavour_id,
            quantity_liters,
            location,
            batch_value,
            inserted_at AS valid_from,
            NULL AS valid_to,
            TRUE AS active,
            inserted_at AS load_timestamp,
            origin_date AS source_date
        FROM dp_stocks_staging
        WHERE row_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(insert_new_versions_query, updated_row_ids)
    
    # 5. Identificar os row_id dos registros que não mudaram
    no_change_query = """
    SELECT 
        stg.row_id
    FROM dp_stocks_staging stg
    JOIN dp_stocks_silver sil
        ON stg.stock_id = sil.stock_id
        AND stg.flavour_id = sil.flavour_id
    WHERE stg.merge_status = 0
    AND sil.active = TRUE
    AND stg.quantity_liters = sil.quantity_liters
    AND stg.location = sil.location
    AND stg.batch_value = sil.batch_value;
    """
    no_change_row_ids = [row[0] for row in conn.execute(no_change_query).fetchall()]
    processed_row_ids.extend(no_change_row_ids)
    
    # 6. Atualizar o status na tabela staging para todos os row_id processados
    if processed_row_ids:
        placeholders = ', '.join(['?'] * len(processed_row_ids))
        mark_processed_query = f"""
        UPDATE dp_stocks_staging
        SET merge_status = 1
        WHERE row_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(mark_processed_query, processed_row_ids)
    
    logger.info("Processamento SCD Tipo 2 para stocks concluído com sucesso.")
    conn.close()

# Função para verificar a qualidade dos dados e adicionar logs de observabilidade
def check_data_quality():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Contar o número total de registros
    total_records_query = "SELECT COUNT(*) FROM dp_stocks_silver;"
    total_records = conn.execute(total_records_query).fetchone()[0]
    logger.info(f"Total de registros na tabela dp_stocks_silver: {total_records}")
    
    # Verificar registros com chaves nulas
    null_keys_query = """
    SELECT COUNT(*) 
    FROM dp_stocks_silver 
    WHERE stock_id IS NULL OR flavour_id IS NULL;
    """
    null_keys_count = conn.execute(null_keys_query).fetchone()[0]
    if null_keys_count > 0:
        logger.warning(f"Encontrados {null_keys_count} registros com chaves nulas.")
    else:
        logger.info("Nenhum registro com chaves nulas encontrado.")
    
    # Verificar duplicatas de chaves com active = TRUE
    duplicate_active_query = """
    SELECT stock_id, flavour_id, COUNT(*) 
    FROM dp_stocks_silver 
    WHERE active = TRUE 
    GROUP BY stock_id, flavour_id 
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(duplicate_active_query).fetchall()
    if duplicates:
        for duplicate in duplicates:
            logger.warning(f"Duplicata encontrada para (stock_id={duplicate[0]}, flavour_id={duplicate[1]}) com {duplicate[2]} registros ativos.")
    else:
        logger.info("Nenhuma duplicata de chaves com active = TRUE encontrada.")
    
    # Verificar registros não processados na staging com detalhes
    unprocessed_records_query = """
    SELECT row_id, stock_id, flavour_id, quantity_liters, location, batch_value, inserted_at
    FROM dp_stocks_staging
    WHERE merge_status = 0;
    """
    unprocessed_records = conn.execute(unprocessed_records_query).fetchall()
    if unprocessed_records:
        logger.warning(f"Encontrados {len(unprocessed_records)} registros não processados na dp_stocks_staging.")
        for record in unprocessed_records:
            logger.warning(
                f"Registro não inserido - row_id={record[0]}, valores: "
                f"stock_id={record[1]}, flavour_id={record[2]}, quantity_liters={record[3]}, "
                f"location={record[4]}, batch_value={record[5]}, inserted_at={record[6]}"
            )
    else:
        logger.info("Todos os registros da dp_stocks_staging foram processados com sucesso.")
    
    logger.info("Execução da DAG concluída com sucesso.")
    conn.close()

# Definição do DAG
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': None
}

with DAG(
    dag_id='dag_bronze_to_silver_stocks',
    default_args=default_args,
    description='Pipeline SCD Tipo 2 para dp_stocks_silver',
    catchup=False,
    schedule_interval=None
) as dag:
    setup_task = PythonOperator(
        task_id='setup_silver_table',
        python_callable=setup_silver_table
    )
    
    process_task = PythonOperator(
        task_id='process_scd2_stocks',
        python_callable=process_scd2_stocks
    )
    
    check_data_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality
    )
    
    setup_task >> process_task >> check_data_quality_task
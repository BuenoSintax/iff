from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
import logging

# Configuração do banco de dados
DATABASE_PATH = 'iff_db.duckdb'

# Configuração de logging
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
    CREATE TABLE IF NOT EXISTS dp_flavours_silver (
        flavour_id INTEGER,
        name VARCHAR,
        description VARCHAR,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        active BOOLEAN,
        load_timestamp TIMESTAMP,
        source_date DATE
    );
    """
    conn.execute(create_table_query)
    logger.info("Tabela dp_flavours_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 2
def process_scd2_flavours():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Lista para armazenar os flavour_id processados
    processed_flavour_ids = []
    
    # 1. Inserir novos registros (flavours que ainda não existem na silver)
    new_records_query = """
    INSERT INTO dp_flavours_silver
    SELECT 
        flavour_id,
        name,
        description,
        inserted_at AS valid_from,
        NULL AS valid_to,
        TRUE AS active,
        inserted_at AS load_timestamp,
        origin_date AS source_date
    FROM dp_flavours_staging
    WHERE merge_status = 0
    AND flavour_id NOT IN (SELECT flavour_id FROM dp_flavours_silver WHERE active = TRUE);
    """
    conn.execute(new_records_query)
    
    # Obter os flavour_id dos novos registros inseridos
    new_flavour_ids_query = """
    SELECT DISTINCT flavour_id
    FROM dp_flavours_staging
    WHERE merge_status = 0
    AND flavour_id NOT IN (SELECT flavour_id FROM dp_flavours_silver WHERE active = TRUE);
    """
    new_flavour_ids = [row[0] for row in conn.execute(new_flavour_ids_query).fetchall()]
    processed_flavour_ids.extend(new_flavour_ids)
    
    # 2. Identificar os flavour_id que precisam ser atualizados
    identify_updates_query = """
    SELECT 
        stg.flavour_id
    FROM dp_flavours_staging stg
    JOIN dp_flavours_silver sil
        ON stg.flavour_id = sil.flavour_id
    WHERE stg.merge_status = 0
    AND sil.active = TRUE
    AND (stg.name != sil.name OR stg.description != sil.description);
    """
    updated_flavour_ids = [row[0] for row in conn.execute(identify_updates_query).fetchall()]
    processed_flavour_ids.extend(updated_flavour_ids)
    
    # 3. Fechar registros antigos para os flavour_id identificados
    if updated_flavour_ids:
        placeholders = ', '.join(['?'] * len(updated_flavour_ids))
        close_old_records_query = f"""
        UPDATE dp_flavours_silver
        SET valid_to = (
            SELECT MAX(inserted_at)
            FROM dp_flavours_staging
            WHERE flavour_id = dp_flavours_silver.flavour_id
            AND merge_status = 0
        ),
            active = FALSE
        WHERE flavour_id IN ({placeholders})
        AND active = TRUE;
        """
        conn.execute(close_old_records_query, updated_flavour_ids)
        
        # 4. Inserir novas versões para os flavour_id identificados
        insert_new_versions_query = f"""
        INSERT INTO dp_flavours_silver
        SELECT 
            flavour_id,
            name,
            description,
            inserted_at AS valid_from,
            NULL AS valid_to,
            TRUE AS active,
            inserted_at AS load_timestamp,
            origin_date AS source_date
        FROM dp_flavours_staging
        WHERE flavour_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(insert_new_versions_query, updated_flavour_ids)
    
    # 5. Atualizar o status na tabela staging apenas para os flavour_id processados
    if processed_flavour_ids:
        placeholders = ', '.join(['?'] * len(processed_flavour_ids))
        mark_processed_query = f"""
        UPDATE dp_flavours_staging
        SET merge_status = 1
        WHERE flavour_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(mark_processed_query, processed_flavour_ids)
    
    logger.info("Processamento SCD Tipo 2 concluído com sucesso.")
    conn.close()

# Função para verificar a qualidade dos dados e registrar logs
def check_data_quality():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Contar o número total de registros
    total_records_query = "SELECT COUNT(*) FROM dp_flavours_silver;"
    total_records = conn.execute(total_records_query).fetchone()[0]
    logger.info(f"Total de registros na tabela dp_flavours_silver: {total_records}")
    
    # Verificar valores nulos em campos críticos
    null_flavour_id_query = "SELECT COUNT(*) FROM dp_flavours_silver WHERE flavour_id IS NULL;"
    null_flavour_id_count = conn.execute(null_flavour_id_query).fetchone()[0]
    if null_flavour_id_count > 0:
        logger.warning(f"Encontrados {null_flavour_id_count} registros com flavour_id nulo.")
    else:
        logger.info("Nenhum registro com flavour_id nulo encontrado.")
    
    null_name_query = "SELECT COUNT(*) FROM dp_flavours_silver WHERE name IS NULL;"
    null_name_count = conn.execute(null_name_query).fetchone()[0]
    if null_name_count > 0:
        logger.warning(f"Encontrados {null_name_count} registros com name nulo.")
    else:
        logger.info("Nenhum registro com name nulo encontrado.")
    
    # Verificar duplicatas de flavour_id com active = TRUE
    duplicate_active_query = """
    SELECT flavour_id, COUNT(*) 
    FROM dp_flavours_silver 
    WHERE active = TRUE 
    GROUP BY flavour_id 
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(duplicate_active_query).fetchall()
    if duplicates:
        for duplicate in duplicates:
            logger.warning(f"Duplicata encontrada para flavour_id {duplicate[0]} com {duplicate[1]} registros ativos.")
    else:
        logger.info("Nenhuma duplicata de flavour_id com active = TRUE encontrada.")
    
    logger.info("Execução da DAG concluída com sucesso.")
    conn.close()

# Definição do DAG
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': None  # Desativa o agendamento automático
}

with DAG(
    dag_id='dag_bronze_to_silver_flavours',
    default_args=default_args,
    description='Pipeline SCD Tipo 2 para dp_flavours_silver',
    catchup=False,
    schedule_interval=None
) as dag:
    # Tarefa 1: Configurar a tabela silver
    setup_task = PythonOperator(
        task_id='setup_silver_table_flavours',
        python_callable=setup_silver_table
    )
    
    # Tarefa 2: Processar os dados com SCD Tipo 2
    process_task = PythonOperator(
        task_id='process_scd2_flavours',
        python_callable=process_scd2_flavours
    )
    
    # Tarefa 3: Verificar qualidade dos dados e emitir logs
    check_data_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality
    )
    
    # Definir a ordem das tarefas
    setup_task >> process_task >> check_data_quality_task
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
    CREATE TABLE IF NOT EXISTS dp_rawmaterialtype_silver (
        raw_material_type_id INTEGER,
        name VARCHAR,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        active BOOLEAN,
        load_timestamp TIMESTAMP,
        source_date DATE
    );
    """
    conn.execute(create_table_query)
    logger.info("Tabela dp_rawmaterialtype_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 2
def process_scd2_rawmaterialtype():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Lista para armazenar os raw_material_type_id processados
    processed_ids = []
    
    # 1. Inserir novos registros (raw_material_type que ainda não existem na silver)
    new_records_query = """
    INSERT INTO dp_rawmaterialtype_silver
    SELECT 
        raw_material_type_id,
        name,
        inserted_at AS valid_from,
        NULL AS valid_to,
        TRUE AS active,
        inserted_at AS load_timestamp,
        origin_date AS source_date
    FROM dp_rawmaterialtype_staging
    WHERE merge_status = 0
    AND raw_material_type_id NOT IN (SELECT raw_material_type_id FROM dp_rawmaterialtype_silver WHERE active = TRUE);
    """
    conn.execute(new_records_query)
    
    # Obter os raw_material_type_id dos novos registros inseridos
    new_ids_query = """
    SELECT DISTINCT raw_material_type_id
    FROM dp_rawmaterialtype_staging
    WHERE merge_status = 0
    AND raw_material_type_id NOT IN (SELECT raw_material_type_id FROM dp_rawmaterialtype_silver WHERE active = TRUE);
    """
    new_ids = [row[0] for row in conn.execute(new_ids_query).fetchall()]
    processed_ids.extend(new_ids)
    
    # 2. Identificar os raw_material_type_id que precisam ser atualizados
    identify_updates_query = """
    SELECT 
        stg.raw_material_type_id
    FROM dp_rawmaterialtype_staging stg
    JOIN dp_rawmaterialtype_silver sil
        ON stg.raw_material_type_id = sil.raw_material_type_id
    WHERE stg.merge_status = 0
    AND sil.active = TRUE
    AND stg.name != sil.name;
    """
    updated_ids = [row[0] for row in conn.execute(identify_updates_query).fetchall()]
    processed_ids.extend(updated_ids)
    
    # 3. Fechar registros antigos para os raw_material_type_id identificados
    if updated_ids:
        placeholders = ', '.join(['?'] * len(updated_ids))
        close_old_records_query = f"""
        UPDATE dp_rawmaterialtype_silver
        SET valid_to = (
            SELECT MAX(inserted_at)
            FROM dp_rawmaterialtype_staging
            WHERE raw_material_type_id = dp_rawmaterialtype_silver.raw_material_type_id
            AND merge_status = 0
        ),
            active = FALSE
        WHERE raw_material_type_id IN ({placeholders})
        AND active = TRUE;
        """
        conn.execute(close_old_records_query, updated_ids)
        
        # 4. Inserir novas versões para os raw_material_type_id identificados
        insert_new_versions_query = f"""
        INSERT INTO dp_rawmaterialtype_silver
        SELECT 
            raw_material_type_id,
            name,
            inserted_at AS valid_from,
            NULL AS valid_to,
            TRUE AS active,
            inserted_at AS load_timestamp,
            origin_date AS source_date
        FROM dp_rawmaterialtype_staging
        WHERE raw_material_type_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(insert_new_versions_query, updated_ids)
    
    # 5. Atualizar o status na tabela staging apenas para os raw_material_type_id processados
    if processed_ids:
        placeholders = ', '.join(['?'] * len(processed_ids))
        mark_processed_query = f"""
        UPDATE dp_rawmaterialtype_staging
        SET merge_status = 1
        WHERE raw_material_type_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(mark_processed_query, processed_ids)
    
    logger.info("Processamento SCD Tipo 2 concluído com sucesso.")
    conn.close()

# Função para verificar a qualidade dos dados e registrar logs
def check_data_quality():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Contar o número total de registros
    total_records_query = "SELECT COUNT(*) FROM dp_rawmaterialtype_silver;"
    total_records = conn.execute(total_records_query).fetchone()[0]
    logger.info(f"Total de registros na tabela dp_rawmaterialtype_silver: {total_records}")
    
    # Verificar valores nulos em campos críticos
    null_raw_material_type_id_query = "SELECT COUNT(*) FROM dp_rawmaterialtype_silver WHERE raw_material_type_id IS NULL;"
    null_raw_material_type_id_count = conn.execute(null_raw_material_type_id_query).fetchone()[0]
    if null_raw_material_type_id_count > 0:
        logger.warning(f"Encontrados {null_raw_material_type_id_count} registros com raw_material_type_id nulo.")
    else:
        logger.info("Nenhum registro com raw_material_type_id nulo encontrado.")
    
    null_name_query = "SELECT COUNT(*) FROM dp_rawmaterialtype_silver WHERE name IS NULL;"
    null_name_count = conn.execute(null_name_query).fetchone()[0]
    if null_name_count > 0:
        logger.warning(f"Encontrados {null_name_count} registros com name nulo.")
    else:
        logger.info("Nenhum registro com name nulo encontrado.")
    
    # Verificar duplicatas de raw_material_type_id com active = TRUE
    duplicate_active_query = """
    SELECT raw_material_type_id, COUNT(*) 
    FROM dp_rawmaterialtype_silver 
    WHERE active = TRUE 
    GROUP BY raw_material_type_id 
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(duplicate_active_query).fetchall()
    if duplicates:
        for duplicate in duplicates:
            logger.warning(f"Duplicata encontrada para raw_material_type_id {duplicate[0]} com {duplicate[1]} registros ativos.")
    else:
        logger.info("Nenhuma duplicata de raw_material_type_id com active = TRUE encontrada.")
    
    logger.info("Verificação de qualidade dos dados concluída com sucesso.")
    conn.close()

# Definição do DAG
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': None  # Desativa o agendamento automático
}

with DAG(
    dag_id='dag_bronze_to_silver_rawmaterialtype',
    default_args=default_args,
    description='Pipeline SCD Tipo 2 para dp_rawmaterialtype_silver',
    catchup=False,
    schedule_interval=None
) as dag:
    # Tarefa 1: Configurar a tabela silver
    setup_task = PythonOperator(
        task_id='setup_silver_table_rawmaterialtype',
        python_callable=setup_silver_table
    )
    
    # Tarefa 2: Processar os dados com SCD Tipo 2
    process_task = PythonOperator(
        task_id='process_scd2_rawmaterialtype',
        python_callable=process_scd2_rawmaterialtype
    )
    
    # Tarefa 3: Verificar qualidade dos dados e emitir logs
    check_data_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality
    )
    
    # Definir a ordem das tarefas
    setup_task >> process_task >> check_data_quality_task
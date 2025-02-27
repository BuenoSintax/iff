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
    CREATE TABLE IF NOT EXISTS dp_ingredients_silver (
        ingredient_id INTEGER,
        name VARCHAR,
        chemical_formula VARCHAR,
        molecular_weight DOUBLE,
        cost_per_gram DOUBLE,
        provider_id INTEGER,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        active BOOLEAN,
        load_timestamp TIMESTAMP,
        source_date DATE
    );
    """
    conn.execute(create_table_query)
    logger.info("Tabela dp_ingredients_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 2
def process_scd2_ingredients():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Lista para armazenar os ingredient_id processados
    processed_ingredient_ids = []
    
    # 1. Inserir novos registros (ingredients que ainda não existem na silver)
    new_records_query = """
    INSERT INTO dp_ingredients_silver
    SELECT 
        ingredient_id,
        name,
        chemical_formula,
        molecular_weight,
        cost_per_gram,
        provider_id,
        inserted_at AS valid_from,
        NULL AS valid_to,
        TRUE AS active,
        inserted_at AS load_timestamp,
        origin_date AS source_date
    FROM dp_ingredients_staging
    WHERE merge_status = 0
    AND ingredient_id NOT IN (SELECT ingredient_id FROM dp_ingredients_silver WHERE active = TRUE);
    """
    conn.execute(new_records_query)
    
    # Obter os ingredient_id dos novos registros inseridos
    new_ingredient_ids_query = """
    SELECT DISTINCT ingredient_id
    FROM dp_ingredients_staging
    WHERE merge_status = 0
    AND ingredient_id NOT IN (SELECT ingredient_id FROM dp_ingredients_silver WHERE active = TRUE);
    """
    new_ingredient_ids = [row[0] for row in conn.execute(new_ingredient_ids_query).fetchall()]
    processed_ingredient_ids.extend(new_ingredient_ids)
    
    # 2. Identificar os ingredient_id que precisam ser atualizados
    identify_updates_query = """
    SELECT 
        stg.ingredient_id
    FROM dp_ingredients_staging stg
    JOIN dp_ingredients_silver sil
        ON stg.ingredient_id = sil.ingredient_id
    WHERE stg.merge_status = 0
    AND sil.active = TRUE
    AND (stg.name != sil.name OR 
         stg.chemical_formula != sil.chemical_formula OR 
         stg.molecular_weight != sil.molecular_weight OR 
         stg.cost_per_gram != sil.cost_per_gram OR 
         stg.provider_id != sil.provider_id);
    """
    updated_ingredient_ids = [row[0] for row in conn.execute(identify_updates_query).fetchall()]
    processed_ingredient_ids.extend(updated_ingredient_ids)
    
    # 3. Fechar registros antigos para os ingredient_id identificados
    if updated_ingredient_ids:
        placeholders = ', '.join(['?'] * len(updated_ingredient_ids))
        close_old_records_query = f"""
        UPDATE dp_ingredients_silver
        SET valid_to = (
            SELECT MAX(inserted_at)
            FROM dp_ingredients_staging
            WHERE ingredient_id = dp_ingredients_silver.ingredient_id
            AND merge_status = 0
        ),
            active = FALSE
        WHERE ingredient_id IN ({placeholders})
        AND active = TRUE;
        """
        conn.execute(close_old_records_query, updated_ingredient_ids)
        
        # 4. Inserir novas versões para os ingredient_id identificados
        insert_new_versions_query = f"""
        INSERT INTO dp_ingredients_silver
        SELECT 
            ingredient_id,
            name,
            chemical_formula,
            molecular_weight,
            cost_per_gram,
            provider_id,
            inserted_at AS valid_from,
            NULL AS valid_to,
            TRUE AS active,
            inserted_at AS load_timestamp,
            origin_date AS source_date
        FROM dp_ingredients_staging
        WHERE ingredient_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(insert_new_versions_query, updated_ingredient_ids)
    
    # 5. Atualizar o status na tabela staging apenas para os ingredient_id processados
    if processed_ingredient_ids:
        placeholders = ', '.join(['?'] * len(processed_ingredient_ids))
        mark_processed_query = f"""
        UPDATE dp_ingredients_staging
        SET merge_status = 1
        WHERE ingredient_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(mark_processed_query, processed_ingredient_ids)
    
    logger.info("Processamento SCD Tipo 2 concluído com sucesso.")
    conn.close()

# Função para verificar a qualidade dos dados e registrar logs
def check_data_quality():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Contar o número total de registros
    total_records_query = "SELECT COUNT(*) FROM dp_ingredients_silver;"
    total_records = conn.execute(total_records_query).fetchone()[0]
    logger.info(f"Total de registros na tabela dp_ingredients_silver: {total_records}")
    
    # Verificar valores nulos em campos críticos
    null_ingredient_id_query = "SELECT COUNT(*) FROM dp_ingredients_silver WHERE ingredient_id IS NULL;"
    null_ingredient_id_count = conn.execute(null_ingredient_id_query).fetchone()[0]
    if null_ingredient_id_count > 0:
        logger.warning(f"Encontrados {null_ingredient_id_count} registros com ingredient_id nulo.")
    else:
        logger.info("Nenhum registro com ingredient_id nulo encontrado.")
    
    null_name_query = "SELECT COUNT(*) FROM dp_ingredients_silver WHERE name IS NULL;"
    null_name_count = conn.execute(null_name_query).fetchone()[0]
    if null_name_count > 0:
        logger.warning(f"Encontrados {null_name_count} registros com name nulo.")
    else:
        logger.info("Nenhum registro com name nulo encontrado.")
    
    # Verificar duplicatas de ingredient_id com active = TRUE
    duplicate_active_query = """
    SELECT ingredient_id, COUNT(*) 
    FROM dp_ingredients_silver 
    WHERE active = TRUE 
    GROUP BY ingredient_id 
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(duplicate_active_query).fetchall()
    if duplicates:
        for duplicate in duplicates:
            logger.warning(f"Duplicata encontrada para ingredient_id {duplicate[0]} com {duplicate[1]} registros ativos.")
    else:
        logger.info("Nenhuma duplicata de ingredient_id com active = TRUE encontrada.")
    
    logger.info("Verificação de qualidade dos dados concluída com sucesso.")
    conn.close()

# Definição do DAG
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': None  # Desativa o agendamento automático
}

with DAG(
    dag_id='dag_bronze_to_silver_ingredients',
    default_args=default_args,
    description='Pipeline SCD Tipo 2 para dp_ingredients_silver',
    catchup=False,
    schedule_interval=None
) as dag:
    # Tarefa 1: Configurar a tabela silver
    setup_task = PythonOperator(
        task_id='setup_silver_table_ingredients',
        python_callable=setup_silver_table
    )
    
    # Tarefa 2: Processar os dados com SCD Tipo 2
    process_task = PythonOperator(
        task_id='process_scd2_ingredients',
        python_callable=process_scd2_ingredients
    )
    
    # Tarefa 3: Verificar qualidade dos dados
    check_data_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality
    )
    
    # Definir a ordem das tarefas
    setup_task >> process_task >> check_data_quality_task
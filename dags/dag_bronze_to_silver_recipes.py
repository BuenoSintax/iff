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
    CREATE TABLE IF NOT EXISTS dp_recipes_silver (
        recipe_id INTEGER,
        flavour_id INTEGER,
        ingredient_id INTEGER,
        quantity_grams DOUBLE,
        heat_process VARCHAR,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        active BOOLEAN,
        load_timestamp TIMESTAMP,
        source_date DATE
    );
    """
    conn.execute(create_table_query)
    logger.info("Tabela dp_recipes_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 2
def process_scd2_recipes():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Lista para armazenar os row_id processados
    processed_row_ids = []
    
    # 1. Inserir novos registros (combinações de chaves que não possuem registro ativo na silver)
    new_records_query = """
    INSERT INTO dp_recipes_silver
    SELECT 
        recipe_id,
        flavour_id,
        ingredient_id,
        quantity_grams,
        heat_process,
        inserted_at AS valid_from,
        NULL AS valid_to,
        TRUE AS active,
        inserted_at AS load_timestamp,
        origin_date AS source_date
    FROM dp_recipes_staging stg
    WHERE merge_status = 0
    AND NOT EXISTS (
        SELECT 1
        FROM dp_recipes_silver sil
        WHERE sil.recipe_id = stg.recipe_id
        AND sil.flavour_id = stg.flavour_id
        AND sil.ingredient_id = stg.ingredient_id
        AND sil.active = TRUE
    );
    """
    conn.execute(new_records_query)
    
    # Obter os row_id dos novos registros inseridos
    new_row_ids_query = """
    SELECT stg.row_id
    FROM dp_recipes_staging stg
    WHERE stg.merge_status = 0
    AND NOT EXISTS (
        SELECT 1
        FROM dp_recipes_silver sil
        WHERE sil.recipe_id = stg.recipe_id
        AND sil.flavour_id = stg.flavour_id
        AND sil.ingredient_id = stg.ingredient_id
        AND sil.active = TRUE
    );
    """
    new_row_ids = [row[0] for row in conn.execute(new_row_ids_query).fetchall()]
    processed_row_ids.extend(new_row_ids)
    
    # 2. Identificar os row_id que precisam ser atualizados
    identify_updates_query = """
    SELECT 
        stg.row_id
    FROM dp_recipes_staging stg
    JOIN dp_recipes_silver sil
        ON stg.recipe_id = sil.recipe_id
        AND stg.flavour_id = sil.flavour_id
        AND stg.ingredient_id = sil.ingredient_id
    WHERE stg.merge_status = 0
    AND sil.active = TRUE
    AND (stg.quantity_grams != sil.quantity_grams OR stg.heat_process != sil.heat_process);
    """
    updated_row_ids = [row[0] for row in conn.execute(identify_updates_query).fetchall()]
    processed_row_ids.extend(updated_row_ids)
    
    # 3. Fechar registros antigos para os row_id identificados
    if updated_row_ids:
        placeholders = ', '.join(['?'] * len(updated_row_ids))
        close_old_records_query = f"""
        UPDATE dp_recipes_silver
        SET valid_to = (
            SELECT MAX(stg.inserted_at)
            FROM dp_recipes_staging stg
            WHERE stg.recipe_id = dp_recipes_silver.recipe_id
            AND stg.flavour_id = dp_recipes_silver.flavour_id
            AND stg.ingredient_id = dp_recipes_silver.ingredient_id
            AND stg.row_id IN ({placeholders})
        ),
            active = FALSE
        WHERE (recipe_id, flavour_id, ingredient_id) IN (
            SELECT recipe_id, flavour_id, ingredient_id
            FROM dp_recipes_staging
            WHERE row_id IN ({placeholders})
        )
        AND active = TRUE;
        """
        conn.execute(close_old_records_query, updated_row_ids + updated_row_ids)
        
        # 4. Inserir novas versões para os row_id identificados
        insert_new_versions_query = f"""
        INSERT INTO dp_recipes_silver
        SELECT 
            recipe_id,
            flavour_id,
            ingredient_id,
            quantity_grams,
            heat_process,
            inserted_at AS valid_from,
            NULL AS valid_to,
            TRUE AS active,
            inserted_at AS load_timestamp,
            origin_date AS source_date
        FROM dp_recipes_staging
        WHERE row_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(insert_new_versions_query, updated_row_ids)
    
    # 5. Atualizar o status na tabela staging apenas para os row_id processados
    if processed_row_ids:
        placeholders = ', '.join(['?'] * len(processed_row_ids))
        mark_processed_query = f"""
        UPDATE dp_recipes_staging
        SET merge_status = 1
        WHERE row_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(mark_processed_query, processed_row_ids)
    
    logger.info("Processamento SCD Tipo 2 para recipes concluído com sucesso.")
    conn.close()

# Função para verificar a qualidade dos dados e adicionar logs de observabilidade
def check_data_quality():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Contar o número total de registros na silver
    total_records_query = "SELECT COUNT(*) FROM dp_recipes_silver;"
    total_records = conn.execute(total_records_query).fetchone()[0]
    logger.info(f"Total de registros na tabela dp_recipes_silver: {total_records}")
    
    # Verificar registros com chaves nulas
    null_keys_query = """
    SELECT COUNT(*) 
    FROM dp_recipes_silver 
    WHERE recipe_id IS NULL OR flavour_id IS NULL OR ingredient_id IS NULL;
    """
    null_keys_count = conn.execute(null_keys_query).fetchone()[0]
    if null_keys_count > 0:
        logger.warning(f"Encontrados {null_keys_count} registros com chaves nulas.")
    else:
        logger.info("Nenhum registro com chaves nulas encontrado.")
    
    # Verificar duplicatas de chaves com active = TRUE
    duplicate_active_query = """
    SELECT recipe_id, flavour_id, ingredient_id, COUNT(*) 
    FROM dp_recipes_silver 
    WHERE active = TRUE 
    GROUP BY recipe_id, flavour_id, ingredient_id 
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(duplicate_active_query).fetchall()
    if duplicates:
        for duplicate in duplicates:
            logger.warning(f"Duplicata encontrada para (recipe_id={duplicate[0]}, flavour_id={duplicate[1]}, ingredient_id={duplicate[2]}) com {duplicate[3]} registros ativos.")
    else:
        logger.info("Nenhuma duplicata de chaves com active = TRUE encontrada.")
    
    # Verificar registros não processados na staging com detalhes
    unprocessed_records_query = """
    SELECT row_id, recipe_id, flavour_id, ingredient_id, quantity_grams, heat_process, inserted_at
    FROM dp_recipes_staging
    WHERE merge_status = 0;
    """
    unprocessed_records = conn.execute(unprocessed_records_query).fetchall()
    if unprocessed_records:
        logger.warning(f"Encontrados {len(unprocessed_records)} registros não processados na dp_recipes_staging.")
        for record in unprocessed_records:
            logger.warning(
                f"Registro não inserido - row_id={record[0]}, valores: "
                f"recipe_id={record[1]}, flavour_id={record[2]}, ingredient_id={record[3]}, "
                f"quantity_grams={record[4]}, heat_process={record[5]}, inserted_at={record[6]}"
            )
    else:
        logger.info("Todos os registros da dp_recipes_staging foram processados com sucesso.")
    
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
    dag_id='dag_bronze_to_silver_recipes',
    default_args=default_args,
    description='Pipeline SCD Tipo 2 para dp_recipes_silver',
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
        task_id='process_scd2_recipes',
        python_callable=process_scd2_recipes
    )
    
    # Tarefa 3: Verificar qualidade dos dados e emitir logs
    check_data_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality
    )
    
    # Definir a ordem das tarefas
    setup_task >> process_task >> check_data_quality_task
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
    
    # 1. Identificar as últimas versões de cada chave natural na staging
    latest_staging_query = """
    SELECT recipe_id, flavour_id, ingredient_id, quantity_grams, inserted_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY recipe_id, flavour_id, ingredient_id, quantity_grams 
                   ORDER BY inserted_at DESC
               ) AS rn
        FROM dp_recipes_staging
        WHERE merge_status = 0
    ) WHERE rn = 1;
    """
    latest_staging = conn.execute(latest_staging_query).fetchall()
    
    # 2. Processar novas inserções
    new_records_query = """
    INSERT INTO dp_recipes_silver
    SELECT 
        stg.recipe_id,
        stg.flavour_id,
        stg.ingredient_id,
        stg.quantity_grams,
        stg.heat_process,
        stg.inserted_at AS valid_from,
        NULL AS valid_to,
        TRUE AS active,
        stg.inserted_at AS load_timestamp,
        stg.origin_date AS source_date
    FROM dp_recipes_staging stg
    JOIN (VALUES {}) AS ls(recipe_id, flavour_id, ingredient_id, quantity_grams, inserted_at)
        ON stg.recipe_id = ls.recipe_id
        AND stg.flavour_id = ls.flavour_id
        AND stg.ingredient_id = ls.ingredient_id
        AND stg.quantity_grams = ls.quantity_grams
        AND stg.inserted_at = ls.inserted_at
    WHERE NOT EXISTS (
        SELECT 1
        FROM dp_recipes_silver sil
        WHERE sil.recipe_id = stg.recipe_id
        AND sil.flavour_id = stg.flavour_id
        AND sil.ingredient_id = stg.ingredient_id
        AND sil.quantity_grams = stg.quantity_grams
        AND sil.active = TRUE
    );
    """
    if latest_staging:
        placeholders = ", ".join(f"({r}, {f}, {i}, {q}, TIMESTAMP '{ts}')" for r, f, i, q, ts in latest_staging)
        conn.execute(new_records_query.format(placeholders))
    
    # 3. Processar atualizações
    update_query = """
    SELECT 
        stg.recipe_id,
        stg.flavour_id,
        stg.ingredient_id,
        stg.quantity_grams,
        stg.inserted_at
    FROM dp_recipes_staging stg
    JOIN dp_recipes_silver sil 
        ON stg.recipe_id = sil.recipe_id
        AND stg.flavour_id = sil.flavour_id
        AND stg.ingredient_id = sil.ingredient_id
        AND stg.quantity_grams = sil.quantity_grams
    JOIN (VALUES {}) AS ls(recipe_id, flavour_id, ingredient_id, quantity_grams, inserted_at)
        ON stg.recipe_id = ls.recipe_id
        AND stg.flavour_id = ls.flavour_id
        AND stg.ingredient_id = ls.ingredient_id
        AND stg.quantity_grams = ls.quantity_grams
        AND stg.inserted_at = ls.inserted_at
    WHERE sil.active = TRUE
    AND stg.heat_process != sil.heat_process;
    """
    if latest_staging:
        placeholders = ", ".join(f"({r}, {f}, {i}, {q}, TIMESTAMP '{ts}')" for r, f, i, q, ts in latest_staging)
        updates = conn.execute(update_query.format(placeholders)).fetchall()
    else:
        updates = []
    
    if updates:
        # Fechar versões antigas
        close_query = """
        UPDATE dp_recipes_silver
        SET valid_to = updates.inserted_at,
            active = FALSE
        FROM (VALUES {}) AS updates(recipe_id, flavour_id, ingredient_id, quantity_grams, inserted_at)
        WHERE dp_recipes_silver.recipe_id = updates.recipe_id
        AND dp_recipes_silver.flavour_id = updates.flavour_id
        AND dp_recipes_silver.ingredient_id = updates.ingredient_id
        AND dp_recipes_silver.quantity_grams = updates.quantity_grams
        AND dp_recipes_silver.active = TRUE;
        """.format(", ".join(
            f"({r}, {f}, {i}, {q}, TIMESTAMP '{ts}')" 
            for r, f, i, q, ts in updates
        ))
        conn.execute(close_query)
        
        # Inserir novas versões
        insert_query = """
        INSERT INTO dp_recipes_silver
        SELECT 
            stg.recipe_id,
            stg.flavour_id,
            stg.ingredient_id,
            stg.quantity_grams,
            stg.heat_process,
            stg.inserted_at AS valid_from,
            NULL AS valid_to,
            TRUE AS active,
            stg.inserted_at AS load_timestamp,
            stg.origin_date AS source_date
        FROM dp_recipes_staging stg
        JOIN (VALUES {}) AS updates(recipe_id, flavour_id, ingredient_id, quantity_grams, inserted_at)
            ON stg.recipe_id = updates.recipe_id
            AND stg.flavour_id = updates.flavour_id
            AND stg.ingredient_id = updates.ingredient_id
            AND stg.quantity_grams = updates.quantity_grams
            AND stg.inserted_at = updates.inserted_at;
        """.format(", ".join(
            f"({r}, {f}, {i}, {q}, TIMESTAMP '{ts}')" 
            for r, f, i, q, ts in updates
        ))
        conn.execute(insert_query)
    
    # 4. Marcar todos os registros processados na staging
    if latest_staging:
        mark_processed_query = """
        UPDATE dp_recipes_staging
        SET merge_status = 1
        WHERE (recipe_id, flavour_id, ingredient_id, quantity_grams, inserted_at) IN ({});
        """.format(", ".join(
            f"({r}, {f}, {i}, {q}, TIMESTAMP '{ts}')" 
            for r, f, i, q, ts in latest_staging
        ))
        conn.execute(mark_processed_query)
    
    logger.info("Processamento SCD Tipo 2 concluído com sucesso.")
    conn.close()

# Função para verificar a qualidade dos dados
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
    WHERE recipe_id IS NULL OR flavour_id IS NULL OR ingredient_id IS NULL OR quantity_grams IS NULL;
    """
    null_keys_count = conn.execute(null_keys_query).fetchone()[0]
    if null_keys_count > 0:
        logger.warning(f"Encontrados {null_keys_count} registros com chaves nulas.")
    else:
        logger.info("Nenhum registro com chaves nulas encontrado.")
    
    # Verificar duplicatas de chaves com active = TRUE
    duplicate_active_query = """
    SELECT recipe_id, flavour_id, ingredient_id, quantity_grams, COUNT(*) 
    FROM dp_recipes_silver 
    WHERE active = TRUE 
    GROUP BY recipe_id, flavour_id, ingredient_id, quantity_grams 
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(duplicate_active_query).fetchall()
    if duplicates:
        for duplicate in duplicates:
            logger.warning(f"Duplicata encontrada para (recipe_id={duplicate[0]}, flavour_id={duplicate[1]}, ingredient_id={duplicate[2]}, quantity_grams={duplicate[3]}) com {duplicate[4]} registros ativos.")
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
    setup_task = PythonOperator(
        task_id='setup_silver_table',
        python_callable=setup_silver_table
    )
    
    process_task = PythonOperator(
        task_id='process_scd2_recipes',
        python_callable=process_scd2_recipes
    )
    
    check_data_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality
    )
    
    setup_task >> process_task >> check_data_quality_task
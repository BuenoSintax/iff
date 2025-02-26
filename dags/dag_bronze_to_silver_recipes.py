from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb

# Configuração do banco de dados
DATABASE_PATH = 'iff_db.duckdb'

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
    print("Tabela dp_recipes_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 2
def process_scd2_recipes():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Lista para armazenar os recipe_id processados
    processed_recipe_ids = []
    
    # 1. Inserir novos registros (receitas que ainda não existem na silver)
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
    FROM dp_recipes_staging
    WHERE merge_status = 0
    AND recipe_id NOT IN (SELECT recipe_id FROM dp_recipes_silver WHERE active = TRUE);
    """
    conn.execute(new_records_query)
    
    # Obter os recipe_id dos novos registros inseridos
    new_recipe_ids_query = """
    SELECT DISTINCT recipe_id
    FROM dp_recipes_staging
    WHERE merge_status = 0
    AND recipe_id NOT IN (SELECT recipe_id FROM dp_recipes_silver WHERE active = TRUE);
    """
    new_recipe_ids = [row[0] for row in conn.execute(new_recipe_ids_query).fetchall()]
    processed_recipe_ids.extend(new_recipe_ids)
    
    # 2. Identificar os recipe_id que precisam ser atualizados
    identify_updates_query = """
    SELECT 
        stg.recipe_id
    FROM dp_recipes_staging stg
    JOIN dp_recipes_silver sil
        ON stg.recipe_id = sil.recipe_id
    WHERE stg.merge_status = 0
    AND sil.active = TRUE
    AND (stg.flavour_id != sil.flavour_id OR 
         stg.ingredient_id != sil.ingredient_id OR 
         stg.quantity_grams != sil.quantity_grams OR 
         stg.heat_process != sil.heat_process);
    """
    updated_recipe_ids = [row[0] for row in conn.execute(identify_updates_query).fetchall()]
    processed_recipe_ids.extend(updated_recipe_ids)
    
    # 3. Fechar registros antigos para os recipe_id identificados
    if updated_recipe_ids:
        placeholders = ', '.join(['?'] * len(updated_recipe_ids))
        close_old_records_query = f"""
        UPDATE dp_recipes_silver
        SET valid_to = (
            SELECT MAX(inserted_at)
            FROM dp_recipes_staging
            WHERE recipe_id = dp_recipes_silver.recipe_id
            AND merge_status = 0
        ),
            active = FALSE
        WHERE recipe_id IN ({placeholders})
        AND active = TRUE;
        """
        conn.execute(close_old_records_query, updated_recipe_ids)
        
        # 4. Inserir novas versões para os recipe_id identificados
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
        WHERE recipe_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(insert_new_versions_query, updated_recipe_ids)
    
    # 5. Atualizar o status na tabela staging apenas para os recipe_id processados
    if processed_recipe_ids:
        placeholders = ', '.join(['?'] * len(processed_recipe_ids))
        mark_processed_query = f"""
        UPDATE dp_recipes_staging
        SET merge_status = 1
        WHERE recipe_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(mark_processed_query, processed_recipe_ids)
    
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
        task_id='setup_silver_table_recipes',
        python_callable=setup_silver_table
    )
    
    # Tarefa 2: Processar os dados com SCD Tipo 2
    process_task = PythonOperator(
        task_id='process_scd2_recipes',
        python_callable=process_scd2_recipes
    )
    
    # Definir a ordem das tarefas
    setup_task >> process_task
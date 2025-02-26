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
    CREATE TABLE IF NOT EXISTS dp_ingredientsrawmaterial_silver (
        ingredient_rawmaterial_id INTEGER,
        ingredient_id INTEGER,
        raw_material_type_id INTEGER,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        active BOOLEAN,
        load_timestamp TIMESTAMP,
        source_date DATE
    );
    """
    conn.execute(create_table_query)
    print("Tabela dp_ingredientsrawmaterial_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 2
def process_scd2_ingredientsrawmaterial():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Lista para armazenar os ingredient_rawmaterial_id processados
    processed_ids = []
    
    # 1. Inserir novos registros (ingredientes que ainda não existem na silver)
    new_records_query = """
    INSERT INTO dp_ingredientsrawmaterial_silver
    SELECT 
        ingredient_rawmaterial_id,
        ingredient_id,
        raw_material_type_id,
        inserted_at AS valid_from,
        NULL AS valid_to,
        TRUE AS active,
        inserted_at AS load_timestamp,
        origin_date AS source_date
    FROM dp_ingredientsrawmaterial_staging
    WHERE merge_status = 0
    AND ingredient_rawmaterial_id NOT IN (SELECT ingredient_rawmaterial_id FROM dp_ingredientsrawmaterial_silver WHERE active = TRUE);
    """
    conn.execute(new_records_query)
    
    # Obter os ingredient_rawmaterial_id dos novos registros inseridos
    new_ids_query = """
    SELECT DISTINCT ingredient_rawmaterial_id
    FROM dp_ingredientsrawmaterial_staging
    WHERE merge_status = 0
    AND ingredient_rawmaterial_id NOT IN (SELECT ingredient_rawmaterial_id FROM dp_ingredientsrawmaterial_silver WHERE active = TRUE);
    """
    new_ids = [row[0] for row in conn.execute(new_ids_query).fetchall()]
    processed_ids.extend(new_ids)
    
    # 2. Identificar os ingredient_rawmaterial_id que precisam ser atualizados
    identify_updates_query = """
    SELECT 
        stg.ingredient_rawmaterial_id
    FROM dp_ingredientsrawmaterial_staging stg
    JOIN dp_ingredientsrawmaterial_silver sil
        ON stg.ingredient_rawmaterial_id = sil.ingredient_rawmaterial_id
    WHERE stg.merge_status = 0
    AND sil.active = TRUE
    AND (stg.ingredient_id != sil.ingredient_id OR stg.raw_material_type_id != sil.raw_material_type_id);
    """
    updated_ids = [row[0] for row in conn.execute(identify_updates_query).fetchall()]
    processed_ids.extend(updated_ids)
    
    # 3. Fechar registros antigos para os ingredient_rawmaterial_id identificados
    if updated_ids:
        placeholders = ', '.join(['?'] * len(updated_ids))
        close_old_records_query = f"""
        UPDATE dp_ingredientsrawmaterial_silver
        SET valid_to = (
            SELECT MAX(inserted_at)
            FROM dp_ingredientsrawmaterial_staging
            WHERE ingredient_rawmaterial_id = dp_ingredientsrawmaterial_silver.ingredient_rawmaterial_id
            AND merge_status = 0
        ),
            active = FALSE
        WHERE ingredient_rawmaterial_id IN ({placeholders})
        AND active = TRUE;
        """
        conn.execute(close_old_records_query, updated_ids)
        
        # 4. Inserir novas versões para os ingredient_rawmaterial_id identificados
        insert_new_versions_query = f"""
        INSERT INTO dp_ingredientsrawmaterial_silver
        SELECT 
            ingredient_rawmaterial_id,
            ingredient_id,
            raw_material_type_id,
            inserted_at AS valid_from,
            NULL AS valid_to,
            TRUE AS active,
            inserted_at AS load_timestamp,
            origin_date AS source_date
        FROM dp_ingredientsrawmaterial_staging
        WHERE ingredient_rawmaterial_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(insert_new_versions_query, updated_ids)
    
    # 5. Atualizar o status na tabela staging apenas para os ingredient_rawmaterial_id processados
    if processed_ids:
        placeholders = ', '.join(['?'] * len(processed_ids))
        mark_processed_query = f"""
        UPDATE dp_ingredientsrawmaterial_staging
        SET merge_status = 1
        WHERE ingredient_rawmaterial_id IN ({placeholders})
        AND merge_status = 0;
        """
        conn.execute(mark_processed_query, processed_ids)
    
    conn.close()

# Definição do DAG
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': None  # Desativa o agendamento automático
}

with DAG(
    dag_id='dag_bronze_to_silver_ingredientsrawmaterial',
    default_args=default_args,
    description='Pipeline SCD Tipo 2 para dp_ingredientsrawmaterial_silver',
    catchup=False,
    schedule_interval=None
) as dag:
    # Tarefa 1: Configurar a tabela silver
    setup_task = PythonOperator(
        task_id='setup_silver_table_ingredientsrawmaterial',
        python_callable=setup_silver_table
    )
    
    # Tarefa 2: Processar os dados com SCD Tipo 2
    process_task = PythonOperator(
        task_id='process_scd2_ingredientsrawmaterial',
        python_callable=process_scd2_ingredientsrawmaterial
    )
    
    # Definir a ordem das tarefas
    setup_task >> process_task
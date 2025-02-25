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
    print("Tabela dp_customers_silver verificada/criada com sucesso.")
    conn.close()

# Função para processar os dados com lógica SCD Tipo 2
def process_scd2_customers():
    conn = duckdb.connect(DATABASE_PATH)
    
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
    
    # 5. Atualizar o status na tabela staging
    mark_processed_query = """
    UPDATE dp_customers_staging
    SET merge_status = 1
    WHERE merge_status = 0;
    """
    conn.execute(mark_processed_query)
    
    conn.close()
    
# Definição do DAG
default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': None  # Desativa o agendamento automático
}

with DAG(
    dag_id='dag_upsert_customers',
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
    
    # Definir a ordem das tarefas
    setup_task >> process_task
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
    
    # 1. Inserir novos registros (última versão de cada ingredient)
    new_records_query = """
    INSERT INTO dp_ingredients_silver
    SELECT 
        stg.ingredient_id,
        stg.name,
        stg.chemical_formula,
        stg.molecular_weight,
        stg.cost_per_gram,
        stg.provider_id,
        stg.inserted_at AS valid_from,
        NULL AS valid_to,
        TRUE AS active,
        stg.inserted_at AS load_timestamp,
        stg.origin_date AS source_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ingredient_id ORDER BY inserted_at DESC) AS rn
        FROM dp_ingredients_staging
        WHERE merge_status = 0
    ) stg
    WHERE stg.rn = 1
    AND NOT EXISTS (
        SELECT 1
        FROM dp_ingredients_silver sil
        WHERE sil.ingredient_id = stg.ingredient_id
        AND sil.active = TRUE
    );
    """
    conn.execute(new_records_query)

    # 2. Identificar atualizações necessárias
    updates_query = """
    WITH latest_staging AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ingredient_id ORDER BY inserted_at DESC) AS rn
        FROM dp_ingredients_staging
        WHERE merge_status = 0
    )
    SELECT 
        stg.ingredient_id,
        stg.inserted_at
    FROM latest_staging stg
    INNER JOIN dp_ingredients_silver sil 
        ON stg.ingredient_id = sil.ingredient_id
    WHERE stg.rn = 1
    AND sil.active = TRUE
    AND (
        stg.name <> sil.name OR
        stg.chemical_formula <> sil.chemical_formula OR
        stg.molecular_weight <> sil.molecular_weight OR
        stg.cost_per_gram <> sil.cost_per_gram OR
        stg.provider_id <> sil.provider_id
    );
    """
    updates = conn.execute(updates_query).fetchall()

    if updates:
        # 3. Fechar registros antigos
        close_query = """
        UPDATE dp_ingredients_silver
        SET valid_to = updates.inserted_at,
            active = FALSE
        FROM (VALUES {}) AS updates(ingredient_id, inserted_at)
        WHERE dp_ingredients_silver.ingredient_id = updates.ingredient_id
        AND dp_ingredients_silver.active = TRUE;
        """.format(", ".join(f"({id}, TIMESTAMP '{ts}')" for id, ts in updates))
        
        conn.execute(close_query)

        # 4. Inserir novas versões
        insert_query = """
        INSERT INTO dp_ingredients_silver
        SELECT 
            stg.ingredient_id,
            stg.name,
            stg.chemical_formula,
            stg.molecular_weight,
            stg.cost_per_gram,
            stg.provider_id,
            stg.inserted_at AS valid_from,
            NULL AS valid_to,
            TRUE AS active,
            stg.inserted_at AS load_timestamp,
            stg.origin_date AS source_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY ingredient_id ORDER BY inserted_at DESC) AS rn
            FROM dp_ingredients_staging
            WHERE merge_status = 0
        ) stg
        WHERE stg.rn = 1
        AND stg.ingredient_id IN ({});
        """.format(", ".join(map(str, [id for id, _ in updates])))
        
        conn.execute(insert_query)

    # 5. Marcar registros processados (Query Corrigida)
    mark_processed_query = """
    UPDATE dp_ingredients_staging AS target
    SET merge_status = 1
    FROM (
        SELECT 
            ingredient_id, 
            MAX(inserted_at) AS max_inserted_at
        FROM dp_ingredients_staging
        WHERE merge_status = 0
        GROUP BY ingredient_id
    ) AS latest
    WHERE target.ingredient_id = latest.ingredient_id
    AND target.inserted_at = latest.max_inserted_at
    AND target.merge_status = 0;
    """
    conn.execute(mark_processed_query)

    logger.info("Processamento SCD Tipo 2 concluído com sucesso.")
    conn.close()
# Função para verificar a qualidade dos dados e registrar logs
def check_data_quality():
    conn = duckdb.connect(DATABASE_PATH)
    
    # Verificação de registros ativos únicos
    active_check = """
    SELECT ingredient_id, COUNT(*) 
    FROM dp_ingredients_silver 
    WHERE active = TRUE 
    GROUP BY ingredient_id 
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(active_check).fetchall()
    if duplicates:
        logger.error(f"Duplicatas ativas detectadas: {duplicates}")
        raise ValueError("Registros duplicados ativos encontrados!")
    
    # Verificação de sobreposições temporais (Corrigida)
    overlap_check = """
    SELECT COUNT(*)
    FROM dp_ingredients_silver t1
    JOIN dp_ingredients_silver t2
        ON t1.ingredient_id = t2.ingredient_id
        AND t1.valid_from < t2.valid_to
        AND t1.valid_to > t2.valid_from
        AND (t1.valid_from != t2.valid_from OR t1.valid_to != t2.valid_to);
    """
    overlaps = conn.execute(overlap_check).fetchone()[0]
    if overlaps > 0:
        logger.error(f"Sobreposições temporais detectadas: {overlaps}")
        raise ValueError("Sobreposições temporais encontradas!")
    
    # Verificação de valores nulos em colunas críticas
    null_check = """
    SELECT COUNT(*)
    FROM dp_ingredients_silver
    WHERE ingredient_id IS NULL OR name IS NULL OR valid_from IS NULL OR active IS NULL;
    """
    nulls = conn.execute(null_check).fetchone()[0]
    if nulls > 0:
        logger.error(f"Valores nulos encontrados em colunas críticas: {nulls}")
        raise ValueError("Valores nulos em colunas críticas!")
    
    # Verificação de consistência de datas
    date_consistency_check = """
    SELECT COUNT(*)
    FROM dp_ingredients_silver
    WHERE active = FALSE AND valid_from >= valid_to;
    """
    date_errors = conn.execute(date_consistency_check).fetchone()[0]
    if date_errors > 0:
        logger.error(f"Inconsistências nas datas detectadas: {date_errors}")
        raise ValueError("valid_from posterior a valid_to em registros inativos!")
    
    # Logs de observabilidade
    total_records = conn.execute("SELECT COUNT(*) FROM dp_ingredients_silver;").fetchone()[0]
    active_records = conn.execute("SELECT COUNT(*) FROM dp_ingredients_silver WHERE active = TRUE;").fetchone()[0]
    inactive_records = conn.execute("SELECT COUNT(*) FROM dp_ingredients_silver WHERE active = FALSE;").fetchone()[0]
    
    logger.info(f"Total de registros: {total_records}")
    logger.info(f"Registros ativos: {active_records}")
    logger.info(f"Registros inativos: {inactive_records}")
    logger.info("Verificação de qualidade passou com sucesso.")
    
    conn.close()

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
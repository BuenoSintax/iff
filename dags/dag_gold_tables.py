from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
import logging

# -----------------------------------------------------------------------------
# 1) Configuração do banco de dados e logging
# -----------------------------------------------------------------------------
DATABASE_PATH = 'iff_db.duckdb'

logging.basicConfig(
    filename='data_quality_gold.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# 2) Função para criar/verificar a tabela Silver (opcional)

def setup_silver_table():
    """
    Cria a tabela dp_salestransactions_silver caso ela não exista.
    """
    conn = duckdb.connect(DATABASE_PATH)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS dp_salestransactions_silver (
        transaction_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        flavour_id INTEGER,
        quantity_liters DOUBLE,
        transaction_date DATE,
        country VARCHAR,
        town VARCHAR,
        postal_code VARCHAR,
        amount_dollar DOUBLE,
        load_timestamp TIMESTAMP,
        source_date DATE
    );
    """
    conn.execute(create_table_query)
    logger.info("Tabela dp_salestransactions_silver existe ce está pronta para camada gold.")
    conn.close()


# 3) Função para processar dados usando SCD Tipo 1 (se houver staging)


    """
    Lê dados de dp_salestransactions_staging e aplica SCD Tipo 1 na Silver.
    """
    conn = duckdb.connect(DATABASE_PATH)
    # Exemplo: rankear e inserir/atualizar
    cte_query = """
    WITH ranked_staging AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id 
                ORDER BY batch_value DESC, inserted_at DESC
            ) AS rn
        FROM dp_salestransactions_staging
        WHERE merge_status = 0
    )
    """
    
    insert_new_records_query = cte_query + """
    INSERT INTO dp_salestransactions_silver
    SELECT 
        transaction_id,
        customer_id,
        flavour_id,
        quantity_liters,
        transaction_date,
        country,
        town,
        postal_code,
        amount_dollar,
        inserted_at AS load_timestamp,
        origin_date AS source_date
    FROM ranked_staging
    WHERE rn = 1
    AND transaction_id NOT IN (SELECT transaction_id FROM dp_salestransactions_silver);
    """
    conn.execute(insert_new_records_query)

    update_existing_records_query = cte_query + """
    UPDATE dp_salestransactions_silver AS sil
    SET 
        customer_id = ranked.customer_id,
        flavour_id = ranked.flavour_id,
        quantity_liters = ranked.quantity_liters,
        transaction_date = ranked.transaction_date,
        country = ranked.country,
        town = ranked.town,
        postal_code = ranked.postal_code,
        amount_dollar = ranked.amount_dollar,
        load_timestamp = ranked.inserted_at,
        source_date = ranked.origin_date
    FROM ranked_staging AS ranked
    WHERE ranked.rn = 1
    AND sil.transaction_id = ranked.transaction_id;
    """
    conn.execute(update_existing_records_query)

    mark_processed_query = cte_query + """
    UPDATE dp_salestransactions_staging AS stg
    SET merge_status = 1
    FROM ranked_staging AS ranked
    WHERE stg.row_id = ranked.row_id
    AND ranked.rn = 1;
    """
    conn.execute(mark_processed_query)

    logger.info("Processamento SCD Tipo 1 concluído com sucesso.")
    conn.close()


# 4) Função para criar a VIEW na camada GOLD

def create_gold_view():
    """
    Cria ou substitui a view vw_salestransaction_analytics, consolida dados de venda
    e cálculo de custo/lucro.
    """
    conn = duckdb.connect(DATABASE_PATH)
    
    create_view_query = """
    CREATE OR REPLACE VIEW vw_salestransaction_analytics AS
    SELECT distinct
        st.transaction_id,
        st.customer_id,
        c.name AS customer_name,
        st.flavour_id,
        f.name AS flavour_name,
        st.quantity_liters,
        st.transaction_date,
        st.amount_dollar AS revenue,
        
        fc.cost_per_liter,
        
        -- Custo total
        ROUND((st.quantity_liters * fc.cost_per_liter), 2) AS total_cost,
        
        -- Lucro
        ROUND((st.amount_dollar - (st.quantity_liters * fc.cost_per_liter)), 2) AS profit
        
    FROM dp_salestransactions_silver st
    JOIN dp_customers_silver c
        ON st.customer_id = c.customer_id
        AND c.active = TRUE
    JOIN dp_flavours_silver f
        ON st.flavour_id = f.flavour_id
        AND f.active = TRUE
    
    JOIN (
        SELECT
            r.flavour_id,
            ROUND(SUM(r.quantity_grams * i.cost_per_gram) / SUM(r.quantity_grams), 2) AS cost_per_liter
        FROM dp_recipes_silver r
        JOIN dp_ingredients_silver i
            ON r.ingredient_id = i.ingredient_id
            WHERE r.active = TRUE
            AND i.active = TRUE
        GROUP BY r.flavour_id
    ) fc
        ON st.flavour_id = fc.flavour_id
    
    ORDER BY
        st.transaction_date,
        st.transaction_id;
    """
    
    conn.execute(create_view_query)
    logger.info("View vw_salestransaction_analytics criada ou substituída com sucesso.")
    conn.close()


# 5) Função para checar qualidade dos dados SOMENTE na view GOLD

def check_view_quality():
    """
    Executa checagens de qualidade diretamente na view vw_salestransaction_analytics
    Exemplo:
    - Contagem de registros
    - Checar nulos em colunas críticas
    - Procurar duplicatas de transaction_id
    """
    conn = duckdb.connect(DATABASE_PATH)
    
    # Exemplo 1: Contar registros
    total_records_query = "SELECT COUNT(*) FROM vw_salestransaction_analytics;"
    total_records = conn.execute(total_records_query).fetchone()[0]
    logger.info(f"Total de registros na view vw_salestransaction_analytics: {total_records}")
    
    # Exemplo 2: Verificar nulos em transaction_id
    null_tx_query = "SELECT COUNT(*) FROM vw_salestransaction_analytics WHERE transaction_id IS NULL;"
    null_tx_count = conn.execute(null_tx_query).fetchone()[0]
    if null_tx_count > 0:
        logger.warning(f"Encontrados {null_tx_count} registros com transaction_id nulo na view.")
    else:
        logger.info("Nenhum registro com transaction_id nulo na view.")
    
    # Exemplo 3: Procurar duplicatas de transaction_id na view
    dup_tx_query = """
    SELECT transaction_id, COUNT(*) 
    FROM vw_salestransaction_analytics
    GROUP BY transaction_id
    HAVING COUNT(*) > 1;
    """
    duplicates = conn.execute(dup_tx_query).fetchall()
    if duplicates:
        for d in duplicates:
            logger.warning(f"Duplicata encontrada para transaction_id {d[0]} com {d[1]} registros (na view).")
    else:
        logger.info("Nenhuma duplicata na view vw_salestransaction_analytics.")
    
    logger.info("Checagem de qualidade da view (camada GOLD) concluída.")
    conn.close()


# 6) Definição do DAG e tarefas do Airflow

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2023, 10, 1),
    'schedule_interval': None
}

with DAG(
    dag_id='dag_gold_sales_transactions_view',
    default_args=default_args,
    description='Cria view vw_salestransaction_analytics e checa qualidade somente nela',
    catchup=False,
    schedule_interval=None
) as dag:
    
    # Tarefa 1 (opcional): Setup tabela Silver
    setup_silver_task = PythonOperator(
        task_id='setup_silver_table',
        python_callable=setup_silver_table
    )
    
    
    # Tarefa 3: Criar a VIEW Gold
    create_gold_view_task = PythonOperator(
        task_id='create_gold_view',
        python_callable=create_gold_view
    )
    
    # Tarefa 4: Checar Qualidade SOMENTE da view Gold
    check_view_quality_task = PythonOperator(
        task_id='check_gold_view_quality',
        python_callable=check_view_quality
    )
    
    # Definindo a ordem:
    setup_silver_task  >> create_gold_view_task >> check_view_quality_task

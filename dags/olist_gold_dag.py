#Classe do Airflow para a criação de DAGs.
from airflow import DAG
#Operador para utilizar comandos SQL diretamente no DB PostgreSQL na DAG.
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#Ferramenta que cria a conexão com o DB com segurança.
from airflow.providers.postgres.hooks.postgres import PostgresHook
#Classe para trabalhar com horas.
from datetime import datetime

#Variáveis de configuração.
conn_id = 'postgres_default' #Conexão padrão do Astro CLI.

with DAG(
    dag_id='olist_gold_star_schema',
    start_date=datetime(2024, 1, 1),
    schedule=None, #Para rodar manualmente.
    catchup=False
)as dag:
    #Task 1. Cria o Schema "gold" dentro do DB caso não exista.
    create_gold_schema_task = SQLExecuteQueryOperator(
    task_id='create_gold_schema',
    conn_id=conn_id,
    sql="""CREATE SCHEMA IF NOT EXISTS gold;"""
    )
    #Task 2. Cria a tabela fato da nossa dimensão.
    create_gold_fact_table_task = SQLExecuteQueryOperator(
    task_id='create_gold_orders_fact',
    conn_id=conn_id,
    sql=""" DROP TABLE IF EXISTS gold.orders_fact;
            
            CREATE TABLE gold.orders_fact AS
            SELECT 
            oi.order_id,          -- ID do Pedido
            oi.product_id,        -- ID do Produto (o que foi vendido)
            oi.seller_id,         -- ID do Vendedor (quem vendeu)
            o.purchase_timestamp,  -- Data/Hora da compra (da tabela de pedidos)

            -- Métricas Financeiras
            oi.price,             -- Preço do produto
            oi.freight_value,     -- Valor do frete
            (oi.price + oi.freight_value) AS total_amount, -- Cálculo na hora: Preço + Frete
            
            -- Métricas de Performance (o foco logístico da nossa análise)
            o.delay_days,         -- Dias de atraso
            o.is_late          -- Flag de atraso (TRUE/FALSE)                
        
            -- Tabela origem 1 (Itens) com apelido 'oi'
            FROM silver.orders_items AS oi
            -- Tabela origem 2 (Pedidos) com apelido 'o'
            INNER JOIN silver.orders AS o
                -- A CHAVE: Como elas se conectam
                ON oi.order_id = o.order_id
            -- Filtro de negócio: Nossa modelagem considerará apenas os pedidos entregues.
            WHERE o.order_status = 'delivered';
    """
    )
    #Task 3. Cria a tabela de dimensão dos vendendores.
    create_gold_dim_sellers_task = SQLExecuteQueryOperator(
    task_id='create_gold_dim_sellers',
    conn_id='postgres_default',
    sql="""
        DROP TABLE IF EXISTS gold.dim_sellers;

        CREATE TABLE gold.dim_sellers AS
        SELECT 
            seller_id,
            city,
            state,
            -- Flag de região
            CASE 
                WHEN state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Sudeste'
                WHEN state IN ('PR', 'SC', 'RS') THEN 'Sul'
                WHEN state IN ('AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO') THEN 'Norte' 
                WHEN state IN ('AL', 'BA', 'CE', 'MA', 'PB', 'PE', 'PI', 'RN', 'SE') THEN 'Nordeste' 
                WHEN state IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste' 
            END AS region
        FROM silver.sellers;
    """
    )
    #Task 4. Cria a tabela de dimensão dos produtos.
    create_gold_dim_products_task = SQLExecuteQueryOperator(
    task_id='create_gold_dim_products',
    conn_id='postgres_default',
    sql="""
        DROP TABLE IF EXISTS gold.dim_products;

        CREATE TABLE gold.dim_products AS
        SELECT 
            product_id,
            product_category,
            -- Adicionamos colunas técnicas que podem influenciar a logística
            -- (Produtos maiores/mais pesados podem ter fretes mais lentos)
            weight_g,
            (length_cm * height_cm * width_cm) AS product_volume_cm3
        FROM silver.products;
    """
)
    #Task 3. Cria a tabela de dimensão das datas.
    create_gold_dim_date_task = SQLExecuteQueryOperator(
    task_id='create_gold_dim_date',
    conn_id='postgres_default',
    sql="""
        DROP TABLE IF EXISTS gold.dim_date;
        
        CREATE TABLE gold.dim_date AS
        SELECT 
            -- 1. Criação do ID único (Chave Primária)
            TO_CHAR(date_series, 'YYYYMMDD')::INTEGER AS date_id,                
            
            -- 2. Colunas de Contexto de Tempo
            date_series AS full_date,                
            -- Usando o EXTRACT para capturar intervalos de tempo específicos
            EXTRACT(YEAR FROM date_series) AS year,   
            EXTRACT(MONTH FROM date_series) AS month, 
            TO_CHAR(date_series, 'Month') AS month_name, 
            EXTRACT(QUARTER FROM date_series) AS quarter, 
            EXTRACT(WEEK FROM date_series) AS week_of_year, 
            
            -- 3. Detalhes do Dia da Semana
            EXTRACT(DOW FROM date_series) AS day_of_week, 
            TO_CHAR(date_series, 'Day') AS day_name, 
            
            -- 4. Flag de Negócio
            CASE 
                -- Captura apenas os domingos (0) e os sabados (6)
                WHEN EXTRACT(DOW FROM date_series) IN (0, 6) THEN TRUE 
                ELSE FALSE 
            END AS is_weekend
        FROM 
            -- 5. Gera a lista de datas a partir de duas sub-queries
            generate_series(
                (SELECT MIN(purchase_timestamp) FROM gold.orders_fact),
                (SELECT MAX(purchase_timestamp) FROM gold.orders_fact),
                '1 day'::interval
            ) AS date_series;
        """
    )
create_gold_schema_task>>[create_gold_fact_table_task,create_gold_dim_products_task, create_gold_dim_products_task, create_gold_dim_date_task]
#Classe do Airflow para a criação de DAGs.
from airflow import DAG
#Operador para utilizar código Python em uma DAG. 
from airflow.providers.standard.operators.python import PythonOperator
#Operador para utilizar comandos SQL diretamente no DB PostgreSQL.
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#Ferramenta que cria a conexão com o DB com segurança.
from airflow.providers.postgres.hooks.postgres import PostgresHook
#Classe para trabalhar com horas.
from datetime import datetime
#Biblioteca para trabalhar com dados em tabelas (Data Frames).
import pandas as pd
#Módulo Python para interagir com o Sistema Operacional.
import os

#Variáveis de configuração.
conn_id = 'postgres_default' #Conexão padrão do Astro CLI.

with DAG(
    dag_id='olist_clean_up_silver',
    start_date=datetime(2024, 1, 1),
    schedule=None, #Para rodar manualmente.
    catchup=False
)as dag:
    #Task 1. Cria o Schema "silver" dentro do DB caso não exista.
    create_silver_schema_task = SQLExecuteQueryOperator(
    task_id='create_silver_schema',
    conn_id=conn_id,
    sql="""CREATE SCHEMA IF NOT EXISTS silver;"""
    )
    #Task 2.
    create_silver_orders_table = SQLExecuteQueryOperator(
    task_id='create_silver_orders_table',
    conn_id='postgres_default',
    sql="""
        DROP TABLE IF EXISTS silver.orders;

        CREATE TABLE silver.orders AS
        WITH de_duplicated_cte AS (
            SELECT 
                order_id,
                customer_id,
                LOWER(order_status) AS order_status,
                CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase_timestamp,
                CAST(order_approved_at AS TIMESTAMP) AS approved_at,
                CAST(order_delivered_carrier_date AS TIMESTAMP) AS delivered_carrier_date,
                CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_date,
                CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_date,
                /*
                Separa todos os order_id distintos em grupos, os ordena em ordem decrescente pelo 
                order_purchase_timestamp e os enumera. 
                */
                ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_purchase_timestamp DESC) as row_num
            FROM bronze.bronze_orders
        )
        SELECT 
            order_id,
            customer_id,
            order_status,
            purchase_timestamp,
            approved_at,
            delivered_carrier_date,
            delivered_customer_date,
            estimated_delivery_date,
            /*
            Em SQL ao calcularmos dois TIMESTAMPS nos é retornado um INTERVAL. Para capturar 
            apenas o DIA deste intervalo usamos o EXTRACT(DAY ...)
            Um valor positivo = atrasado, negativo = adiantado.
            */
            CASE 
                WHEN order_status = 'delivered' AND delivered_customer_date IS NOT NULL 
                THEN EXTRACT(DAY FROM (delivered_customer_date - estimated_delivery_date))
                ELSE NULL 
            END AS delay_days,
            -- Flag de atraso para facilitar filtros no PowerBI/Looker
            CASE 
                WHEN order_status = 'delivered' AND 
                delivered_customer_date > estimated_delivery_date THEN TRUE
                ELSE FALSE 
            END AS is_late,
            NOW() AS updated_at
        FROM de_duplicated_cte
        -- Garante unicidade do pedido
        WHERE row_num = 1;
    """
    )
    create_silver_orders_items_table = SQLExecuteQueryOperator(
    task_id='create_silver_orders_items_table',
    conn_id='postgres_default',
    sql="""
        DROP TABLE IF EXISTS silver.orders_items;
        
        CREATE TABLE silver.orders_items AS
        WITH de_duplicated_cte AS (
            SELECT 
                order_id,
                order_item_id,
                product_id,
                seller_id,
                CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
                -- Convertendo para numérico acurato para calculos financeiros.
                CAST(price AS NUMERIC(10,2)) AS price,
                CAST(freight_value AS NUMERIC(10,2)) AS freight_value,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id, order_item_id 
                    ORDER BY shipping_limit_date DESC
                ) as row_num
            FROM bronze.bronze_orders_items
        )
        SELECT 
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            -- Lógica de Negócio: Valor total do item (Produto + Frete)
            (price + freight_value) AS total_item_value,
            NOW() AS updated_at
        FROM de_duplicated_cte
        WHERE row_num = 1;
    """
    )       

    create_silver_products_table = SQLExecuteQueryOperator(
    task_id='create_silver_products_table',
    conn_id='postgres_default',
    sql="""
        DROP TABLE IF EXISTS silver.products;
        
        CREATE TABLE silver.products AS
        WITH de_duplicated_cte AS (
            SELECT 
                product_id,
                -- Limpeza da Categoria
                INITCAP(REPLACE(LOWER(product_category_name), '_', ' ')) AS category_name,
                CAST(product_name_lenght AS INTEGER) AS name_length,
                CAST(product_description_lenght AS INTEGER) AS description_length,
                CAST(product_photos_qty AS INTEGER) AS photos_qty,
                CAST(product_weight_g AS NUMERIC(10,2)) AS weight_g,
                CAST(product_length_cm AS NUMERIC(10,2)) AS length_cm,
                CAST(product_height_cm AS NUMERIC(10,2)) AS height_cm,
                CAST(product_width_cm AS NUMERIC(10,2)) AS width_cm,
                ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) as row_num
            FROM bronze.bronze_products
        )
        SELECT 
            product_id,
            -- Se a categoria for nula, chamamos de 'Sem Categoria'
            COALESCE(category_name, 'Sem Categoria') AS product_category,
            name_length,
            description_length,
            photos_qty,
            weight_g,
            length_cm,
            height_cm,
            width_cm,
            NOW() AS updated_at
        FROM de_duplicated_cte
        WHERE row_num = 1;
    """
    )
    
    create_silver_sellers_table = SQLExecuteQueryOperator(
    task_id='create_silver_sellers_table',
    conn_id='postgres_default',
    sql="""
        DROP TABLE IF EXISTS silver.sellers;
        
        CREATE TABLE silver.sellers AS
        WITH de_duplicated_cte AS (
            SELECT 
                -- Padronização de Colunas
                seller_id,
                CAST(seller_zip_code_prefix AS INTEGER) AS zip_code_prefix,
                INITCAP(LOWER(seller_city)) AS city,
                UPPER(seller_state) AS state,
                ROW_NUMBER() OVER (PARTITION  BY seller_id ORDER BY seller_id) AS row_num
            FROM bronze.bronze_sellers
        )
        SELECT
            seller_id,
            zip_code_prefix,
            city,
            state,
            NOW() AS updated_at
        FROM de_duplicated_cte    
        WHERE row_num = 1;
    """            
    )
#Roda a task de criação de Schema primeiro.
create_silver_schema_task>>[create_silver_orders_table, create_silver_orders_items_table, create_silver_products_table, create_silver_sellers_table]
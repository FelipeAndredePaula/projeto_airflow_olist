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
data_path = '/usr/local/airflow/include/data/'

def load_csv_to_bronze(file_name, table_name):
    #1. Localiza o arquivo. 
    path = os.path.join(data_path, file_name)
    
    #2. Lê o CSV, o tranformando em um Data Frame onde todas as colunas são do tipo string.
    df = pd.read_csv(path, dtype=str)
    
    #2.1. Remove possíveis espaços em branco dos registros.
    df.columns = [c.strip() for c in df.columns]
    
    #2.2 Adiciona colunas de auditoria.
    df["ingestion_date"] = datetime.now()
    df["source_file"] = file_name 
    
    #3. Conecta ao Postgres usando o Hook do Airflow.
    hook = PostgresHook(postgres_conn_id=conn_id)
    engine = hook.get_sqlalchemy_engine()
    
    #4. Salva no banco (Subscreve caso já a tabela já exista).
    #Criaremos as tabelas no schema 'bronze' para melhor organização.
    df.to_sql(table_name, engine, schema='bronze', if_exists='replace', index=False)
    print(f"Tabela {table_name} carregada para o schema 'bronze' com sucesso!")

with DAG(
    dag_id='olist_ingestion_bronze',
    start_date=datetime(2024, 1, 1),
    schedule=None, #Para rodar manualmente.
    catchup=False
) as dag:
    #Task 1. Cria o Schema "bronze" dentro do DB caso não exista.
    create_bronze_schema_task = SQLExecuteQueryOperator(
        task_id='create_bronze_schema',
        conn_id=conn_id,
        sql="CREATE SCHEMA IF NOT EXISTS bronze;"
    )
    
    #Task 2. Carrega o CSV de pedidos.
    task_load_orders = PythonOperator(
        task_id='load_orders',
        python_callable=load_csv_to_bronze,
        op_kwargs={
            'file_name': 'olist_orders_dataset.csv',
            'table_name': 'bronze_orders'
        }
    )

    #Task 3. Carrega o CSV do detalhamento dos pedidos.
    task_load_orders_items = PythonOperator(
        task_id='load_orders_items',
        python_callable=load_csv_to_bronze,
        op_kwargs={
            'file_name': 'olist_order_items_dataset.csv',
            'table_name': 'bronze_orders_items'
        }
    )

    #Task 4. Carrega o CSV de detalhamento dos produtos.
    task_load_products = PythonOperator(
        task_id='load_products',
        python_callable=load_csv_to_bronze,
        op_kwargs={
            'file_name': 'olist_products_dataset.csv',
            'table_name': 'bronze_products'
        }
    )

    #Task 5. Carrega o CSV de detalhamento dos vendedores.
    task_load_sellers = PythonOperator(
        task_id='load_sellers',
        python_callable=load_csv_to_bronze,
        op_kwargs={
            'file_name': 'olist_sellers_dataset.csv',
            'table_name': 'bronze_sellers'
        }
    )

    #Task 6. Carrega o CSV de posição geográfica.
    task_load_geolocation = PythonOperator(
        task_id='load_geolocation',
        python_callable=load_csv_to_bronze,
        op_kwargs={
            'file_name': 'olist_geolocation_dataset.csv',
            'table_name': 'bronze_geolocation'
        }
    )

#Roda a task de criação de Schema primeiro, depois roda as tasks de carga dos CSV's em paralelo.
create_bronze_schema_task >> [task_load_orders, task_load_orders_items, task_load_products, task_load_sellers, task_load_geolocation]
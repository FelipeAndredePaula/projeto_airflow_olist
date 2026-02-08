# 📦 Olist: Logística e Retenção de Clientes

## 🚩 O Problema: "A Crise de Retenção e o Gargalo Logístico"
A diretoria da Olist notou que, apesar do alto volume de vendas, a taxa de recompra está caindo. O time de Customer Experience (CX) suspeita que o culpado não é o preço, mas a experiência de entrega e a performance dos vendedores.

### ❓ Pergunta de Negócio:
> "Quais categorias de produtos, regiões do Brasil e vendedores apresentam os maiores atrasos na entrega? Qual a porcentagem dos pedidos atrasados em relação ao total?"

### 📊 KPIs de Negócio
Este projeto entrega a infraestrutura de dados necessária para responder o problema de negócio descrito acima. Sendo possível obter, por exemplo, os seguintes KPI's:

* **SLA (Service Level Agreement):** % de pedidos entregues no prazo.
* **Average Delay:** Média de dias de atraso por categoria/vendedor.
* **Top Offenders:** Ranking de vendedores com maior volume de atrasos.

## 🏗️ Arquitetura do Pipeline (Medallion Architecture)

O projeto foi estruturado seguindo o padrão Medallion Architecture para garantir a qualidade e governança dos dados.

### 🥉 Camada Bronze (Raw Data)
* **Objetivo:** Ingestão bruta dos dados do Kaggle para o Postgres via PostgresOperator, garantindo a persistência do histórico sem transformações (Raw Data). Consulte o código para mais detalhes.
* **Processo:** `olist_bronze_dag.py`

### 🥈 Camada Silver (Cleaned Data)
* **Objetivo:** Limpeza, tipagem e deduplicação de dados. Aplicação de regras de negócio iniciais, como cálculo de atraso de entrega e padronização de localização de vendedores. Consulte o código para mais detalhes.
* **Processo:** `olist_silver_dag.py`
### 🥇 Camada Gold (Business Data - Star Schema)
* **Objetivo:** Modelagem dimensional dos dados logísticos em uma tabela fato e três dimensões de detalhamentos prontos para consumo em relatórios e cálculos dos KPI's.
* **Processo:** `olist_gold_dag.py`
* **Modelagem:**    
     * **`gold.orders_fact`**: Tabela fato com ID's e métricas de tempo e custo de frete.
    * **`gold.dim_sellers`**: Dimensão de detalhamento geográfico dos vendedores.
    * **`gold.dim_products`**: Dimensão de detalhamento das categorias e o volume de produtos.
    * **`gold.dim_calendar`**: Dimensão de datas para análises temporais.

## 🛠️ Environment Setup (Passo a Passo)

### 1. Fonte de Dados
**Dataset:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Versão 2, 2021/10/01)

### 2. Estrutura inicial da pasta do projeto 
Vale notar que ao instalar o Astro para gerenciar nosso ambiente Apache Airflow novas pastas e arquivos serão criados para além dos descritos abaixo:


    .
    ├── dags/
    │   ├── (.py contendo as DAGS)
    ├── include/
    │   └── data/ 
            └── (CSVs)
    ├── requirements.txt
    └── README.md

### 3. Infraestrutura e Orquestração
* **Astro CLI (3.1.6+astro.1):** Gerenciamento do Apache Airflow via Docker.
* **PostgreSQL:** Banco de dados para as camadas Bronze, Silver e Gold.

**Configurações Importantes:**
* **requirements.txt:** Adição da linha 

    `apache-airflow-providers-postgres`

    para adicionar o provider Postgres em nosso ambiente Apache Airflow.
* **.env:** Adição da linha  
    
    `AIRFLOW_CONN_POSTGRES_DEFAULT`
    
    para criar a configuração padrão com o banco de dados Postgres.

## Como Executar
1. [Instale o Astro CLI.](https://www.astronomer.io/docs/astro/cli/install-cli)
2. Clone o repositório.
3. Rode `astro dev start` em um terminal dentro do projeto.
4. Acesse o Airflow UI, habilite e rode as DAGs nesta sequência: 

    `olist_ingestion_bronze` `olist_clean_up_silver` `olist_gold_star_schema`
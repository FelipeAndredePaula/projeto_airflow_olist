# 📦 Olist: Logística e Retenção de Clientes

## 🚩 O Problema: "A Crise de Retenção e o Gargalo Logístico"
A diretoria da Olist notou que, apesar do alto volume de vendas, a taxa de recompra está caindo. O time de **Customer Experience (CX)** suspeita que o culpado não é o preço, mas a experiência de entrega e a performance dos vendedores.

### ❓ Pergunta de Negócio:
> "Quais categorias de produtos, regiões do Brasil e vendedores apresentam os maiores atrasos na entrega? Qual a porcentagem dos pedidos atrasados em relação ao total?"

### 📊 KPIs de Negócio
* **SLA (Service Level Agreement):** % de pedidos entregues no prazo.
* **Average Delay:** Média de dias de atraso por categoria/vendedor.
* **Top Offenders:** Ranking de vendedores com maior volume de atrasos.

## 🛠️ Environment Setup (Passo a Passo)

### 1. Fonte de Dados
* **Dataset:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Versão 2, 2021/10/01)

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
* **requirements.txt:** Adição do provider `apache-airflow-providers-postgres`.
* **.env:** Configuração da conexão `AIRFLOW_CONN_POSTGRES_DEFAULT`.

### 4. Directed Acyclic Graphs (DAGS)

* **olist_bronze_dag.py**: Ingestão bruta dos dados do Kaggle para o Postgres via PostgresOperator, garantindo a persistência do histórico sem transformações (Raw Data). Consulte o código para mais detalhes.

* **olist_silver_dag.py**: Limpeza, tipagem e deduplicação de dados. Aplicação de regras de negócio iniciais, como cálculo de atraso de entrega e padronização de localização de vendedores. Consulte o código para mais detalhes.
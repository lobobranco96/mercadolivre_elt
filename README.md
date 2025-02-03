Overview
========
![Image](https://github.com/user-attachments/assets/c1fc1ea6-8431-47eb-8786-a40b8a6381b7)


# Pipeline de EL e ELT 

**Descrição do Projeto**

1️⃣ API Flask e Interface Interativa
  - Foi desenvolvida uma API REST com Flask para gerenciar a entrada de produtos como o backend.
  - A interface interativa em HTML, CSS e JavaScript permite ao usuário adicionar produtos via método POST e consulta GET.
  - O Apache Airflow monitora a API e inicia a coleta dos produtos adicionados.

2️⃣ DAGs do Apache Airflow
Foram criadas duas DAGs para processar os dados:

DAG 1 - Coleta de Dados (Web Scraping)
  - Através de um sensor, iniciada automaticamente quando um produto é cadastrado na api.
  - Utiliza Python para realizar Web Scraping no Mercado Livre.
  - Os dados extraídos são armazenados em um bucket no Google Cloud Storage (GCS) no formato CSV.

DAG 2 - Pipeline de ELT
  - Lê os dados do GCS e carrega para o BigQuery usando Astro Python SDK.
  - Aplica transformações nos dados com DBT, incluindo:
      - Limpeza dos dados
      - Conversão de tipos de dados
      - Normalização de colunas

🛠 **Tecnologias Utilizadas**
  - Python (Flask, Web Scraping, Airflow)
  - HTML, CSS, JavaScript (Interface Interativa)
  - Google Cloud Storage (Armazenamento de Dados)
  - BigQuery (Data Warehouse)
  - DBT (Transformação de Dados)
  - Astro Python SDK (Integração Airflow + BigQuery)
  - Docker (Conteinerização do Ambiente)
  - Apache Airflow (Orquestração da Pipeline)

**Fluxo da Pipeline**
  - Usuário adiciona um produto via interface interativa.
  - API Flask armazena a entrada e Airflow detecta a alteração.
  - DAG de Web Scraping coleta os dados do Mercado Livre e armazena no GCS.
  - DAG de ELT carrega os dados para o BigQuery.
  - DBT transforma os dados para uso analítico.

## ▶ Como Executar o Projeto

Para rodar o projeto localmente, siga os passos abaixo:

1. **Clone o repositório** para sua máquina:
   ```sh
   git clone https://github.com/lobobranco96/mercadolivre_elt.git
   cd mercadolivre_elt

2. Inicie o ambiente de desenvolvimento utilizando o Astro CLI:
   ```sh
   astro dev start

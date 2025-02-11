import pendulum
from pendulum import datetime
from pathlib import Path
import logging
from python.mercado_livre import coletar_dados_produtos
import requests
import os

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.bash import BashOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from google.cloud import storage


DBT_PATH = "/usr/local/airflow/dags/dbt/"
DBT_PROFILE = "dbt_project"
DBT_TARGETS = "dev"
os.environ["DBT_PROFILES_DIR"] = "/usr/local/airflow/dags/dbt"

HOJE = pendulum.now().format('YYYY-MM-DD')
GCP_CONN = "gcp_default"
BUCKET_NAME = "mercado-livre-datalake"

profile_config = ProfileConfig(
    profile_name=DBT_PROFILE,
    target_name=DBT_TARGETS,
    profiles_yml_filepath=Path(f'{DBT_PATH}/profiles.yml')
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PATH,
    models_relative_path="models"
)

default_args = {
    "owner": "github/lobobranco96",
    "retries": 1,
    "retry_delay": 0
}

# Configuração do logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # Ajuste o schedule conforme necessário
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["build a data source"],
)
def pipeline():

    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    endpoint_api = f"/api/produtos/date/{HOJE}"
    # Configuração do HttpSensor para verificar se há novos produtos
    sensor = HttpSensor(
        task_id="sensor_produtos_novos",
        http_conn_id="http_default", 
        endpoint=endpoint_api,
        poke_interval=600,
        timeout=600,
        mode="poke",
        retries=5,
    )

    def buscar_produtos_api():
        url = f'http://host.docker.internal:5000/api/produtos/date/{HOJE}'
        response = requests.get(url)
        if response.status_code == 200 and len(response.json()) > 0:
            return response.json()  # Retorna a lista de produtos
        else:
            logger.info("Nenhum produto encontrado ou erro na requisição.")
            return []

    # Função que processa cada produto
    def coleta_produto(produto):
        gcs_path = "raw"
        try:
            logger.info('Iniciando a coleta de produtos...')
            nome_produto = produto['produto'].replace(" ", "-")
            url_produto = f"https://lista.mercadolivre.com.br/{nome_produto}"
            data_insercao = produto['data_insercao']  # Ex. 2025-01-25
            gcs_full_path = f"{gcs_path}/{data_insercao}/{nome_produto}.csv"

            # Coleta os dados e armazena no GCS
            coletar_dados_produtos(url_produto, BUCKET_NAME, gcs_full_path)
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao coletar dados: {e}")

    # Função para iniciar a coleta de dados dos produtos
    @task(task_id="iniciar_mercado_livre")
    def iniciar_mercado_livre():
        produtos = buscar_produtos_api()
        if produtos:  # Verifica se há produtos antes de coletar
            for produto in produtos:
                coleta_produto(produto)
        else:
            logger.info("Nenhum produto encontrado para coleta.")

    ml_extract = iniciar_mercado_livre()

    gcs_data_path = f"gs://{BUCKET_NAME}/raw/{HOJE}/tenis-masculino.csv"
    gcs_to_bigquery_task = aql.load_file(
            task_id="product_data",
            input_file=File(path=gcs_data_path,conn_id=GCP_CONN),
            output_table=Table(
        name="produtos",
        conn_id=GCP_CONN,
        metadata={"schema": "mercadolivre", "database": "projeto-lobobranco"}),
            use_native_support=True,
            columns_names_capitalization="original"
        )
  
    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config,
        default_args={"retries": 2},
    )

    # Definindo o fluxo de execução
    init >> sensor >> ml_extract >> gcs_to_bigquery_task >> dbt_running_models >> finish

data_source = pipeline()
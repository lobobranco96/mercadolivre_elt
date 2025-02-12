import pendulum
from pendulum import datetime
from pathlib import Path
import logging
from python.mercado_livre import coletar_dados_produtos
import requests
import os
import time

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

DBT_PATH = "/usr/local/airflow/dags/dbt/"
DBT_PROFILE = "dbt_project"
DBT_TARGETS = "dev"
os.environ["DBT_PROFILES_DIR"] = DBT_PATH

HOJE = pendulum.now().format('YYYY-MM-DD')
GCP_CONN = "gcp_default"
BUCKET_NAME = "mercado-livre-datalake"
PREFIX = f"raw/{HOJE}"

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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["build a data source"],
)
def pipeline():
    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    endpoint_api = f"/api/produtos/date/{HOJE}"
    sensor = HttpSensor(
        task_id="sensor_produtos_novos",
        http_conn_id="http_default", 
        endpoint=endpoint_api,
        poke_interval=60,
        timeout=600,
        mode="poke",
        retries=5,
    )

    def buscar_produtos_api():
        url = f'http://host.docker.internal:5000/api/produtos/date/{HOJE}'
        response = requests.get(url)
        if response.status_code == 200 and response.json():
            return response.json()
        else:
            logger.info("Nenhum produto encontrado ou erro na requisição.")
            return []

    def coleta_produto(produto):
        gcs_path = "raw"
        try:
            logger.info('Iniciando a coleta de produtos...')
            nome_produto = produto['produto'].replace(" ", "-")
            url_produto = f"https://lista.mercadolivre.com.br/{nome_produto}"
            data_insercao = produto['data_insercao']
            gcs_full_path = f"{gcs_path}/{data_insercao}/{nome_produto}.csv"
            coletar_dados_produtos(url_produto, BUCKET_NAME, gcs_full_path)
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao coletar dados: {e}")

    @task(task_id="iniciar_extracao_mercado_livre")
    def iniciar_mercado_livre():
        produtos = buscar_produtos_api()
        if produtos:
            for produto in produtos:
                coleta_produto(produto)
                time.sleep(1500) # 25minutos para iniciar a coleta do proximo produto
        else:
            logger.info("Nenhum produto encontrado para coleta.")

    ml_extract = iniciar_mercado_livre()

    @task
    def carregar_arquivo(arquivo: str):
        """Carrega um arquivo específico para o BigQuery."""
        gcs_data_path = f"gs://{BUCKET_NAME}/{arquivo}"

        logger.info(f"Arquivo {arquivo} processado!")

        load_file =  aql.load_file(
            task_id=f"load_{arquivo.replace('/', '_')}",
            input_file=File(path=gcs_data_path, conn_id=GCP_CONN),
            output_table=Table(
                name="produtos",
                conn_id=GCP_CONN,
                metadata={"schema": "mercadolivre", "database": "projeto-lobobranco"}
            ),
            use_native_support=False,
            columns_names_capitalization="original"
        )
        load_file

    @task
    def processar_arquivos(**kwargs) -> list:
        """Recupera os arquivos listados e retorna a lista."""
        ti = kwargs['ti']
        arquivos = ti.xcom_pull(task_ids="listar_arquivos")

        if not arquivos:
            logger.info("Nenhum arquivo encontrado para processamento.")
            return []

        return arquivos  # ✅ Retorna a lista de arquivos normalmente

    arquivos = processar_arquivos()

    carregar_tasks = carregar_arquivo.expand(arquivo=arquivos)

    listar_arquivos = GCSListObjectsOperator(
        task_id="listar_arquivos",
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        gcp_conn_id=GCP_CONN,
        do_xcom_push=True
    )

    #gcs_to_bigquery = PythonOperator(
     #   task_id="gcs_to_bigquery",
      #  python_callable=processar_arquivos
    #)

    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config
    )

    init >> sensor >> ml_extract >> listar_arquivos >> arquivos >> carregar_tasks >> dbt_running_models >> finish

data_source = pipeline()
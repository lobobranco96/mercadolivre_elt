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

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

# Diretórios e configurações para o DBT
DBT_PATH = "/usr/local/airflow/dags/dbt/"
DBT_PROFILE = "dbt_project"
DBT_TARGETS = "dev"
os.environ["DBT_PROFILES_DIR"] = DBT_PATH

HOJE = pendulum.now().format('YYYY-MM-DD') # Data de hoje para organizar os arquivos

#Google Cloud variaveis
GCP_CONN = "gcp_default"
BUCKET_NAME = "mercado-livre-datalake"
PREFIX = f"raw/{HOJE}"

# Configuração do DBT
profile_config = ProfileConfig(
    profile_name=DBT_PROFILE,
    target_name=DBT_TARGETS,
    profiles_yml_filepath=Path(f'{DBT_PATH}/profiles.yml')
)

# Configuração do DBT
project_config = ProjectConfig(
    dbt_project_path=DBT_PATH,
    models_relative_path="models"
)

# Argumentos padrão do Airflow
default_args = {
    "owner": "github/lobobranco96",
    "retries": 1,
    "retry_delay": 0
}

# Configuração do logger para monitoramento de logs
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
    """
      Pipeline de EL e ELT.
      
      Este DAG tem como objetivo realizar a coleta de dados dos produtos do Mercado Livre, 
      carregar esses dados no Google Cloud Storage (GCS) e, posteriormente, processá-los no 
      BigQuery. Após a coleta e processamento dos dados, a pipeline executa modelos DBT para 
      transformar os dados conforme a necessidade.

      0. Inserção dos produtos desejados em uma interface interativa

      O DAG possui as seguintes etapas principais:
      1. Sensor HTTP para verificar novos produtos na API.
      2. Coleta de dados de produtos via API.
      3. Armazenamento de dados coletados no GCS.
      4. Carregamento dos arquivos no BigQuery.
      5. Execução dos modelos DBT para transformação dos dados.
      """
    # Inicialização e finalização do fluxo
    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    # Sensor HTTP que monitora a API para novos produtos
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
        """
        Realiza uma requisição GET para a API de produtos, buscando dados do dia atual.

        Retorna uma lista de produtos, caso a resposta da API seja bem-sucedida.
        Caso contrário, retorna uma lista vazia.
        """
        url = f'http://host.docker.internal:5000/api/produtos/date/{HOJE}'
        response = requests.get(url)
        if response.status_code == 200 and response.json():
            return response.json()
        else:
            logger.info("Nenhum produto encontrado ou erro na requisição.")
            return []

    def coleta_produto(produto):
        """
        Coleta dados de um produto específico do Mercado Livre e armazena no GCS.

        Args:
            produto (dict): Dicionário com os dados do produto para coleta.
        """
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
        """
        Inicia o processo de coleta de dados dos produtos a partir da API e armazena
        os dados no Google Cloud Storage.
        """
        produtos = buscar_produtos_api()
        if produtos:
            for produto in produtos:
                coleta_produto(produto)
                time.sleep(1500) # 25minutos para iniciar a coleta do proximo produto para evitar erro de requisição
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
        """
        Recupera os arquivos listados do GCS e retorna uma lista com os arquivos.

        Args:
            **kwargs: Argumentos adicionais do Airflow.

        Returns:
            list: Lista de arquivos a serem processados.
        """
        ti = kwargs['ti']
        arquivos = ti.xcom_pull(task_ids="listar_arquivos")

        if not arquivos:
            logger.info("Nenhum arquivo encontrado para processamento.")
            return []

        return arquivos  # Retorna a lista de arquivos normalmente

    arquivos = processar_arquivos()

    # Carregar todos os arquivos no BigQuery
    carregar_tasks = carregar_arquivo.expand(arquivo=arquivos)

    listar_arquivos = GCSListObjectsOperator(
        task_id="listar_arquivos",
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        gcp_conn_id=GCP_CONN,
        do_xcom_push=True
    )

    # Executar modelos DBT após o carregamento
    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config
    )
    
    # Fluxo do DAG
    init >> sensor >> ml_extract >> listar_arquivos >> arquivos >> carregar_tasks >> dbt_running_models >> finish

data_source = pipeline()
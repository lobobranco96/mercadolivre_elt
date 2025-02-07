from datetime import datetime
import logging
from python.mercado_livre import coletar_dados_produtos
import requests

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor

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
    default_args={"owner": "lobobranco", "retries": 3},
    tags=["build a data source"],
)
def data_source():

    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")
    today = datetime.now().strftime('%Y-%m-%d')

    # Função para buscar dados da API
    def buscar_produtos_api(today):
        url = f'http://localhost:5000/api/produtos'#date/{today}'
        response = requests.get(url)
        if response.status_code == 200 and len(response.json()) > 0:
            return response.json()  # Retorna a lista de produtos
        else:
            logger.info("Nenhum produto encontrado ou erro na requisição.")
            return []

    # Função que processa cada produto
    #@task(task_id="coletando_produtos")
    def coleta_produto(produto):
        bucket_name = "mercado-livre-datalake"
        gcs_path = "stading"
        try:
            logger.info('Iniciando a coleta de produtos...')
            nome_produto = produto['produto'].replace(" ", "-")
            url_produto = f"https://lista.mercadolivre.com.br/{nome_produto}"
            data_insercao = produto['data_insercao']  # Ex. 2025-01-25
            gcs_full_path = f"{gcs_path}/{data_insercao}/{nome_produto}.csv"

            # Coleta os dados e armazena no GCS
            coletar_dados_produtos(url_produto, bucket_name, gcs_full_path)
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao coletar dados: {e}")

    # Função para iniciar a coleta de dados dos produtos
    @task(task_id="iniciar_mercado_livre")
    def iniciar_mercado_livre():
        produtos = buscar_produtos_api(today)
        if produtos:  # Verifica se há produtos antes de coletar
            for produto in produtos:
                coleta_produto(produto)
        else:
            logger.info("Nenhum produto encontrado para coleta.")

    endpoint_api = f"api/produtos/date/{today}"
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

    # Definindo o fluxo de execução
    init >> sensor >> iniciar_mercado_livre() >> finish

data_source = data_source()
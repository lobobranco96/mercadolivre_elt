import pendulum
from pendulum import datetime
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
HOJE = pendulum.now().format('YYYY-MM-DD')

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

    def buscar_produtos_api():
            url = f'http://host.docker.internal:5000/api/produtos/date/{HOJE}'
            response = requests.get(url)
            if response.status_code == 200 and len(response.json()) > 0:
                return response.json()  # Retorna a lista de produtos
            else:
                logger.info("Nenhum produto encontrado ou erro na requisição.")
                return []

    # Função para iniciar a coleta de dados dos produtos
    @task(task_id="iniciar_mercado_livre")
    def iniciar_mercado_livre():
        bucket_name = "mercado-livre-datalake"
        gcs_path = "stading"
        #url = f'http://host.docker.internal:5000/api/produtos/date/{HOJE}'
        #response = requests.get(url)
        produtos = buscar_produtos_api()
        for produto in produtos:
            try:
                #if response.status_code == 200 and len(response.json()) > 0:
                    #produto =  response.json()
                    logger.info('Iniciando a coleta de produtos...')
                    nome_produto = produto['produto'].replace(" ", "-")
                    url_produto = f"https://lista.mercadolivre.com.br/{nome_produto}"
                    data_insercao = produto['data_insercao']  # Ex. 2025-01-25
                    gcs_full_path = f"{gcs_path}/{data_insercao}/{nome_produto}.csv"
                    coletar_dados_produtos(url_produto, bucket_name, gcs_full_path)
            except requests.exceptions.RequestException as e:
                logger.error(f"Erro ao coletar dados: {e}")

    #endpoint_api = f"api/produtos/date/{HOJE}"
    # Configuração do HttpSensor para verificar se há novos produtos
    #sensor = HttpSensor(
     #   task_id="sensor_produtos_novos",
      #  http_conn_id="http_default", 
       # endpoint=endpoint_api,
        #poke_interval=600,
        #timeout=600,
        #mode="poke",
        #retries=5,
    #)
    scrap_init = iniciar_mercado_livre()
    # Definindo o fluxo de execução
    init >> scrap_init >> finish

data_source = data_source()
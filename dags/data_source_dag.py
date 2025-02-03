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
    hoje = datetime.now().strftime('%Y-%m-%d')

    # coletar os produtos novos
    @task(task_id="coletar_novos_produtos")
    def novos_produtos():
        url = f'http://172.19.0.2:5000/api/produtos/date/{hoje}'  # URL da API Flask
        BUCKET_NAME = "mercado-livre-datalake"
        GCS_PATH = "raw"

        try:
            logger.info("Coletando produtos...")
            response = requests.get(url)
            if response.status_code == 200:
                produtos_coletados = response.json()

                # Verifica se existem produtos na API
                if produtos_coletados:
                    logger.info(f"{len(produtos_coletados)} novos produtos encontrados.")
                    for produto in produtos_coletados:
                        produto_nome = produto['produto'].replace(" ", "-")

                        url_produto = f"https://lista.mercadolivre.com.br/{produto_nome}"
                        data_insercao = produto['data_insercao']
                        coletar_dados_produtos(url_produto, BUCKET_NAME, GCS_PATH, data_insercao)
                else:
                    logger.info("Nenhum produto novo encontrado.")
            else:
                logger.error(f"Erro ao coletar dados: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro ao coletar dados: {e}")

    # Configuração do HttpSensor para verificar se há novos produtos
    sensor = HttpSensor(
        task_id="sensor_produtos_novos",
        http_conn_id="http_default",
        endpoint=f"api/produtos/date/{hoje}",  
        poke_interval=600,
        timeout=600,
        mode="poke",
        retries=5,
    )

    # Definição do fluxo de execução das tasks
    init >> sensor >> novos_produtos() >> finish

# Instantiate the DAG
mercado_livre = data_source()

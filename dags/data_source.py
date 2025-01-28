from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from pendulum import datetime
import requests
from python.ml_data_source import coletar_dados_ml

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # Ajuste o schedule conforme necessário
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "lobobranco", "retries": 3},
    tags=["build a data source"],
)

BUCKET_NAME = "lobobranco-datalake"
GCS_PATH = "raw"

def data_source():

    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    # Função para coletar os dados de web scraping dos produtos
    @task
    def extract_mercado_livre_product(produtos_coletados):

        for produto in produtos_coletados:
            produto_nome = produto['produto'].replace(" ", "-")

            url = f"https://lista.mercadolivre.com.br/{produto_nome}"
            data_insercao = produto['data_insercao']
            coletar_dados_ml(url, BUCKET_NAME, GCS_PATH, data_insercao)

    # Função para verificar se há produtos novos
    @task
    def check_for_new_products():
        url = 'http://localhost:5000/api/produtos/'  # URL da API Flask

        try:
            response = requests.get(url)
            if response.status_code == 200:
                produtos_coletados = response.json()

                # Verifica se existem produtos na API
                if produtos_coletados:
                    extract_mercado_livre_product(produtos_coletados)
                else:
                    print("Nenhum produto novo encontrado.")
            else:
                print(f"Erro ao coletar dados: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Erro ao coletar dados: {e}")

    # Configuração do HttpSensor para verificar se há novos produtos
    sensor = HttpSensor(
        task_id="sensor_produtos_novos",
        http_conn_id="http_default",  # Esse ID deve ser configurado no Airflow Connections
        endpoint="api/produtos/",
        poke_interval=600,  # Intervalo em segundos (600s = 10 minutos)
        timeout=600,  # Tempo máximo para aguardar o produto
        mode="poke",  # Usando o modo "poke", que verifica a condição a cada intervalo
        retries=5,  # Quantas vezes o sensor irá tentar
    )

    # Definição do fluxo de execução das tasks
    init >> sensor >> check_for_new_products() >> finish

# Instantiate the DAG
data_source()

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
import requests
from python.ml_data_source import coletar_dados_ml

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "lobobranco", "retries": 3},
    tags=["build a data source"],
)
def data_source():

    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")
    # Define tasks
    @task
    def extract_mercado_livre_product():
        url = 'http://localhost:5000/api/produtos/'

# Enviando o GET para o servidor
        try:
            response = requests.get(url)
            if response.status_code == 200:
                produtos_coletados = response.json()
                lista_produtos = [produto['produto'] for produto in produtos_coletados]  # Converte a resposta JSON em um dicionário
                for produ in lista_produtos:
                    produto = produ.replace(" ", "-")
                    url = f"https://lista.mercadolivre.com.br/{produto}"
                    coletar_dados_ml(url)
                    #print("Produtos coletados:", produtos)
            else:
                print(f"Erro ao coletar dados: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Erro ao coletar dados: {e}")

        #produtos = ["https://lista.mercadolivre.com.br/tenis-corrida-masculino", "https://lista.mercadolivre.com.br/tenis-corrida-feminino"]

    init >> extract_mercado_livre_product >> finish

# Instantiate the DAG
data_source()

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

from python.ml_data_source import coletar_dados_produtos

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
        produtos = ["https://lista.mercadolivre.com.br/tenis-corrida-masculino", "https://lista.mercadolivre.com.br/tenis-corrida-feminino"]
        for produto in produtos:
            coletar_dados_produtos(produto)

    init >> extract_mercado_livre_product >> finish

# Instantiate the DAG
data_source()

from airflow.decorators import dag, task
import pendulum
from pendulum import datetime
from pathlib import Path
from google.cloud import storage

from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Configurações
GCP_CONN_ID = "gcp_default"
BUCKET_NAME = "mercado-livre-datalake"

PREFIX = "raw/2025-02-12"  # Exemplo: "dados/raw/"

# Função para processar a lista de arquivos
def processar_arquivos(**kwargs):
    ti = kwargs['ti']
    arquivos = ti.xcom_pull(task_ids="listar_arquivos")  # Recupera a lista da task anterior
    if arquivos:
        for arquivo in arquivos:
            print(f"Processando arquivo: {arquivo}")  # Aqui você pode substituir pelo processamento desejado

default_args = {
    "owner": "github/lobobranco96",
    "retries": 1,
    "retry_delay": 0
}
# Definição da DAG
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # Ajuste o schedule conforme necessário
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["build a data source"],
)
def teste():
    # Task para listar os arquivos dentro do bucket/pasta
    listar_arquivos = GCSListObjectsOperator(
        task_id="listar_arquivos",
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        gcp_conn_id=GCP_CONN_ID
    )

    # Task para processar os arquivos retornados
    processar_lista = PythonOperator(
        task_id="processar_lista",
        python_callable=processar_arquivos,
        provide_context=True
    )

    # Definir a ordem das tasks
    listar_arquivos >> processar_lista

teste()
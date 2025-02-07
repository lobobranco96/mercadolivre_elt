from pathlib import Path
import os
import pendulum
from pendulum import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
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

GCP_CONN = "gcp_default"

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

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
    default_args=default_args,
    tags=["duckdb", "aws", "dbt", "snowflake"],
)
def elt_datapipeline():
    """
    Função principal que coordena o processo de extração e carregamento de dados.
    - 
    - 
    """
    init = EmptyOperator(task_id="inicio")
    finish = EmptyOperator(task_id="fim_pipeline")

    @task
    def data_ingestion():
      
      # key_path = "/usr/local/airflow/dags/credentials/google_credential.json"
      # client = storage.Client.from_service_account_json(key_path)

      # bucket_name = "mercado-livre-datalake"
      # bucket = client.get_bucket(bucket_name)
      # blobs = bucket.list_blobs()

      # gs_paths = []
      # # Itera sobre os arquivos no bucket e constrói o gs_path
      # for blob in blobs:
      #     gs_path = f"gs://{bucket_name}/{blob.name}"
      #     gs_paths.append(gs_path)

      # for caminho_arquivo in gs_paths[2:]:
      bucket_name = "mercado-livre-datalake"
      hoje = pendulum.now().format('YYYY-MM-DD')

      gcs_data_path = f"gs://{bucket_name}/{hoje}/*.csv"
      gcs_to_bigquery_task = aql.load_file(
              task_id="product_data",
              input_file=File(path=gcs_data_path, conn_id=GCP_CONN),
              output_table=Table(name="produtos", conn_id=GCP_CONN),
              use_native_support=True,
              columns_names_capitalization="original"
          )
  
    dbt_running_models = DbtTaskGroup(
        group_id="dbt_running_models",
        project_config=project_config,
        profile_config=profile_config,
        default_args={"retries": 2},
    )

  # Tarefa para rodar o Soda após o DBT
    run_soda_checks = BashOperator(
        task_id="run_soda_checks",
        bash_command="soda run --checks /usr/local/airflow/dags/soda/soda-checks.yml",
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    data_ing = data_ingestion()

    init >> data_ing >> dbt_running_models >> run_soda_checks >> finish

elt_datapipeline()

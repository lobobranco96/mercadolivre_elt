FROM quay.io/astronomer/astro-runtime:12.6.0

ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled

# install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate
    
ENV DBT_PROFILES_DIR=/usr/local/airflow/dags/dbt
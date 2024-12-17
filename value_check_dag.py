from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'bigquery_value_check_example',
    default_args=default_args,
    description='Example DAG to fail on unmet BigQuery value check condition',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Check specific value in BigQuery
    check_value = BigQueryValueCheckOperator(
        task_id='check_value',
        sql="""
            SELECT COUNT(*) FROM `sample-project-123.sample_dataset.emp`
            WHERE department_id = 103
        """,
        pass_value=5,  # expected value or threshold
        tolerance=0.1,  # optional, if you allow some margin
        use_legacy_sql=False,
        location='US'
    )

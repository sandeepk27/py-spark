from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'bigquery_operator_example1',
    default_args=default_args,
    description='An example DAG demonstrating BigQueryOperator usage',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 2: Insert Data into BigQuery Table
    insert_data = BigQueryInsertJobOperator(
        task_id='insert_bq_data',
        configuration={
            "query": {
                "query": """
                    INSERT INTO `sample-project-123.sample_dataset.emp` 
                    SELECT * except(hire_date) FROM `sample-project-123.sample_dataset.emp_details` where age>30
                """,
                "useLegacySql": False,
            }
        },
    )
    # Task 3: Query Data from BigQuery Table
    query_data = BigQueryInsertJobOperator(
        task_id='query_bq_data',
        configuration={
            "query": {
                "query": """
                    SELECT * FROM `sample-project-123.sample_dataset.emp`
                """,
                "useLegacySql": False,
            }
        },
    )

    # Set task dependencies
    insert_data >> query_data

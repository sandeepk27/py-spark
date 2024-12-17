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
    'bigquery_operator_example',
    default_args=default_args,
    description='An example DAG demonstrating BigQueryOperator usage',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Create a BigQuery Table
    create_table = BigQueryCreateEmptyTableOperator(
        task_id='create_bq_table',
        project_id='sample-project-123',
        dataset_id='sample_dataset',
        table_id='emp',
        schema_fields=[
  {
    "name": "employee_id",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "department_id",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "name",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "age",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  },
  {
    "name": "gender",
    "mode": "NULLABLE",
    "type": "STRING",
    "description": "",
    "fields": []
  },
  {
    "name": "salary",
    "mode": "NULLABLE",
    "type": "INTEGER",
    "description": "",
    "fields": []
  }
],
    )

    # Task 3: Query Data from BigQuery Table
    query_data = BigQueryInsertJobOperator(
        task_id='query_bq_data',
        configuration={
            "query": {
                "query": """
                    SELECT * except(hire_date) FROM `sample-project-123.sample_dataset.emp_details` where age>30
                """,
                "useLegacySql": False,
            }
        },
    )

    # Set task dependencies
    create_table >> query_data

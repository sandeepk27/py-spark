from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from datetime import datetime
import os,re,logging

# Configuration
BUCKET_NAME = "dataproc_test05"
FOLDER_PATH = "csv_data/"
DATASET_NAME = "csv_files_data"
AUDIT_TABLE_NAME = "audit_table"


# Default args for DAG
default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}


# Function to capture audit details and push via XCom
def capture_audit_details(file_name, task_instance, **context):
    start_time_str = context['execution_date'].isoformat()
    task_instance.xcom_push(key=f'{file_name}_start_time', value=start_time_str)
    task_instance.xcom_push(key=f'{file_name}_file_name', value=file_name)
    task_instance.xcom_push(key=f'{file_name}_end_time', value=datetime.now().isoformat())


# Function to dynamically create tasks for each file and store in a unique table
def create_file_tasks(file_name, dag):
    # Sanitize file_name to make it a valid task_id
    sanitized_file_name = re.sub(r'[^0-9a-zA-Z._-]', '_', file_name)

    table_name = (os.path.splitext(file_name)[0]).split("/")[1]  # Use file name (without extension) as table name

    # Capture start time and file name for audit
    start_audit = PythonOperator(
        task_id=f'start_audit_capture_{sanitized_file_name}',
        python_callable=capture_audit_details,
        op_kwargs={'file_name': file_name},
        provide_context=True,
        dag=dag,
    )
    logging.info(f"file_name is: {file_name}")
    # Task: Load data from GCS to a separate BigQuery table per file
    load_to_bigquery = GCSToBigQueryOperator(
        task_id=f'load_to_bigquery_{sanitized_file_name}',
        bucket=BUCKET_NAME,
        source_objects=[f"{file_name}"],
        destination_project_dataset_table=f"{DATASET_NAME}.{table_name}",
        source_format='CSV',
        write_disposition='WRITE_EMPTY',
        skip_leading_rows=1,
        field_delimiter=',',
        max_bad_records=10,
        dag=dag,
    )
    logging.info(f"table_name is created: {table_name}")
    # Insert audit data into BigQuery audit table
    insert_audit_data = BigQueryExecuteQueryOperator(
        task_id=f'insert_audit_data_{sanitized_file_name}',
        sql="""
            INSERT INTO `{{ params.dataset }}.{{ params.audit_table }}`
            (load_date, start_time, end_time, row_count, status, error_message, file_name)
            VALUES (
                CURRENT_DATE(),
                '{{ task_instance.xcom_pull(task_ids="start_audit_capture_{{ params.file_name }}", key="{{ params.file_name }}_start_time") }}',
                '{{ task_instance.xcom_pull(task_ids="start_audit_capture_{{ params.file_name }}", key="{{ params.file_name }}_end_time") }}',
                (SELECT COUNT(*) FROM `{{ params.dataset }}.{{ params.table }}`),
                'Success',
                NULL,
                '{{ params.file_name }}'
            )
        """,
        use_legacy_sql=False,
        params={
            'dataset': DATASET_NAME,
            'table': table_name,
            'audit_table': AUDIT_TABLE_NAME,
            'file_name': sanitized_file_name,
        },
        dag=dag,
    )

    # Define task dependencies within each file's group
    start_audit >> load_to_bigquery >> insert_audit_data


# Define DAG
with DAG(
        dag_id='gcs_to_bigquery_separate_tables_with_audit',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    # List all CSV files in GCS bucket folder
    list_csv_files = GCSListObjectsOperator(
        task_id='list_csv_files',
        bucket=BUCKET_NAME,
        prefix=FOLDER_PATH,
    )


    # Process each CSV file
    def process_csv_files(**kwargs):
        csv_files = kwargs['ti'].xcom_pull(task_ids='list_csv_files')
        if csv_files:
            for file_name in csv_files:
                if file_name.endswith('.csv'):
                    create_file_tasks(file_name, dag)


    # Dynamically create tasks for each file
    process_files = PythonOperator(
        task_id='process_files',
        python_callable=process_csv_files,
        provide_context=True,
    )

    # Task Dependencies
    list_csv_files >> process_files

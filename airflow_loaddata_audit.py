from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from datetime import datetime

# Configuration
BUCKET_NAME = "dataproc_test05"
SOURCE_OBJECT = "csv_files/employees.csv"  # File name to include in the audit log
DATASET_NAME = "sample_dataset"
TABLE_NAME = "csv_data"
AUDIT_TABLE_NAME = "audit_table"

# Default args for DAG
default_args = {
    'start_date': days_ago(1),
    'retries': 1,
}

# Function to log audit details
def log_audit_details(task_instance, **context):
    client = bigquery.Client()
    
    # Extract details from task instance
    start_time = context['data_interval_start']
    end_time = datetime.now()
    row_count = task_instance.xcom_pull(task_ids='check_row_count')
    status = "Success" if task_instance.xcom_pull(task_ids='load_to_bigquery') else "Failed"
    error_message = task_instance.xcom_pull(task_ids='load_to_bigquery', key='error') if status == "Failed" else None
    
    # Insert audit log with file name
    audit_query = f"""
        INSERT `{DATASET_NAME}.{AUDIT_TABLE_NAME}` (load_date, start_time, end_time, row_count, status, error_message, file_name)
        VALUES ('{start_time}', '{start_time}', '{end_time}', {row_count}, '{status}', '{error_message}', '{SOURCE_OBJECT}')
    """
    client.query(audit_query)

# Define DAG
with DAG(
    dag_id='gcs_to_bigquery_with_audit',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Task: Load data from GCS to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=[SOURCE_OBJECT],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        max_bad_records=10,
    )

    # Task: Check row count in BigQuery table
    check_row_count = BigQueryCheckOperator(
        task_id='check_row_count',
        sql=f"SELECT COUNT(*) FROM `{DATASET_NAME}.{TABLE_NAME}`",
        use_legacy_sql=False,
    )

    # Task: Log audit details
    audit_log = PythonOperator(
        task_id='log_audit_details',
        python_callable=log_audit_details,
        provide_context=True,
    )

    # Task dependencies
    load_to_bigquery >> check_row_count >> audit_log

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator,BigQueryExecuteQueryOperator
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

# Function to capture audit details and push via XCom
def capture_audit_details(task_instance, **context):
    start_time_str = context['execution_date'].isoformat()
    task_instance.xcom_push(key='start_time', value=start_time_str)
    task_instance.xcom_push(key='file_name', value=SOURCE_OBJECT)
    task_instance.xcom_push(key='end_time', value=datetime.now())

# Define DAG
with DAG(
    dag_id='gcs_to_bigquery_with_audit2',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Capture start time and file name for audit
    start_audit = PythonOperator(
        task_id='start_audit_capture',
        python_callable=capture_audit_details,
        provide_context=True,
    )
    
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
    
    # Insert audit data into BigQuery audit table
    insert_audit_data = BigQueryExecuteQueryOperator(
        task_id='insert_audit_data',
        sql="""
            INSERT INTO `{{ params.dataset }}.{{ params.audit_table }}`
            (load_date, start_time, end_time, row_count, status, error_message, file_name)
            VALUES (
                CURRENT_DATE(),
                '{{ task_instance.xcom_pull(task_ids="start_audit_capture", key="start_time") }}',
                '{{ execution_date }}',
                (SELECT COUNT(*) FROM `{{ params.dataset }}.{{ params.table }}`),
                'Success',
                NULL,
                '{{ task_instance.xcom_pull(task_ids="start_audit_capture", key="file_name") }}'
            )
        """,
        use_legacy_sql=False,
        params={
            'dataset': DATASET_NAME,
            'table': TABLE_NAME,
            'audit_table': AUDIT_TABLE_NAME,
        },
    )

    # Task dependencies
    load_to_bigquery >> check_row_count >> start_audit >> insert_audit_data

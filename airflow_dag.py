from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Define DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

# Define the DAG
with DAG(
    "dataproc_pyspark_job",
    default_args=default_args,
    schedule_interval="@once",  # Run once; adjust as needed
    catchup=False,
) as dag:

    # Define PySpark job configuration for DataProc
    pyspark_job = {
        "reference": {"project_id": "sample-project-123"},
        "placement": {"cluster_name": "cluster-3"},
        "pyspark_job": {
            "main_python_file_uri": "gs://dataproc_test05/create_table.py",
            # Optional: You can specify arguments to pass to the PySpark script
            #"args": ["arg1", "arg2"],
        },
    }

    # Submit PySpark job to Dataproc
    submit_pyspark = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=pyspark_job,
        region="us-central1",  # e.g., "us-central1"
        project_id="sample-project-123",
    )

    submit_pyspark

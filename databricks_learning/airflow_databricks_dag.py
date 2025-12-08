from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='databricks_gcp_etl_pipeline',
    default_args=default_args,
    description='A DAG to trigger Databricks ETL job connecting to GCP',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['databricks', 'gcp', 'etl'],
    catchup=False,
) as dag:

    # Define the Databricks job configuration
    # This configuration submits a new cluster for the run (ephemeral cluster).
    # You can also use an existing cluster by specifying 'existing_cluster_id'.
    new_cluster = {
        'spark_version': '13.3.x-scala2.12',  # Choose a version compatible with your needs
        'node_type_id': 'n1-standard-4',      # GCP machine type
        'driver_node_type_id': 'n1-standard-4',
        'num_workers': 2,
        'gcp_attributes': {
            'use_preemptible_executors': False,
            'google_service_account': 'your-service-account@your-project.iam.gserviceaccount.com', # Important for GCS/BQ access
            'availability': 'ON_DEMAND_GCP'
        },
        # Ensure BigQuery connector is available
        'spark_env_vars': {
            'PYSPARK_PYTHON': '/databricks/python3/bin/python3'
        }
    }

    # Libraries to install on the cluster
    libraries = [
        {
            "maven": {
                "coordinates": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0"
            }
        }
    ]

    # The task to submit the Spark job
    submit_spark_job = DatabricksSubmitRunOperator(
        task_id='submit_spark_job',
        # Connection ID configured in Airflow Admin > Connections
        databricks_conn_id='databricks_default',
        new_cluster=new_cluster,
        libraries=libraries,
        spark_python_task={
            'python_file': 'gs://your-bucket-name/scripts/etl_pipeline.py', # Path to the script in GCS or DBFS
            'parameters': [] # Optional parameters to pass to the script
        },
    )

    submit_spark_job

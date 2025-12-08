# Learning Databricks on Google Cloud Platform (GCP)

This directory contains resources and code examples to help you learn how to connect Databricks to GCP and build data pipelines.

## Prerequisites

To use these examples, you will need:

1.  **GCP Account**: A Google Cloud Platform account with billing enabled.
2.  **Databricks Account**: A Databricks account. You can sign up for a free trial or subscribe via the GCP Marketplace.
3.  **Google Cloud Storage (GCS) Bucket**: For storing data and scripts.
4.  **BigQuery Dataset**: For storing the results of your analysis.

## Setup Guide

### 1. Set up Databricks on GCP

1.  Go to the [GCP Console](https://console.cloud.google.com/).
2.  Navigate to **Marketplace**.
3.  Search for "Databricks".
4.  Select **Databricks** and click **Subscribe**.
5.  Follow the prompts to configure the workspace (Region, Network, etc.).
6.  Once the workspace is created, you can access the Databricks UI.

### 2. Configure Service Account for Access

Databricks needs permissions to access GCS and BigQuery.

1.  Create a **Service Account** in GCP IAM.
2.  Grant the following roles to the Service Account:
    *   `Storage Object Admin` (for GCS access)
    *   `BigQuery Data Editor` (for BigQuery access)
    *   `BigQuery User` (to run jobs)
3.  In your Databricks Workspace, configure the cluster to use this Service Account.
    *   When creating a cluster, go to **Advanced Options** > **Google Service Account**.
    *   Enter the email of the service account you created.

### 3. Prepare the Data

1.  Create a GCS bucket (e.g., `gs://my-databricks-learning-bucket`).
2.  Upload the `employees.csv` file (found in this directory) to the bucket.
    ```bash
    gsutil cp employees.csv gs://your-bucket-name/employees.csv
    ```
3.  Upload the python script `etl_pipeline.py` to the bucket (or a `scripts/` folder).
    ```bash
    gsutil cp etl_pipeline.py gs://your-bucket-name/scripts/etl_pipeline.py
    ```

### 4. Running the Pipeline

You have two options to run the pipeline: manually via Databricks Notebook/Job or orchestrated via Airflow.

#### Option A: Run manually in Databricks

1.  Log in to your Databricks Workspace.
2.  Create a new **Notebook**.
3.  Paste the code from `etl_pipeline.py` into a cell.
4.  Update the configuration variables (`GCS_BUCKET`, `PROJECT_ID`, etc.) at the top of the script.
5.  Attach the notebook to your cluster.
6.  Ensure the cluster has the **Spark BigQuery Connector** installed.
    *   Cluster > Libraries > Install New > Maven.
    *   Coordinates: `com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0` (Check for the latest version).
7.  Run the notebook.

#### Option B: Orchestrate with Airflow

The `airflow_databricks_dag.py` file contains an Airflow DAG that submits the job to Databricks.

1.  This requires an Airflow environment (e.g., Cloud Composer on GCP).
2.  Configure a **Databricks Connection** in Airflow.
    *   Conn Id: `databricks_default`
    *   Conn Type: `Databricks`
    *   Host: `https://<your-databricks-instance-url>`
    *   Login: `token`
    *   Password: `<your-databricks-access-token>` (Generate this in Databricks User Settings).
3.  Deploy the DAG file to your Airflow DAGs folder.
4.  Update the DAG file to point to your script location in GCS and your specific cluster configuration (Service Account, Machine Type).
5.  Trigger the DAG in the Airflow UI.

## File Descriptions

*   `etl_pipeline.py`: A PySpark script that:
    *   Reads `employees.csv` from GCS.
    *   Calculates the average salary per department.
    *   Writes the result to a BigQuery table.
    *   Writes the result to GCS in Parquet format.

*   `airflow_databricks_dag.py`: An Airflow DAG that demonstrates how to use the `DatabricksSubmitRunOperator` to run the ETL script on a new Databricks cluster.

## Key Concepts Learned

*   **Integration**: How Databricks integrates with GCP services like GCS and BigQuery.
*   **PySpark on Databricks**: Writing Spark code compatible with the platform.
*   **Orchestration**: Using Airflow to manage Databricks jobs.

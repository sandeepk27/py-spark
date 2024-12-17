*PySpark is the Python API for Apache Spark, enabling the use of Python to harness the power of distributed data processing. It’s widely used for big data analytics and ETL tasks. Key points include:*

# Core Concepts:
RDD (Resilient Distributed Dataset): Immutable distributed collection of objects.
DataFrame: A distributed collection of data organized into named columns (optimized for SQL-like operations).
Dataset: Type-safe, object-oriented data structure (not available in PySpark, but in Scala/Java).
Features:

Distributed computation with fault tolerance.
Compatible with SQL and supports structured data processing.
Integration with various storage systems (e.g., HDFS, GCS, S3).

# Key Components:
SparkContext: Entry point for Spark functionality.
SparkSession: Unified entry point for DataFrame and Dataset APIs.
Transformations and Actions:
Transformations: Create a new dataset (e.g., map, filter).
Actions: Trigger computations (e.g., collect, count).

# Libraries:
Spark SQL: For SQL and DataFrame operations.
MLlib: Machine learning.
GraphX: Graph processing.
Spark Streaming: Real-time stream processing.

# Integration with GCP:
Use GCS (Google Cloud Storage) as input/output storage.
BigQuery connector for reading/writing directly to/from BigQuery.
Run on Dataproc, Google Cloud’s managed Spark/Hadoop service.
PySpark Code for GCP Example: Read from GCS and Write to BigQuery

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("GCP Example") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0") \
    .getOrCreate()

# Reading data from GCS
gcs_bucket = "gs://your-bucket-name/input-data.csv"
df = spark.read.option("header", "true").csv(gcs_bucket)

# Data transformations (example)
df_transformed = df.filter(df["column_name"] > 100).select("column_name", "another_column")

# Write data to BigQuery
project_id = "your-gcp-project-id"
bq_table = "your_dataset.your_table"

df_transformed.write \
    .format("bigquery") \
    .option("table", f"{project_id}:{bq_table}") \
    .option("writeMethod", "direct") \
    .save()

print("Data successfully written to BigQuery!")

# Stop the Spark session
spark.stop()

Steps to Run PySpark Code on GCP (Dataproc):
Setup:
# Create a Google Cloud Storage bucket for staging files.
Create a Dataproc cluster.
Ensure Spark and BigQuery connectors are installed.

Submit Job:
Upload your PySpark script to GCS.

Submit the job via the GCP console or gcloud CLI:
gcloud dataproc jobs submit pyspark gs://your-bucket-name/your-script.py \
    --cluster=your-cluster-name \
    --region=your-region

Monitor:
Use the GCP Console or CLI to monitor the job status.
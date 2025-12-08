from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round

def main():
    # Initialize SparkSession
    # In Databricks, SparkSession is often already available as 'spark',
    # but it's good practice to get or create it for portability.
    spark = SparkSession.builder \
        .appName("Databricks GCP ETL") \
        .getOrCreate()

    # Configuration Variables (Replace these with your actual values)
    # You can also pass these as arguments to the script
    GCS_BUCKET = "gs://your-bucket-name"
    PROJECT_ID = "your-gcp-project-id"
    BQ_DATASET = "your_dataset"
    BQ_TABLE = "department_avg_salary"

    # Input path
    input_path = f"{GCS_BUCKET}/employees.csv"

    # Output paths
    output_gcs_path = f"{GCS_BUCKET}/output/department_avg_salary"
    output_bq_table = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    print(f"Reading data from {input_path}...")

    # Read CSV from GCS
    # Using 'header' option since the file has a header
    # 'inferSchema' helps to automatically detect types, or we can define schema manually
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)

    print("Data read successfully. Schema:")
    df.printSchema()

    # Transformation: Clean and Aggregate
    # 1. Cast SALARY to double (if not already inferred)
    # 2. Group by DEPARTMENT_ID
    # 3. Calculate Average Salary

    # Ensuring SALARY is numeric
    df_clean = df.withColumn("SALARY", col("SALARY").cast("double"))

    # Aggregation
    df_agg = df_clean.groupBy("DEPARTMENT_ID") \
        .agg(
            round(avg("SALARY"), 2).alias("AVG_SALARY")
        ) \
        .orderBy("DEPARTMENT_ID")

    print("Transformed Data (First 20 rows):")
    df_agg.show()

    # Write to BigQuery
    # Note: This requires the Spark BigQuery connector to be installed on the cluster.
    # In Databricks, you can add the Maven coordinate: com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.x.x

    print(f"Writing data to BigQuery table: {output_bq_table}...")

    # Saving mode 'overwrite' will replace the table content
    # 'temporaryGcsBucket' is required for BigQuery writes to stage data
    df_agg.write \
        .format("bigquery") \
        .option("table", output_bq_table) \
        .option("temporaryGcsBucket", GCS_BUCKET + "/temp") \
        .mode("overwrite") \
        .save()

    print("Write to BigQuery complete.")

    # Write to GCS as Parquet
    print(f"Writing data to GCS path: {output_gcs_path}...")

    df_agg.write \
        .mode("overwrite") \
        .parquet(output_gcs_path)

    print("Write to GCS complete.")

if __name__ == "__main__":
    main()

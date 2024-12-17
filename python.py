from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.csv("gs://dataproc_test05/csv_files/name.csv", header=True, inferSchema=True)

table_id = "sample-project-123.sample_dataset.name"

# Write DataFrame to BigQuery

df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "dataproc_test05") \
    .option("table", table_id) \
    .mode("overwrite") \
    .save()

from pyspark.sql import SparkSession
import sys

spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)

df = spark.read.csv("gs://dataproc_test05/csv_files/name1.csv", header=True, inferSchema=True)
arg1 = sys.argv[1]
arg1=int(arg1)
print("Argument 1:", arg1)
table_id = "sample-project-123.sample_dataset.emp_details_new"

df = df.where(f"salary>{arg1}")

# Write DataFrame to BigQuery

df.write \
    .format("bigquery") \
    .option("temporaryGcsBucket", "dataproc_test05") \
    .option("table", table_id) \
    .mode("overwrite") \
    .save()

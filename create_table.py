from pyspark.sql import SparkSession


spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)
from pyspark.sql import Row

# Sample data to write to BigQuery
data = [
    Row(id=1, name="Alice", age=30, salary=50000),
    Row(id=2, name="Bob", age=35, salary=60000),
    Row(id=3, name="Catherine", age=45, salary=70000)
]

# Create a DataFrame from the sample data
df = spark.createDataFrame(data)

table_id = "sample-project-123.sample_dataset.details_new"

df.write \
    .format("bigquery") \
    .option("table", table_id) \
    .option("temporaryGcsBucket", "dataproc_test05") \
    .mode("overwrite") \
    .save()

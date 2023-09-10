from pipelines.jobs import batch_raw_to_processed
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# Use this script to test executing the pipleines locally

batch_raw_to_processed.etl()

df = spark.sql("SELECT * FROM sjugo.nscorp_demo.processed_customer_table")
df.show()
from pipelines.jobs import streaming_kafka_to_raw
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
# Use this script to test executing the pipleines locally

streaming_kafka_to_raw.etl()

df = spark.sql("SELECT * FROM sjugo.nscorp_demo.raw_customer_table")
df.show()
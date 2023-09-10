from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from pipelines.transformations import validate_schema

spark = SparkSession.builder.getOrCreate()

def load_kafka_xml(xml_data_file_path):
  kafka_df = spark.read \
      .format("com.databricks.spark.xml") \
      .options(rowTag="Customer") \
      .load(xml_data_file_path)
  
  kafka_df \
    .select(to_json(struct("*")).alias("value")).selectExpr("CAST(value AS STRING)") \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("topic", "topic1") \
    .option("key.serializer", "org.apache.kafka.common.serialization.StringSerializer") \
    .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") \
    .start()

def extract_kafka_xml():
  dsr = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_read_broker") \
    .option("subscribe", "kafka_read_topic") \
    .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .option("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "max_offsets_per_trigger")
  
  return dsr

def load_xml(xml_data_file_path, row_tag, xml_schema_file_path):
  spark.sparkContext.addFile(xml_schema_file_path)

  xsd_file_name = xml_schema_file_path.split("/")[-1]

  df = spark.read \
      .format("com.databricks.spark.xml") \
      .options(rowTag=row_tag) \
      .options(rowValidationXSDPath=xsd_file_name) \
      .load(xml_data_file_path)
  
  return df

def verify_schema_and_quarantine(df):
    raw_df_parsed_verified, raw_df_quarantine = validate_schema.raw_filter_and_quarantine(df)
    return raw_df_parsed_verified

def load_raw_table(df, table_name):
  df.write.mode("append").format("delta").saveAsTable(table_name)
  return

def etl():
  df = load_xml("dbfs:/FileStore/tables/sample_Customers.xml", "Customer", "dbfs:/FileStore/tables/sample_Customers.xsd")
  df = verify_schema_and_quarantine(df)
  load_raw_table(df, "sjugo.nscorp_demo.raw_customer_table")